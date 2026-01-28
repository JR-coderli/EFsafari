"""
Clickflare Offers ETL
拉取 Clickflare Offers 详情并存储到 ClickHouse
"""
import os
import sys
import yaml
import logging
from datetime import datetime
from typing import Dict, Any
import clickhouse_connect

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cf_offers_api import ClickflareOffersAPI
from logger import setup_logger

# 加载配置
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# 设置日志
logger_config = {
    "level": CONFIG["logging"]["level"],
    "log_dir": CONFIG["logging"]["log_dir"],
    "log_file": CONFIG["logging"].get("offers_log_file", "cf_offers_etl.log"),
    "max_bytes": CONFIG["logging"].get("max_bytes", 10485760),
    "backup_count": CONFIG["logging"].get("backup_count", 5)
}
logger = setup_logger(logger_config)


class ClickflareOffersETL:
    """Clickflare Offers ETL 处理类"""

    def __init__(self, config: Dict):
        """
        初始化 ETL

        Args:
            config: 配置字典
        """
        self.config = config
        self.api = ClickflareOffersAPI(config)

        # ClickHouse 连接配置
        ch_config = config["clickhouse"]
        self.ch_client = clickhouse_connect.get_client(
            host=ch_config["host"],
            port=ch_config["port"],
            database=ch_config["database"],
            username=ch_config["username"],
            password=ch_config["password"],
            secure=ch_config.get("secure", False)
        )
        self.table = "clickflare_offers_details"

    def transform_offer(self, offer: Dict) -> Dict:
        """
        转换单个 Offer 数据为 ClickHouse 表格式

        Args:
            offer: API 返回的 Offer 数据

        Returns:
            转换后的数据字典
        """
        payout = offer.get("payout", {})

        return {
            "offer_id": offer.get("_id", offer.get("id", "")),
            "workspace_id": str(offer.get("workspace_id", "")),
            "user_id": offer.get("user_id", 0),
            "name": offer.get("name", ""),
            "url": offer.get("url", ""),
            "notes": offer.get("notes", ""),
            "payout_type": payout.get("type", ""),
            "payout_amount": float(payout.get("payout", 0)),
            "payout_currency": payout.get("currency", "USD"),
            "affiliate_network_id": offer.get("affiliateNetworkID", ""),
            "direct_url": offer.get("url", "") if offer.get("direct", False) else "",
            "static_url": offer.get("staticUrl", ""),
            "is_direct": 1 if offer.get("direct", False) else 0,
            "keyword_builder_mode": offer.get("keywordBuilderMode", ""),
            "tags": offer.get("tags", [])
        }

    def insert_to_clickhouse(self, offers: list) -> int:
        """
        先清空旧数据，再批量插入新数据到 ClickHouse

        Args:
            offers: Offer 数据列表

        Returns:
            插入的行数
        """
        if not offers:
            return 0

        # 先清空旧数据（覆盖更新）
        logger.info("清空旧数据...")
        self.ch_client.command(f"TRUNCATE TABLE ad_platform.{self.table}")
        logger.info("旧数据已清空")

        # 转换数据
        transformed_data = [self.transform_offer(offer) for offer in offers]

        # 准备批量插入的数据
        columns = [
            "offer_id", "workspace_id", "user_id", "name", "url", "notes",
            "payout_type", "payout_amount", "payout_currency", "affiliate_network_id",
            "direct_url", "static_url", "is_direct", "keyword_builder_mode", "tags"
        ]

        # 构建数据行
        data = []
        for item in transformed_data:
            row = [
                item["offer_id"],
                item["workspace_id"],
                item["user_id"],
                item["name"],
                item["url"],
                item["notes"],
                item["payout_type"],
                item["payout_amount"],
                item["payout_currency"],
                item["affiliate_network_id"],
                item["direct_url"],
                item["static_url"],
                item["is_direct"],
                item["keyword_builder_mode"],
                item["tags"]
            ]
            data.append(row)

        try:
            self.ch_client.insert(
                table=self.table,
                column_names=columns,
                data=data
            )
            logger.info(f"成功插入 {len(data)} 行数据到 ClickHouse")
            return len(data)
        except Exception as e:
            logger.error(f"插入 ClickHouse 失败: {e}")
            raise

    def run(self, max_pages: int = 100) -> Dict:
        """
        执行 ETL 流程

        Args:
            max_pages: 最大拉取页数

        Returns:
            执行结果统计
        """
        start_time = datetime.now()
        logger.info("=" * 50)
        logger.info("Clickflare Offers ETL 开始执行")
        logger.info("=" * 50)

        result = {
            "success": False,
            "offers_fetched": 0,
            "offers_inserted": 0,
            "error": None,
            "start_time": start_time.isoformat(),
            "end_time": None
        }

        try:
            # 测试连接
            logger.info("测试 API 连接...")
            success, msg = self.api.test_connection()
            if not success:
                raise Exception(f"API 连接失败: {msg}")
            logger.info(f"API 连接成功: {msg}")

            # 拉取所有 Offers
            logger.info("开始拉取 Offers 数据...")
            offers, error = self.api.fetch_all_offers(
                page_size=1000,
                max_pages=max_pages
            )

            if error:
                raise Exception(f"拉取 Offers 失败: {error}")

            result["offers_fetched"] = len(offers)
            logger.info(f"成功拉取 {len(offers)} 条 Offers")

            if not offers:
                logger.warning("没有拉取到任何数据")
                result["success"] = True
                return result

            # 显示部分样本数据
            logger.info("样本数据:")
            for i, offer in enumerate(offers[:3]):
                logger.info(f"  [{i+1}] {offer.get('name', 'N/A')} - {offer.get('_id', 'N/A')}")

            # 插入数据到 ClickHouse
            logger.info("开始插入数据到 ClickHouse...")
            inserted = self.insert_to_clickhouse(offers)
            result["offers_inserted"] = inserted

            result["success"] = True
            logger.info(f"ETL 执行成功! 共处理 {inserted} 条数据")

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"ETL 执行失败: {e}")
            raise

        finally:
            end_time = datetime.now()
            result["end_time"] = end_time.isoformat()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"ETL 执行时长: {duration:.2f} 秒")
            logger.info("=" * 50)

        return result


def main():
    """主函数"""
    etl = ClickflareOffersETL(CONFIG)
    result = etl.run(max_pages=100)

    if result["success"]:
        print(f"\n✅ ETL 执行成功!")
        print(f"   拉取 Offers: {result['offers_fetched']}")
        print(f"   插入数据: {result['offers_inserted']}")
        print(f"   执行时长: {(datetime.fromisoformat(result['end_time']) - datetime.fromisoformat(result['start_time'])).total_seconds():.2f} 秒")
        return 0
    else:
        print(f"\n❌ ETL 执行失败: {result.get('error', 'Unknown error')}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
