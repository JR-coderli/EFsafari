#!/usr/bin/env python3
"""
Clickflare Hourly Report ETL

按小时拉取 Clickflare 数据，支持 UTC+0 和 UTC+8 两个时区
每次拉取过去24小时的数据，使用时间戳范围进行精确删除和插入

删除策略（按时间戳范围）：
- 使用 reportDate + reportHour 组合成时间戳
- DELETE WHERE (toDateTime(reportDate) + toIntervalHour(reportHour)) >= start_dt
           AND (toDateTime(reportDate) + toIntervalHour(reportHour)) < end_dt
- 只删除指定时间范围内的数据，不影响其他小时

Usage:
    python cf_hourly_etl.py        # 拉取 UTC+0 数据
    python cf_hourly_etl.py --utc8  # 拉取 UTC+8 数据
    python cf_hourly_etl.py --hours 1  # 测试模式：只拉最近1小时
"""
import os
import sys
import yaml
import argparse
import requests
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
from logging.handlers import TimedRotatingFileHandler

# ==================== 配置日志 ====================
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "cf_hourly_etl.log")

# 配置日志（同时输出到文件和控制台）
logger = logging.getLogger("cf_hourly_etl")
logger.setLevel(logging.INFO)

# 清除已有的 handlers
logger.handlers.clear()

# 文件 handler（按天轮转，保留 7 天）
file_handler = TimedRotatingFileHandler(
    LOG_FILE,
    when="midnight",  # 每天午夜轮转
    interval=1,
    backupCount=7,  # 保留 7 天
    encoding='utf-8'
)
file_handler.suffix = "%Y-%m-%d.log"  # 轮转文件后缀
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# 控制台 handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)
# ==================== 日志配置完成 ====================

# Add api directory to PYTHONPATH
# Get the script's directory and find the backend/api directory
_script_dir = os.path.dirname(os.path.abspath(__file__))
_backend_dir = os.path.dirname(_script_dir)
_api_dir = os.path.join(_backend_dir, "api")

# Ensure the api directory is in the Python path
if _api_dir not in sys.path:
    sys.path.insert(0, _api_dir)

# Also add backend_dir to path for imports
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

import clickhouse_connect
from api.cache import set_cache, init_redis

# Initialize Redis client for cache operations
# Read config for Redis connection
_config_path = os.path.join(_backend_dir, "clickflare_etl", "config.yaml")
try:
    with open(_config_path, encoding="utf-8") as f:
        config_data = yaml.safe_load(f)
    redis_config = config_data.get("redis", {
        "host": "localhost",
        "port": 6379
    })
    init_redis(redis_config)
except Exception as e:
    logger.warning(f"Failed to initialize Redis: {e}")


class HourlyETL:
    """Clickflare Hourly ETL 处理器"""

    PAGE_SIZE = 5000  # 增加页大小，减少 API 调用次数
    API_TIMEOUT = 120  # API 超时时间（秒）

    def __init__(self, config_path: str = None, timezone: str = "UTC", test_hours: int = 0):
        # Use dynamic config path if not provided
        if config_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            backend_dir = os.path.dirname(script_dir)
            config_path = os.path.join(backend_dir, "clickflare_etl", "config.yaml")

        self.config_path = config_path
        self.timezone = timezone
        self.test_hours = test_hours

        with open(config_path, encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        self.api_config = self.config["api"]
        self.api_config["timezone"] = timezone

        self.ch_config = self.config["clickhouse"]
        self.ch_config["table"] = "hourly_report"
        self.etl_config = self.config.get("etl", {})

        self.ch_client = clickhouse_connect.get_client(
            host=self.ch_config["host"],
            port=self.ch_config["port"],
            database=self.ch_config["database"],
            username=self.ch_config.get("username", "default"),
            password=self.ch_config.get("password", ""),
            send_receive_timeout=300,
            connect_timeout=30,
            query_limit=0
        )

        self._init_table()

    def _init_table(self):
        # 只创建表，不删除（数据由 DELETE 操作清理）
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.ch_config["database"]}.hourly_report (
                reportDate Date,
                reportHour UInt8,
                timezone String,
                Media String,
                MediaID String,
                offer String,
                offerID String,
                advertiser String,
                advertiserID String,
                Campaign String,
                CampaignID String,
                Adset String,
                AdsetID String,
                impressions UInt64,
                clicks UInt64,
                conversions UInt64,
                spend Float64,
                revenue Float64,
                updated_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(reportDate)
            ORDER BY (reportDate, reportHour, Media, AdsetID)
            TTL reportDate + INTERVAL 1 MONTH
            """
        self.ch_client.command(create_sql)
        logger.info("[Table] Table ready (CREATE IF NOT EXISTS)")

    def _get_group_by_config(self) -> List[str]:
        return ["dateTime", "trafficSourceID", "offerID", "affiliateNetworkID",
                "trackingField1", "trackingField2", "trackingField5", "trackingField6"]

    def _get_metrics_config(self) -> List[str]:
        return ["trafficSourceName", "offerName", "affiliateNetworkName",
                "uniqueVisits", "uniqueClicks", "conversions", "revenue", "cost"]

    def _build_api_request(self, start_dt: datetime, end_dt: datetime, page: int = 1) -> Dict[str, Any]:
        return {
            "startDate": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "endDate": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "groupBy": self._get_group_by_config(),
            "metrics": self._get_metrics_config(),
            "timezone": self.timezone,
            "sortBy": "uniqueVisits",
            "orderType": "desc",
            "includeAll": False,
            "page": page,
            "pageSize": self.PAGE_SIZE
        }

    def _fetch_api_data(self, start_dt: datetime, end_dt: datetime) -> List[Dict]:
        url = f"{self.api_config['base_url']}{self.api_config['endpoint']}"
        headers = {"api-key": self.api_config["api_key"]}

        all_items = []
        page = 1
        max_pages = self.etl_config.get("max_pages", 100)

        while page <= max_pages:
            request_data = self._build_api_request(start_dt, end_dt, page)
            logger.info(f"[API] Fetching page {page}...")

            try:
                response = requests.post(url, json=request_data, headers=headers, timeout=self.API_TIMEOUT)
                response.raise_for_status()
                result = response.json()

                items = result.get("items", [])
                if not items:
                    logger.info("No more data")
                    break

                all_items.extend(items)
                logger.info(f"Page {page}: {len(items):,} records, Total: {len(all_items):,}")

                if len(items) < self.PAGE_SIZE:
                    break

                page += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"[ERROR] API request failed: {e}")
                raise

        logger.info(f"[API] Total records fetched: {len(all_items):,}")
        return all_items

    def _get_hourly_special_media(self) -> List[str]:
        """获取 hourly 特殊媒体列表，这些媒体的 spend = revenue

        可在 Config 页面的 "Hourly 特殊媒体配置" 中配置。
        直接从 JSON 配置文件读取。
        """
        import json
        # 默认特殊媒体关键词
        default_keywords = ["mintegral", "hastraffic", "jmmobi", "brainx"]

        try:
            # Get backend directory
            backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_file = os.path.join(backend_dir, "config", "special_media.json")

            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    configured_media = config.get("hourly_special_media", [])
                    # 合并配置和默认值
                    all_media = list(set(configured_media + default_keywords))
                    return all_media
            else:
                logger.warning(f"Special media config file not found: {config_file}")
        except Exception as e:
            logger.error(f"Failed to read special media config: {e}")

        # 回退到默认值
        return default_keywords

    def _is_special_media(self, media_name: str) -> bool:
        """检查是否为特殊媒体（关键词匹配）"""
        if not media_name:
            return False
        media_lower = media_name.lower()
        special_keywords = self._get_hourly_special_media()
        return any(keyword in media_lower for keyword in special_keywords)

    def _transform_data(self, raw_data: List[Dict]) -> List[Dict]:
        """转换 API 数据为数据库格式

        API 使用 UTC 时区，返回的 dateTime 就是 UTC 时间，直接存储即可。

        特殊媒体处理：对于 hourly_special_media 中的媒体，spend = revenue
        """
        transformed = []
        special_media_count = 0

        for item in raw_data:
            date_time_str = item.get("dateTime", "")
            try:
                dt = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
                report_date = dt.date()
                report_hour = dt.hour
            except (ValueError, TypeError):
                date_str = item.get("date", "")
                hour_of_day = item.get("hourOfDay", 0)
                try:
                    report_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    report_hour = int(hour_of_day)
                except (ValueError, TypeError):
                    continue

            media_name = item.get("trafficSourceName", "")
            revenue = float(item.get("revenue", 0))
            spend = float(item.get("cost", 0))

            # 特殊媒体：spend = revenue
            is_special = self._is_special_media(media_name)
            if is_special:
                spend = revenue
                special_media_count += 1

            record = {
                "reportDate": report_date,
                "reportHour": report_hour,
                "timezone": "UTC",
                "Media": media_name,
                "MediaID": item.get("trafficSourceID", ""),
                "offer": item.get("offerName", ""),
                "offerID": item.get("offerID", ""),
                "advertiser": item.get("affiliateNetworkName", ""),
                "advertiserID": item.get("affiliateNetworkID", ""),
                "Campaign": item.get("trackingField2", ""),
                "CampaignID": item.get("trackingField2", ""),
                "Adset": item.get("trackingField6", ""),
                "AdsetID": item.get("trackingField5", ""),
                "impressions": item.get("uniqueVisits", 0),
                "clicks": item.get("uniqueClicks", 0),
                "conversions": item.get("conversions", 0),
                "spend": spend,
                "revenue": revenue
            }
            transformed.append(record)

        if special_media_count > 0:
            logger.info(f"[Special Media] Applied spend=revenue to {special_media_count:,} records")

        return transformed

    def _delete_existing_data(self, start_dt: datetime, end_dt: datetime):
        """按时间戳范围删除指定时间范围内的数据

        使用 reportDate + reportHour 组合成时间戳进行精确删除：
        DELETE WHERE (toDateTime(reportDate, 'UTC') + toIntervalHour(reportHour)) >= start_dt
               AND (toDateTime(reportDate, 'UTC') + toIntervalHour(reportHour)) < end_dt

        优点：
        - 不需要改表结构
        - 逻辑简单，一条 SQL 搞定
        - 精确删除指定时间范围，不影响其他小时
        - 使用 UTC 时区，避免服务器时区影响
        """
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        delete_sql = f"""
            ALTER TABLE {self.ch_config['database']}.hourly_report
            DELETE WHERE timezone = 'UTC'
                AND (toDateTime(reportDate, 'UTC') + toIntervalHour(reportHour)) >= toDateTime('{start_str}', 'UTC')
                AND (toDateTime(reportDate, 'UTC') + toIntervalHour(reportHour)) < toDateTime('{end_str}', 'UTC')
            """

        logger.info(f"[DELETE] Deleting data where timestamp >= {start_str} AND < {end_str}...")
        self.ch_client.command(delete_sql)
        logger.info("Delete completed")

    def _delete_existing_data_by_hour(self, start_dt: datetime, end_dt: datetime):
        """按小时删除（已弃用，保留兼容性）"""
        return self._delete_existing_data(start_dt, end_dt)

    def _delete_data_by_actual_hours(self, data: List[Dict]):
        """按实际数据小时删除（已弃用，保留兼容性）"""
        if not data:
            return
        # 找出数据中的最小和最大时间
        min_date = min(r["reportDate"] for r in data)
        max_date = max(r["reportDate"] for r in data)
        min_hour = min(r["reportHour"] for r in data if r["reportDate"] == min_date)
        max_hour = max(r["reportHour"] for r in data if r["reportDate"] == max_date)

        start_dt = datetime.combine(min_date, datetime.min.time()) + timedelta(hours=min_hour)
        end_dt = datetime.combine(max_date, datetime.min.time()) + timedelta(hours=max_hour + 1)
        self._delete_existing_data(start_dt, end_dt)

    def _insert_data(self, data: List[Dict]):
        if not data:
            logger.warning("[INSERT] No data to insert")
            return

        columns = ["reportDate", "reportHour", "timezone", "Media", "MediaID",
                   "offer", "offerID", "advertiser", "advertiserID", "Campaign", "CampaignID",
                   "Adset", "AdsetID", "impressions", "clicks", "conversions", "spend", "revenue"]

        rows = []
        for record in data:
            row = [record["reportDate"], record["reportHour"], record["timezone"],
                   record["Media"], record["MediaID"], record["offer"], record["offerID"],
                   record["advertiser"], record["advertiserID"], record["Campaign"], record["CampaignID"],
                   record["Adset"], record["AdsetID"], record["impressions"], record["clicks"],
                   record["conversions"], record["spend"], record["revenue"]]
            rows.append(row)

        batch_size = 500  # 减小批量大小避免超时
        total_rows = len(rows)
        total_inserted = 0
        max_retries = 3

        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            retries = 0
            success = False

            while retries < max_retries and not success:
                try:
                    logger.info(f"[INSERT] Inserting {min(i + batch_size, total_rows):,}/{total_rows:,} rows...")
                    self.ch_client.insert(
                        table=f'{self.ch_config["database"]}.hourly_report',
                        data=batch,
                        column_names=columns
                    )
                    success = True
                    total_inserted += len(batch)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"[ERROR] Failed to insert batch after {max_retries} retries: {e}")
                        raise
                    logger.warning(f"[RETRY] Retry {retries}/{max_retries}...")

        logger.info(f"Insert completed! Total: {total_inserted:,} rows")

    def run(self):
        start_time = datetime.now()

        logger.info("=" * 60)
        logger.info(f"Hourly ETL Started - Timezone: {self.timezone}")
        if self.test_hours > 0:
            logger.info(f"TEST MODE: Only pulling recent {self.test_hours} hours")
        logger.info(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        logger.info("[INFO] Storing UTC data only, timezone conversion done at query time")
        logger.info("[INFO] Using timestamp range DELETE: (reportDate + reportHour) >= start AND < end")
        logger.info("[INFO] Safe ETL mode: fetch -> delete (by timestamp range) -> insert")

        # 使用 UTC 时间获取数据
        utc_now = datetime.now(timezone.utc)

        if self.test_hours > 0:
            # 测试模式：向下取整到小时，避免首小时未删除导致重复
            start_dt_utc = (utc_now - timedelta(hours=self.test_hours)).replace(minute=0, second=0, microsecond=0)
            end_dt_utc = utc_now
        else:
            # 正常模式：拉取过去24小时的数据（向下取整到小时边界）
            start_dt_utc = (utc_now - timedelta(hours=24)).replace(minute=0, second=0, microsecond=0)
            end_dt_utc = utc_now

        # API 使用 UTC 时区，直接使用 UTC 时间（不需要转换）
        start_dt = start_dt_utc
        end_dt = end_dt_utc

        logger.info(f"[Time Range] UTC: {start_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} - {end_dt_utc.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"[Time Range] API (UTC): {start_dt.strftime('%Y-%m-%d %H:%M:%S')} - {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        # ========== 新流程：先拉取数据，成功后再删除旧数据 ==========
        logger.info("[Step 1/4] Fetching data from Clickflare API (using UTC)...")
        # 使用 UTC 时区获取数据
        original_timezone = self.timezone
        self.timezone = "UTC"
        try:
            raw_data = self._fetch_api_data(start_dt, end_dt)
        except Exception as e:
            logger.error(f"[ERROR] Failed to fetch data from API: {e}")
            logger.info("[INFO] Old data preserved due to API failure")
            raise
        finally:
            self.timezone = original_timezone  # 恢复原始设置

        logger.info(f"[Step 2/4] Processing {len(raw_data):,} records...")
        transformed_data = self._transform_data(raw_data)
        logger.info(f"[Step 2/4] Processed {len(transformed_data):,} records")

        logger.info(f"[Step 3/4] Deleting existing data in range [{start_dt_utc.strftime('%Y-%m-%d %H:%M:%S')}, {end_dt_utc.strftime('%Y-%m-%d %H:%M:%S')})...")
        self._delete_existing_data(start_dt_utc, end_dt_utc)

        logger.info("[Step 4/4] Inserting data to ClickHouse...")
        self._insert_data(transformed_data)

        # 更新缓存（所有时区共享同一个 UTC 数据源的更新时间）
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for tz in ["UTC", "Asia/Shanghai", "EST", "PST"]:
            etl_status = {
                "last_update": now,
                "timezone": tz,
                "start_date": start_dt_utc.strftime("%Y-%m-%d"),
                "end_date": end_dt_utc.strftime("%Y-%m-%d"),
                "record_count": len(transformed_data)
            }
            set_cache(f"hourly_etl:last_update:{tz}", etl_status, ttl=24*3600)

        # 显示汇总（只有 UTC 数据）
        summary_sql = f"""
            SELECT reportDate,
                sum(impressions) as impressions, sum(clicks) as clicks,
                sum(conversions) as conversions, sum(spend) as spend, sum(revenue) as revenue
            FROM {self.ch_config['database']}.hourly_report
            WHERE reportDate >= '{start_dt_utc.strftime('%Y-%m-%d')}'
                AND reportDate <= '{end_dt_utc.strftime('%Y-%m-%d')}'
                AND timezone = 'UTC'
            GROUP BY reportDate ORDER BY reportDate
            """
        result = self.ch_client.query(summary_sql)

        logger.info("=" * 60)
        logger.info("ETL SUMMARY")
        logger.info("=" * 60)
        for row in result.named_results():
            logger.info(f"Date: {row['reportDate']} (UTC, all other timezones calculated at query time)")
            logger.info(f"  Impressions: {row['impressions']:,}")
            logger.info(f"  Clicks:      {row['clicks']:,}")
            logger.info(f"  Conversions: {row['conversions']:,}")
            logger.info(f"  Spend:       ${row['spend']:,.2f}")
            logger.info(f"  Revenue:     ${row['revenue']:,.2f}")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"Hourly ETL Completed Successfully!")
        logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 60)

        return True


def main():
    parser = argparse.ArgumentParser(description="Clickflare Hourly ETL")
    parser.add_argument("--utc8", action="store_true", help="Use UTC+8 timezone (default: UTC+0)")
    parser.add_argument("--config", default=None, help="Config file path (default: auto-detect)")
    parser.add_argument("--hours", type=int, default=0, help="Test mode: only pull recent N hours (0 = full day)")
    args = parser.parse_args()

    # 校验 test_hours 必须为非负数
    if args.hours < 0:
        logger.error(f"[ERROR] --hours must be non-negative, got {args.hours}")
        sys.exit(1)

    timezone = "Asia/Shanghai" if args.utc8 else "UTC"

    try:
        etl = HourlyETL(config_path=args.config, timezone=timezone, test_hours=args.hours)
        success = etl.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"[ERROR] ETL failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
