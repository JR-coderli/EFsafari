"""
同步 ClickFlare Lander URL 到本地映射表

使用方法:
    python sync_lander_urls.py

功能:
    1. 从 ClickFlare API 获取所有 Lander 数据
    2. 将 landerID, landerName, landerUrl 同步到本地映射表
    3. 支持增量更新 (先删除旧数据，再插入新数据)
"""
import requests
import clickhouse_connect
import yaml
import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

# 设置日志目录
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "sync_lander_urls.log")

# 配置日志（同时输出到文件和控制台）
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 清除已有的 handlers
logger.handlers.clear()

# 文件 handler（按天轮转，保留 7 天）
file_handler = TimedRotatingFileHandler(
    LOG_FILE,
    when="midnight",
    interval=1,
    backupCount=7,
    encoding='utf-8'
)
file_handler.suffix = "%Y-%m-%d.log"
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

# ClickFlare API 配置
CF_API_KEY = "406561a67ff45389757647c936537da98f6c89a11776566dbe6efc8241c357f9.da59c8abbd8fbf4af7c3a5c72612d871a30273fa"
CF_LANDER_URL = "https://public-api.clickflare.io/api/landings"

# 读取 ClickHouse 配置
config_path = os.path.join(os.path.dirname(__file__), "api", "config.yaml")
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

ch_config = config.get("clickhouse", {})
client = clickhouse_connect.get_client(
    host=ch_config.get("host", "localhost"),
    port=ch_config.get("port", 8123),
    database=ch_config.get("database", "ad_platform"),
    username=ch_config.get("username", "default"),
    password=ch_config.get("password", ""),
)


def fetch_landers_from_cf():
    """从 ClickFlare API 获取所有 Lander 数据"""
    logger.info("="*60)
    logger.info("Fetching landers from ClickFlare API...")
    response = requests.get(CF_LANDER_URL, headers={
        "Accept": "application/json",
        "api-key": CF_API_KEY
    })

    if response.status_code != 200:
        logger.error(f"Failed to fetch landers: {response.status_code} - {response.text}")
        return []

    data = response.json()
    logger.info(f"Fetched {len(data)} landers from ClickFlare API")
    return data


def sync_to_clickhouse(landers):
    """同步 Lander 数据到 ClickHouse"""
    if not landers:
        logger.warning("No landers to sync")
        return

    logger.info("Syncing landers to ClickHouse...")

    # 准备数据
    now = datetime.now()
    data_to_insert = []
    for item in landers:
        data_to_insert.append([
            item.get('_id', ''),           # landerID
            item.get('name', ''),          # landerName
            item.get('url', ''),           # landerUrl
            now,                           # created_at
            now                            # updated_at
        ])

    # 删除旧数据
    logger.info("Deleting old data (TRUNCATE)...")
    client.command("TRUNCATE TABLE ad_platform.dim_lander_url_mapping")

    # 批量插入新数据
    logger.info(f"Inserting {len(data_to_insert)} records...")
    client.insert('ad_platform.dim_lander_url_mapping', data_to_insert)

    # 验证
    result = client.query("SELECT count(*) as cnt FROM ad_platform.dim_lander_url_mapping")
    count = result.first_row[0]
    logger.info(f"Sync complete! Total records in table: {count}")


def main():
    """主函数"""
    start_time = datetime.now()
    logger.info("="*60)
    logger.info(f"Lander URL Sync Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)

    try:
        # 获取数据
        landers = fetch_landers_from_cf()

        # 同步到数据库
        sync_to_clickhouse(landers)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("="*60)
        logger.info(f"Lander URL Sync Completed Successfully!")
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("="*60)
        return 0

    except Exception as e:
        end_time = datetime.now()
        logger.error("="*60)
        logger.error(f"Lander URL Sync Failed!")
        logger.error(f"Error: {e}")
        logger.error(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error("="*60)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
