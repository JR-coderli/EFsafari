"""
Clickflare Hourly Report ETL

按小时拉取 Clickflare 数据，支持 UTC+0 和 UTC+8 两个时区
每次拉取今天的数据，DELETE + INSERT 模式

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
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any

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
try:
    with open("config.yaml", encoding="utf-8") as f:
        config_data = yaml.safe_load(f)
    redis_config = config_data.get("redis", {
        "host": "localhost",
        "port": 6379
    })
    init_redis(redis_config)
except Exception as e:
    print(f"[WARNING] Failed to initialize Redis: {e}")


class HourlyETL:
    """Clickflare Hourly ETL 处理器"""

    PAGE_SIZE = 5000  # 增加页大小，减少 API 调用次数
    API_TIMEOUT = 120  # API 超时时间（秒）

    def __init__(self, config_path: str = "config.yaml", timezone: str = "UTC", test_hours: int = 0):
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
        print("[Table] Table ready (CREATE IF NOT EXISTS)")

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
            sys.stdout.write(f"\r[API] Fetching page {page}... ")
            sys.stdout.flush()

            try:
                response = requests.post(url, json=request_data, headers=headers, timeout=self.API_TIMEOUT)
                response.raise_for_status()
                result = response.json()

                items = result.get("items", [])
                if not items:
                    print(f"No more data")
                    break

                all_items.extend(items)
                print(f"Page {page}: {len(items):,} records, Total: {len(all_items):,}")

                if len(items) < self.PAGE_SIZE:
                    break

                page += 1
            except requests.exceptions.RequestException as e:
                print(f"\n[ERROR] API request failed: {e}")
                raise

        print(f"[API] Total records fetched: {len(all_items):,}")
        return all_items

    def _transform_data(self, raw_data: List[Dict]) -> List[Dict]:
        """转换 API 数据为数据库格式

        只存储 UTC 时区的数据，查询时根据需要转换时区。
        Clickflare API 返回的是 UTC+8 时区的数据，需要转换为 UTC 存储。
        """
        transformed = []

        for item in raw_data:
            date_time_str = item.get("dateTime", "")
            try:
                dt = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
                report_date_utc8 = dt.date()
                report_hour_utc8 = dt.hour
            except (ValueError, TypeError):
                date_str = item.get("date", "")
                hour_of_day = item.get("hourOfDay", 0)
                try:
                    report_date_utc8 = datetime.strptime(date_str, "%Y-%m-%d").date()
                    report_hour_utc8 = int(hour_of_day)
                except (ValueError, TypeError):
                    continue

            # 将 UTC+8 转换为 UTC（减 8 小时）
            utc_dt = datetime.combine(report_date_utc8, datetime.min.time()) + timedelta(hours=report_hour_utc8) - timedelta(hours=8)

            record = {
                "reportDate": utc_dt.date(),
                "reportHour": utc_dt.hour,
                "timezone": "UTC",
                "Media": item.get("trafficSourceName", ""),
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
                "spend": float(item.get("cost", 0)),
                "revenue": float(item.get("revenue", 0))
            }
            transformed.append(record)

        return transformed

    def _delete_existing_data(self, start_dt: datetime, end_dt: datetime):
        """删除指定日期范围内的所有数据（清理旧时区数据）"""
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")
        # 删除所有时区的数据（清理旧的多时区数据）
        delete_sql = f"""
            ALTER TABLE {self.ch_config['database']}.hourly_report
            DELETE WHERE reportDate >= '{start_date}'
                AND reportDate <= '{end_date}'
            """
        sys.stdout.write(f"[DELETE] Deleting ALL data for {start_date} to {end_date}... ")
        sys.stdout.flush()
        self.ch_client.command(delete_sql)
        print("Done")

    def _insert_data(self, data: List[Dict]):
        if not data:
            print("[INSERT] No data to insert")
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
                    sys.stdout.write(f"\r[INSERT] Inserting {min(i + batch_size, total_rows):,}/{total_rows:,} rows... ")
                    sys.stdout.flush()
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
                        print(f"\n[ERROR] Failed to insert batch after {max_retries} retries: {e}")
                        raise
                    print(f"\n[RETRY] Retry {retries}/{max_retries}...")

        print(f"Done! Total: {total_inserted:,} rows")

    def run(self):
        print("=" * 60)
        print(f"Hourly ETL - Timezone: UTC")
        if self.test_hours > 0:
            print(f"TEST MODE: Only pulling recent {self.test_hours} hours")
        print("=" * 60)
        print("[INFO] Storing UTC data only, timezone conversion done at query time")

        # 使用 UTC 时间获取数据（避免服务器本地时区影响）
        utc_now = datetime.now(timezone.utc)

        if self.test_hours > 0:
            start_dt_utc = utc_now - timedelta(hours=self.test_hours)
            end_dt_utc = utc_now
        else:
            # 获取今天 UTC 的 0 点到现在
            start_dt_utc = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_dt_utc = utc_now

        # 将 UTC 时间转换为 UTC+8 用于 API 请求
        # API 用 UTC+8 时区执行查询，所以需要把 UTC 时间 +8 小时
        start_dt = start_dt_utc + timedelta(hours=8)
        end_dt = end_dt_utc + timedelta(hours=8)

        print(f"[Time Range] UTC: {start_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} - {end_dt_utc.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[Time Range] API (UTC+8): {start_dt.strftime('%Y-%m-%d %H:%M:%S')} - {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        print("\n[Step 1/4] Deleting existing UTC data...")
        self._delete_existing_data(start_dt_utc, end_dt_utc)

        print("\n[Step 2/4] Fetching data from Clickflare API (using UTC+8)...")
        # 临时使用 UTC+8 获取数据
        original_timezone = self.timezone
        self.timezone = "Asia/Shanghai"
        raw_data = self._fetch_api_data(start_dt, end_dt)
        self.timezone = original_timezone  # 恢复原始设置

        print(f"\n[Step 3/4] Transforming {len(raw_data):,} records to UTC...")
        transformed_data = self._transform_data(raw_data)
        print(f"[Step 3/4] Transformed to {len(transformed_data):,} UTC records")

        print("\n[Step 4/4] Inserting data to ClickHouse...")
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

        print("\n" + "=" * 60)
        print("ETL SUMMARY")
        print("=" * 60)
        for row in result.named_results():
            print(f"Date: {row['reportDate']} (UTC, all other timezones calculated at query time)")
            print(f"  Impressions: {row['impressions']:,}")
            print(f"  Clicks:      {row['clicks']:,}")
            print(f"  Conversions: {row['conversions']:,}")
            print(f"  Spend:       ${row['spend']:,.2f}")
            print(f"  Revenue:     ${row['revenue']:,.2f}")
        print("=" * 60)

        return True


def main():
    parser = argparse.ArgumentParser(description="Clickflare Hourly ETL")
    parser.add_argument("--utc8", action="store_true", help="Use UTC+8 timezone (default: UTC+0)")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--hours", type=int, default=0, help="Test mode: only pull recent N hours (0 = full day)")
    args = parser.parse_args()

    timezone = "Asia/Shanghai" if args.utc8 else "UTC"

    try:
        etl = HourlyETL(config_path=args.config, timezone=timezone, test_hours=args.hours)
        success = etl.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"[ERROR] ETL failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
