"""
Clickflare Report ETL Main Process
Extracts data from Clickflare API, transforms, and loads into ClickHouse

Enhanced with MTG data integration:
- Fetches MTG spend data in the same ETL run
- Merges MTG data with CF data in memory
- Single INSERT operation (no DELETE + extra UPDATE)
"""
import os
import sys
import yaml
import time
import hashlib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import clickhouse_connect

from cf_api import ClickflareAPI
from logger import ETLLogger


class MTGAPIClient:
    """
    MTG Report API Client with retry and polling support
    """

    # API response codes
    CODE_SUCCESS = 200
    CODE_RECEIVED = 201
    CODE_GENERATING = 202
    CODE_NO_REQUEST = 203
    CODE_NOT_READY = 204
    CODE_EXPIRED = 205
    CODE_ERROR = 10000

    def __init__(self, config, logger):
        """Initialize API client"""
        self.base_url = config["base_url"]
        self.endpoint = config["endpoint"]
        self.access_key = config["access_key"]
        self.api_key = config["api_key"]
        self.timezone = config.get("timezone", "0")  # UTC+0
        self.dimension_option = config["dimension_option"]
        self.time_granularity = config.get("time_granularity", "daily")

        # Retry config
        self.max_attempts = config["retry"]["max_attempts"]
        self.backoff_factor = config["retry"]["backoff_factor"]

        # Poll config
        self.poll_max_attempts = config["poll"]["max_attempts"]
        self.poll_interval = config["poll"]["interval_seconds"]
        self.poll_timeout = config["poll"]["timeout_seconds"]

        self.logger = logger
        self.session = requests.Session()

    def _generate_token(self, timestamp: str) -> str:
        """Generate token using MD5(API_KEY + MD5(timestamp))"""
        ts_md5 = hashlib.md5(timestamp.encode("utf-8")).hexdigest()
        raw = self.api_key + ts_md5
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _get_headers(self) -> Dict[str, str]:
        """Generate request headers with Token"""
        timestamp = str(int(time.time()))
        token = self._generate_token(timestamp)
        return {
            "access-key": self.access_key,
            "Token": token,
            "Timestamp": timestamp
        }

    def _do_request_with_retry(self, url: str, params: Dict) -> requests.Response:
        """Make HTTP request with retry logic"""
        last_error = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                headers = self._get_headers()
                response = self.session.get(url, params=params, headers=headers, timeout=60)
                if response.status_code == 200:
                    return response
                self.logger.warning(f"API request returned status {response.status_code}, attempt {attempt}/{self.max_attempts}")
            except requests.exceptions.RequestException as e:
                last_error = e
                self.logger.warning(f"API request failed: {str(e)}, attempt {attempt}/{self.max_attempts}")
            if attempt < self.max_attempts:
                wait_time = self.backoff_factor ** attempt
                time.sleep(wait_time)
        raise Exception(f"API request failed after {self.max_attempts} attempts")

    def get_report_data(self, report_date: str) -> Tuple[bool, str]:
        """Full workflow: initiate -> poll -> download"""
        url = f"{self.base_url}{self.endpoint}"

        # Initiate (type=1)
        params = {
            "start_time": report_date,
            "end_time": report_date,
            "type": 1,
            "timezone": self.timezone,
            "dimension_option": self.dimension_option,
            "time_granularity": self.time_granularity
        }

        headers = self._get_headers()
        response = self.session.get(url, params=params, headers=headers, timeout=60)
        data = response.json()

        code = data.get("code")
        self.logger.info(f"Response code: {code}, message: {data.get('msg', '')}")

        # Poll if needed
        if code != self.CODE_SUCCESS:
            start_time = time.time()
            for attempt in range(1, self.poll_max_attempts + 1):
                if time.time() - start_time > self.poll_timeout:
                    return False, "Poll timeout"
                response = self.session.get(url, params=params, headers=headers, timeout=30)
                data = response.json()
                if data.get("code") == self.CODE_SUCCESS:
                    break
                time.sleep(self.poll_interval)

        # Download (type=2)
        params["type"] = 2
        response = self.session.get(url, params=params, headers=headers, timeout=120)
        if response.status_code == 200:
            content = response.content.decode("utf-8")
            self.logger.info(f"Downloaded {len(content)} bytes of data")
            return True, content
        return False, f"Download failed: {response.status_code}"

    def parse_tsv_data(self, tsv_content: str) -> List[Dict]:
        """Parse TSV content into list of dictionaries"""
        lines = tsv_content.strip().split("\n")
        if len(lines) < 2:
            return []
        headers = [h.strip() for h in lines[0].split("\t")]
        rows = []
        for line in lines[1:]:
            if not line.strip():
                continue
            values = line.split("\t")
            row = {}
            for i, header in enumerate(headers):
                if i < len(values):
                    row[header] = values[i].strip()
            rows.append(row)  # Fix: 应该在 for 循环外面
        self.logger.info(f"Parsed {len(rows)} data rows from TSV")
        return rows

    def get_parsed_data(self, report_date: str) -> Tuple[bool, List[Dict]]:
        """Get and parse report data"""
        success, content = self.get_report_data(report_date)
        if not success:
            return False, []
        return True, self.parse_tsv_data(content)


class ClickflareETL:
    """
    Clickflare ETL Process: Extract -> Transform -> Load
    """

    # Field mapping from API response to target table
    FIELD_MAPPING = {
        "reportDate": "date",
        "Media": "trafficSourceName",
        "MediaID": "trafficSourceID",
        "offer": "offerName",
        "offerID": "offerID",
        "advertiser": "affiliateNetworkName",
        "advertiserID": "affiliateNetworkID",
        "lander": "landingName",
        "landerID": "landingID",
        "Campaign": "trackingField4",
        "CampaignID": "trackingField3",
        "Adset": "trackingField6",
        "AdsetID": "trackingField5",
        "Ads": "trackingField2",
        "AdsID": "trackingField1",
    }

    # Metrics mapping
    METRICS_MAPPING = {
        "impressions": "uniqueVisits",
        "clicks": "uniqueClicks",
        "conversions": "conversions",
        "revenue": "revenue",
        "spend": "cost"
    }

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize ETL process

        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = ETLLogger(self.config["logging"])

        # ClickHouse config
        ch_config = self.config["clickhouse"]
        self.ch_host = ch_config["host"]
        self.ch_port = ch_config["port"]
        self.ch_database = ch_config["database"]
        self.ch_table = ch_config["table"]
        self.ch_user = ch_config["username"]
        self.ch_password = ch_config["password"]

        # ETL config
        self.media_source = self.config["etl"]["media_source"]
        self.batch_size = self.config["etl"]["batch_size"]
        self.date_offset_days = self.config["etl"]["date_offset_days"]
        self.page_size = self.config["etl"]["page_size"]
        self.max_pages = self.config["etl"]["max_pages"]
        self.group_by = self.config["etl"]["group_by"]
        self.metrics = self.config["etl"]["metrics"]
        self.group_by_pass2 = self.config["etl"].get("group_by_pass2", [])
        self.metrics_pass2 = self.config["etl"].get("metrics_pass2", [])
        self.exclude_spend_media = self.config["etl"].get("exclude_spend_media", [])

        # MTG integration config
        mtg_config = self.config["etl"].get("mtg_integration", {})
        self.mtg_enabled = mtg_config.get("enabled", False)
        self.mtg_accounts = mtg_config.get("accounts", [])
        self.mtg_media_keywords = mtg_config.get("mtg_media_keywords", ["Mintegral", "Hastraffic"])
        self.mtg_api_config = {
            "base_url": mtg_config.get("api_base_url", "https://ss-api.mintegral.com"),
            "endpoint": mtg_config.get("api_endpoint", "/api/v2/reports/data"),
            "timezone": mtg_config.get("api_timezone", "0"),
            "dimension_option": mtg_config.get("dimension_option", "Offer,Campaign,Creative"),
            "time_granularity": mtg_config.get("time_granularity", "daily"),
            "retry": self.config.get("retry", {}),
            "poll": self.config.get("poll", {})
        }

        # Timeout config: 如果总运行时间超过这个限制，插入部分数据并中断
        self.timeout_minutes = self.config["etl"].get("timeout_minutes", 30)
        self.start_time = None  # 将在 run() 开始时设置

        self.ch_client = None
        self.api_client = None

        # MTG 状态跟踪
        self.mtg_accounts_status = {}  # {account_name: success}

    def _load_config(self, config_path: str) -> Dict:
        """
        Load configuration from YAML file

        Args:
            config_path: Path to config file

        Returns:
            Dict: Configuration data
        """
        # Get the directory of this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(script_dir, config_path)

        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Config file not found: {full_path}")

        with open(full_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def connect_clickhouse(self) -> bool:
        """
        Establish ClickHouse connection

        Returns:
            bool: Connection success status
        """
        try:
            self.logger.info(f"Connecting to ClickHouse: {self.ch_host}:{self.ch_port}")

            self.ch_client = clickhouse_connect.get_client(
                host=self.ch_host,
                port=self.ch_port,
                database=self.ch_database,
                username=self.ch_user,
                password=self.ch_password,
                connect_timeout=10,
                send_receive_timeout=30
            )

            self.logger.info("ClickHouse connection initialized")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse: {str(e)}")
            return False

    def init_api_client(self):
        """Initialize Clickflare API client"""
        self.api_client = ClickflareAPI(self.config)
        self.logger.info("Clickflare API client initialized")

    def get_report_date(self) -> str:
        """
        Calculate target report date based on offset

        Returns:
            str: Report date in YYYY-MM-DD format
        """
        target_date = datetime.now() - timedelta(days=self.date_offset_days)
        return target_date.strftime("%Y-%m-%d")

    def transform_row(self, raw_row: Dict) -> Optional[Dict]:
        """
        Transform raw API data to target table schema

        Args:
            raw_row: Raw data row from API

        Returns:
            Optional[Dict]: Transformed row or None if invalid
        """
        try:
            # Parse date from API (format: YYYY-MM-DD)
            api_date = raw_row.get("date", "")
            if api_date:
                try:
                    date_obj = datetime.strptime(api_date, "%Y-%m-%d").date()
                except:
                    self.logger.warning(f"Invalid date format: {api_date}")
                    return None
            else:
                self.logger.warning("Missing date field")
                return None

            # Get dimension values
            traffic_source_name = raw_row.get("trafficSourceName", "")
            traffic_source_id = raw_row.get("trafficSourceID", "")
            offer_name = raw_row.get("offerName", "")
            offer_id = raw_row.get("offerID", "")
            affiliate_network_name = raw_row.get("affiliateNetworkName", "")
            affiliate_network_id = raw_row.get("affiliateNetworkID", "")
            landing_name = raw_row.get("landingName", "")
            landing_id = raw_row.get("landingID", "")

            # Get tracking field values
            tracking_field_1 = raw_row.get("trackingField1", "")
            tracking_field_2 = raw_row.get("trackingField2", "")
            tracking_field_3 = raw_row.get("trackingField3", "")
            tracking_field_4 = raw_row.get("trackingField4", "")
            tracking_field_5 = raw_row.get("trackingField5", "")
            tracking_field_6 = raw_row.get("trackingField6", "")

            # Get metrics
            impressions = self._safe_int(raw_row.get("uniqueVisits"))
            clicks = self._safe_int(raw_row.get("uniqueClicks"))
            conversions = self._safe_int(raw_row.get("conversions"))
            revenue = self._safe_float(raw_row.get("revenue"))
            cost = self._safe_float(raw_row.get("cost"))

            # Special Media 逻辑：
            # - exclude_spend_media (Mintegral/Hastraffic/JMmobi/Brain) 初始 spend = revenue
            # - mtg_media_keywords (只有 Mintegral) 会被 MTG API 真实 spend 覆盖
            # - 其他 media (Hastraffic/JMmobi/Brain) 保持 spend = revenue
            media_name = traffic_source_name if traffic_source_name else self.media_source
            should_exclude_spend = any(
                excluded.lower() in media_name.lower()
                for excluded in self.exclude_spend_media
            )

            transformed = {
                "reportDate": date_obj,
                "dataSource": "Clickflare",  # 数据来源标识
                "Media": media_name,
                "MediaID": traffic_source_id if traffic_source_id else "",
                "offer": offer_name if offer_name else "",
                "offerID": offer_id if offer_id else "",
                "advertiser": affiliate_network_name if affiliate_network_name else "",
                "advertiserID": affiliate_network_id if affiliate_network_id else "",
                "lander": landing_name if landing_name else "",
                "landerID": landing_id if landing_id else "",
                "Campaign": tracking_field_4 if tracking_field_4 else "",
                "CampaignID": tracking_field_3 if tracking_field_3 else "",
                "Adset": tracking_field_6 if tracking_field_6 else "",
                "AdsetID": tracking_field_5 if tracking_field_5 else "",
                "Ads": tracking_field_2 if tracking_field_2 else "",
                "AdsID": tracking_field_1 if tracking_field_1 else "",
                # CF metrics
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "revenue": revenue,
                # Media metrics
                "spend": revenue if should_exclude_spend else cost,  # For special media, spend = revenue"
                "m_imp": impressions,
                "m_clicks": clicks,
                "m_conv": conversions
            }

            return transformed

        except Exception as e:
            self.logger.warning(f"Failed to transform row: {str(e)}, row: {raw_row}")
            return None

    def _safe_int(self, value: any) -> int:
        """
        Safely convert value to int

        Args:
            value: Value to convert

        Returns:
            int: Converted value or 0
        """
        try:
            if value is None:
                return 0
            return int(float(value))
        except:
            return 0

    def _safe_float(self, value: any) -> float:
        """
        Safely convert value to float

        Args:
            value: Value to convert

        Returns:
            float: Converted value or 0.0
        """
        try:
            if value is None:
                return 0.0
            return float(value)
        except:
            return 0.0

    def delete_existing_data(self, report_date: str) -> bool:
        """
        Delete all existing data for the report date before inserting new data.
        This ensures no duplicate accumulation when ETL is re-run.

        Args:
            report_date: Report date to delete

        Returns:
            bool: Operation success status
        """
        try:
            delete_sql = f"""
                ALTER TABLE {self.ch_database}.{self.ch_table}
                DELETE
                WHERE reportDate = '{report_date}'
            """

            self.logger.info(f"Deleting ALL existing data for {report_date}")
            self.logger.debug(f"SQL: {delete_sql}")

            self.ch_client.command(delete_sql)
            self.logger.info("Existing data deleted successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to delete existing data: {str(e)}")
            return False

    def insert_data(self, data: List[Dict]) -> bool:
        """
        Insert transformed data into ClickHouse

        Args:
            data: List of transformed rows

        Returns:
            bool: Operation success status
        """
        if not data:
            self.logger.warning("No data to insert")
            return True

        try:
            self.logger.info(f"Inserting {len(data)} rows into {self.ch_database}.{self.ch_table}")

            # Log first row for debugging
            self.logger.info(f"First row data: {data[0]}")

            # Column names in our data (must match ClickHouse table columns)
            columns = ['reportDate', 'dataSource', 'Media', 'MediaID', 'offer', 'offerID',
                       'advertiser', 'advertiserID', 'lander', 'landerID',
                       'Campaign', 'CampaignID', 'Adset', 'AdsetID', 'Ads', 'AdsID',
                       'impressions', 'clicks', 'conversions', 'revenue',
                       'spend', 'm_imp', 'm_clicks', 'm_conv']

            # Convert dict to list of lists
            rows = []
            for row in data:
                rows.append([row[col] for col in columns])

            # Insert data
            self.ch_client.insert(
                table=f"{self.ch_database}.{self.ch_table}",
                column_names=columns,
                data=rows
            )

            self.logger.info("Data inserted successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to insert data: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def extract_data(self, report_date: str) -> tuple[bool, List[Dict]]:
        """
        Extract data from Clickflare API

        Args:
            report_date: Report date string (YYYY-MM-DD)

        Returns:
            tuple[bool, List[Dict]]: (success, raw_data)
        """
        try:
            # Format datetime for API
            start_datetime = f"{report_date} 00:00:00"
            end_datetime = f"{report_date} 23:59:59"

            self.logger.info(f"Extracting data for {report_date}")
            self.logger.debug(f"Date range: {start_datetime} to {end_datetime}")

            # Fetch all pages
            all_items, error = self.api_client.fetch_all_pages(
                start_date=start_datetime,
                end_date=end_datetime,
                group_by=self.group_by,
                metrics=self.metrics,
                page_size=self.page_size,
                max_pages=self.max_pages
            )

            if error:
                self.logger.error(f"API extraction failed: {error}")
                return False, []

            if not all_items:
                self.logger.warning("No data returned from API")
                return True, []

            self.logger.info(f"Extracted {len(all_items)} raw rows from API")
            return True, all_items

        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            return False, []

    def extract_data_pass2(self, report_date: str) -> tuple[bool, List[Dict]]:
        """
        Extract data from Clickflare API (Second Pass - for landingID/landingName)

        Args:
            report_date: Report date string (YYYY-MM-DD)

        Returns:
            tuple[bool, List[Dict]]: (success, raw_data)
        """
        if not self.group_by_pass2 or not self.metrics_pass2:
            self.logger.info("No second pass configuration, skipping...")
            return True, []

        try:
            # Format datetime for API
            start_datetime = f"{report_date} 00:00:00"
            end_datetime = f"{report_date} 23:59:59"

            self.logger.info(f"Extracting data (PASS 2) for {report_date}")
            self.logger.debug(f"Date range: {start_datetime} to {end_datetime}")

            # Fetch all pages with pass 2 configuration
            all_items, error = self.api_client.fetch_all_pages(
                start_date=start_datetime,
                end_date=end_datetime,
                group_by=self.group_by_pass2,
                metrics=self.metrics_pass2,
                page_size=self.page_size,
                max_pages=self.max_pages
            )

            if error:
                self.logger.error(f"API extraction (PASS 2) failed: {error}")
                return False, []

            if not all_items:
                self.logger.warning("No data returned from API (PASS 2)")
                return True, []

            self.logger.info(f"Extracted {len(all_items)} raw rows from API (PASS 2)")
            return True, all_items

        except Exception as e:
            self.logger.error(f"Extraction (PASS 2) failed: {str(e)}")
            return False, []

    def merge_two_pass_data(self, pass1_data: List[Dict], pass2_data: List[Dict]) -> List[Dict]:
        """
        Merge data from two passes: pass1 has advertiser, pass2 has landing

        Uses 9 fields as merge key: date + trafficSourceID + offerID + trackingField1-6

        Args:
            pass1_data: Data from first pass (contains advertiser info)
            pass2_data: Data from second pass (contains landing info)

        Returns:
            List[Dict]: Merged data with landingID/landingName filled in
        """
        if not pass2_data:
            self.logger.info("No pass 2 data to merge, returning pass 1 data as-is")
            return pass1_data

        self.logger.info(f"Merging {len(pass1_data)} pass1 rows with {len(pass2_data)} pass2 rows")

        # Build a lookup dict from pass2 data using composite key
        pass2_lookup = {}

        for row in pass2_data:
            # Create composite key
            key = self._make_merge_key(row)
            if key not in pass2_lookup:
                pass2_lookup[key] = {
                    "landingID": row.get("landingID", ""),
                    "landingName": row.get("landingName", ""),
                }

        # Merge pass2 data into pass1
        merged_count = 0
        for row in pass1_data:
            key = self._make_merge_key(row)
            if key in pass2_lookup:
                row["landingID"] = pass2_lookup[key]["landingID"]
                row["landingName"] = pass2_lookup[key]["landingName"]
                merged_count += 1
            else:
                # No matching landing data
                row["landingID"] = ""
                row["landingName"] = ""

        self.logger.info(f"Merged landing data for {merged_count}/{len(pass1_data)} rows")
        return pass1_data

    def _make_merge_key(self, row: Dict) -> str:
        """
        Create a composite key for merging two-pass data

        Args:
            row: Data row

        Returns:
            str: Composite key string
        """
        return (
            f"{row.get('date', '')}|"
            f"{row.get('trafficSourceID', '')}|"
            f"{row.get('offerID', '')}|"
            f"{row.get('trackingField1', '')}|"
            f"{row.get('trackingField2', '')}|"
            f"{row.get('trackingField3', '')}|"
            f"{row.get('trackingField4', '')}|"
            f"{row.get('trackingField5', '')}|"
            f"{row.get('trackingField6', '')}"
        )

    def transform_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Transform raw API data to target schema

        Args:
            raw_data: List of raw rows from API

        Returns:
            List[Dict]: Transformed rows
        """
        self.logger.info("Transforming data...")
        transformed_data = []

        for raw_row in raw_data:
            transformed = self.transform_row(raw_row)
            if transformed:
                transformed_data.append(transformed)

        self.logger.info(f"Transformed {len(transformed_data)} rows")
        return transformed_data

    def _check_timeout(self, stage: str = "") -> bool:
        """
        检查是否超过超时限制

        Args:
            stage: 当前阶段名称，用于日志

        Returns:
            bool: True if timeout exceeded
        """
        if self.start_time is None:
            return False

        elapsed = (datetime.now() - self.start_time).total_seconds() / 60
        if elapsed >= self.timeout_minutes:
            self.logger.warning(f"Timeout check at stage '{stage}': {elapsed:.1f} minutes elapsed (limit: {self.timeout_minutes} min)")
            return True
        return False

    def _handle_timeout(self, report_date: str, partial_data: List[Dict], stage: str) -> None:
        """
        处理超时情况：插入已拉取的部分数据并中断程序

        Args:
            report_date: 报告日期
            partial_data: 已拉取并转换的部分数据
            stage: 超时发生的阶段
        """
        self.logger.error(f"=== TIMEOUT TRIGGERED at stage: {stage} ===")
        self.logger.error(f"Elapsed time: {(datetime.now() - self.start_time).total_seconds() / 60:.1f} minutes")

        if not partial_data:
            self.logger.warning("No partial data to insert, exiting without saving")
            return

        # 插入部分数据
        self.logger.info(f"Inserting partial data ({len(partial_data)} rows) before timeout...")
        self.delete_existing_data(report_date)
        self.insert_data(partial_data)

        total_revenue = sum(row.get('revenue', 0) for row in partial_data)
        total_spend = sum(row.get('spend', 0) for row in partial_data)
        self.logger.warning(f"PARTIAL DATA INSERTED: revenue=${total_revenue:.2f}, spend=${total_spend:.2f}")
        self.logger.warning(f"Next scheduled task will continue fetching remaining data")

    def _is_special_media(self, media_name: str) -> bool:
        """
        判断是否为 MTG 需要更新 spend 的媒体（只有 Mintegral）

        注意：这与 exclude_spend_media 不同
        - exclude_spend_media: 初始 spend = revenue (包括 Mintegral/Hastraffic/JMmobi/Brain)
        - mtg_media_keywords: 只有 Mintegral，会被 MTG API 真实 spend 覆盖

        Args:
            media_name: Media name to check

        Returns:
            bool: True if this media should be updated with MTG spend data
        """
        if not media_name:
            return False
        media_lower = media_name.lower()
        return any(keyword.lower() in media_lower for keyword in self.mtg_media_keywords)

    def _fetch_mtg_data_for_date(self, report_date: str) -> List[Dict]:
        """
        Fetch MTG data from all MTG accounts for the given date

        Args:
            report_date: Report date string (YYYY-MM-DD)

        Returns:
            List[Dict]: All MTG data rows from all accounts
        """
        if not self.mtg_enabled or not self.mtg_accounts:
            self.logger.info("MTG integration disabled or no accounts configured")
            return []

        self.logger.info(f"Fetching MTG data for {report_date} from {len(self.mtg_accounts)} accounts...")

        all_mtg_data = []

        for account in self.mtg_accounts:
            account_name = account.get("name", "Unknown")
            self.logger.info(f"Fetching MTG data from account: {account_name}")

            try:
                # Create MTG API client for this account
                api_config = {
                    **self.mtg_api_config,
                    "access_key": account["access_key"],
                    "api_key": account["api_key"]
                }
                mtg_client = MTGAPIClient(api_config, self.logger)

                # Get data from MTG API
                success, mtg_rows = mtg_client.get_parsed_data(report_date)

                if success and mtg_rows:
                    # Transform MTG rows to our schema
                    for row in mtg_rows:
                        transformed = self._transform_mtg_row(row, report_date)
                        if transformed:
                            all_mtg_data.append(transformed)

                    self.logger.info(f"Fetched {len(mtg_rows)} rows from {account_name}")
                else:
                    self.logger.warning(f"No data from {account_name}")

            except Exception as e:
                self.logger.error(f"Failed to fetch MTG data from {account_name}: {str(e)}")

        self.logger.info(f"Total MTG rows fetched: {len(all_mtg_data)}")
        return all_mtg_data

    def _transform_mtg_row(self, raw_row: Dict, report_date: str) -> Optional[Dict]:
        """
        Transform raw MTG API data to our internal format

        MTG API fields:
        - Date: YYYYMMDD format
        - Campaign Id, Offer Id (AdsetID), Creative Id (AdsID)
        - Offer Name (Adset), Creative Name (Ads)
        - Impression, Click, Conversion, Spend

        Args:
            raw_row: Raw MTG API data row
            report_date: Report date string (YYYY-MM-DD)

        Returns:
            Optional[Dict]: Transformed row with AdsetID as key
        """
        try:
            # Parse date
            api_date = raw_row.get("Date", "")
            if api_date:
                try:
                    year = int(api_date[:4])
                    month = int(api_date[4:6])
                    day = int(api_date[6:8])
                    date_obj = datetime(year, month, day).date()
                except:
                    date_obj = datetime.strptime(report_date, "%Y-%m-%d").date()
            else:
                date_obj = datetime.strptime(report_date, "%Y-%m-%d").date()

            return {
                "reportDate": date_obj,
                "dataSource": "MTG",
                "CampaignID": raw_row.get("Campaign Id", "") or "0",
                "AdsetID": raw_row.get("Offer Id", "") or "0",  # Key for matching
                "AdsID": raw_row.get("Creative Id", "") or "0",
                "Adset": raw_row.get("Offer Name", ""),
                "Ads": raw_row.get("Creative Name", ""),
                "spend": self._safe_float(raw_row.get("Spend", "0").replace(",", "")),
                "m_imp": self._safe_int(raw_row.get("Impression", "0").replace(",", "")),
                "m_clicks": self._safe_int(raw_row.get("Click", "0").replace(",", "")),
                "m_conv": self._safe_int(raw_row.get("Conversion", "0").replace(",", ""))
            }

        except Exception as e:
            self.logger.warning(f"Failed to transform MTG row: {str(e)}")
            return None

    def _merge_mtg_data_to_cf(self, cf_data: List[Dict], mtg_data: List[Dict]) -> List[Dict]:
        """
        Merge MTG spend data into CF data in memory

        Strategy:
        1. Group MTG data by AdsetID (sum spend per AdsetID)
        2. For each AdsetID, either update existing CF rows or create new MTG-only rows
        3. This ensures ALL MTG spend is captured, not just the portion matching CF impressions

        Args:
            cf_data: Clickflare transformed data
            mtg_data: MTG transformed data

        Returns:
            List[Dict]: Merged data ready for INSERT
        """
        if not mtg_data:
            self.logger.info("No MTG data to merge, returning CF data as-is")
            return cf_data

        self.logger.info(f"Merging {len(mtg_data)} MTG rows into {len(cf_data)} CF rows...")

        # Step 1: Aggregate MTG data by AdsetID (一个 AdsetID 可能有多个 MTG 行)
        mtg_by_adset = {}
        for mtg_row in mtg_data:
            adset_id = mtg_row.get("AdsetID", "")
            if not adset_id or adset_id == "0":
                continue

            if adset_id not in mtg_by_adset:
                mtg_by_adset[adset_id] = {
                    "spend": 0,
                    "m_imp": 0,
                    "m_clicks": 0,
                    "m_conv": 0,
                    "CampaignID": mtg_row.get("CampaignID", ""),
                    "Adset": mtg_row.get("Adset", ""),
                    "AdsID": mtg_row.get("AdsID", ""),
                    "Ads": mtg_row.get("Ads", ""),
                }

            mtg_by_adset[adset_id]["spend"] += mtg_row.get('spend', 0)
            mtg_by_adset[adset_id]["m_imp"] += mtg_row.get('m_imp', 0)
            mtg_by_adset[adset_id]["m_clicks"] += mtg_row.get('m_clicks', 0)
            mtg_by_adset[adset_id]["m_conv"] += mtg_row.get('m_conv', 0)

        self.logger.info(f"Aggregated MTG data into {len(mtg_by_adset)} unique AdsetIDs")

        # Step 2: Build CF lookup by AdsetID
        cf_lookup = {}
        for row in cf_data:
            adset_id = row.get("AdsetID", "")
            if adset_id not in cf_lookup:
                cf_lookup[adset_id] = []
            cf_lookup[adset_id].append(row)

        # Step 3: Merge - update existing CF rows or create new MTG-only rows
        matched_count = 0
        unmatched_count = 0
        new_rows = []
        report_date = None

        # Find report_date from existing data
        if cf_data:
            report_date = cf_data[0].get('reportDate')

        for adset_id, mtg_agg in mtg_by_adset.items():
            if adset_id in cf_lookup and cf_lookup[adset_id]:
                # Has matching CF rows - update them with MTG data
                matching_cf_rows = cf_lookup[adset_id]

                # 只更新 special media 的 CF 行
                special_cf_rows = [r for r in matching_cf_rows if self._is_special_media(r.get("Media", ""))]

                if not special_cf_rows:
                    # 这个 AdsetID 的 CF 行都不是 special media，跳过
                    continue

                # 按 CF impressions 比例分配 MTG spend
                total_cf_impressions = sum(r.get('impressions', 0) for r in special_cf_rows)

                if total_cf_impressions == 0:
                    # 没有 CF impressions，平均分配
                    count = len(special_cf_rows)
                    for cf_row in special_cf_rows:
                        cf_row['spend'] = mtg_agg['spend'] / count
                        cf_row['m_imp'] = int(round(mtg_agg['m_imp'] / count)) if count > 0 else 0
                        cf_row['m_clicks'] = int(round(mtg_agg['m_clicks'] / count)) if count > 0 else 0
                        cf_row['m_conv'] = int(round(mtg_agg['m_conv'] / count)) if count > 0 else 0
                else:
                    # 按 CF impressions 比例分配
                    for cf_row in special_cf_rows:
                        cf_impressions = cf_row.get('impressions', 0)
                        ratio = cf_impressions / total_cf_impressions
                        cf_row['spend'] = mtg_agg['spend'] * ratio
                        cf_row['m_imp'] = int(round(mtg_agg['m_imp'] * ratio))
                        cf_row['m_clicks'] = int(round(mtg_agg['m_clicks'] * ratio))
                        cf_row['m_conv'] = int(round(mtg_agg['m_conv'] * ratio))

                matched_count += 1
            else:
                # No matching CF rows - create a new MTG-only row
                unmatched_count += 1

                # 创建一个 MTG 专用行（没有 CF 追踪的数据）
                new_row = {
                    "reportDate": report_date,
                    "dataSource": "MTG",
                    "Media": "Mintegral",  # 默认媒体名称
                    "MediaID": "",
                    "offer": "",
                    "offerID": "",
                    "advertiser": "",
                    "advertiserID": "",
                    "lander": "",
                    "landerID": "",
                    "Campaign": "",
                    "CampaignID": mtg_agg["CampaignID"],
                    "Adset": mtg_agg["Adset"],
                    "AdsetID": adset_id,
                    "Ads": mtg_agg["Ads"],
                    "AdsID": mtg_agg["AdsID"],
                    # CF metrics - MTG 不提供，设为 0
                    "impressions": 0,
                    "clicks": 0,
                    "conversions": 0,
                    "revenue": 0.0,
                    # MTG metrics
                    "spend": mtg_agg['spend'],
                    "m_imp": mtg_agg['m_imp'],
                    "m_clicks": mtg_agg['m_clicks'],
                    "m_conv": mtg_agg['m_conv'],
                }
                new_rows.append(new_row)

        self.logger.info(f"MTG merge complete: {matched_count} matched (updated CF rows), {unmatched_count} unmatched (created new rows)")

        # Add new MTG-only rows to the data
        cf_data.extend(new_rows)

        # Log merged totals
        merged_spend = sum(row.get('spend', 0) for row in cf_data if self._is_special_media(row.get('Media', '')) or row.get('dataSource') == 'MTG')
        mtg_total = sum(m['spend'] for m in mtg_by_adset.values())
        self.logger.info(f"Total MTG spend: ${mtg_total:.2f}, Merged spend in data: ${merged_spend:.2f}, New rows created: {len(new_rows)}")

        return cf_data

    def run(self, report_date: Optional[str] = None) -> bool:
        """
        Run the complete ETL process

        Args:
            report_date: Optional report date (YYYY-MM-DD).
                        If not provided, calculated from config offset.

        Returns:
            bool: ETL success status
        """
        # 设置开始时间，用于超时检查
        self.start_time = datetime.now()

        # Determine report date
        if report_date is None:
            report_date = self.get_report_date()

        self.logger.log_etl_start(report_date)

        try:
            # Step 1: Connect to ClickHouse
            self.logger.info("Step 1: Connecting to ClickHouse...")
            if not self.connect_clickhouse():
                self.logger.log_etl_failed(report_date, "ClickHouse connection failed")
                return False

            # Step 2: Initialize API client
            self.logger.info("Step 2: Initializing API client...")
            self.init_api_client()

            # Step 3: Extract data (PASS 1 - with advertiser)
            self.logger.info("Step 3: Extracting data from Clickflare API (PASS 1)...")
            success, raw_data = self.extract_data(report_date)

            if not success:
                self.logger.log_etl_failed(report_date, "Data extraction failed")
                return False

            # 超时检查: CF PASS 1 完成后
            if self._check_timeout("after CF PASS 1"):
                # CF 只有 PASS 1 数据，没有 landing 数据，先处理这部分
                raw_data_pass2 = []
                raw_data = self.merge_two_pass_data(raw_data, raw_data_pass2)
                transformed_data = self.transform_data(raw_data)
                self._handle_timeout(report_date, transformed_data, "after CF PASS 1")
                return False

            if not raw_data:
                self.logger.info("No data to process")
                self.logger.log_etl_complete(report_date, 0)
                return True

            # Step 3.5: Extract data (PASS 2 - for landingID/landingName)
            self.logger.info("Step 3.5: Extracting data from Clickflare API (PASS 2 - landing)...")
            success_pass2, raw_data_pass2 = self.extract_data_pass2(report_date)

            # 超时检查: CF PASS 2 完成后
            if self._check_timeout("after CF PASS 2"):
                if not success_pass2:
                    raw_data_pass2 = []
                raw_data = self.merge_two_pass_data(raw_data, raw_data_pass2)
                transformed_data = self.transform_data(raw_data)
                self._handle_timeout(report_date, transformed_data, "after CF PASS 2")
                return False

            if not success_pass2:
                self.logger.warning("PASS 2 extraction failed, continuing without landing data")
                raw_data_pass2 = []

            # Step 3.6: Merge two-pass data
            if raw_data_pass2:
                self.logger.info("Step 3.6: Merging PASS 1 and PASS 2 data...")
                raw_data = self.merge_two_pass_data(raw_data, raw_data_pass2)

            # Step 4: Transform data
            self.logger.info("Step 4: Transforming data...")
            transformed_data = self.transform_data(raw_data)

            if not transformed_data:
                self.logger.warning("No valid data after transformation")
                self.logger.log_etl_complete(report_date, 0)
                return True

            # Step 4.5: Fetch and merge MTG data (if enabled)
            if self.mtg_enabled:
                self.logger.info("Step 4.5: Fetching and merging MTG data...")

                # 逐步拉取 MTG 数据，每个账户后检查超时
                all_mtg_data = []
                mtg_accounts_fetched = 0

                for account in self.mtg_accounts:
                    account_name = account.get("name", "Unknown")
                    self.logger.info(f"Fetching MTG data from account: {account_name}")

                    # 初始化该账户状态为 False
                    self.mtg_accounts_status[account_name] = False

                    try:
                        api_config = {
                            **self.mtg_api_config,
                            "access_key": account["access_key"],
                            "api_key": account["api_key"]
                        }
                        mtg_client = MTGAPIClient(api_config, self.logger)
                        success, mtg_rows = mtg_client.get_parsed_data(report_date)

                        if success and mtg_rows:
                            for row in mtg_rows:
                                transformed = self._transform_mtg_row(row, report_date)
                                if transformed:
                                    all_mtg_data.append(transformed)
                            mtg_accounts_fetched += 1
                            # 标记该账户拉取成功
                            self.mtg_accounts_status[account_name] = True
                        else:
                            # 账户拉取失败，保持状态为 False
                            self.logger.warning(f"MTG account {account_name} returned no data or failed")

                        # 超时检查: 每个 MTG 账户拉取后
                        if self._check_timeout(f"after MTG account {account_name}"):
                            if all_mtg_data:
                                transformed_data = self._merge_mtg_data_to_cf(transformed_data, all_mtg_data)
                            self._handle_timeout(report_date, transformed_data, f"after MTG account {account_name}")
                            return False

                    except Exception as e:
                        self.logger.error(f"Failed to fetch MTG data from {account_name}: {str(e)}")

                self.logger.info(f"Fetched MTG data from {mtg_accounts_fetched}/{len(self.mtg_accounts)} accounts")

                if all_mtg_data:
                    transformed_data = self._merge_mtg_data_to_cf(transformed_data, all_mtg_data)
                    self.logger.info("MTG data merged successfully")
                else:
                    self.logger.info("No MTG data fetched, continuing with CF data only")
            else:
                self.logger.info("Step 4.5: MTG integration disabled, skipping...")

            # Step 5: Delete existing data
            self.logger.info("Step 5: Deleting existing data...")
            self.delete_existing_data(report_date)

            # Step 6: Load data into ClickHouse
            self.logger.info("Step 6: Loading data into ClickHouse...")
            if not self.insert_data(transformed_data):
                self.logger.log_etl_failed(report_date, "Data insertion failed")
                return False

            # Calculate and log revenue summary
            total_revenue = sum(row.get('revenue', 0) for row in transformed_data)
            total_spend = sum(row.get('spend', 0) for row in transformed_data)
            self.logger.log_etl_complete(report_date, len(transformed_data))

            # 计算 MTG 状态
            mtg_all_success = True
            if self.mtg_enabled and self.mtg_accounts:
                # 如果有任何一个 MTG 账户失败，则不是全部成功
                mtg_all_success = all(self.mtg_accounts_status.values())
                # 如果所有账户都是 False（可能刚初始化），也算失败
                if not self.mtg_accounts_status:
                    mtg_all_success = True  # 没有启用 MTG 或没有账户
            else:
                # MTG 未启用，视为全部成功
                mtg_all_success = True

            # Print summary for run_etl.py to parse
            print(f"SUMMARY: revenue={total_revenue:.2f}, spend={total_spend:.2f}, mtg_all_success={mtg_all_success}")

            return True

        except Exception as e:
            self.logger.log_etl_failed(report_date, str(e))
            return False

        finally:
            # Clean up
            if self.ch_client:
                try:
                    self.ch_client.close()
                except:
                    pass


def main():
    """
    Main entry point
    """
    import argparse

    parser = argparse.ArgumentParser(description="Clickflare Report ETL")
    parser.add_argument(
        "-d", "--date",
        type=str,
        help="Report date in YYYY-MM-DD format (default: yesterday)"
    )
    parser.add_argument(
        "-c", "--config",
        type=str,
        default="config.yaml",
        help="Path to config file (default: config.yaml)"
    )

    args = parser.parse_args()

    # Validate date format if provided
    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD.")
            sys.exit(1)

    try:
        etl = ClickflareETL(config_path=args.config)
        success = etl.run(report_date=args.date)

        if success:
            sys.exit(0)
        else:
            sys.exit(1)

    except Exception as e:
        print(f"ETL process failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
