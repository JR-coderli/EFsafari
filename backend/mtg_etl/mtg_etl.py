"""
MTG Report ETL Main Process
Extracts data from MTG API (multiple accounts), transforms, and loads into ClickHouse
"""
import os
import sys
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import clickhouse_connect

from mtg_api import MTGAPIClient
from logger import ETLLogger


class MTGETL:
    """
    MTG ETL Process: Extract -> Transform -> Load
    Supports multiple MTG accounts
    """

    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize ETL process

        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = ETLLogger(self.config["logging"])
        self.accounts = self.config.get("accounts", [])

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
        self.mtg_media_keywords = self.config["etl"].get("mtg_media_keywords", ["Mintegral", "Hastraffic"])

        self.ch_client = None

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

            self.logger.info("ClickHouse connection initialized (will test on first query)")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse: {str(e)}")
            return False

    def get_report_date(self) -> str:
        """
        Calculate target report date based on offset

        Returns:
            str: Report date in YYYY-MM-DD format
        """
        target_date = datetime.now() - timedelta(days=self.date_offset_days)
        return target_date.strftime("%Y-%m-%d")

    def transform_row(self, raw_row: Dict, report_date: str) -> Optional[Dict]:
        """
        Transform raw API data to target table schema

        Args:
            raw_row: Raw data row from API
            report_date: Report date string

        Returns:
            Optional[Dict]: Transformed row or None if invalid
        """
        try:
            # Parse date from API (format: 20240601) -> datetime.date object
            api_date = raw_row.get("Date", "")
            if api_date:
                try:
                    year = int(api_date[:4])
                    month = int(api_date[4:6])
                    day = int(api_date[6:8])
                    date_obj = datetime(year, month, day).date()
                except:
                    # Fallback to report_date
                    date_obj = datetime.strptime(report_date, "%Y-%m-%d").date()
            else:
                date_obj = datetime.strptime(report_date, "%Y-%m-%d").date()

            # Get ID values as strings (ClickHouse IDs are often String type)
            campaign_id = raw_row.get("Campaign Id", "")
            adset_id = raw_row.get("Offer Id", "")
            ads_id = raw_row.get("Creative Id", "")

            # Handle impression/click/conversion values (may be empty or have special format)
            impression = raw_row.get("Impression", "0").replace(",", "")
            click = raw_row.get("Click", "0").replace(",", "")
            conversion = raw_row.get("Conversion", "0").replace(",", "")
            spend = raw_row.get("Spend", "0").replace(",", "")

            transformed = {
                "reportDate": date_obj,  # datetime.date object for ClickHouse Date type
                "dataSource": "MTG",  # 数据来源标识
                "Media": "",  # 留空，等 Clickflare UPDATE (trafficsourceName)
                "MediaID": "",  # 留空，等 Clickflare UPDATE (trafficsourceID)
                "Campaign": "",  # API doesn't provide Campaign Name
                "CampaignID": campaign_id if campaign_id else "0",
                "Adset": raw_row.get("Offer Name", ""),
                "AdsetID": adset_id if adset_id else "0",
                "Ads": raw_row.get("Creative Name", ""),
                "AdsID": ads_id if ads_id else "0",
                "spend": self._safe_float(spend),
                "m_imp": self._safe_int(impression),
                "m_clicks": self._safe_int(click),
                "m_conv": self._safe_int(conversion)
            }

            return transformed

        except Exception as e:
            self.logger.warning(f"Failed to transform row: {str(e)}, row: {raw_row}")
            return None

    def _safe_int(self, value: str) -> int:
        """
        Safely convert string to int

        Args:
            value: String value to convert

        Returns:
            int: Converted value or 0
        """
        try:
            return int(float(value)) if value else 0
        except:
            return 0

    def _safe_float(self, value: str) -> float:
        """
        Safely convert string to float

        Args:
            value: String value to convert

        Returns:
            float: Converted value or 0.0
        """
        try:
            return float(value) if value else 0.0
        except:
            return 0.0

    def _get_mtg_media_cf_rows(self, report_date: str) -> List[Dict]:
        """
        Get ALL CF rows for MTG media (完整数据，用于 DELETE + INSERT).

        只读取 MTG 负责的媒体数据，避免影响其他媒体。

        Args:
            report_date: Report date string

        Returns:
            List[Dict]: Complete CF rows for MTG media
        """
        try:
            media_conditions = " OR ".join([f"Media LIKE '%{keyword}%'" for keyword in self.mtg_media_keywords])

            query = f"""
                SELECT
                    reportDate, dataSource, Media, MediaID,
                    offer, offerID, advertiser, advertiserID,
                    lander, landerID,
                    Campaign, CampaignID, Adset, AdsetID, Ads, AdsID,
                    impressions, clicks, conversions, revenue, spend,
                    m_imp, m_clicks, m_conv
                FROM {self.ch_database}.{self.ch_table}
                WHERE reportDate = '{report_date}'
                AND dataSource = 'Clickflare'
                AND ({media_conditions})
            """

            result = self.ch_client.query(query)
            rows = list(result.named_results())

            self.logger.info(f"Loaded {len(rows)} CF rows for MTG media ({self.mtg_media_keywords})")
            return rows

        except Exception as e:
            self.logger.error(f"Failed to query CF rows for MTG media: {str(e)}")
            return []

    def insert_data(self, data: List[Dict]) -> bool:
        """
        使用 DELETE + INSERT 方式更新 MTG 数据（优化后，减少 ClickHouse merge 负担）

        流程:
        1. 读取 MTG 媒体的完整 CF 数据
        2. 内存中合并 MTG 的 spend/metrics
        3. DELETE MTG 媒体的数据
        4. INSERT 合并后的完整数据

        优点:
        - 只执行 2 次 SQL 操作（DELETE + INSERT）
        - 避免 10000+ 次 UPDATE
        - 减少 ClickHouse merge 负担

        Args:
            data: List of transformed MTG rows

        Returns:
            bool: Operation success status
        """
        if not data:
            self.logger.warning("No MTG data to process")
            return True

        try:
            report_date = data[0]['reportDate']

            # 统计 MTG 数据
            total_spend = sum(row.get('spend', 0) for row in data)
            total_m_imp = sum(row.get('m_imp', 0) for row in data)
            total_m_clicks = sum(row.get('m_clicks', 0) for row in data)
            total_m_conv = sum(row.get('m_conv', 0) for row in data)

            self.logger.info(f"MTG API data - Spend: ${total_spend:.2f}, Imp: {total_m_imp:,}, Clicks: {total_m_clicks:,}, Conv: {total_m_conv:,}")

            # Step 1: 读取 MTG 媒体的完整 CF 数据
            self.logger.info("Step 1: Loading CF data for MTG media...")
            cf_rows = self._get_mtg_media_cf_rows(report_date)

            if not cf_rows:
                self.logger.warning("No CF data found for MTG media")
                return True

            self.logger.info(f"Loaded {len(cf_rows)} CF rows for MTG media")

            # Step 2: 内存中合并 MTG 数据到 CF 数据
            self.logger.info("Step 2: Merging MTG data with CF data in memory...")

            # 构建 CF 数据的查找字典 (key = AdsetID)
            cf_lookup = {}
            for row in cf_rows:
                key = row.get('AdsetID', '')
                if key not in cf_lookup:
                    cf_lookup[key] = []
                cf_lookup[key].append(row)

            # 合并 MTG 数据
            merged_rows = []
            unmatched_mtg_count = 0

            for mtg_row in data:
                adset_id = mtg_row.get('AdsetID', '')

                if adset_id not in cf_lookup or not cf_lookup[adset_id]:
                    unmatched_mtg_count += 1
                    continue

                matching_cf_rows = cf_lookup[adset_id]

                # 计算总 impressions 用于按比例分配
                total_impressions = sum(r.get('impressions', 0) for r in matching_cf_rows)

                mtg_spend = mtg_row.get('spend', 0)
                mtg_imp = mtg_row.get('m_imp', 0)
                mtg_clicks = mtg_row.get('m_clicks', 0)
                mtg_conv = mtg_row.get('m_conv', 0)

                if total_impressions == 0:
                    # 平均分配
                    count = len(matching_cf_rows)
                    for cf_row in matching_cf_rows:
                        merged_row = cf_row.copy()
                        merged_row['spend'] = mtg_spend / count
                        merged_row['m_imp'] = mtg_imp / count
                        merged_row['m_clicks'] = mtg_clicks / count
                        merged_row['m_conv'] = mtg_conv / count
                        merged_rows.append(merged_row)
                else:
                    # 按 impressions 比例分配
                    for cf_row in matching_cf_rows:
                        ratio = cf_row.get('impressions', 0) / total_impressions
                        merged_row = cf_row.copy()
                        merged_row['spend'] = mtg_spend * ratio
                        merged_row['m_imp'] = mtg_imp * ratio
                        merged_row['m_clicks'] = mtg_clicks * ratio
                        merged_row['m_conv'] = mtg_conv * ratio
                        merged_rows.append(merged_row)

            # 处理 CF 中有但 MTG 中没有的行（spend 保持原值或设为 0）
            matched_adset_ids = set(row.get('AdsetID', '') for row in data)
            for cf_row in cf_rows:
                if cf_row.get('AdsetID', '') not in matched_adset_ids:
                    # CF 有但 MTG 没有，spend 保持 CF 的值（或者可以设为 0）
                    merged_rows.append(cf_row.copy())

            self.logger.info(f"Merged {len(merged_rows)} rows ({unmatched_mtg_count} MTG rows had no CF match)")

            # Step 3: DELETE MTG 媒体的数据
            self.logger.info("Step 3: Deleting existing MTG media data...")
            self._delete_mtg_media_data(report_date)

            # Step 4: INSERT 合并后的完整数据
            self.logger.info("Step 4: Inserting merged data...")
            self._insert_merged_data(merged_rows)

            # 验证汇总
            inserted_spend = sum(float(row.get('spend', 0)) for row in merged_rows)
            inserted_imp = sum(int(row.get('m_imp', 0)) for row in merged_rows)

            self.logger.info(f"Verification - Inserted Spend: ${inserted_spend:.2f}, Imp: {inserted_imp:,}")
            self.logger.info(f"Optimization: Used DELETE + INSERT instead of {len(cf_rows)}+ UPDATE statements")

            return True

        except Exception as e:
            self.logger.error(f"Failed to process MTG data: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _delete_mtg_media_data(self, report_date: str) -> bool:
        """
        Delete MTG media data for the report date.

        Args:
            report_date: Report date to delete

        Returns:
            bool: Operation success status
        """
        try:
            media_conditions = " OR ".join([f"Media LIKE '%{keyword}%'" for keyword in self.mtg_media_keywords])

            delete_sql = f"""
                ALTER TABLE {self.ch_database}.{self.ch_table}
                DELETE
                WHERE reportDate = '{report_date}'
                AND dataSource = 'Clickflare'
                AND ({media_conditions})
            """

            self.logger.info(f"Deleting MTG media data for: {self.mtg_media_keywords}")
            self.logger.debug(f"SQL: {delete_sql}")

            self.ch_client.command(delete_sql)
            self.logger.info("MTG media data deleted successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to delete MTG media data: {str(e)}")
            return False

    def _insert_merged_data(self, data: List[Dict]) -> bool:
        """
        Insert merged data into ClickHouse.

        Args:
            data: List of merged rows

        Returns:
            bool: Operation success status
        """
        if not data:
            self.logger.warning("No data to insert")
            return True

        try:
            self.logger.info(f"Inserting {len(data)} rows into {self.ch_database}.{self.ch_table}")

            # Column names (must match ClickHouse table columns)
            columns = [
                'reportDate', 'dataSource', 'Media', 'MediaID',
                'offer', 'offerID', 'advertiser', 'advertiserID',
                'lander', 'landerID',
                'Campaign', 'CampaignID', 'Adset', 'AdsetID', 'Ads', 'AdsID',
                'impressions', 'clicks', 'conversions', 'revenue',
                'spend', 'm_imp', 'm_clicks', 'm_conv'
            ]

            # Convert dict to list of lists
            rows = []
            for row in data:
                rows.append([
                    row.get('reportDate'),
                    row.get('dataSource', 'Clickflare'),
                    row.get('Media', ''),
                    row.get('MediaID', ''),
                    row.get('offer', ''),
                    row.get('offerID', ''),
                    row.get('advertiser', ''),
                    row.get('advertiserID', ''),
                    row.get('lander', ''),
                    row.get('landerID', ''),
                    row.get('Campaign', ''),
                    row.get('CampaignID', ''),
                    row.get('Adset', ''),
                    row.get('AdsetID', ''),
                    row.get('Ads', ''),
                    row.get('AdsID', ''),
                    row.get('impressions', 0),
                    row.get('clicks', 0),
                    row.get('conversions', 0),
                    row.get('revenue', 0.0),
                    row.get('spend', 0.0),
                    row.get('m_imp', 0),
                    row.get('m_clicks', 0),
                    row.get('m_conv', 0)
                ])

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

    def run_account(self, account: Dict, report_date: str) -> tuple[bool, int]:
        """
        Run ETL for a single account

        Args:
            account: Account configuration dict with name, access_key, api_key
            report_date: Report date string

        Returns:
            tuple[bool, int]: (success, row_count)
        """
        account_name = account.get("name", "Unknown")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Processing account: {account_name}")
        self.logger.info(f"{'='*60}")

        try:
            # Create API client for this account
            api_config = {
                **self.config["api"],
                "access_key": account["access_key"],
                "api_key": account["api_key"],
                "retry": self.config["retry"],
                "poll": self.config["poll"]
            }
            api_client = MTGAPIClient(api_config, self.logger)

            # Extract data from API
            self.logger.info(f"[{account_name}] Step 1: Extracting data from MTG API...")
            success, raw_data = api_client.get_parsed_data(report_date)

            if not success:
                self.logger.error(f"[{account_name}] API extraction failed: {raw_data}")
                return False, 0

            if not raw_data:
                self.logger.warning(f"[{account_name}] No data returned from API")
                return True, 0

            self.logger.info(f"[{account_name}] Extracted {len(raw_data)} raw rows from API")

            # Transform data
            self.logger.info(f"[{account_name}] Step 2: Transforming data...")
            transformed_data = []

            for raw_row in raw_data:
                transformed = self.transform_row(raw_row, report_date)
                if transformed:
                    transformed_data.append(transformed)

            self.logger.info(f"[{account_name}] Transformed {len(transformed_data)} rows")

            return True, len(transformed_data)

        except Exception as e:
            self.logger.error(f"[{account_name}] Failed: {str(e)}")
            return False, 0

    def run(self, report_date: Optional[str] = None) -> bool:
        """
        Run the complete ETL process for all accounts

        Args:
            report_date: Optional report date (YYYY-MM-DD).
                        If not provided, calculated from config offset.

        Returns:
            bool: ETL success status
        """
        # Determine report date
        if report_date is None:
            report_date = self.get_report_date()

        self.logger.info(f"{'='*60}")
        self.logger.info(f"MTG ETL Job Started for report_date: {report_date}")
        self.logger.info(f"Total accounts: {len(self.accounts)}")
        self.logger.info(f"{'='*60}")

        try:
            # Step 1: Connect to ClickHouse
            if not self.connect_clickhouse():
                self.logger.error("ClickHouse connection failed")
                return False

            # Step 2: Extract data from all accounts
            all_transformed_data = []
            accounts_succeeded = 0
            accounts_failed = 0

            for account in self.accounts:
                success, row_count = self.run_account(account, report_date)
                if success:
                    # Collect data for this account
                    api_config = {
                        **self.config["api"],
                        "access_key": account["access_key"],
                        "api_key": account["api_key"],
                        "retry": self.config["retry"],
                        "poll": self.config["poll"]
                    }
                    api_client = MTGAPIClient(api_config, self.logger)
                    _, raw_data = api_client.get_parsed_data(report_date)

                    # Transform and collect
                    for raw_row in raw_data:
                        transformed = self.transform_row(raw_row, report_date)
                        if transformed:
                            all_transformed_data.append(transformed)

                    accounts_succeeded += 1
                else:
                    accounts_failed += 1

            # Step 3: Summary
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Extraction Summary:")
            self.logger.info(f"  Accounts succeeded: {accounts_succeeded}/{len(self.accounts)}")
            self.logger.info(f"  Accounts failed: {accounts_failed}/{len(self.accounts)}")
            self.logger.info(f"  Total rows: {len(all_transformed_data)}")
            self.logger.info(f"{'='*60}")

            if not all_transformed_data:
                self.logger.warning("No data to insert from any account")
                return True

            # Step 4: Load data into ClickHouse using DELETE + INSERT approach
            self.logger.info("Step 3: Loading data into ClickHouse (DELETE + INSERT mode)...")
            if not self.insert_data(all_transformed_data):
                self.logger.error("Data insertion failed")
                return False

            self.logger.info(f"{'='*60}")
            self.logger.info(f"ETL Job Completed for report_date: {report_date}")
            self.logger.info(f"Total rows processed: {len(all_transformed_data)}")
            self.logger.info(f"{'='*60}")
            return True

        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}")
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

    parser = argparse.ArgumentParser(description="MTG Report ETL")
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
        etl = MTGETL(config_path=args.config)
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
