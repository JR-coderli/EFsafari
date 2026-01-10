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

    def delete_existing_data(self, report_date: str, campaign_ids: List[str] = None) -> bool:
        """
        Delete existing data for the report date (idempotent operation)
        Since Media is empty, we delete by CampaignID list from the accounts

        Args:
            report_date: Report date to delete
            campaign_ids: List of CampaignIDs to delete (if None, delete by date only - use with caution)

        Returns:
            bool: Operation success status
        """
        try:
            if campaign_ids:
                # Delete by CampaignID list (safer, only affects these accounts)
                campaign_ids_str = ",".join([f"'{cid}'" for cid in campaign_ids])
                delete_sql = f"""
                    ALTER TABLE {self.ch_database}.{self.ch_table}
                    DELETE
                    WHERE reportDate = '{report_date}'
                    AND CampaignID IN ({campaign_ids_str})
                """
                self.logger.info(f"Deleting existing data for {report_date} (CampaignIDs: {len(campaign_ids)} accounts)")
            else:
                # Delete by date only (will affect ALL data for this date - use with caution!)
                delete_sql = f"""
                    ALTER TABLE {self.ch_database}.{self.ch_table}
                    DELETE
                    WHERE reportDate = '{report_date}'
                """
                self.logger.warning(f"Deleting ALL data for {report_date} (affects all media!)")

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
            self.logger.debug(f"First row data: {data[0]}")

            # Column names in our data (must match ClickHouse table columns)
            columns = ['reportDate', 'dataSource', 'Media', 'MediaID', 'Campaign', 'CampaignID', 'Adset', 'AdsetID', 'Ads', 'AdsID', 'spend', 'm_imp', 'm_clicks', 'm_conv']

            # Convert dict to list of lists with explicit column order
            rows = []
            for row in data:
                rows.append([row[col] for col in columns])

            # Insert with explicit column names
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
            all_campaign_ids = set()
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
                            all_campaign_ids.add(transformed["CampaignID"])

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

            # Step 4: Delete existing data
            self.logger.info("Step 3: Deleting existing data...")
            if all_campaign_ids:
                self.delete_existing_data(report_date, list(all_campaign_ids))
            else:
                self.logger.warning("No CampaignIDs to delete, skipping delete")

            # Step 5: Load data into ClickHouse
            self.logger.info("Step 4: Loading data into ClickHouse...")
            if not self.insert_data(all_transformed_data):
                self.logger.error("Data insertion failed")
                return False

            self.logger.info(f"{'='*60}")
            self.logger.info(f"ETL Job Completed for report_date: {report_date}")
            self.logger.info(f"Total rows inserted: {len(all_transformed_data)}")
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
