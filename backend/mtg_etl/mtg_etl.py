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

    def reset_mtg_metrics(self, report_date: str) -> bool:
        """
        Reset MTG metrics to 0 for media that use MTG spend data.
        Only reset media specified in mtg_media_keywords config - other media keep CF spend.

        Args:
            report_date: Report date to reset

        Returns:
            bool: Operation success status
        """
        try:
            # Build WHERE clause with media keywords from config
            media_conditions = " OR ".join([f"Media LIKE '%{keyword}%'" for keyword in self.mtg_media_keywords])

            reset_sql = f"""
                ALTER TABLE {self.ch_database}.{self.ch_table}
                UPDATE spend = 0, m_imp = 0, m_clicks = 0, m_conv = 0
                WHERE reportDate = '{report_date}'
                AND ({media_conditions})
            """

            self.logger.info(f"Resetting MTG metrics for media: {self.mtg_media_keywords}")
            self.logger.debug(f"SQL: {reset_sql}")

            self.ch_client.command(reset_sql)
            self.logger.info("MTG metrics reset successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to reset MTG metrics: {str(e)}")
            return False

    def _get_cf_rows_for_update(self, report_date: str) -> Dict[str, Dict]:
        """
        Get ALL CF rows for the report date that need MTG updates.
        Returns a dict keyed by AdsetID with list of CF rows.

        This is much more efficient - one query instead of thousands!

        Args:
            report_date: Report date string

        Returns:
            Dict: {AdsetID: [list of CF rows with their data]}
        """
        try:
            query = f"""
                SELECT CampaignID, AdsetID, AdsID, offerID, impressions
                FROM {self.ch_database}.{self.ch_table}
                WHERE reportDate = '{report_date}'
                AND dataSource = 'Clickflare'
            """

            result = self.ch_client.query(query)

            # Group by AdsetID only (精准匹配)
            grouped = {}
            for row in result.named_results():
                key = row.get('AdsetID', '')
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append({
                    'CampaignID': row['CampaignID'],
                    'AdsetID': row.get('AdsetID', ''),
                    'AdsID': row.get('AdsID', ''),
                    'offerID': row.get('offerID', ''),
                    'impressions': row['impressions']
                })

            total_rows = sum(len(rows) for rows in grouped.values())
            self.logger.info(f"Loaded {total_rows} CF rows in {len(grouped)} AdsetID groups")

            return grouped
        except Exception as e:
            self.logger.error(f"Failed to query CF rows: {str(e)}")
            return {}

    def insert_data(self, data: List[Dict]) -> bool:
        """
        Update ClickHouse data with MTG metrics using BATCH approach.
        This is MUCH more efficient than row-by-row UPDATE.

        Logic:
        1. Load ALL CF rows once (grouped by AdsetID)
        2. For each MTG row, match by AdsetID (精准匹配)
        3. Distribute spend/metrics by impressions ratio

        Args:
            data: List of transformed MTG rows

        Returns:
            bool: Operation success status
        """
        if not data:
            self.logger.warning("No data to update")
            return True

        try:
            self.logger.info(f"Processing {len(data)} MTG rows with batch update approach")

            total_spend = sum(row.get('spend', 0) for row in data)
            total_m_imp = sum(row.get('m_imp', 0) for row in data)
            total_m_clicks = sum(row.get('m_clicks', 0) for row in data)
            total_m_conv = sum(row.get('m_conv', 0) for row in data)

            self.logger.info(f"MTG total - Spend: {total_spend:.2f}, Imp: {total_m_imp}, Clicks: {total_m_clicks}, Conv: {total_m_conv}")

            # Step 1: Load ALL CF rows at once (much more efficient!)
            self.logger.info("Loading CF data for batch update...")
            cf_groups = self._get_cf_rows_for_update(data[0]['reportDate'])

            if not cf_groups:
                self.logger.warning("No CF data found, cannot update")
                return True

            # Step 2: Accumulate updates in memory (key = unique CF row identifier)
            # Key format: CampaignID_AdsetID_AdsID_offerID
            updates_accumulator = {}
            skipped_count = 0

            for row in data:
                adset_id = row.get('AdsetID', '')

                mtg_spend = row.get('spend', 0)
                mtg_imp = row.get('m_imp', 0)
                mtg_clicks = row.get('m_clicks', 0)
                mtg_conv = row.get('m_conv', 0)

                # Find matching CF group by AdsetID only (精准匹配)
                if adset_id not in cf_groups:
                    skipped_count += 1
                    continue

                cf_rows = cf_groups[adset_id]

                # Calculate total impressions for distribution
                total_cf_impressions = sum(r['impressions'] for r in cf_rows)

                if total_cf_impressions == 0:
                    # Distribute evenly
                    for cf_row in cf_rows:
                        cf_key = f"{cf_row['CampaignID']}_{cf_row['AdsetID']}_{cf_row['AdsID']}_{cf_row['offerID']}"
                        if cf_key not in updates_accumulator:
                            updates_accumulator[cf_key] = {
                                'CampaignID': cf_row['CampaignID'],
                                'AdsetID': cf_row['AdsetID'],
                                'AdsID': cf_row['AdsID'],
                                'offerID': cf_row['offerID'],
                                'spend': 0,
                                'm_imp': 0,
                                'm_clicks': 0,
                                'm_conv': 0
                            }

                        count = len(cf_rows)
                        updates_accumulator[cf_key]['spend'] += mtg_spend / count
                        updates_accumulator[cf_key]['m_imp'] += mtg_imp / count
                        updates_accumulator[cf_key]['m_clicks'] += mtg_clicks / count
                        updates_accumulator[cf_key]['m_conv'] += mtg_conv / count
                else:
                    # Distribute by impressions ratio
                    for cf_row in cf_rows:
                        cf_key = f"{cf_row['CampaignID']}_{cf_row['AdsetID']}_{cf_row['AdsID']}_{cf_row['offerID']}"
                        if cf_key not in updates_accumulator:
                            updates_accumulator[cf_key] = {
                                'CampaignID': cf_row['CampaignID'],
                                'AdsetID': cf_row['AdsetID'],
                                'AdsID': cf_row['AdsID'],
                                'offerID': cf_row['offerID'],
                                'spend': 0,
                                'm_imp': 0,
                                'm_clicks': 0,
                                'm_conv': 0
                            }

                        ratio = cf_row['impressions'] / total_cf_impressions
                        updates_accumulator[cf_key]['spend'] += mtg_spend * ratio
                        updates_accumulator[cf_key]['m_imp'] += mtg_imp * ratio
                        updates_accumulator[cf_key]['m_clicks'] += mtg_clicks * ratio
                        updates_accumulator[cf_key]['m_conv'] += mtg_conv * ratio

            self.logger.debug(f"Calculated updates for {len(updates_accumulator)} unique CF rows, {skipped_count} MTG rows skipped (no match)")

            # Step 3: Batch UPDATE - each CF row updated only ONCE!
            self.logger.info("Executing batch UPDATE...")
            updated_count = 0

            for cf_key, update_data in updates_accumulator.items():
                if not self._update_single_row_batch(
                    data[0]['reportDate'],
                    update_data['CampaignID'],
                    update_data['AdsetID'],
                    update_data['AdsID'],
                    update_data['offerID'],
                    update_data['spend'],
                    update_data['m_imp'],
                    update_data['m_clicks'],
                    update_data['m_conv']
                ):
                    # Skip logging - some rows are expected to not match (按 imp 分配)
                    pass
                else:
                    updated_count += 1

            self.logger.info(f"Batch update completed: {updated_count} CF rows updated successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to update data: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _update_single_row_batch(self, report_date: str, campaign_id: str, adset_id: str, ads_id: str, offer_id: str,
                                 spend_add: float, imp_add: int, clicks_add: int, conv_add: int) -> bool:
        """
        Update a specific CF row with MTG metrics (optimized for batch processing).

        Args:
            report_date: Report date
            campaign_id: Campaign ID
            adset_id: Adset ID
            ads_id: Ads ID
            offer_id: Offer ID
            spend_add: Spend to add
            imp_add: Impressions to add
            clicks_add: Clicks to add
            conv_add: Conversions to add

        Returns:
            bool: Success status
        """
        try:
            # Build WHERE clause for precise matching
            where_clause = f"reportDate = '{report_date}' AND CampaignID = '{campaign_id}'"
            if adset_id and adset_id != '0':
                where_clause += f" AND AdsetID = '{adset_id}'"

            # Handle AdsID - may be empty
            if ads_id and ads_id != '0':
                where_clause += f" AND AdsID = '{ads_id}'"
            else:
                where_clause += " AND (AdsID = '' OR empty(AdsID))"

            # Handle offerID - may be empty
            if offer_id and offer_id != '0':
                where_clause += f" AND offerID = '{offer_id}'"
            else:
                where_clause += " AND (offerID = '' OR empty(offerID))"

            # Round to 2 decimals for spend to avoid precision issues
            spend_add = round(spend_add, 2)

            update_sql = f"ALTER TABLE {self.ch_database}.{self.ch_table} UPDATE "
            update_sql += f"spend = spend + {spend_add}, "
            update_sql += f"m_imp = m_imp + {imp_add}, "
            update_sql += f"m_clicks = m_clicks + {clicks_add}, "
            update_sql += f"m_conv = m_conv + {conv_add} "
            update_sql += f"WHERE {where_clause}"

            self.ch_client.command(update_sql)
            return True
        except Exception as e:
            # Data not found is expected for some MTG rows (按 imp 分配的情况)
            self.logger.debug(f"Update row failed (expected for some rows): {str(e)}")
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

            # Step 4: Reset MTG metrics to 0 (don't delete data, CF already inserted)
            self.logger.info("Step 3: Resetting MTG metrics...")
            self.reset_mtg_metrics(report_date)

            # Step 5: Load data into ClickHouse using batch approach
            self.logger.info("Step 4: Loading data into ClickHouse (batch mode)...")
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
