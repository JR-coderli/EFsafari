"""
Clickflare Report ETL Main Process
Extracts data from Clickflare API, transforms, and loads into ClickHouse
"""
import os
import sys
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import clickhouse_connect

from cf_api import ClickflareAPI
from logger import ETLLogger


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
        "Campaign": "trackingField2",
        "CampaignID": "trackingField2",
        "Adset": "trackingField6",
        "AdsetID": "trackingField5",
        "Ads": "trackingField1",
        "AdsID": "trackingField1",
    }

    # Metrics mapping
    METRICS_MAPPING = {
        "impressions": "uniqueVisits",
        "clicks": "uniqueClicks",
        "conversions": "conversions",
        "revenue": "revenue"
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

        self.ch_client = None
        self.api_client = None

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

            # Get tracking field values
            tracking_field_1 = raw_row.get("trackingField1", "")
            tracking_field_2 = raw_row.get("trackingField2", "")
            tracking_field_5 = raw_row.get("trackingField5", "")
            tracking_field_6 = raw_row.get("trackingField6", "")

            # Get metrics
            impressions = self._safe_int(raw_row.get("uniqueVisits"))
            clicks = self._safe_int(raw_row.get("uniqueClicks"))
            conversions = self._safe_int(raw_row.get("conversions"))
            revenue = self._safe_float(raw_row.get("revenue"))

            transformed = {
                "reportDate": date_obj,
                "dataSource": "Clickflare",  # 数据来源标识
                "Media": traffic_source_name if traffic_source_name else self.media_source,
                "MediaID": traffic_source_id if traffic_source_id else "",
                "offer": offer_name if offer_name else "",
                "offerID": offer_id if offer_id else "",
                "advertiser": affiliate_network_name if affiliate_network_name else "",
                "advertiserID": affiliate_network_id if affiliate_network_id else "",
                "Campaign": tracking_field_2 if tracking_field_2 else "",
                "CampaignID": tracking_field_2 if tracking_field_2 else "",
                "Adset": tracking_field_6 if tracking_field_6 else "",
                "AdsetID": tracking_field_5 if tracking_field_5 else "",
                "Ads": tracking_field_1 if tracking_field_1 else "",
                "AdsID": tracking_field_1 if tracking_field_1 else "",
                # CF metrics
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "revenue": revenue,
                # Media metrics
                "spend": revenue,  # Use revenue as spend (tracker perspective)
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
        Delete existing data for the report date and media source

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
                AND Media = '{self.media_source}'
            """

            self.logger.info(f"Deleting existing data for {report_date}, Media={self.media_source}")
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
                       'advertiser', 'advertiserID', 'Campaign', 'CampaignID',
                       'Adset', 'AdsetID', 'Ads', 'AdsID',
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

    def run(self, report_date: Optional[str] = None) -> bool:
        """
        Run the complete ETL process

        Args:
            report_date: Optional report date (YYYY-MM-DD).
                        If not provided, calculated from config offset.

        Returns:
            bool: ETL success status
        """
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

            # Step 3: Extract data
            self.logger.info("Step 3: Extracting data from Clickflare API...")
            success, raw_data = self.extract_data(report_date)

            if not success:
                self.logger.log_etl_failed(report_date, "Data extraction failed")
                return False

            if not raw_data:
                self.logger.info("No data to process")
                self.logger.log_etl_complete(report_date, 0)
                return True

            # Step 4: Transform data
            self.logger.info("Step 4: Transforming data...")
            transformed_data = self.transform_data(raw_data)

            if not transformed_data:
                self.logger.warning("No valid data after transformation")
                self.logger.log_etl_complete(report_date, 0)
                return True

            # Step 5: Delete existing data
            self.logger.info("Step 5: Deleting existing data...")
            self.delete_existing_data(report_date)

            # Step 6: Load data into ClickHouse
            self.logger.info("Step 6: Loading data into ClickHouse...")
            if not self.insert_data(transformed_data):
                self.logger.log_etl_failed(report_date, "Data insertion failed")
                return False

            self.logger.log_etl_complete(report_date, len(transformed_data))
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
