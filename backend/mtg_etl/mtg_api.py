"""
MTG Report API Client
Handles API requests with retry logic and status polling
"""
import time
import hashlib
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime


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
        """
        Initialize API client

        Args:
            config: Dict with API configuration
            logger: ETLLogger instance
        """
        self.base_url = config["base_url"]
        self.endpoint = config["endpoint"]
        self.access_key = config["access_key"]
        self.api_key = config["api_key"]
        self.timezone = config.get("timezone", "+8")
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
        """
        Generate token using MD5(API_KEY + MD5(timestamp))

        According to MTG docs: Md5(API key.md5(timestamp))

        Args:
            timestamp: Current timestamp string

        Returns:
            str: MD5 hashed token
        """
        ts_md5 = hashlib.md5(timestamp.encode("utf-8")).hexdigest()
        raw = self.api_key + ts_md5
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _get_headers(self) -> Dict[str, str]:
        """
        Generate request headers with Token

        Returns:
            Dict with request headers
        """
        timestamp = str(int(time.time()))
        token = self._generate_token(timestamp)

        return {
            "access-key": self.access_key,
            "Token": token,
            "Timestamp": timestamp
        }

    def _build_url(self, params: Dict) -> str:
        """
        Build full URL with query parameters

        Args:
            params: Query parameters

        Returns:
            str: Full URL
        """
        return f"{self.base_url}{self.endpoint}"

    def _do_request_with_retry(self, url: str, params: Dict) -> requests.Response:
        """
        Make HTTP request with retry logic

        Args:
            url: Request URL
            params: Query parameters

        Returns:
            requests.Response: API response

        Raises:
            Exception: If all retry attempts fail
        """
        last_error = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                headers = self._get_headers()
                response = self.session.get(url, params=params, headers=headers, timeout=60)

                if response.status_code == 200:
                    return response

                # Log retry for non-200 status
                self.logger.warning(
                    f"API request returned status {response.status_code}, "
                    f"attempt {attempt}/{self.max_attempts}"
                )

            except requests.exceptions.RequestException as e:
                last_error = e
                self.logger.warning(
                    f"API request failed: {str(e)}, "
                    f"attempt {attempt}/{self.max_attempts}"
                )

            # Exponential backoff
            if attempt < self.max_attempts:
                wait_time = self.backoff_factor ** attempt
                self.logger.info(f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)

        raise Exception(f"API request failed after {self.max_attempts} attempts. Last error: {last_error}")

    def initiate_report(self, start_date: str, end_date: str) -> Tuple[bool, Dict]:
        """
        Step 1: Initiate report generation (type=1)

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Tuple[bool, Dict]: (success, response_data)
        """
        url = self._build_url({})
        params = {
            "start_time": start_date,
            "end_time": end_date,
            "type": 1,
            "timezone": self.timezone,
            "dimension_option": self.dimension_option,
            "time_granularity": self.time_granularity
        }

        self.logger.info(f"Initiating report: {start_date} to {end_date}")
        self.logger.debug(f"Request params: {params}")

        try:
            response = self._do_request_with_retry(url, params)
            data = response.json()

            code = data.get("code")
            msg = data.get("msg", "")

            self.logger.info(f"Response code: {code}, message: {msg}")

            if code == self.CODE_SUCCESS:
                report_info = data.get("data", {})
                self.logger.info(
                    f"Report ready: hours={report_info.get('hours')}, "
                    f"is_complete={report_info.get('is_complete')}"
                )
                return True, data

            elif code in [self.CODE_RECEIVED, self.CODE_GENERATING]:
                self.logger.info("Report is being generated, polling required...")
                return True, data

            else:
                self.logger.error(f"Unexpected response code: {code}, message: {msg}")
                self.logger.error(f"Full response: {data}")
                return False, data

        except Exception as e:
            self.logger.error(f"Failed to initiate report: {str(e)}")
            return False, {"error": str(e)}

    def poll_report_status(self, start_date: str, end_date: str) -> Tuple[bool, Dict]:
        """
        Poll report generation status until complete

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Tuple[bool, Dict]: (success, response_data)
        """
        url = self._build_url({})
        params = {
            "start_time": start_date,
            "end_time": end_date,
            "type": 1,
            "timezone": self.timezone,
            "dimension_option": self.dimension_option,
            "time_granularity": self.time_granularity
        }

        start_time = time.time()

        for attempt in range(1, self.poll_max_attempts + 1):
            elapsed = time.time() - start_time
            if elapsed > self.poll_timeout:
                self.logger.error(f"Poll timeout after {elapsed:.0f}s")
                return False, {"error": "Poll timeout"}

            try:
                headers = self._get_headers()
                response = self.session.get(url, params=params, headers=headers, timeout=30)
                data = response.json()

                code = data.get("code")

                if code == self.CODE_SUCCESS:
                    self.logger.info(f"Report generation complete (attempt {attempt})")
                    return True, data

                elif code in [self.CODE_RECEIVED, self.CODE_GENERATING]:
                    wait_hours = data.get("data", {}).get("hours", 0)
                    self.logger.info(
                        f"Polling... attempt {attempt}/{self.poll_max_attempts}, "
                        f"hours available: {wait_hours}"
                    )
                    time.sleep(self.poll_interval)

                else:
                    self.logger.error(f"Error during polling: code={code}, msg={data.get('msg')}")
                    return False, data

            except Exception as e:
                self.logger.warning(f"Poll attempt {attempt} failed: {str(e)}")
                if attempt < self.poll_max_attempts:
                    time.sleep(self.poll_interval)

        self.logger.error(f"Poll max attempts ({self.poll_max_attempts}) reached")
        return False, {"error": "Max poll attempts reached"}

    def download_report(self, start_date: str, end_date: str) -> Tuple[bool, str]:
        """
        Step 2: Download report data (type=2)

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Tuple[bool, str]: (success, tsv_content or error_message)
        """
        url = self._build_url({})
        params = {
            "start_time": start_date,
            "end_time": end_date,
            "type": 2,
            "timezone": self.timezone,
            "dimension_option": self.dimension_option,
            "time_granularity": self.time_granularity
        }

        self.logger.info(f"Downloading report data...")

        try:
            headers = self._get_headers()
            response = self.session.get(url, params=params, headers=headers, timeout=120)

            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "")
                if "octet-stream" in content_type or "text" in content_type:
                    # Raw TSV data
                    content = response.content.decode("utf-8")
                    self.logger.info(f"Downloaded {len(content)} bytes of data")
                    return True, content
                else:
                    # Might be JSON error response
                    try:
                        data = response.json()
                        code = data.get("code")
                        msg = data.get("msg", "Unknown error")
                        self.logger.error(f"Download error: code={code}, msg={msg}")
                        return False, f"API error: {msg}"
                    except:
                        content = response.content.decode("utf-8")
                        return True, content
            else:
                self.logger.error(f"Download failed with status {response.status_code}")
                try:
                    data = response.json()
                    return False, data.get("msg", f"HTTP {response.status_code}")
                except:
                    return False, f"HTTP {response.status_code}"

        except Exception as e:
            self.logger.error(f"Failed to download report: {str(e)}")
            return False, str(e)

    def get_report_data(self, report_date: str) -> Tuple[bool, str]:
        """
        Full workflow: initiate -> poll -> download

        Args:
            report_date: Report date in YYYY-MM-DD format

        Returns:
            Tuple[bool, str]: (success, tsv_content or error_message)
        """
        # Step 1: Initiate
        success, data = self.initiate_report(report_date, report_date)
        if not success:
            return False, f"Initiate failed: {data.get('error', data.get('msg', 'Unknown'))}"

        code = data.get("code")

        # Step 2: Poll if needed
        if code != self.CODE_SUCCESS:
            success, data = self.poll_report_status(report_date, report_date)
            if not success:
                return False, f"Poll failed: {data.get('error', 'Unknown error')}"

        # Step 3: Download
        success, content = self.download_report(report_date, report_date)
        return success, content

    def parse_tsv_data(self, tsv_content: str) -> List[Dict]:
        """
        Parse TSV content into list of dictionaries

        Args:
            tsv_content: Raw TSV string content

        Returns:
            List[Dict]: Parsed data rows
        """
        self.logger.debug(f"Raw TSV content (first 500 chars): {tsv_content[:500]}")

        lines = tsv_content.strip().split("\n")
        if len(lines) < 2:
            self.logger.warning(f"No data rows found in TSV content. Lines: {lines}")
            return []

        # Parse header
        headers = [h.strip() for h in lines[0].split("\t")]
        self.logger.debug(f"TSV headers: {headers}")

        # Parse data rows
        rows = []
        for line in lines[1:]:
            if not line.strip():
                continue
            values = line.split("\t")
            row = {}
            for i, header in enumerate(headers):
                if i < len(values):
                    row[header] = values[i].strip()
                else:
                    row[header] = ""
            rows.append(row)

        self.logger.info(f"Parsed {len(rows)} data rows from TSV")
        return rows

    def get_parsed_data(self, report_date: str) -> Tuple[bool, List[Dict]]:
        """
        Get and parse report data

        Args:
            report_date: Report date in YYYY-MM-DD format

        Returns:
            Tuple[bool, List[Dict]]: (success, parsed_data_rows)
        """
        success, content = self.get_report_data(report_date)
        if not success:
            return False, []

        data = self.parse_tsv_data(content)
        return True, data
