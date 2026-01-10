"""
Clickflare API Client
"""
import time
import requests
from typing import Dict, List, Optional, Tuple


class ClickflareAPI:
    """
    Clickflare Report API Client
    """

    def __init__(self, config: Dict):
        """
        Initialize Clickflare API client

        Args:
            config: Configuration dictionary
        """
        self.base_url = config["api"]["base_url"]
        self.endpoint = config["api"]["endpoint"]
        self.api_key = config["api"]["api_key"]
        self.timezone = config["api"].get("timezone", "UTC")

        # Retry configuration
        self.max_attempts = config["retry"]["max_attempts"]
        self.backoff_factor = config["retry"]["backoff_factor"]
        self.retry_status_codes = config["retry"]["retry_status_codes"]

        # Create session with api-key header
        self.session = requests.Session()
        self.session.headers.update({
            "api-key": self.api_key,
            "Content-Type": "application/json"
        })

    def _do_request_with_retry(self, url: str, data: Dict) -> Optional[requests.Response]:
        """
        Make POST request with exponential backoff retry

        Args:
            url: Full URL for the request
            data: Request body as dictionary

        Returns:
            Response object or None if all retries failed
        """
        last_error = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.post(url, json=data, timeout=60)

                # Success
                if response.status_code == 200:
                    return response

                # Check if we should retry
                if response.status_code in self.retry_status_codes:
                    last_error = f"HTTP {response.status_code}: {response.text}"
                    if attempt < self.max_attempts:
                        wait_time = self.backoff_factor ** attempt
                        time.sleep(wait_time)
                        continue

                # Non-retryable error
                return response

            except requests.exceptions.Timeout:
                last_error = "Request timeout"
                if attempt < self.max_attempts:
                    time.sleep(self.backoff_factor ** attempt)
                    continue

            except requests.exceptions.RequestException as e:
                last_error = str(e)
                if attempt < self.max_attempts:
                    time.sleep(self.backoff_factor ** attempt)
                    continue

        return None

    def fetch_report(
        self,
        start_date: str,
        end_date: str,
        group_by: List[str],
        metrics: List[str],
        page: int = 1,
        page_size: int = 1000,
        sort_by: Optional[str] = None,
        order_type: str = "desc",
        include_all: bool = False
    ) -> Optional[Dict]:
        """
        Fetch report data from Clickflare API

        Args:
            start_date: Start date in format "YYYY-MM-DD HH:mm:ss"
            end_date: End date in format "YYYY-MM-DD HH:mm:ss"
            group_by: List of dimensions to group by
            metrics: List of metrics (with cm_ prefix)
            page: Page number (1-based)
            page_size: Number of records per page
            sort_by: Field to sort by (must be in metrics)
            order_type: Sort order "asc" or "desc"
            include_all: True to include all data, False for stats only

        Returns:
            Dictionary with "items" and "totals" or None on error
        """
        url = f"{self.base_url}{self.endpoint}"

        # Use first metric as default sort field
        if sort_by is None:
            sort_by = metrics[0]

        payload = {
            "startDate": start_date,
            "endDate": end_date,
            "groupBy": group_by,
            "metrics": metrics,
            "timezone": self.timezone,
            "sortBy": sort_by,
            "orderType": order_type,
            "includeAll": include_all,
            "page": page,
            "pageSize": page_size
        }

        response = self._do_request_with_retry(url, payload)

        if response is None:
            return None

        if response.status_code == 200:
            return response.json()

        return {
            "error": True,
            "statusCode": response.status_code,
            "message": response.text
        }

    def fetch_all_pages(
        self,
        start_date: str,
        end_date: str,
        group_by: List[str],
        metrics: List[str],
        page_size: int = 1000,
        max_pages: int = 100,
        sort_by: Optional[str] = None
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Fetch all pages of report data

        Args:
            start_date: Start date in format "YYYY-MM-DD HH:mm:ss"
            end_date: End date in format "YYYY-MM-DD HH:mm:ss"
            group_by: List of dimensions to group by
            metrics: List of metrics (with cm_ prefix)
            page_size: Number of records per page
            max_pages: Maximum number of pages to fetch
            sort_by: Field to sort by

        Returns:
            Tuple of (list of all items, error message)
        """
        all_items = []

        for page in range(1, max_pages + 1):
            result = self.fetch_report(
                start_date=start_date,
                end_date=end_date,
                group_by=group_by,
                metrics=metrics,
                page=page,
                page_size=page_size,
                sort_by=sort_by
            )

            if result is None:
                return all_items, f"Failed to fetch page {page}"

            if result.get("error"):
                return all_items, f"API Error: {result.get('message')}"

            items = result.get("items", [])
            if not items:
                # No more data
                break

            all_items.extend(items)

            # Check if this might be the last page
            if len(items) < page_size:
                break

        return all_items, None

    def test_connection(self) -> Tuple[bool, str]:
        """
        Test API connection with a minimal request

        Returns:
            Tuple of (success, message)
        """
        result = self.fetch_report(
            start_date="2026-01-09 00:00:00",
            end_date="2026-01-09 23:59:59",
            group_by=["date"],
            metrics=["cm_uniqueVisits"],
            page=1,
            page_size=1
        )

        if result is None:
            return False, "Connection failed: No response"

        if result.get("error"):
            return False, f"API Error: {result.get('message')}"

        return True, "Connection successful"
