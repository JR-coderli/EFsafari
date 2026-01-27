"""
Clickflare Offers API Client
用于拉取 Offer 详情数据
"""
import time
import requests
from typing import Dict, List, Optional, Tuple


class ClickflareOffersAPI:
    """
    Clickflare Offers API Client
    GET https://public-api.clickflare.io/api/offers
    """

    def __init__(self, config: Dict):
        """
        Initialize Clickflare Offers API client

        Args:
            config: Configuration dictionary
        """
        self.base_url = config["api"]["base_url"]
        self.api_key = config["api"]["api_key"]

        # Retry configuration
        self.max_attempts = config["retry"]["max_attempts"]
        self.backoff_factor = config["retry"]["backoff_factor"]
        self.retry_status_codes = config["retry"]["retry_status_codes"]

        # Create session with api-key header
        self.session = requests.Session()
        self.session.headers.update({
            "api-key": self.api_key,
            "Accept": "application/json"
        })

    def _do_request_with_retry(self, url: str, params: Dict = None) -> Optional[requests.Response]:
        """
        Make GET request with exponential backoff retry

        Args:
            url: Full URL for the request
            params: Query parameters

        Returns:
            Response object or None if all retries failed
        """
        last_error = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.get(url, params=params, timeout=60)

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

    def fetch_offers(
        self,
        page: int = 1,
        page_size: int = 1000,
        search: Optional[str] = None,
        fields: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """
        Fetch offers data from Clickflare API

        Args:
            page: Page number (1-based)
            page_size: Number of records per page
            search: Search keyword
            fields: Optional list of fields to return

        Returns:
            Dictionary with offers list or None on error
        """
        url = f"{self.base_url}/api/offers"

        params = {
            "page": page,
            "pageSize": page_size
        }

        if search:
            params["search"] = search

        if fields:
            for field in fields:
                params["fields[]"] = field

        response = self._do_request_with_retry(url, params)

        if response is None:
            return None

        if response.status_code == 200:
            # API returns array directly
            return {"data": response.json()}

        return {
            "error": True,
            "statusCode": response.status_code,
            "message": response.text
        }

    def fetch_all_offers(
        self,
        page_size: int = 1000,
        max_pages: int = 100
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Fetch all offers

        Args:
            page_size: Number of records per page
            max_pages: Maximum number of pages to fetch

        Returns:
            Tuple of (list of all offers, error message)
        """
        all_offers = []

        for page in range(1, max_pages + 1):
            result = self.fetch_offers(
                page=page,
                page_size=page_size
            )

            if result is None:
                return all_offers, f"Failed to fetch page {page}"

            if result.get("error"):
                return all_offers, f"API Error: {result.get('message')}"

            offers = result.get("data", [])

            if not offers:
                # No more data
                break

            all_offers.extend(offers)

            # Check if this might be the last page
            if len(offers) < page_size:
                break

        return all_offers, None

    def test_connection(self) -> Tuple[bool, str]:
        """
        Test API connection with a minimal request

        Returns:
            Tuple of (success, message)
        """
        result = self.fetch_offers(
            page=1,
            page_size=1
        )

        if result is None:
            return False, "Connection failed: No response"

        if result.get("error"):
            return False, f"API Error: {result.get('message')}"

        return True, "Connection successful"
