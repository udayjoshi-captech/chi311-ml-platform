"""
Chicago 311 API Client - Robust Socrata API client with pagination and retry.
"""

import requests
import time
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """Configuration for Chicago 311 Socrata API."""

    base_url: str = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"
    app_token: Optional[str] = None
    page_size: int = 50000
    max_retries: int = 3
    retry_delay: float = 2.0
    timeout: int = 60


class Chi311APIClient:
    """Client for Chicago 311 Open Data API."""

    def __init__(self, config: Optional[APIConfig] = None):
        self.config = config or APIConfig()
        self.session = requests.Session()
        self._closed = False
        if self.config.app_token:
            self.session.headers.update({"X-App-Token": self.config.app_token})

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures session is closed."""
        self.close()
        return False  # Don't suppress exceptions

    def close(self):
        """Close the session and release resources."""
        if not self._closed:
            self.session.close()
            self._closed = True
            logger.debug("Chi311APIClient session closed")

    def __del__(self):
        """Destructor - close session if not already closed."""
        self.close()

    def fetch_records(
        self, start_date: str, end_date: str, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch a page of 311 records for a date range.

        Args:
            start_date: ISO format datetime (e.g., "2024-01-01T00:00:00")
            end_date: ISO format datetime (e.g., "2024-01-31T23:59:59")
            offset: Starting record offset for pagination (default: 0)

        Returns:
            List of record dictionaries from the API

        Raises:
            ValueError: If JSON response is malformed
            requests.exceptions.RequestException: For network/API errors
        """
        params = {
            "$where": f"created_date >= '{start_date}' AND created_date < '{end_date}'",
            "$limit": self.config.page_size,
            "$offset": offset,
            "$order": "created_date ASC",
        }

        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(
                    self.config.base_url, params=params, timeout=self.config.timeout
                )
                response.raise_for_status()

                try:
                    records = response.json()
                except (requests.exceptions.JSONDecodeError, ValueError) as e:
                    logger.error(
                        "Invalid JSON response from API: %s. "
                        "Response status: %d, Content-Type: %s",
                        e,
                        response.status_code,
                        response.headers.get("Content-Type", "unknown")
                    )
                    raise ValueError(f"API returned non-JSON response: {e}") from e

                logger.info(f"Fetched {len(records)} records (offset={offset})")
                return records

            except requests.exceptions.HTTPError as e:
                # Don't retry client errors (4xx) - they won't resolve with retries
                if e.response is not None and 400 <= e.response.status_code < 500:
                    logger.error(
                        "HTTP client error %d: %s. URL: %s, Params: %s",
                        e.response.status_code,
                        e,
                        self.config.base_url,
                        params
                    )
                    raise  # Don't retry 4xx errors
                # Fall through to retry logic for 5xx errors
                logger.warning(
                    "Attempt %d/%d failed with server error: %s",
                    attempt + 1,
                    self.config.max_retries,
                    e
                )
                if attempt < self.config.max_retries - 1:
                    wait = self.config.retry_delay * (2**attempt)
                    logger.info(f"Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                logger.warning(
                    "Attempt %d/%d failed: %s. Retrying in %d seconds...",
                    attempt + 1,
                    self.config.max_retries,
                    e,
                    int(self.config.retry_delay * (2**attempt)) if attempt < self.config.max_retries - 1 else 0
                )
                if attempt < self.config.max_retries - 1:
                    wait = self.config.retry_delay * (2**attempt)
                    time.sleep(wait)
                else:
                    raise

        return []

    def fetch_all(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Fetch all records for a date range with pagination"""
        all_records = []
        offset = 0

        while True:
            batch = self.fetch_records(start_date, end_date, offset)
            if not batch:
                break
            all_records.extend(batch)
            logger.info(f"Total fetched: {len(all_records)}")

            if len(batch) < self.config.page_size:
                break
            offset += self.config.page_size

        logger.info(f"Completed: {len(all_records)} total records")
        return all_records

    def health_check(self) -> bool:
        """Check if the API is reachable and responding.

        Returns:
            True if API responds with 200, False otherwise
        """
        try:
            response = self.session.get(
                self.config.base_url,
                params={"$limit": 1},
                timeout=self.config.timeout  # Use config timeout
            )
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.warning("Health check failed: %s", e)
            return False
