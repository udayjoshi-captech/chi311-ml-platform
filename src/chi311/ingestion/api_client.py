"""
Chicago 311 API Client - Robust Socrata API client with pagination and retry.
"""
import requests
import time
import logging
from dataclasses import dataclass
from typing import Optional

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
        self.sessions = requests.Session()
        if self.config.app_token:
            self.session.headers.update({"X-App-Token": self.config.app_token})
    
    def fetch_records(
            self,
            start_date: str,
            end_date: str,
            offset: int = 0
    ) -> list[dict]:
        """Fetch a page of 311 records for a date range"""
        params = {
            "$where": f"created_date >= '{start_date}' AND created_date < '{end_date}'",
            "$limit": self.config.page_size,
            "$offset": offset,
            "$order": "created_date ASC"
        }

        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(
                    self.config.base_url,
                    params=params,
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                records = response.json()
                logger.info(f"Fetched{len(records)} records (offset={offset})")
                return records
            
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{self.config.max_retries} failed: {e}")
                if attempt < self.config.max_retries - 1:
                    wait = self.config.retry_delay * (2 ** attempt)
                    logger.info(f"Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise
        
        return []
    
    def fetch_all(self, start_date: str, end_date: str) -> list(dict):
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
        """Verify API is reachable"""
        try:
            response = self.session.get(
                self.config.base_url,
                params={"$limit": 1},
                timeout=10
            )
            return response.status_code == 200
        except Exception:
            return False