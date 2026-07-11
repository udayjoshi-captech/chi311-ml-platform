"""Unit tests for Chi311 API Client."""

import pytest
import requests
from unittest.mock import patch, MagicMock
from chi311.ingestion.api_client import Chi311APIClient
from chi311.config import APIConfig


class TestChi311APIClient:
    """Tests for Chicago 311 API client."""

    def setup_method(self):
        self.config = APIConfig(
            page_size=100, max_retries=2, retry_delay=0.1, timeout=5
        )
        self.client = Chi311APIClient(self.config)

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_fetch_records_success(self, mock_get):
        """Test successful record fetch"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"sr_number": "SR001", "status": "Open"},
            {"sr_number": "SR002", "status": "Completed"},
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        records = self.client.fetch_records("2024-01-01", "2024-01-02")
        assert len(records) == 2
        assert records[0]["sr_number"] == "SR001"

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_fetch_records_empty(self, mock_get):
        """Test empty response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        records = self.client.fetch_records("2024-01-01", "2024-01-02")
        assert len(records) == 0

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_fetch_all_pagination(self, mock_get):
        """Test pagination across multiple pages."""
        page1 = [{"sr_number": "SR{i:03d}"} for i in range(100)]
        page2 = [{"sr_number": "SR{i:03d}"} for i in range(100, 150)]

        mock_response1 = MagicMock()
        mock_response1.json.return_value = page1
        mock_response1.raise_for_status = MagicMock()

        mock_response2 = MagicMock()
        mock_response2.json.return_value = page2
        mock_response2.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_response1, mock_response2]

        records = self.client.fetch_all("2024-01-01", "2024-01-02")
        assert len(records) == 150

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_health_check_success(self, mock_get):
        """Test successful health check."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        assert self.client.health_check() is True

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_health_check_failure(self, mock_get):
        """Test failed health check."""
        mock_get.side_effect = Exception("Connection refused")
        assert self.client.health_check() is False

    def test_config_with_app(self):
        """Test client initialization with app token."""
        config = APIConfig(app_token="test_token_123")
        client = Chi311APIClient(config)
        assert "X-App-Token" in client.session.headers

    def test_default_config(self):
        """Test default configuration values."""
        config = APIConfig()
        assert config.page_size == 50000
        assert config.max_retries == 3
        assert config.timeout == 60

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_fetch_records_handles_json_decode_error(self, mock_get):
        """Should raise ValueError for non-JSON response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.headers = {"Content-Type": "text/html"}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="API returned non-JSON response"):
            self.client.fetch_records("2024-01-01T00:00:00", "2024-01-02T00:00:00")

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_fetch_records_does_not_retry_4xx_errors(self, mock_get):
        """Should not retry client errors (4xx)."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_error = requests.exceptions.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = mock_error
        mock_get.return_value = mock_response

        # Should raise immediately without retries
        with pytest.raises(requests.exceptions.HTTPError):
            self.client.fetch_records("2024-01-01T00:00:00", "2024-01-02T00:00:00")

        # Should only call once (no retries for 4xx)
        assert mock_get.call_count == 1

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_fetch_records_retries_5xx_errors(self, mock_get):
        """Should retry server errors (5xx) with exponential backoff."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_error = requests.exceptions.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = mock_error
        mock_get.return_value = mock_response

        # Should retry max_retries times
        with pytest.raises(requests.exceptions.HTTPError):
            self.client.fetch_records("2024-01-01T00:00:00", "2024-01-02T00:00:00")

        # Should call max_retries times (2 in test config)
        assert mock_get.call_count == 2

    def test_context_manager_closes_session(self):
        """Should close session when used as context manager."""
        config = APIConfig()
        with Chi311APIClient(config) as client:
            assert not client._closed

        assert client._closed  # Should be closed after exiting context

    def test_close_method_closes_session(self):
        """Should close session when close() is called."""
        client = Chi311APIClient(self.config)
        assert not client._closed

        client.close()

        assert client._closed

    def test_close_is_idempotent(self):
        """Should handle multiple calls to close() safely."""
        client = Chi311APIClient(self.config)

        client.close()
        client.close()  # Should not raise error

        assert client._closed

    @patch("chi311.ingestion.api_client.requests.Session.get")
    def test_health_check_uses_config_timeout(self, mock_get):
        """Should use configured timeout for health check."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        self.client.health_check()

        # Check that timeout was passed from config
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["timeout"] == self.config.timeout
