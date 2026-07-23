"""
Integration tests for the full pipeline.
These require a running Databricks workspace
"""

import pytest


@pytest.mark.integration
class TestPipelineE2E:
    """End-to-end pipeline tests (require Databricks connection)."""

    @pytest.mark.skip(reason="Requires live Databricks workspace")
    def test_bronze_table_exists(self):
        """Verify Bronze tables was created by Autoloader."""

    @pytest.mark.skip(reason="Requires live Databricks workspace")
    def test_sliver_scd2_populated(self):
        """Verify Silver SCD2 table has history records."""

    @pytest.mark.skip(reason="Requires live Databricks workspace")
    def test_gold_daily_summary_populated(self):
        """Verify Gold daily summary has data after pipeline run."""

    @pytest.mark.skip(reason="Requires live Databricks workspace")
    def test_ml_model_registered(self):
        """Verify ML model is registered in MLflow."""
