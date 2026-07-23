"""Shared pytest fixtures for all tests."""
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_timeseries():
    """Create a standard time series for testing (60 days)."""
    np.random.seed(42)
    dates = pd.date_range(start="2024-01-01", periods=60, freq="D")
    return pd.DataFrame({
        "ds": dates,
        "y": np.random.randint(2000, 5000, size=len(dates))
    })


@pytest.fixture
def mock_spark_session():
    """Create a mock SparkSession for Delta table tests."""
    spark = MagicMock()
    spark.catalog.currentDatabase.return_value = "chi311"
    spark.catalog.databaseExists.return_value = True
    spark.catalog.tableExists.return_value = False
    spark.createDataFrame.return_value = MagicMock()

    return spark


@pytest.fixture
def mock_mlflow():
    """Mock MLflow tracking for testing."""
    from unittest.mock import patch

    with patch("mlflow.set_experiment"), \
         patch("mlflow.start_run"), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metrics"), \
         patch("mlflow.prophet.log_model"):
        yield
