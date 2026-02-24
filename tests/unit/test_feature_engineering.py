"""Unit tests for feature engineering."""
import pytest 
import pandas as pd
import numpy as np
from chi311.features.feature_engineering import (
    add_temporal_features,
    add_lag_features,
    add_rolling_features,
    prepare_features,
)

@pytest.fixture
def sample_daily_data():
    """Create sample daily data for testing."""
    dates = pd.date_range("2024-01-01", period=60, freq="D")
    np.random.seed(42)
    values = np.random.normal(3000, 500, 60).clip(1500, 500)
    return pd.DataFrame({"ds": dates, "y": values})


class TestTemporalFeatures:
    def test_adds_day_of_week(self, sample_daiy_data):
        result = add_temporal_features(sample_daily_data)
        assert "day_of_week" in result.columns
        assert result["day_of_week"].between(0, 6).all()
    
    def test_adds_is_weekend(self, sample_daily_data):
        result = add_temporal_features(sample_daily_data)
        assert "is_weekend" in result.columns
        assert set(result["is_weekend"].unique()).issubset({0, 1})

    def test_does_not_modify_original(self, sample_daily_data):
        original_cols = set(sample_daily_data.columns)
        add_temporal_features(sample_daily_data)
        assert set(sample_daily_data.columns) == original_cols


class TestLagFeatures:
    def test_default_lags(self, sample_daily_data):
        result = add_lag_features(sample_daily_data)
        for lag in [1, 7, 14, 28]:
            assert f"lag_{lag}" in result.columns

    def test_custom_lags(self, sample_daily_data):
        result = add_lag_features(sample_daily_data, lags=[3, 5])
        assert "lag_3" in result.columns
        assert "lag_5" in result.columns
        assert "lag_1" not in result.columns



class TestRollingFeatures:
    def test_default_windows(self, sample_daily_data):
        result = add_rolling_features(sample_daily_data)
        for w in [7, 14, 30]:
            assert f"rolling_mean_{w}d" in result.columns
            assert f"rolling_std_{w}d" in result.columns


class TestPrepareFeatures:
    def test_full_pipeline(self, sample_daily_data):
        result = prepare_features(sample_daily_data)
        assert "day_of_week" in result.columns
        assert "lag_7" in result.columns
        assert "rolling_mean_30d" in result.columns
        assert len(result) == len(sample_daily_data)
