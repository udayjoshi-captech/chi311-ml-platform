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
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    np.random.seed(42)
    values = np.random.normal(3000, 500, 60).clip(1500, 5000)
    return pd.DataFrame({"ds": dates, "y": values})


class TestTemporalFeatures:
    def test_adds_day_of_week(self, sample_daily_data):
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

    def test_temporal_features_raises_on_empty_dataframe(self):
        """Should raise ValueError for empty DataFrame."""
        df = pd.DataFrame()

        with pytest.raises(ValueError, match="Cannot add temporal features to empty DataFrame"):
            add_temporal_features(df)

    def test_temporal_features_raises_on_missing_ds_column(self):
        """Should raise ValueError if 'ds' column is missing."""
        df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=10, freq="D")})

        with pytest.raises(ValueError, match="Missing required column: 'ds'"):
            add_temporal_features(df)

    def test_temporal_features_raises_on_non_datetime_ds(self):
        """Should raise TypeError if 'ds' is not datetime type."""
        df = pd.DataFrame({
            "ds": ["2024-01-01"] * 10
        })

        with pytest.raises(TypeError, match="Column 'ds' must be datetime type"):
            add_temporal_features(df)


class TestLagFeatures:
    def test_default_lags(self, sample_daily_data):
        result = add_lag_features(sample_daily_data)
        for lag in [1, 7, 14, 28]:
            assert f"lag_{lag}" in result.columns

    def test_custom_lags(self, sample_daily_data):
        result = add_lag_features(sample_daily_data, lags=[3, 7])
        assert "lag_3" in result.columns
        assert "lag_7" in result.columns
        assert "lag_1" not in result.columns

    def test_lag_features_calculate_correctly(self, sample_daily_data):
        """Lag features should contain correct shifted values."""
        result = add_lag_features(sample_daily_data, lags=[3, 7])

        # Validate columns exist
        assert "lag_3" in result.columns
        assert "lag_7" in result.columns

        # Validate values are correctly shifted
        # lag_3 at index 3 should equal y at index 0
        assert result.loc[3, "lag_3"] == result.loc[0, "y"]
        # lag_7 at index 7 should equal y at index 0
        assert result.loc[7, "lag_7"] == result.loc[0, "y"]

        # First 3 rows of lag_3 should be NaN
        assert result["lag_3"].iloc[:3].isna().all()
        # First 7 rows of lag_7 should be NaN
        assert result["lag_7"].iloc[:7].isna().all()

    def test_lag_features_raises_on_empty_dataframe(self):
        """Should raise ValueError for empty DataFrame."""
        df = pd.DataFrame()

        with pytest.raises(ValueError, match="Cannot add lag features to empty DataFrame"):
            add_lag_features(df)

    def test_lag_features_raises_on_missing_y_column(self):
        """Should raise ValueError if 'y' column is missing."""
        df = pd.DataFrame({"ds": pd.date_range("2024-01-01", periods=10, freq="D")})

        with pytest.raises(ValueError, match="Missing required column: 'y'"):
            add_lag_features(df)

    def test_lag_features_raises_on_non_numeric_y(self):
        """Should raise TypeError if 'y' is not numeric."""
        df = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=10, freq="D"),
            "y": ["abc"] * 10
        })

        with pytest.raises(TypeError, match="Column 'y' must be numeric"):
            add_lag_features(df)

    def test_lag_features_raises_on_invalid_lags(self):
        """Should raise ValueError for invalid lag values."""
        df = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=10, freq="D"),
            "y": range(10)
        })

        with pytest.raises(ValueError, match="All lags must be positive integers"):
            add_lag_features(df, lags=[1, -5])

        with pytest.raises(ValueError, match="All lags must be positive integers"):
            add_lag_features(df, lags=[1, 0])


class TestRollingFeatures:
    def test_default_windows(self, sample_daily_data):
        result = add_rolling_features(sample_daily_data)
        for w in [7, 14, 30]:
            assert f"rolling_mean_{w}d" in result.columns
            assert f"rolling_std_{w}d" in result.columns

    def test_rolling_features_calculate_correctly(self, sample_daily_data):
        """Rolling features should match pandas rolling calculations."""
        result = add_rolling_features(sample_daily_data, windows=[7])

        # Manually calculate expected values
        expected_mean = sample_daily_data["y"].rolling(7, min_periods=1).mean()
        expected_std = sample_daily_data["y"].rolling(7, min_periods=1).std()

        # Validate rolling mean matches
        pd.testing.assert_series_equal(
            result["rolling_mean_7d"],
            expected_mean,
            check_names=False
        )

        # Validate rolling std matches
        pd.testing.assert_series_equal(
            result["rolling_std_7d"],
            expected_std,
            check_names=False
        )


class TestPrepareFeatures:
    def test_full_pipeline(self, sample_daily_data):
        result = prepare_features(sample_daily_data)
        assert "day_of_week" in result.columns
        assert "lag_7" in result.columns
        assert "rolling_mean_30d" in result.columns
        assert len(result) == len(sample_daily_data)
