"""Tests for Prophet forecaster module."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import timedelta

from chi311.models.prophet_forecaster import Chi311Forecaster


@pytest.fixture
def sample_training_data():
    """Create sample time series data for testing."""
    dates = pd.date_range(start="2024-01-01", periods=90, freq="D")
    data = pd.DataFrame({
        "ds": dates,
        "y": np.random.randint(1000, 5000, size=len(dates))
    })
    return data


@pytest.fixture
def forecaster():
    """Create a forecaster instance."""
    return Chi311Forecaster(
        changepoint_prior_scale=0.1,
        seasonality_prior_scale=10.0,
        seasonality_mode="multiplicative",
    )


class TestPrepareData:
    """Tests for prepare_data static method."""

    def test_prepare_data_validates_required_columns(self):
        """Should raise ValueError if ds or y columns are missing."""
        df = pd.DataFrame({"date": ["2024-01-01"], "value": [100]})

        with pytest.raises(ValueError, match="Missing required columns"):
            Chi311Forecaster.prepare_data(df)

    def test_prepare_data_removes_nulls(self, sample_training_data):
        """Should remove rows with null values."""
        sample_training_data.loc[5, "y"] = np.nan

        result = Chi311Forecaster.prepare_data(sample_training_data)

        assert result.isna().sum().sum() == 0
        assert len(result) == len(sample_training_data) - 1

    def test_prepare_data_removes_duplicates(self, sample_training_data):
        """Should remove duplicate date rows."""
        duplicate_row = sample_training_data.iloc[0:1].copy()
        df_with_dups = pd.concat([sample_training_data, duplicate_row], ignore_index=True)

        result = Chi311Forecaster.prepare_data(df_with_dups)

        assert len(result) == len(sample_training_data)

    def test_prepare_data_sorts_by_date(self):
        """Should sort data by ds column."""
        dates = pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"] +
                               [f"2024-01-{d:02d}" for d in range(4, 12)])
        df = pd.DataFrame({
            "ds": dates,
            "y": [300, 100, 200] + list(range(400, 1200, 100))
        })

        result = Chi311Forecaster.prepare_data(df)

        assert result["ds"].is_monotonic_increasing
        assert result.iloc[0]["y"] == 100  # First sorted value

    def test_prepare_data_truncates_to_yesterday_by_default(self, sample_training_data):
        """Should remove future dates by default."""
        tomorrow = pd.Timestamp.now().normalize() + timedelta(days=1)
        future_row = pd.DataFrame({
            "ds": [tomorrow],
            "y": [9999]
        })
        df_with_future = pd.concat([sample_training_data, future_row], ignore_index=True)

        result = Chi311Forecaster.prepare_data(df_with_future, truncate_to_yesterday=True)

        assert result["ds"].max() < pd.Timestamp.now().normalize()
        assert 9999 not in result["y"].values

    def test_prepare_data_validates_minimum_points(self):
        """Should raise ValueError if insufficient data after cleaning."""
        df = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
            "y": [100, 200, 300, 400, 500]
        })

        with pytest.raises(ValueError, match="Insufficient data.*need at least"):
            Chi311Forecaster.prepare_data(df)

    def test_prepare_data_converts_ds_to_datetime(self):
        """Should convert ds column to datetime if it's not already."""
        df = pd.DataFrame({
            "ds": [f"2024-01-{i:02d}" for i in range(1, 14)],  # 13 unique dates
            "y": list(range(100, 1400, 100))
        })

        result = Chi311Forecaster.prepare_data(df)

        assert pd.api.types.is_datetime64_any_dtype(result["ds"])

    def test_prepare_data_raises_on_invalid_datetime_conversion(self):
        """Should raise TypeError if ds cannot be converted to datetime."""
        df = pd.DataFrame({
            "ds": ["invalid", "dates", "here"] + ["2024-01-01"] * 10,
            "y": [100, 200, 300] + [400] * 10
        })

        with pytest.raises(TypeError, match="Could not convert 'ds' to datetime"):
            Chi311Forecaster.prepare_data(df)

    def test_prepare_data_raises_on_non_numeric_y(self):
        """Should raise TypeError if y is not numeric."""
        df = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=15, freq="D"),
            "y": ["abc"] * 15
        })

        with pytest.raises(TypeError, match="Column 'y' must be numeric"):
            Chi311Forecaster.prepare_data(df)

    def test_prepare_data_raises_on_negative_values(self):
        """Should raise ValueError if y contains negative values."""
        df = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=15, freq="D"),
            "y": [100, 200, -50, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400]
        })

        with pytest.raises(ValueError, match="negative values"):
            Chi311Forecaster.prepare_data(df)


class TestTrain:
    """Tests for train method."""

    @patch("chi311.models.prophet_forecaster.Prophet")
    def test_train_calls_prophet_with_correct_params(
        self, mock_prophet_class, forecaster, sample_training_data
    ):
        """Should initialize Prophet with configured parameters."""
        mock_model = MagicMock()
        mock_prophet_class.return_value = mock_model
        mock_model.fit.return_value = mock_model
        mock_model.predict.return_value = sample_training_data.rename(columns={"y": "yhat"})

        forecaster.train(sample_training_data, log_to_mlflow=False)

        mock_prophet_class.assert_called_once()
        call_kwargs = mock_prophet_class.call_args[1]
        assert call_kwargs["changepoint_prior_scale"] == 0.1
        assert call_kwargs["seasonality_prior_scale"] == 10.0
        assert call_kwargs["seasonality_mode"] == "multiplicative"

    @patch("chi311.models.prophet_forecaster.Prophet")
    def test_train_returns_metrics(self, mock_prophet_class, forecaster, sample_training_data):
        """Should return dict with MAPE, RMSE, MAE."""
        mock_model = MagicMock()
        mock_prophet_class.return_value = mock_model
        mock_model.fit.return_value = mock_model
        mock_model.predict.return_value = sample_training_data.rename(columns={"y": "yhat"})

        metrics = forecaster.train(sample_training_data, log_to_mlflow=False)

        assert "mape" in metrics
        assert "rmse" in metrics
        assert "mae" in metrics
        assert all(isinstance(v, (int, float)) or np.isnan(v) for v in metrics.values())

    @patch("chi311.models.prophet_forecaster.Prophet")
    def test_train_handles_zero_actuals_in_mape(self, mock_prophet_class, forecaster):
        """Should handle zero actual values without division error."""
        df = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=15, freq="D"),
            "y": [0] * 15  # All zeros
        })
        mock_model = MagicMock()
        mock_prophet_class.return_value = mock_model
        mock_model.fit.return_value = mock_model
        mock_model.predict.return_value = df.rename(columns={"y": "yhat"})

        metrics = forecaster.train(df, log_to_mlflow=False)

        assert np.isnan(metrics["mape"])  # Should be NaN, not error

    @patch("chi311.models.prophet_forecaster.Prophet")
    @patch("mlflow.set_experiment")
    @patch("mlflow.start_run")
    @patch("mlflow.set_tags")
    @patch("mlflow.log_params")
    @patch("mlflow.log_metrics")
    @patch("mlflow.prophet.log_model")
    @patch("mlflow.log_dict")
    def test_train_logs_to_mlflow_when_enabled(
        self, mock_log_dict, mock_log_model, mock_log_metrics,
        mock_log_params, mock_set_tags, mock_start_run,
        mock_set_experiment, mock_prophet_class,
        forecaster, sample_training_data
    ):
        """Should log parameters, metrics, and model to MLflow."""
        mock_model = MagicMock()
        mock_prophet_class.return_value = mock_model
        mock_model.fit.return_value = mock_model
        mock_model.predict.return_value = sample_training_data.rename(columns={"y": "yhat"})

        # Setup mock run context
        mock_run = MagicMock()
        mock_run.info.run_id = "test_run_123"
        mock_start_run.return_value.__enter__.return_value = mock_run

        forecaster.train(sample_training_data, log_to_mlflow=True)

        mock_set_experiment.assert_called_once()
        mock_start_run.assert_called_once()
        mock_log_params.assert_called_once()
        mock_log_metrics.assert_called_once()
        mock_log_model.assert_called_once()  # Verify model was logged


class TestPredict:
    """Tests for predict method."""

    def test_predict_raises_error_when_untrained(self, forecaster):
        """Should raise ValueError if model is not trained."""
        with pytest.raises(ValueError, match="Model not trained"):
            forecaster.predict(periods=7)

    @patch("chi311.models.prophet_forecaster.Prophet")
    def test_predict_generates_future_dates(
        self, mock_prophet_class, forecaster, sample_training_data
    ):
        """Should generate predictions for specified periods."""
        mock_model = MagicMock()
        mock_prophet_class.return_value = mock_model
        mock_model.fit.return_value = mock_model
        mock_model.predict.return_value = sample_training_data.rename(columns={"y": "yhat"})

        forecaster.train(sample_training_data, log_to_mlflow=False)

        mock_future_df = pd.DataFrame({
            "ds": pd.date_range("2024-04-01", periods=7, freq="D"),
            "yhat": [3000] * 7,
            "yhat_lower": [2500] * 7,
            "yhat_upper": [3500] * 7
        })
        mock_model.make_future_dataframe.return_value = mock_future_df[["ds"]]
        mock_model.predict.return_value = mock_future_df

        result = forecaster.predict(periods=7)

        mock_model.make_future_dataframe.assert_called_once_with(periods=7)
        assert len(result) == 7
        assert "ds" in result.columns
        assert "yhat" in result.columns
