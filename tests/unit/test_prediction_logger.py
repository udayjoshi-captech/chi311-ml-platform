"""Tests for prediction logger module."""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from chi311.monitoring.prediction_logger import PredictionLogger


@pytest.fixture
def mock_spark():
    """Create mock SparkSession."""
    spark = MagicMock()
    spark.catalog.currentDatabase.return_value = "chi311"
    spark.catalog.tableExists.return_value = False
    spark.createDataFrame.return_value = MagicMock()
    return spark


@pytest.fixture
def sample_predictions():
    """Create sample prediction DataFrame."""
    return pd.DataFrame({
        "ds": pd.date_range("2024-01-01", periods=7, freq="D"),
        "y_pred": [3000, 3100, 3200, 3150, 3300, 2900, 2800]
    })


class TestLogPredictions:
    """Tests for log_predictions method."""

    def test_log_predictions_requires_spark_session(self, sample_predictions):
        """Should raise ValueError if spark_session is None."""
        logger = PredictionLogger(catalog="chi311")

        with pytest.raises(ValueError, match="Spark session required"):
            logger.log_predictions(sample_predictions, "v1.0", spark_session=None)

    def test_log_predictions_validates_dataframe_type(self, mock_spark):
        """Should raise TypeError if predictions is not DataFrame."""
        logger = PredictionLogger(catalog="chi311")

        with pytest.raises(TypeError, match="predictions must be pd.DataFrame"):
            logger.log_predictions([1, 2, 3], "v1.0", spark_session=mock_spark)

    def test_log_predictions_validates_model_version(self, mock_spark, sample_predictions):
        """Should raise ValueError if model_version is invalid."""
        logger = PredictionLogger(catalog="chi311")

        with pytest.raises(ValueError, match="model_version must be non-empty string"):
            logger.log_predictions(sample_predictions, "", spark_session=mock_spark)

        with pytest.raises(ValueError, match="model_version must be non-empty string"):
            logger.log_predictions(sample_predictions, "   ", spark_session=mock_spark)

    def test_log_predictions_validates_required_columns(self, mock_spark):
        """Should raise ValueError if required columns missing."""
        logger = PredictionLogger(catalog="chi311")
        df = pd.DataFrame({"date": ["2024-01-01"], "value": [100]})

        with pytest.raises(ValueError, match="Missing required columns"):
            logger.log_predictions(df, "v1.0", spark_session=mock_spark)

    def test_log_predictions_validates_spark_session_is_active(self, sample_predictions):
        """Should raise ConnectionError if SparkSession is not active."""
        logger = PredictionLogger(catalog="chi311")
        mock_spark = MagicMock()
        mock_spark.catalog.currentDatabase.side_effect = Exception("Not connected")

        with pytest.raises(ConnectionError, match="SparkSession is not active"):
            logger.log_predictions(sample_predictions, "v1.0", spark_session=mock_spark)

    def test_log_predictions_creates_table_if_not_exists(
        self, mock_spark, sample_predictions
    ):
        """Should create table on first write."""
        logger = PredictionLogger(catalog="chi311")
        mock_spark.catalog.tableExists.return_value = False
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        logger.log_predictions(sample_predictions, "v1.0", spark_session=mock_spark)

        # Should call overwrite mode for new table
        mock_df.write.format.assert_called_once_with("delta")
        mock_df.write.format().mode.assert_called_once_with("overwrite")

    @patch("chi311.monitoring.prediction_logger.DeltaTable")
    def test_log_predictions_merges_if_table_exists(
        self, mock_delta_table, mock_spark, sample_predictions
    ):
        """Should use MERGE for existing table."""
        logger = PredictionLogger(catalog="chi311")
        mock_spark.catalog.tableExists.return_value = True

        mock_delta = MagicMock()
        mock_delta_table.forName.return_value = mock_delta
        mock_delta.alias.return_value = mock_delta
        mock_delta.merge.return_value = mock_delta
        mock_delta.whenMatchedUpdateAll.return_value = mock_delta
        mock_delta.whenNotMatchedInsertAll.return_value = mock_delta

        logger.log_predictions(sample_predictions, "v1.0", spark_session=mock_spark)

        mock_delta_table.forName.assert_called_once()
        mock_delta.merge.assert_called_once()
        mock_delta.whenMatchedUpdateAll.assert_called_once()
        mock_delta.whenNotMatchedInsertAll.assert_called_once()
        mock_delta.execute.assert_called_once()

    def test_log_predictions_adds_metadata(self, mock_spark, sample_predictions):
        """Should add logged_at and model_version columns."""
        logger = PredictionLogger(catalog="chi311")

        logger.log_predictions(sample_predictions, "v2.0", spark_session=mock_spark)

        call_args = mock_spark.createDataFrame.call_args[0][0]
        assert "logged_at" in call_args.columns
        assert "model_version" in call_args.columns
        assert (call_args["model_version"] == "v2.0").all()

    def test_log_predictions_handles_dataframe_conversion_error(self, mock_spark, sample_predictions):
        """Should raise ValueError if DataFrame conversion fails."""
        logger = PredictionLogger(catalog="chi311")
        mock_spark.createDataFrame.side_effect = Exception("Invalid schema")

        with pytest.raises(ValueError, match="Invalid prediction schema"):
            logger.log_predictions(sample_predictions, "v1.0", spark_session=mock_spark)

    @patch("chi311.monitoring.prediction_logger.DeltaTable")
    @patch("chi311.monitoring.prediction_logger.time")
    def test_log_predictions_retries_on_concurrent_write(
        self, mock_time, mock_delta_table, mock_spark, sample_predictions
    ):
        """Should retry on ConcurrentAppendException."""
        from pyspark.sql.utils import AnalysisException

        logger = PredictionLogger(catalog="chi311")
        mock_spark.catalog.tableExists.return_value = True

        mock_delta = MagicMock()
        mock_delta_table.forName.return_value = mock_delta
        mock_delta.alias.return_value = mock_delta
        mock_delta.merge.return_value = mock_delta
        mock_delta.whenMatchedUpdateAll.return_value = mock_delta
        mock_delta.whenNotMatchedInsertAll.return_value = mock_delta

        # Fail once, then succeed
        mock_delta.execute.side_effect = [
            AnalysisException("ConcurrentAppendException: conflict detected"),
            None
        ]

        logger.log_predictions(sample_predictions, "v1.0", spark_session=mock_spark)

        # Should have called execute twice (1 failure + 1 success)
        assert mock_delta.execute.call_count == 2
        # Should have slept once
        mock_time.sleep.assert_called_once()


class TestCheckDrift:
    """Tests for check_drift method."""

    def test_check_drift_handles_empty_actuals(self):
        """Should return no drift for empty actuals."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame()
        predictions = pd.DataFrame({"ds": ["2024-01-01"], "y_pred": [100]})

        result = logger.check_drift(actuals, predictions)

        assert result["drift_detected"] is False
        assert "empty_actuals" in result.get("error", "")

    def test_check_drift_handles_empty_predictions(self):
        """Should return no drift for empty predictions."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame({"ds": ["2024-01-01"], "y_actual": [100]})
        predictions = pd.DataFrame()

        result = logger.check_drift(actuals, predictions)

        assert result["drift_detected"] is False
        assert "empty_predictions" in result.get("error", "")

    def test_check_drift_validates_required_columns(self):
        """Should return error if columns missing."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame({"date": ["2024-01-01"], "value": [100]})
        predictions = pd.DataFrame({"ds": ["2024-01-01"], "y_pred": [100]})

        result = logger.check_drift(actuals, predictions)

        assert result["drift_detected"] is False
        assert "missing_columns" in result.get("error", "")

    def test_check_drift_handles_no_overlapping_dates(self):
        """Should return no drift if no dates overlap."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=3, freq="D"),
            "y_actual": [100, 200, 300]
        })
        predictions = pd.DataFrame({
            "ds": pd.date_range("2024-02-01", periods=3, freq="D"),
            "y_pred": [110, 190, 310]
        })

        result = logger.check_drift(actuals, predictions)

        assert result["drift_detected"] is False
        assert "no_overlap" in result.get("error", "")

    def test_check_drift_calculates_mape_correctly(self):
        """Should calculate MAPE from actuals vs predictions."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
            "y_actual": [100, 200, 300, 400, 500]
        })
        predictions = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
            "y_pred": [110, 190, 310, 380, 520]  # ~5% error on average
        })

        result = logger.check_drift(actuals, predictions, threshold=0.10)

        assert result["drift_detected"] is False
        assert 0.04 < result["mape"] < 0.06  # ~5% MAPE

    def test_check_drift_detects_drift_above_threshold(self):
        """Should detect drift when MAPE exceeds threshold."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=3, freq="D"),
            "y_actual": [100, 200, 300]
        })
        predictions = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=3, freq="D"),
            "y_pred": [50, 100, 150]  # 50% error
        })

        result = logger.check_drift(actuals, predictions, threshold=0.20)

        assert result["drift_detected"] is True
        assert result["mape"] > 0.40

    def test_check_drift_filters_zero_actuals(self):
        """Should exclude zero actuals from MAPE calculation."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
            "y_actual": [0, 0, 100, 200, 300]  # First 2 are zero
        })
        predictions = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
            "y_pred": [50, 100, 110, 190, 310]
        })

        result = logger.check_drift(actuals, predictions, threshold=0.20)

        # Should only use last 3 rows for MAPE
        assert result["drift_detected"] is False
        assert result["n_observations"] == 3  # Excluded 2 zeros

    def test_check_drift_filters_negative_actuals(self):
        """Should exclude negative actuals from MAPE calculation."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
            "y_actual": [-50, -100, 100, 200, 300]  # First 2 are negative
        })
        predictions = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=5, freq="D"),
            "y_pred": [50, 100, 110, 190, 310]
        })

        result = logger.check_drift(actuals, predictions, threshold=0.20)

        # Should only use last 3 rows for MAPE (positive actuals)
        assert result["drift_detected"] is False
        assert result["n_observations"] == 3  # Excluded 2 negatives

    def test_check_drift_handles_all_invalid_actuals(self):
        """Should return error if all actuals are zero or negative."""
        logger = PredictionLogger(catalog="chi311")
        actuals = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=3, freq="D"),
            "y_actual": [0, 0, -100]
        })
        predictions = pd.DataFrame({
            "ds": pd.date_range("2024-01-01", periods=3, freq="D"),
            "y_pred": [50, 100, 150]
        })

        result = logger.check_drift(actuals, predictions)

        assert result["drift_detected"] is False
        assert "zero_actuals" in result.get("error", "")
