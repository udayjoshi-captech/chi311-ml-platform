"""Tests for pipeline metrics module."""
import pytest
import time
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from chi311.monitoring.pipeline_metrics import PipelineMetrics


@pytest.fixture
def mock_spark():
    """Create mock SparkSession."""
    spark = MagicMock()
    spark.catalog.currentDatabase.return_value = "chi311"
    spark.catalog.databaseExists.return_value = True
    spark.createDataFrame.return_value = MagicMock()
    return spark


class TestPipelineMetrics:
    """Tests for PipelineMetrics class."""

    def test_init_sets_catalog(self):
        """Should initialize with correct catalog."""
        metrics = PipelineMetrics(catalog="test_catalog")
        assert metrics.table == "test_catalog.gold.pipeline_run_log"

    def test_start_initializes_state(self):
        """Should set start time and task info."""
        metrics = PipelineMetrics(catalog="chi311")

        result = metrics.start(task_name="test_task", run_id="run123")

        assert metrics._task_name == "test_task"
        assert metrics._run_id == "run123"
        assert metrics._start_time is not None
        assert result is metrics  # Returns self for chaining

    def test_finish_requires_start_first(self, mock_spark):
        """Should raise RuntimeError if start() not called."""
        metrics = PipelineMetrics(catalog="chi311")

        with pytest.raises(RuntimeError, match="finish\\(\\) called without calling start\\(\\)"):
            metrics.finish(rows_in=100, rows_out=95, spark_session=mock_spark)

    def test_finish_requires_spark_session(self):
        """Should raise ValueError if spark_session is None."""
        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="test", run_id="123")

        with pytest.raises(ValueError, match="spark_session required"):
            metrics.finish(rows_in=100, rows_out=95, spark_session=None)

    def test_finish_validates_spark_session_is_active(self):
        """Should raise ConnectionError if SparkSession is not active."""
        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="test", run_id="123")

        mock_spark = MagicMock()
        mock_spark.catalog.currentDatabase.side_effect = Exception("Not connected")

        with pytest.raises(ConnectionError, match="SparkSession is not active"):
            metrics.finish(rows_in=100, rows_out=95, spark_session=mock_spark)

    def test_finish_validates_schema_exists(self, mock_spark):
        """Should raise ValueError if schema doesn't exist."""
        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="test", run_id="123")

        mock_spark.catalog.databaseExists.return_value = False

        with pytest.raises(ValueError, match="does not exist"):
            metrics.finish(rows_in=100, rows_out=95, spark_session=mock_spark)

    def test_finish_calculates_duration(self, mock_spark):
        """Should calculate correct duration."""
        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="test", run_id="123")

        # Mock start time to be 2.5 seconds ago
        with patch.object(metrics, '_start_time', time.time() - 2.5):
            metrics.finish(rows_in=1000, rows_out=950, spark_session=mock_spark)

        # Check that createDataFrame was called with duration ~2.5 seconds
        call_args = mock_spark.createDataFrame.call_args[0][0]
        record = call_args[0]
        assert 2.4 <= record["duration_seconds"] <= 2.6

    def test_finish_writes_correct_record(self, mock_spark):
        """Should write complete metrics record to Delta."""
        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="bronze_load", run_id="run456")

        metrics.finish(
            rows_in=5000,
            rows_out=4800,
            rows_dropped=200,
            status="SUCCESS",
            spark_session=mock_spark
        )

        call_args = mock_spark.createDataFrame.call_args[0][0]
        record = call_args[0]

        assert record["task_name"] == "bronze_load"
        assert record["run_id"] == "run456"
        assert record["status"] == "SUCCESS"
        assert record["rows_in"] == 5000
        assert record["rows_out"] == 4800
        assert record["rows_dropped"] == 200
        assert "duration_seconds" in record
        assert isinstance(record["logged_at"], datetime)

    def test_finish_resets_state_on_success(self, mock_spark):
        """Should reset state after successful write."""
        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="test", run_id="123")

        metrics.finish(rows_in=100, rows_out=100, spark_session=mock_spark)

        assert metrics._start_time is None
        assert metrics._task_name is None
        assert metrics._run_id is None

    def test_finish_does_not_reset_state_on_write_failure(self, mock_spark):
        """Should not reset state if Delta write fails."""
        mock_spark.createDataFrame.side_effect = Exception("Delta write failed")

        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="test", run_id="123")

        with pytest.raises(RuntimeError, match="Metrics logging failed"):
            metrics.finish(rows_in=100, rows_out=100, spark_session=mock_spark)

        # State should still be set for potential retry
        assert metrics._start_time is not None
        assert metrics._task_name == "test"

    def test_fail_convenience_method(self, mock_spark):
        """Should call finish with FAILED status."""
        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="test", run_id="123")

        metrics.fail(error_message="Something went wrong", spark_session=mock_spark)

        call_args = mock_spark.createDataFrame.call_args[0][0]
        record = call_args[0]

        assert record["status"] == "FAILED"
        assert record["error_message"] == "Something went wrong"

    def test_context_manager_requires_start(self):
        """Should raise error if used as context manager without calling start()."""
        metrics = PipelineMetrics(catalog="chi311")

        with pytest.raises(RuntimeError, match="Call start\\(\\) before using as context manager"):
            with metrics:
                pass

    def test_context_manager_logs_exception(self, mock_spark):
        """Should log exception in __exit__ but not suppress it."""
        metrics = PipelineMetrics(catalog="chi311")
        metrics.start(task_name="test", run_id="123")

        with pytest.raises(ValueError, match="Test error"):
            with metrics:
                raise ValueError("Test error")

        # State should still be set (not reset because finish wasn't called)
        assert metrics._start_time is not None


class TestAssertNonEmpty:
    """Tests for assert_non_empty static method."""

    def test_assert_non_empty_passes_for_non_empty_dataframe(self):
        """Should not raise for non-empty DataFrame."""
        import pandas as pd
        df = pd.DataFrame({"a": [1, 2, 3]})

        # Should not raise
        PipelineMetrics.assert_non_empty(df, "test_df")

    def test_assert_non_empty_raises_for_empty_dataframe(self):
        """Should raise ValueError for empty DataFrame."""
        import pandas as pd
        df = pd.DataFrame()

        with pytest.raises(ValueError, match="Empty dataset at checkpoint 'test_df'"):
            PipelineMetrics.assert_non_empty(df, "test_df")

    def test_assert_non_empty_works_with_spark_dataframes(self):
        """Should work with PySpark DataFrames using .count()."""
        mock_df = MagicMock()
        mock_df.count.return_value = 100

        # Should not raise
        PipelineMetrics.assert_non_empty(mock_df, "spark_df")

    def test_assert_non_empty_raises_for_empty_spark_dataframe(self):
        """Should raise ValueError for empty Spark DataFrame."""
        mock_df = MagicMock()
        mock_df.count.return_value = 0

        with pytest.raises(ValueError, match="Empty dataset"):
            PipelineMetrics.assert_non_empty(mock_df, "empty_spark_df")
