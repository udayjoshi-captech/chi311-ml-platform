"""
Pipeline run metrics logger.
Records per-task row counts, duration, and status to a Delta table
for observability across every job run.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Self

logger = logging.getLogger(__name__)


class PipelineMetrics:
    """Write pipeline run metadata to a Delta table after each task."""

    def __init__(self, catalog: str):
        self.catalog = catalog
        self.table = f"{catalog}.gold.pipeline_run_log"
        self._start_time: float | None = None
        self._task_name: str | None = None
        self._run_id: str | None = None

    # ------------------------------------------------------------------
    # Context manager interface — use with `with` blocks
    # ------------------------------------------------------------------

    def start(self, task_name: str, run_id: str) -> Self:
        """Mark the start of a pipeline task."""
        self._task_name = task_name
        self._run_id = run_id
        self._start_time = time.time()
        logger.info("PipelineMetrics: started task=%s run_id=%s", task_name, run_id)
        return self

    def __enter__(self) -> Self:
        """Enable use as context manager."""
        if self._start_time is None:
            raise RuntimeError("Call start() before using as context manager")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-record failure on exception, but don't suppress it."""
        # This gets called even if finish() was already called successfully
        # Only record failure if we're still in an active state
        if self._start_time is not None and exc_type is not None:
            # An exception occurred before finish() was called
            error_msg = f"{exc_type.__name__}: {exc_val}"
            logger.error("PipelineMetrics: Exception in context — %s", error_msg)
            # Note: We cannot write to Delta here because we don't have spark_session
            # The caller must catch exceptions and call fail() explicitly if they want metrics logged
        return False  # Don't suppress the exception

    def finish(
        self,
        rows_in: int,
        rows_out: int,
        rows_dropped: int = 0,
        status: str = "SUCCESS",
        error_message: str | None = None,
        spark_session: Any | None = None,
    ) -> None:
        """Record task completion metrics to Delta table.

        Args:
            rows_in:       Row count of the input dataset.
            rows_out:      Row count of the output/written dataset.
            rows_dropped:  Rows filtered out by quality checks.
            status:        "SUCCESS" or "FAILED".
            error_message: Optional error description on failure.
            spark_session: Active SparkSession for Delta writes.

        Raises:
            RuntimeError: If state is invalid or Delta write fails
        """
        if self._start_time is None or self._task_name is None or self._run_id is None:
            raise RuntimeError(
                "PipelineMetrics: finish() called without calling start() first. "
                f"State: start_time={self._start_time}, task_name={self._task_name}, run_id={self._run_id}"
            )

        if spark_session is None:
            raise ValueError("spark_session required for Delta writes.")

        # Validate spark_session is active
        try:
            spark_session.catalog.currentDatabase()
        except Exception as e:
            raise ConnectionError(f"SparkSession is not active: {e}") from e

        # Validate catalog/schema exist
        catalog, schema, _table_name = self.table.split(".")
        if not spark_session.catalog.databaseExists(f"{catalog}.{schema}"):
            raise ValueError(
                f"Schema '{catalog}.{schema}' does not exist. "
                "Run setup notebook to create monitoring tables."
            )

        duration_seconds = round(time.time() - self._start_time, 2)
        record = {
            "run_id": self._run_id,
            "task_name": self._task_name,
            "status": status,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_dropped": rows_dropped,
            "duration_seconds": duration_seconds,
            "error_message": error_message,
            "logged_at": datetime.now(tz=timezone.utc),
        }

        logger.info(
            "PipelineMetrics: Recording %s — %s: %d in, %d out, %d dropped, %.2fs",
            status,
            self._task_name,
            rows_in,
            rows_out,
            rows_dropped,
            duration_seconds
        )

        write_error = None
        try:
            df = spark_session.createDataFrame([record])
            df.write.format("delta").mode("append").saveAsTable(self.table)
            logger.info("PipelineMetrics: Successfully logged to %s", self.table)
        except Exception as e:
            write_error = e
            logger.exception(
                "PipelineMetrics: Failed to write metrics to %s — %s",
                self.table,
                str(e),
            )
            raise RuntimeError(f"Metrics logging failed: {e}") from e
        finally:
            # Only reset state if write succeeded
            if write_error is None:
                self._start_time = None
                self._task_name = None
                self._run_id = None

    def fail(
        self,
        error_message: str,
        rows_in: int = 0,
        rows_out: int = 0,
        spark_session=None,
    ) -> None:
        """Convenience method to record a failed task."""
        logger.error(
            "PipelineMetrics: task=%s FAILED — %s", self._task_name, error_message
        )
        self.finish(
            rows_in=rows_in,
            rows_out=rows_out,
            status="FAILED",
            error_message=error_message,
            spark_session=spark_session,
        )

    @staticmethod
    def assert_non_empty(df, label: str) -> None:
        """Assert a DataFrame has at least one row; raise if empty.

        Use after every read/transform to catch silent empty-dataset failures.
        """
        # Handle both pandas DataFrame (len) and Spark DataFrame (count)
        if hasattr(df, "rdd"):  # Spark DataFrame
            count = df.count()
        else:  # Pandas DataFrame
            count = len(df)

        if count == 0:
            raise ValueError(
                f"Empty dataset at checkpoint '{label}'. "
                "Check upstream ingestion or filtering logic."
            )
        logger.info("assert_non_empty: '%s' has %d rows — OK", label, count)
