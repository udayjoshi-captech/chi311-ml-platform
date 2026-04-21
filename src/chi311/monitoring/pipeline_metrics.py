"""
Pipeline run metrics logger.
Records per-task row counts, duration, and status to a Delta table
for observability across every job run.
"""

import logging
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class PipelineMetrics:
    """Write pipeline run metadata to a Delta table after each task."""

    def __init__(self, catalog: str):
        self.catalog = catalog
        self.table = f"{catalog}.gold.pipeline_run_log"
        self._start_time: Optional[float] = None
        self._task_name: Optional[str] = None
        self._run_id: Optional[str] = None

    # ------------------------------------------------------------------
    # Context manager interface — use with `with` blocks
    # ------------------------------------------------------------------

    def start(self, task_name: str, run_id: str) -> "PipelineMetrics":
        """Mark the start of a pipeline task."""
        self._task_name = task_name
        self._run_id = run_id
        self._start_time = time.time()
        logger.info("PipelineMetrics: started task=%s run_id=%s", task_name, run_id)
        return self

    def finish(
        self,
        rows_in: int,
        rows_out: int,
        rows_dropped: int = 0,
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
        spark_session=None,
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
            ValueError: If spark_session is not provided.
            RuntimeError: If start() was not called before finish().
        """
        if self._start_time is None or self._task_name is None:
            raise RuntimeError("Call start() before finish().")
        if spark_session is None:
            raise ValueError("spark_session required for Delta writes.")

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
            "logged_at": datetime.now(),
        }

        logger.info(
            "PipelineMetrics: task=%s status=%s rows_in=%d rows_out=%d "
            "rows_dropped=%d duration=%.2fs",
            self._task_name,
            status,
            rows_in,
            rows_out,
            rows_dropped,
            duration_seconds,
        )

        df = spark_session.createDataFrame([record])
        df.write.format("delta").mode("append").saveAsTable(self.table)

        # Reset state
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
        count = df.count() if hasattr(df, "count") else len(df)
        if count == 0:
            raise ValueError(
                f"Empty dataset at checkpoint '{label}'. "
                "Check upstream ingestion or filtering logic."
            )
        logger.info("assert_non_empty: '%s' has %d rows — OK", label, count)
