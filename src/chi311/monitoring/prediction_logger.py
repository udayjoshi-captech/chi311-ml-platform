"""
Prediction logging and drift monitoring.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class PredictionLogger:
    """Log predictions and monitor for data drift."""

    def __init__(self, catalog: str):
        self.catalog = catalog
        self.table = f"{catalog}.gold.gold_prediction_log"

    def log_predictions(
        self,
        predictions: pd.DataFrame,
        model_version: str,
        spark_session: Any | None = None,
    ) -> None:
        """Log predictions to Delta table with idempotent upsert.

        Args:
            predictions: DataFrame with 'ds' and 'y_pred' columns
            model_version: Model version identifier
            spark_session: Active SparkSession

        Raises:
            TypeError: If predictions is not a DataFrame
            ValueError: If required columns are missing or spark_session is None
            ConnectionError: If spark_session is not active
            RuntimeError: If Delta operations fail
        """
        if spark_session is None:
            raise ValueError("Spark session required for Delta writes.")

        if not isinstance(predictions, pd.DataFrame):
            raise TypeError(f"predictions must be pd.DataFrame, got {type(predictions)}")

        if not isinstance(model_version, str) or not model_version.strip():
            raise ValueError("model_version must be non-empty string")

        # Validate required columns
        required_columns = {"ds", "y_pred"}
        missing = required_columns - set(predictions.columns)
        if missing:
            raise ValueError(
                f"Predictions missing required columns: {missing}. "
                f"Available: {list(predictions.columns)}"
            )

        # Validate spark_session is active
        try:
            spark_session.catalog.currentDatabase()
        except Exception as e:
            raise ConnectionError(f"SparkSession is not active: {e}") from e

        predictions = predictions.copy()
        predictions["logged_at"] = datetime.now(tz=timezone.utc)
        predictions["model_version"] = model_version
        n_rows = len(predictions)

        try:
            df = spark_session.createDataFrame(predictions)
        except Exception as e:
            logger.exception(
                "Failed to create Spark DataFrame from predictions: %s. "
                "Schema: %s",
                e,
                predictions.dtypes,
            )
            raise ValueError(f"Invalid prediction schema: {e}") from e

        try:
            table_exists = spark_session.catalog.tableExists(self.table)
        except Exception as e:
            logger.error("Failed to check table existence for %s: %s", self.table, e)
            raise ConnectionError(f"Cannot access catalog: {e}") from e

        if table_exists:
            from delta.tables import DeltaTable
            from pyspark.sql.utils import AnalysisException

            logger.info("Upserting %d predictions into %s", n_rows, self.table)

            # Retry logic for concurrent write conflicts
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    (
                        DeltaTable.forName(spark_session, self.table)
                        .alias("target")
                        .merge(
                            df.alias("source"),
                            "target.ds = source.ds AND target.model_version = source.model_version",
                        )
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                    break  # Success
                except AnalysisException as e:
                    if "ConcurrentAppendException" in str(e) and attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "Concurrent write detected, retry %d/%d in %ds",
                            attempt + 1,
                            max_retries,
                            wait_time
                        )
                        time.sleep(wait_time)
                    else:
                        logger.exception(
                            "Delta MERGE failed for %s: %s. Predictions will not be logged.",
                            self.table,
                            e,
                        )
                        raise RuntimeError(f"Prediction logging failed: {e}") from e
        else:
            logger.info("Creating prediction log table %s with %d rows", self.table, n_rows)
            try:
                df.write.format("delta").mode("overwrite").saveAsTable(self.table)
            except Exception as e:
                logger.exception("Failed to create table %s: %s", self.table, e)
                raise RuntimeError(f"Table creation failed: {e}") from e

        logger.info(
            "log_predictions complete: %d rows, model_version=%s", n_rows, model_version
        )

    def check_drift(
        self,
        recent_actuals: pd.DataFrame,
        predictions: pd.DataFrame,
        threshold: float = 0.20,
    ) -> dict:
        """Check if predictions have drifted from actuals.

        Args:
            recent_actuals: DataFrame with 'ds' and 'y_actual' columns
            predictions: DataFrame with 'ds' and 'y_pred' columns
            threshold: MAPE threshold for drift detection (default: 0.20)

        Returns:
            Dictionary with drift_detected, mape, threshold, n_observations
        """
        # Validate input DataFrames
        if recent_actuals.empty:
            return {
                "drift_detected": False,
                "message": "No actuals provided",
                "error": "empty_actuals"
            }

        if predictions.empty:
            return {
                "drift_detected": False,
                "message": "No predictions provided",
                "error": "empty_predictions"
            }

        # Check required columns
        if "ds" not in recent_actuals.columns or "y_actual" not in recent_actuals.columns:
            return {
                "drift_detected": False,
                "message": f"Actuals missing required columns. Has: {list(recent_actuals.columns)}",
                "error": "missing_columns"
            }

        if "ds" not in predictions.columns or "y_pred" not in predictions.columns:
            return {
                "drift_detected": False,
                "message": f"Predictions missing required columns. Has: {list(predictions.columns)}",
                "error": "missing_columns"
            }

        merged = recent_actuals.merge(
            predictions, on="ds", suffixes=("_actual", "_pred")
        )

        if merged.empty:
            return {
                "drift_detected": False,
                "message": "No overlapping dates",
                "error": "no_overlap"
            }

        # Check for zero or negative actuals
        invalid_actuals = (merged["y_actual"] <= 0).sum()
        if invalid_actuals > 0:
            logger.warning(
                "check_drift: %d rows have zero or negative actuals, excluding from MAPE",
                invalid_actuals
            )
            merged = merged[merged["y_actual"] > 0]

        if merged.empty:
            logger.warning("check_drift: All actuals are zero, cannot calculate MAPE")
            return {
                "drift_detected": False,
                "message": "All actuals are zero",
                "error": "zero_actuals"
            }

        try:
            mape = float(
                ((merged["y_actual"] - merged["y_pred"]).abs() / merged["y_actual"]).mean()
            )
        except Exception as e:
            logger.exception("check_drift: MAPE calculation failed - %s", e)
            return {
                "drift_detected": False,
                "message": f"MAPE calculation failed: {e}",
                "error": "mape_failed"
            }

        result = {
            "drift_detected": mape > threshold,
            "mape": float(mape),
            "threshold": threshold,
            "n_observations": len(merged),
        }

        if mape > threshold:
            logger.warning(
                "Drift detected: MAPE=%.3f exceeds threshold=%.3f (n=%d)",
                float(mape),
                threshold,
                len(merged),
            )

        return result
