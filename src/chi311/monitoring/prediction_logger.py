"""
Prediction logging and dift monitoring.
"""

import logging

import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class PredictionLogger:
    """Log predictions and monitor for data drift."""

    def __init__(self, catalog: str):
        self.catalog = catalog
        self.table = f"{catalog}.gold.gold_prediction_log"

    def log_predictions(
        self, predictions: pd.DataFrame, model_version: str, spark_session=None
    ):
        """Upsert predictions to Delta table on (ds, model_version) for idempotency."""
        if spark_session is None:
            raise ValueError("Spark session required for Delta writes.")
        predictions = predictions.copy()
        predictions["logged_at"] = datetime.now()
        predictions["model_version"] = model_version
        n_rows = len(predictions)
        df = spark_session.createDataFrame(predictions)
        if spark_session.catalog.tableExists(self.table):
            from delta.tables import DeltaTable

            logger.info("Upserting %d predictions into %s", n_rows, self.table)
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
        else:
            logger.info(
                "Creating prediction log table %s with %d rows", self.table, n_rows
            )
            df.write.format("delta").mode("overwrite").saveAsTable(self.table)
        logger.info(
            "log_predictions complete: %d rows, model_version=%s", n_rows, model_version
        )

    def check_drift(
        self,
        recent_actuals: pd.DataFrame,
        predictions: pd.DataFrame,
        threshold: float = 0.20,
    ) -> dict:
        """Check if predictions have drifted from actuals."""
        merged = recent_actuals.merge(
            predictions, on="ds", suffixes=("_actual", "_pred")
        )
        if merged.empty:
            return {"drift_detected": False, "message": "No overlapping dates"}
        mape = (
            (merged["y_actual"] - merged["y_pred"]).abs() / merged["y_actual"]
        ).mean()
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
