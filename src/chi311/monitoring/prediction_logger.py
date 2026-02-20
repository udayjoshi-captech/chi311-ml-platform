"""
Prediction logging and dift monitoring.
"""
import pandas as pd
from datetime import datetime
from typing import Optional

class PredictionLogger:
    """Log predictions and monitor for data drift."""

    def __init__(self, catalog: str = "workspace"):
        self.catalog = catalog
        self.table = f"{catalog}.gold.gold_prediction_log"
    
    def log_predictions(self, predictions: pd.DataFrame, model_version: str, spark_session=None):
        """Save predictions to Delta table."""
        if spark_session is None:
            raise ValueError("Spark session required for Delta writes.")
        predictions = predictions.copy()
        predictions["logged_at"] = datetime.now()
        predictions["model_version"] = model_version
        df = spark_session.createDataFrame(predictions)
        df.write.mode("append").saveAsTable(self.table)
    
    def check_drift(self, recent_actuals: pd.DataFrame, predictions: pd.DataFrame, threshold: float = 0.20) -> dict:
        """Check if predictions have drifted from actuals."""
        merged = recent_actuals.merge(predictions, on="ds", suffixes=("_actual", "_pred"))
        if merged.empty:
            return {"drift_detected": False, "message": "No overlapping dates"}
        mape = ((merged["y_actual"] - merged["y_pred"]).abs() / merged["y_actual"]).mean()
        return {
            "drift_detected": mape > threshold,
            "mape": float(mape),
            "threshold": threshold,
            "n_observations": len(merged)
        }