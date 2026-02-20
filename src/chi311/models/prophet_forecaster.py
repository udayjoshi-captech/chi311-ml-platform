"""
Prophet model wrapper with MLflow integration.
"""
import mlflow
import mflow.prophet
from prophet import Prophet
import pandas as pd
import numpy as np
from typing import Optional

# Minimum rows required for a reliable Prophet fit
MIN_TRAINING_POINTS = 10

class Chi311Forecaster:
    """Prophet-based forecaster for 311 demand with MLflow tracking."""

    def __init__(
            self,
            changepoint_prior_scale: float = 0.1,
            seasonality_prior_scale: float = 10.0,
            seasonality_mode: str = "multiplicative",
            experiment_name: str = "/Shared/chi311-forecasting"
    ):
            self.params = {
                  "changepoint_prior_scale": changepoint_prior_scale,
                  "seasonality_prior_scale": seasonality_prior_scale,
                  "seasonality_mode": seasonality_mode,
            }
            self.experiment_name = experiment_name
            self.model: Optional[Prophet] = None
    
    @staticmethod
    def prepare_data(
          df: pd.DataFrame,
          truncate_to_yesterday: bool = True,
    ) -> pd.DataFrame:
          """Prepare a DataFrame for Prophet training
          
          Steps:
            1. Subset to required columns (ds, y)
            2. Remove nulls in ds or y
            3. Remove duplicate timestamps (keep last)
            4. Sort chronologically
            5. Truncate to yesterday (exclude today's partial data)
            6. Validate minimum row count
        
          Args:
            df: DataFrame with at least 'ds' and 'y' columns.
            truncate_to_yesterday: If true, exclude rows where ds >= today.

          Returns:
            Cleaned DataFrame ready for Prophet.

          Raises:
            ValueError: If required columns are missing or too few rows remain,
          """
          missing = {"ds", "y"} - set(df.columns)
          if missing:
                raise ValueError(f"Missing required columns: {missing}")
          
          # Subset and copy to avoid mutating the caller's DataFrame
          pdf = df[["ds", "y"]].copy()
          pre_clean = len(pdf)

          # Ensure ds is datetime
          pdf["ds"] = pd.to_datetime(pdf["ds"])

          # Remove nulls - Prophet crashes on NaN in ds or y
          pdf = pdf.dropna(subset=["ds", "y"])

          # Remove duplicate date - Prophet requires unique timestamps
          pdf = pdf.drop_duplicates(subset=["ds"], keep="last")

          # Sort chronologically
          pdf = pdf.sort_values("ds").reset_index(drop=True)

          # Truncate to yesterday - today's data may be incomplete
          if truncate_to_yesterday:
                yesterday = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
                pdf = pdf[pdf["ds"] <= yesterday]
         
          rows_removed = pre_clean - len(pdf)
          if rows_removed > 0:
            print(
               f"Data prep: {pre_clean} -> {len(pdf)} rows"
               f"(removed {rows_removed} nulls/dupes/future)"
          )
            
          # Validate minimum data points
          if len(pdf) < MIN_TRAINING_POINTS:
               raise ValueError(
                    f"Insufficient data: {len(pdf)} rows, need at least"
                    f"{MIN_TRAINING_POINTS}. Run more ingestion batches first"
               )
          
          return pdf

    def train(self, df: pd.DataFrame, log_to_mlflow: bool = True) -> dict:
          """Train Prophet model on DataFrame with ds, y columns.
          The input DataFrame is cleaned via prepare_data() before fitting.
          """
          pdf = self.prepare_data(df)

          self.model = Prophet(
                daily_seasonality=False,
                weekly_seasonality=True,
                yearly_seasonality=True,
                **self.params
          )
          self.model.add_country_holidays(country_name="US")
          self.model.fit(pdf)

          # In-sample metrics
          forecast = self.model.predict(pdf[["ds"]])
          merged = pdf.merge(forecast[["ds", "yhat"]], on="ds")
          mape = (
               np.mean(np.abs((merged["y"] - merged["yhat"]) / merged["y"])) * 100
          )
          rmse = float(np.sqrt(np.mean((merged["y"] - merged["yhat"]) ** 2)))
          mae = float(np.mean(np.abs(merged["y"] - merged["yhat"])))

          metrics = {"mape": mape, "rmse": rmse, "mae": mae}

          if log_to_mlflow:
               mlflow.set_experiment(self.experiment_name)
               with mlflow.start_run():
                    mlflow.log_params(self.params)
                    mlflow.log_param("train_size", len(pdf))
                    mlflow.log_metrics(metrics)
                    mlflow.prophet.log_model(self.model, "prophet_model")
          return metrics
    
    def predict(self, periods: int = 7) -> pd.DataFrame:
         """Generate future predictions."""
         if self.model is None:
              raise ValueError("Model not trained. Call train () first.")
         future = self.model.make_future_dataframe(periods=periods)
         return self.model.predict(future)

        