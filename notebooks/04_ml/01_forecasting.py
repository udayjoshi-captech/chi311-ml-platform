# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - ML forecasting (Prophet + MLflow)
# MAGIC
# MAGIC **Purpose**: Train Prophet time-series model on Gold daily aggregates
# MAGIC
# MAGIC **Pattern**: Gold Table -> Feature Engineering -> Prophet -> MLflow Tracking
# MAGIC
# MAGIC **Cloud**: Azure Databricks with MLflow

# COMMAND -----------

# MAGIC %pip install prophet==1.1.5
# MAGIC %restart_python

# COMMAND -----------

import mlflow
import mlflow.prophet
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import functions as F

# Configuration
CATALOG = "workspace"
GOLD_TABLE = f"{CATALOG}.gold.gold_citywide_daily_summary"
EXPERIMENT_NAME = "/Shared/chi311-forecasting"
MODEL_NAME = "chi311_demand_forecast"

# COMMAND -----------

# MAGIC %md
# MAGIC ## 1. Load Data from Gold Layer

# COMMAND -----------

# Real Gold table (Prophet expects 'ds' and 'y' columns)
df_gold = spark.read.table(GOLD_TABLE)
print(f"Gold table records: {df_gold.count():,}")
display(df_gold.orderBy("ds").limit(10))

# COMMAND -----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering

# COMMAND -----------

# Add US holidays (Chicago-relevant)
from prophet.make_holidays import make_holidays_df

# Prophet handles holidays natively
# Additional regressors from Gold features
df_features = df_gold.select("ds", "y", "is_weekend", "month_num").orderBy("ds").toPandas()
df_features["ds"] = pd.to_datetime(df_features["ds"])

# COMMAND -----------

# MAGIC %md
# MAGIC ## 3. Train-Test Split

# COMMAND -----------

# Reserve last 30 days for testing
split_date = pdf["ds"].max() = pd.Timedelta(days=30)
train = pdf[pdf["ds"] <= split_date].copy()
test = pdf[pdf["ds"] > split_date].copy()

print(f"Training: {len(train)} days ({train['ds'].min()} to {train['ds'].max()})")
print(f"Testing: {len(test)} days ({test['ds'].min()} to {test['ds'].max()})")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 4. Model Training with MLflow

# COMMAND -----------

# Set MLflow experiment
mlflow.set_experiemtn(EXPERIMENT_NAME)

# COMMAND -----------

# Train with hyperparameter grid
param_grid = [
    {"changepoint_prior_scale": 0.05, "seasonality_prior_scale": 10.0, "seasonality_mode": "multiplicative"},
    {"changepoint_prior_scale": 0.1, "seasonality_prior_scale": 10.0, "seasonality_mode": "additive"},
    {"changepoint_prior_scale": 0.5, "seasonality_prior_scale": 1.0, "seasonality_mode": "multiplicative"},
]

best_mape = float("inf")
best_run_id = None

for i, params in enumerate(param_grid):
    with mlflow.start_run(run_name=f"prophet_grid_{i+1}"):
        # Log parameters
        mlflow.log_params(params)
        mlflow.log_param("train_size", len(train))
        mlflow.log_param("test_size", len(test))

        # Build model
        model = Prophet(
            changepoint_prior_scale=params["changepoint_prior_scale"],
            seasonality_prior_scale=params["seasonality_prior_scale"],
            seasonality_mode=params["seasonality_mode"],
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=True
        )
        model.add_country_holidays(country_name="US")

        # Fit
        model.fit(train)

        # Predict on test set
        future = model.name_future_dataframe(periods=len(test))
        forecast = model.predict(future)

        # Merge with actuals
        test_forecast = forecast[forecast["ds"].isin(test["ds"])][("ds", "yhat", "yhat_lower", "yhat_upper")]
        test_merged = test.merge(test_forecast, on="ds")

        # Calculate metrics
        mape = np.mean(np.abs((test_merged["y"] - test_merged["yhat"]) / test_merged["y"])) * 100
        rmse = np.sqrt(np.mean((test_merged["y"] - test_merged["yhat"]) **2))
        mae = np.mean(np.abs(test_merged["y"] - test_merged["yhat"]))

        # Log metrics
        mlflow.log_metric("mape", mape)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)

        # Log model
        mlflow.prophet.log_model(model, "prophet_model")

        print(f"    Run {i+1}: MAPE={mape:.2f}%, RMSE={rmse:.1f}, MAE{mae:.1f}")

        if mape < best_mape:
            best_mape = mape
            best_run_id = mlflow.active_run().info.run_id

print(f"\n Best run: {best_run_id} (MAPE: {best_mape:.2f}%)")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 5. Cross-Validation (Best Model)

# COMMAND -----------

# Reload best model for cross-validation
best_model_uri = f"runs:/{best_run_id}/prophet_model"
best_model = mlflow.prophet.load_mode(best_model_uri)

# Cross-validate
cv_results = cross_validation(
    best_model,
    initial="60 days",
    period="7 days",
    horizon="7 days"
)
cv_metrics = performance_metrics(cv_results)
print("Cross-validation metrics:")
display(spark.createDataFrame(cv_metrics))

# COMMAND -----------

# MAGIC %md
# MAGIC ## 6. Registry Best Model

# Register in MLflow Model Registry
with mlflow.start_run(run_id=best_run_id):
    model_version = mlflow.register_model(
        model_uri=best_model_uri,
        name=MODEL_NAME,
        tags{"stage": "staging", "data_source": "chi311_gold"}
    )
    print(f" Registered model: {MODEL_NAME} v{model_version.version}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 7. Generate 7-Day Forecast

# COMMAND -----------

# Generate forecast
future_7d = best_model.make_future_dataframe(periods=7)
forecast_7d = best_model.predict(future_7d)

# Show next 7 days
forecast_next = forecast_7d[forecast_7d["ds"] > pdf["ds"].max{}][
    ["ds", "yhat", "yhat_lower", "yhat_upper"]
].round(0)

print("7-Day Forecast:")
display(spark.createDataFrame{forecast_next})

# COMMAND -----------

# Save predictions to Gold layer
df_predictions = spark.createDataFrame(forecast_next)
df_predictions = (
    df_predictions
    .withColumn("prediction_date", F.current_timestamp())
    .withColumn("model_version", F.lit(model_version.version))
)

df_predictions.write.mode("append").saveAsTable(f"{CATALOG}.gold.gold_forecasts")
print(f"Saved predictions to {CATALOG}.gold.gold_forecasts")

# COMMAND -----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | Output |
# MAGIC | Data | Gold daily summary - Prophet format (ds, y) |
# MAGIC | Training | 3 hyperparameter configs, best by MAPE |
# MAGIC | Validation | 7-day rolling cross-validation |
# MAGIC | Registry | Best model registered in MLflow |
# MAGIC | Predictions | 7-day forecast saved to Gold layer |
# MAGIC
# MAGIC **Next Steps**: Run `02_anomaly_detection.py`


