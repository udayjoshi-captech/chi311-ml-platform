# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - ML forecasting (Prophet + MLflow)
# MAGIC
# MAGIC **Purpose**: Train Prophet time-series model on Gold daily aggregates
# MAGIC
# MAGIC **Pattern**: Gold Table -> Feature Engineering -> Prophet -> MLflow Tracking
# MAGIC
# MAGIC **Cloud**: Azure Databricks with MLflow
# MAGIC
# MAGIC **Note**: `cmdstanpy==1.2.4` and `prophet==1.1.6` are installed as task
# MAGIC libraries (see `databricks.yml`), so no notebook-scoped `%pip` /
# MAGIC `%restart_python` is needed. cmdstanpy is pinned deliberately: prophet
# MAGIC 1.1.6 bundles a prebuilt Stan binary that cmdstanpy 1.3.0's stricter path
# MAGIC validation rejects, which surfaces as the misleading
# MAGIC "'Prophet' object has no attribute 'stan_backend'". 1.2.4 loads it.

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
CATALOG = "chi311"
# The Lakeflow pipeline produces gold_daily_service_request_summary with
# request_date / total_requests; Prophet needs ds / y (aliased at load below).
GOLD_TABLE = f"{CATALOG}.gold.gold_daily_service_request_summary"
EXPERIMENT_NAME = "/Shared/chi311-forecasting"
MODEL_NAME = "chi311_demand_forecast"

# COMMAND -----------

# MAGIC %md
# MAGIC ## 1. Load Data from Gold Layer

# COMMAND -----------

# Real Gold table (Prophet expects 'ds' and 'y' columns).
# Alias request_date -> ds and total_requests -> y from the daily summary.
df_gold = (
    spark.read.table(GOLD_TABLE)
    .withColumnRenamed("request_date", "ds")
    .withColumnRenamed("total_requests", "y")
)
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

# Reserve last ~33% (243 days) for testing to maintain same proportion as before
split_date = df_features["ds"].max() - pd.Timedelta(days=243)
train = df_features[df_features["ds"] <= split_date].copy()
test = df_features[df_features["ds"] > split_date].copy()

print(f"Training: {len(train)} days ({train['ds'].min()} to {train['ds'].max()})")
print(f"Testing: {len(test)} days ({test['ds'].min()} to {test['ds'].max()})")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 4. Model Training with MLflow

# COMMAND -----------

# Set MLflow experiment
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND -----------

# Train with hyperparameter grid
param_grid = [
    {"changepoint_prior_scale": 0.05, "seasonality_prior_scale": 10.0, "seasonality_mode": "multiplicative"},
    {"changepoint_prior_scale": 0.1, "seasonality_prior_scale": 10.0, "seasonality_mode": "additive"},
    {"changepoint_prior_scale": 0.5, "seasonality_prior_scale": 1.0, "seasonality_mode": "multiplicative"},
]

best_mape = float("inf")
best_run_id = None
best_params = None

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
        future = model.make_future_dataframe(periods=len(test))
        forecast = model.predict(future)

        # Merge with actuals
        test_forecast = forecast[forecast["ds"].isin(test["ds"])][["ds", "yhat", "yhat_lower", "yhat_upper"]]
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

        print(f"    Run {i+1}: MAPE={mape:.2f}%, RMSE={rmse:.1f}, MAE={mae:.1f}")

        if mape < best_mape:
            best_mape = mape
            best_run_id = mlflow.active_run().info.run_id
            best_params = params

print(f"\n Best run: {best_run_id} (MAPE: {best_mape:.2f}%)")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 5. Cross-Validation (Best Model)

# COMMAND -----------

# Reload best model for cross-validation
best_model_uri = f"runs:/{best_run_id}/prophet_model"
best_model = mlflow.prophet.load_model(best_model_uri)

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
# MAGIC ## 6. Refit on Full History + Register Production Model
# MAGIC
# MAGIC The grid-search models were fit on the training split only (to keep the
# MAGIC test split held out for honest MAPE). The production model that generates
# MAGIC the live 7-day forecast must be refit on the ENTIRE history using the
# MAGIC winning hyperparameters — otherwise make_future_dataframe() extends only
# MAGIC 7 days past the training max, which is still ~243 days behind the latest
# MAGIC actuals, and the future-only slice comes back empty.

# COMMAND -----------

# Refit the winning config on all available data
prod_model = Prophet(
    changepoint_prior_scale=best_params["changepoint_prior_scale"],
    seasonality_prior_scale=best_params["seasonality_prior_scale"],
    seasonality_mode=best_params["seasonality_mode"],
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=True
)
prod_model.add_country_holidays(country_name="US")
prod_model.fit(df_features)

# Register the full-history production model in MLflow Model Registry
with mlflow.start_run(run_name="prophet_production_full_fit"):
    mlflow.log_params(best_params)
    mlflow.log_param("train_size", len(df_features))
    mlflow.log_metric("selection_mape", best_mape)
    mlflow.prophet.log_model(prod_model, "prophet_model")
    prod_run_id = mlflow.active_run().info.run_id

    model_version = mlflow.register_model(
        model_uri=f"runs:/{prod_run_id}/prophet_model",
        name=MODEL_NAME,
        tags={"stage": "staging", "data_source": "chi311_gold", "fit": "full_history"}
    )
    print(f" Registered production model: {MODEL_NAME} v{model_version.version}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 7. Generate 7-Day Forecast

# COMMAND -----------

# Generate forecast from the full-history production model
future_7d = prod_model.make_future_dataframe(periods=7)
forecast_7d = prod_model.predict(future_7d)

# Show next 7 days (strictly future of the latest actual)
forecast_next = forecast_7d[forecast_7d["ds"] > df_features["ds"].max()][
    ["ds", "yhat", "yhat_lower", "yhat_upper"]
].round(0)

# Guard: the future slice must have exactly the horizon we asked for.
if forecast_next.empty:
    raise ValueError(
        "7-day forecast is empty — the production model was not fit on full "
        f"history (latest actual: {df_features['ds'].max()}). Check the refit step."
    )
print(f"7-Day Forecast ({len(forecast_next)} days):")
display(spark.createDataFrame(forecast_next))

# COMMAND -----------

# Save predictions to Gold layer.
# Map Prophet's output columns (ds/yhat/...) to the gold_forecasts table schema
# so the append matches the existing Delta table exactly:
#   forecast_date, predicted_requests, prediction_lower, prediction_upper,
#   prediction_generated_at, model_name, model_version
df_predictions = (
    spark.createDataFrame(forecast_next)
    .withColumnRenamed("ds", "forecast_date")
    .withColumnRenamed("yhat", "predicted_requests")
    .withColumnRenamed("yhat_lower", "prediction_lower")
    .withColumnRenamed("yhat_upper", "prediction_upper")
    .withColumn("prediction_generated_at", F.current_timestamp())
    .withColumn("model_name", F.lit(MODEL_NAME))
    .withColumn("model_version", F.lit(model_version.version))
    .select(
        "forecast_date",
        "predicted_requests",
        "prediction_lower",
        "prediction_upper",
        "prediction_generated_at",
        "model_name",
        "model_version",
    )
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


