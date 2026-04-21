# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Anomaly Detection
# MAGIC
# MAGIC **Purpose**: Detect anomalous spokes in 311 service request volumes
# MAGIC
# MAGIC **Methods**: 
# MAGIC 1. Forecast-based (Prophet residuals)
# MAGIC 2. Statistical (Z-Score)
# MAGIC 3. Day-over-day change
# MAGIC
# MAGIC **Cloud**: Azure Databricks

# COMMAND -----------

import mlflow
import mlflow.prophet
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Configuration
CATALOG = "chi311"
GOLD_TABLE = f"{CATALOG}.gold.gold_citywide_daily_summary"
MODEL_NAME = "chi311_demand_forecast"

# Anomaly thresholds (from exploration: mean + 2σ = 4,851)
Z_SCORE_THRESHOLD = 2.0
DAY_OVER_DAY_THRESHOLD = 0.50 # 50% increase
FORECAST_RESIDUAL_THRESHOLD = 2.0 # 2 standard deviations of residuals

# COMMAND -----------

# MAGIC %md
# MAGIC ## 1. Load Data

# COMMAND -----------

df_gold = spark.read.table(GOLD_TABLE)
pdf = df_gold.select("ds", "y").orderBy("ds").toPandas()
pdf["ds"] = pd.to_datetime(pdf["ds"])
print(f"Records: {len(pdf):.}")
print(f"Mean daily requests: {pdf['y'].mean():.0f}")
print(f"Std: {pdf['y'].std():.0f}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 2. Method 1: Forecast-Based Anomalies

# COMMAND -----------

# Load latest registered model
try:
    model = mlflow.prophet.load_model(f"models:/{MODEL_NAME}/latest")
    print(f"Loaded model: {MODEL_NAME}")

    # Generate in-sample predictions
    forecast = model.predict(pdf[["ds"]])

    # Calculate residuals
    pdf_forecast = pdf.merge(
        forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]], on="ds"
    )
    pdf_forecast["residual"] = pdf_forecast["y"] - pdf_forecast["yhat"]
    residual_std = pdf_forecast["residual"].std()

    # Flag anomalies: residual > threshold * std
    pdf_forecast["forecast_anomaly"] = (
        pdf_forecast["residual"].abs() > FORECAST_RESIDUAL_THRESHOLD * residual_std
    )

    forecast_anomalies = pdf_forecast[pdf_forecast]["forecast_anomaly"]
    print(f"Forecast-based anomalies: {len(forecast_anomalies)}")

except Exception as e:
    print(f"Could not load model: {e}")
    print("Skipping forecast-based detection. Run 01_forecasting.py first.")
    pdf_forecast = pdf.copy()
    pdf_forecast["forecast_anomaly"] = False


# COMMAND -----------

# MAGIC %md
# MAGIC ## 3. Method 2: Z-Score Anomalies

# COMMAND -----------

# Rolling 30-day Z-score
pdf["rolling_mean_30d"] = pdf["y"].rolling(30, min_periods=7).mean()
pdf["rolling_std_30d"] = pdf["y"].rolling(30, min_periods=7).std()
pdf["z_score"] = (pdf["y"] - pdf["rolling_mean_30d"]) / pdf("rolling_std_30d")
pdf["zscore_anomaly"] = pdf["z_score"].abs() > Z_SCORE_THRESHOLD

zscore_anomalies = pdf[pdf["zscore_anomaly"]].dropna()
print(f"Z-score anomalies: {len(zscore_anomalies)}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 4. Method 3: Day-over-Day Change

# COMMAND -----------

pdf["prev_day_y"] = pdf["y"].shift(1)
pdf["dod_anomaly"] = pdf["dod_change"].abs() > DAY_OVER_DAY_THRESHOLD

dod_anomalies = pdf[pdf["dod_anomaly"]].dropna()
print(f"Day-over-day anomalies: {len(dod_anomalies)}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 5. Ensemble: Combined Anomaly Score

# COMMAND -----------

# Merge all methods
df_combined = pdf[["ds", "y", "zscore_anomaly", "dod_anomaly"]].copy()

if "forecast_anomaly" in pdf_forecast.columns:
    df_combined = df_combined.merge(
        pdf_forecast[["ds", "forecast_anomaly"]], on="ds", how="left"
    )
    df_combined["forest_anomaly"] = df_combined["forecast_anomaly"].fillna(False)
else:
    df_combined["forecast_anomaly"] = False

# Anomaly score: number of methods that flagged the day
df_combined["anomaly_score"] = (
    df_combined["zscore_anomaly"].astype(int) +
    df_combined["dod_anomaly"].astype(int) +
    df_combined["forecast_anomaly"].astype(int)
)

# Final anomaly flag: 2+ methods agree
df_combined["is_anomaly"] = df_combined["anomaly_score"] >= 2

total_anomalies = df_combined["is_anomaly"].sum()
print(f"\n Total ensemble anomalies (2+ methods): {total_anomalies}")
print(f"Anomaly rate: {total_anomalies / len(df_combined) *100:.1f}%")

# COMMAND -----------

# Display top anomalies
display(
    spark.createDataFrame(
        df_combined[df_combined["is_anomaly"]].sort_values("ds", ascending=False).head(20)
    )
)

# COMMAND -----------

# MAGIC %md
# MAGIC ## 6. Save Results

# COMMAND -----------

# Save anomaly results to Gold
df_anomaly_results = spark.createDataFrame(
    df_combined[["ds", "y", "anomaly_score", "is_anomaly",
                 "zscore_anomaly", "dod_anomaly", "forecast_anomaly"]]
)
df_anomaly_results = df_anomaly_results.withColumn(
    "detection_timestamp", F.current_timestamp()
)

df_anomaly_results.write.mode("overwrite").saveAsTable(
    f"{CATALOG}.gold.gold_anomaly_results"
)
print(f"Saved anomaly results to {CATALOG}.gold.gold_anomaly_results")

# COMMAND -----------

# Log to MLflow
with mlflow.start_run(run_name="anomaly_detection"):
    mlflow.log_param("z_score_threshold", Z_SCORE_THRESHOLD)
    mlflow.log_param("dod_threshold", DAY_OVER_DAY_THRESHOLD)
    mlflow.log_param("forecast_residual_threshold", FORECAST_RESIDUAL_THRESHOLD)
    mlflow.log_metric("total_anomalies", int(total_anomalies))
    mlflow.log_metric("anomaly_rate_pct", float(total_anomalies / len(df_combined) * 100))
    mlflow.log_metric("zscore_anomalies", int(df_combined["dod_anomaly"].sum()))
    print("Logged anomaly detection run to MLflow")

# COMMAND -----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Method | Description | Threshold | Anomalies |
# MAGIC |------- |------------ |-----------|-----------|
# MAGIC | Forecast | Prophet residuals > 2σ | 2.0 | Varies |
# MAGIC | Z-Score | 30-day rolling z > 2 | 2.0 | Varies |
# MAGIC | Day-over-Day | >50% change | 0.50 | Varies |
# MAGIC | **Ensemble** | 2+ methods agree | 2/3 | Final count |