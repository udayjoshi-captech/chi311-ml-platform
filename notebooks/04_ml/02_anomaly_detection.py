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
# Pipeline produces gold_daily_service_request_summary (request_date/total_requests);
# aliased to ds/y at load below.
GOLD_TABLE = f"{CATALOG}.gold.gold_daily_service_request_summary"
MODEL_NAME = "chi311_demand_forecast"

# Anomaly thresholds (from exploration: mean + 2σ = 4,851)
Z_SCORE_THRESHOLD = 2.0
DAY_OVER_DAY_THRESHOLD = 0.50 # 50% increase
FORECAST_RESIDUAL_THRESHOLD = 2.0 # 2 standard deviations of residuals

# COMMAND -----------

# MAGIC %md
# MAGIC ## 1. Load Data

# COMMAND -----------

df_gold = (
    spark.read.table(GOLD_TABLE)
    .withColumnRenamed("request_date", "ds")
    .withColumnRenamed("total_requests", "y")
)
pdf = df_gold.select("ds", "y").orderBy("ds").toPandas()
pdf["ds"] = pd.to_datetime(pdf["ds"])
print(f"Records: {len(pdf)}")
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

    forecast_anomalies = pdf_forecast[pdf_forecast["forecast_anomaly"]]
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
pdf["z_score"] = (pdf["y"] - pdf["rolling_mean_30d"]) / pdf["rolling_std_30d"]
pdf["zscore_anomaly"] = pdf["z_score"].abs() > Z_SCORE_THRESHOLD

zscore_anomalies = pdf[pdf["zscore_anomaly"]].dropna()
print(f"Z-score anomalies: {len(zscore_anomalies)}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 4. Method 3: Day-over-Day Change

# COMMAND -----------

# Calculate day-over-day change
pdf["dod_change"] = pdf["y"].diff()
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
    df_combined["forecast_anomaly"] = df_combined["forecast_anomaly"].fillna(False)
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
# MAGIC ## 5b. Per-Request-Type Anomalies
# MAGIC
# MAGIC The citywide ensemble above answers "was today unusual for the city?" but
# MAGIC not "which request type drove it?". This block runs detection at the
# MAGIC `sr_type` x day grain so the dashboard can drill into *what* spiked.
# MAGIC
# MAGIC Differences from the citywide model, by design:
# MAGIC - **Two detectors only** (z-score + day-over-day). The forecast-based
# MAGIC   method needs a Prophet model per series; training 100+ models is not
# MAGIC   worth it, so per-type detection omits it.
# MAGIC - **Volume floor**: only types averaging >= `MIN_TYPE_DAILY_VOLUME`
# MAGIC   requests/day over the window are scored. Rare types make rolling std
# MAGIC   unstable (a 1->3 jump reads as a 200% spike), so we exclude them.

# COMMAND -----------

MIN_TYPE_DAILY_VOLUME = 20  # avg requests/day required to score a request type
TYPE_TABLE = f"{CATALOG}.gold.gold_request_type_daily_summary"

pdf_type = (
    spark.read.table(TYPE_TABLE)
    .select("request_date", "sr_type", "total_requests")
    .orderBy("sr_type", "request_date")
    .toPandas()
)
pdf_type = pdf_type.rename(columns={"request_date": "ds", "total_requests": "y"})
pdf_type["ds"] = pd.to_datetime(pdf_type["ds"])

# Volume floor: keep only types whose mean daily volume clears the threshold.
type_means = pdf_type.groupby("sr_type")["y"].mean()
eligible_types = type_means[type_means >= MIN_TYPE_DAILY_VOLUME].index
pdf_type = pdf_type[pdf_type["sr_type"].isin(eligible_types)].copy()
print(
    f"Scoring {len(eligible_types)} request types "
    f"(>= {MIN_TYPE_DAILY_VOLUME}/day) out of {len(type_means)} total"
)


def _score_type(group: pd.DataFrame) -> pd.DataFrame:
    """Compute per-type rolling z-score and day-over-day anomaly flags."""
    group = group.sort_values("ds").copy()

    # Method 2: 30-day rolling z-score (per type)
    roll_mean = group["y"].rolling(30, min_periods=7).mean()
    roll_std = group["y"].rolling(30, min_periods=7).std()
    group["z_score"] = (group["y"] - roll_mean) / roll_std
    group["zscore_anomaly"] = group["z_score"].abs() > Z_SCORE_THRESHOLD

    # Method 3: day-over-day percentage change (per type)
    prev = group["y"].shift(1)
    group["dod_pct_change"] = (group["y"] - prev) / prev
    group["dod_anomaly"] = group["dod_pct_change"].abs() > DAY_OVER_DAY_THRESHOLD

    return group


pdf_type_scored = (
    pdf_type.groupby("sr_type", group_keys=False)
    .apply(_score_type)
    .reset_index(drop=True)
)

# NaN flags (warm-up window / first row of each type) are not anomalies.
pdf_type_scored["zscore_anomaly"] = pdf_type_scored["zscore_anomaly"].fillna(False)
pdf_type_scored["dod_anomaly"] = pdf_type_scored["dod_anomaly"].fillna(False)

# Two-detector score; flag when both agree (stricter than citywide's 2-of-3
# because there are only two detectors here).
pdf_type_scored["anomaly_score"] = (
    pdf_type_scored["zscore_anomaly"].astype(int)
    + pdf_type_scored["dod_anomaly"].astype(int)
)
pdf_type_scored["is_anomaly"] = pdf_type_scored["anomaly_score"] >= 2

type_anomaly_count = int(pdf_type_scored["is_anomaly"].sum())
print(f"Per-type anomalies (both methods agree): {type_anomaly_count}")

# COMMAND -----------

# MAGIC %md
# MAGIC ## 6. Save Results

# COMMAND -----------

# Save citywide anomaly results to Gold
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

# Save per-type anomaly results to Gold
df_type_anomaly_results = spark.createDataFrame(
    pdf_type_scored[["ds", "sr_type", "y", "z_score", "dod_pct_change",
                     "zscore_anomaly", "dod_anomaly", "anomaly_score",
                     "is_anomaly"]]
)
df_type_anomaly_results = df_type_anomaly_results.withColumn(
    "detection_timestamp", F.current_timestamp()
)

df_type_anomaly_results.write.mode("overwrite").saveAsTable(
    f"{CATALOG}.gold.gold_anomaly_results_by_type"
)
print(
    f"Saved per-type anomaly results to "
    f"{CATALOG}.gold.gold_anomaly_results_by_type"
)

# COMMAND -----------

# Log to MLflow
with mlflow.start_run(run_name="anomaly_detection"):
    mlflow.log_param("z_score_threshold", Z_SCORE_THRESHOLD)
    mlflow.log_param("dod_threshold", DAY_OVER_DAY_THRESHOLD)
    mlflow.log_param("forecast_residual_threshold", FORECAST_RESIDUAL_THRESHOLD)
    mlflow.log_metric("total_anomalies", int(total_anomalies))
    mlflow.log_metric("anomaly_rate_pct", float(total_anomalies / len(df_combined) * 100))
    mlflow.log_metric("zscore_anomalies", int(df_combined["dod_anomaly"].sum()))
    mlflow.log_param("min_type_daily_volume", MIN_TYPE_DAILY_VOLUME)
    mlflow.log_metric("eligible_request_types", int(len(eligible_types)))
    mlflow.log_metric("type_anomalies", type_anomaly_count)
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