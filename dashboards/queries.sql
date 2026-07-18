-- =========================================================================
-- Databricks SQL Dashboard Queries for Chi311 ML Platform
-- =========================================================================
-- These queries power the native Databricks dashboard.
-- Column names and table names verified against the actual objects created by
-- the Lakeflow pipeline and ML/DQ notebooks (audited 2026-07-18).
--
-- Table sources & real schemas:
--   gold_daily_service_request_summary (Lakeflow pipeline)
--     request_date, total_requests, open_count, completed_count,
--     canceled_count, unique_request_types, avg_resolution_hours,
--     unique_wards, unique_community_areas, day_of_week_num, is_weekend,
--     month_num, year_num
--   gold_forecasts (04_ml/01_forecasting.py)
--     ds, yhat, yhat_lower, yhat_upper, prediction_date, model_version
--   gold_anomaly_results (04_ml/02_anomaly_detection.py)
--     ds, y, anomaly_score, is_anomaly, zscore_anomaly, dod_anomaly,
--     forecast_anomaly, detection_timestamp
--   dq_checkpoint_results (03_data_quality/01_data_quality_checks.py)
--     run_date, layer, success, expectations_evaluated, expectations_passed,
--     expectations_failed, pass_rate_pct, logged_at
--
-- NOT YET AVAILABLE (tables not created because PipelineMetrics /
-- PredictionLogger are not called from any notebook):
--   pipeline_run_log      -> widgets 1.4, 3.2, 3.3, 3.6, 3.7 (deferred)
--   gold_prediction_log   -> MAPE-over-time (deferred)
-- Wire up PipelineMetrics (see reconciliation work) to enable those, then
-- un-comment the deferred queries at the bottom of each tab.
-- =========================================================================


-- =========================================================================
-- TAB 1: OVERVIEW - KPIs and Recent Trends
-- =========================================================================

-- Query 1.1a: Total Requests Last 7 Days (Counter)
SELECT
  'Total Requests (Last 7 Days)' AS metric,
  SUM(total_requests) AS value
FROM chi311.gold.gold_daily_service_request_summary
WHERE request_date >= CURRENT_DATE() - INTERVAL 7 DAYS;

-- Query 1.1b: Avg Daily Requests Last 30 Days (Counter)
SELECT
  'Avg Daily Requests (Last 30 Days)' AS metric,
  ROUND(AVG(total_requests), 0) AS value
FROM chi311.gold.gold_daily_service_request_summary
WHERE request_date >= CURRENT_DATE() - INTERVAL 30 DAYS;

-- Query 1.1c: Next 7-Day Forecast Avg (Counter)
-- Replaces the old "Latest Model MAPE" counter — MAPE is only logged to MLflow,
-- not persisted to a queryable table. This shows the mean predicted volume from
-- the most recent forecast run instead.
SELECT
  'Avg Forecast (Next 7 Days)' AS metric,
  ROUND(AVG(yhat), 0) AS value
FROM chi311.gold.gold_forecasts
WHERE prediction_date = (SELECT MAX(prediction_date) FROM chi311.gold.gold_forecasts);

-- Query 1.1d: Active Anomalies Last 7 Days (Counter)
SELECT
  'Active Anomalies (Last 7 Days)' AS metric,
  COUNT(*) AS value
FROM chi311.gold.gold_anomaly_results
WHERE ds >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND is_anomaly = TRUE;


-- Query 1.2: Daily Request Trends (Line Chart)
-- X-axis: date, Y-axis: daily_requests
SELECT
  request_date AS date,
  total_requests AS daily_requests
FROM chi311.gold.gold_daily_service_request_summary
WHERE request_date >= CURRENT_DATE() - INTERVAL 90 DAYS
ORDER BY request_date;


-- Query 1.3: Day of Week Pattern (Bar Chart)
-- X-axis: day_name (sort by day_order), Y-axis: avg_requests
SELECT
  CASE DAYOFWEEK(request_date)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name,
  DAYOFWEEK(request_date) AS day_order,
  ROUND(AVG(total_requests), 0) AS avg_requests
FROM chi311.gold.gold_daily_service_request_summary
WHERE request_date >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY DAYOFWEEK(request_date)
ORDER BY day_order;


-- Query 1.4: Recent Pipeline Runs Status (Table) -- DEFERRED
-- Requires chi311.gold.pipeline_run_log, which is not yet created (PipelineMetrics
-- is not called from any notebook). Enable after wiring up PipelineMetrics.
-- SELECT
--   task_name, status, rows_in, rows_out, rows_dropped,
--   ROUND(duration_seconds, 1) AS duration_sec, logged_at
-- FROM chi311.gold.pipeline_run_log
-- ORDER BY logged_at DESC
-- LIMIT 10;


-- =========================================================================
-- TAB 2: FORECASTS - 7-Day Predictions
-- =========================================================================

-- Query 2.1: Latest Forecast + Confidence Interval vs Recent Actuals (Line Chart)
-- Multi-series: actual (historical), predicted, lower/upper bounds.
-- Forecasts come from gold_forecasts; actuals from the daily summary
-- (request_date aliased to ds for the join).
WITH latest_forecast AS (
  SELECT ds, yhat, yhat_lower, yhat_upper
  FROM chi311.gold.gold_forecasts
  WHERE prediction_date = (SELECT MAX(prediction_date) FROM chi311.gold.gold_forecasts)
),
recent_actuals AS (
  SELECT request_date AS ds, total_requests AS actual
  FROM chi311.gold.gold_daily_service_request_summary
  WHERE request_date >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT
  COALESCE(a.ds, f.ds) AS date,
  a.actual,
  f.yhat        AS predicted,
  f.yhat_lower  AS lower_bound,
  f.yhat_upper  AS upper_bound,
  CASE WHEN a.ds IS NOT NULL THEN 'Historical' ELSE 'Forecast' END AS data_type
FROM recent_actuals a
FULL OUTER JOIN latest_forecast f ON a.ds = f.ds
ORDER BY date;


-- Query 2.2: Forecast Summary Table
SELECT
  ds AS forecast_date,
  ROUND(yhat, 0)       AS predicted_requests,
  ROUND(yhat_lower, 0) AS lower_bound,
  ROUND(yhat_upper, 0) AS upper_bound,
  ROUND(yhat_upper - yhat_lower, 0) AS uncertainty_range,
  model_version,
  DATE_FORMAT(prediction_date, 'yyyy-MM-dd HH:mm:ss') AS generated_at
FROM chi311.gold.gold_forecasts
WHERE prediction_date = (SELECT MAX(prediction_date) FROM chi311.gold.gold_forecasts)
ORDER BY ds;


-- Query 2.3: Prediction vs Actual Comparison (Scatter Plot)
-- X-axis: actual, Y-axis: predicted. Points near the y=x line are accurate.
-- Joins gold_forecasts to actuals on ds = request_date.
SELECT
  a.total_requests AS actual,
  f.yhat           AS predicted,
  ROUND(ABS(a.total_requests - f.yhat) / a.total_requests * 100, 1) AS error_pct,
  f.ds AS date
FROM chi311.gold.gold_forecasts f
INNER JOIN chi311.gold.gold_daily_service_request_summary a
  ON f.ds = a.request_date
WHERE a.total_requests > 0
ORDER BY f.ds DESC
LIMIT 100;


-- Query 2.4: Model MAPE Over Time (Line Chart) -- DEFERRED
-- MAPE per run is not persisted anywhere queryable (logged to MLflow only).
-- To enable: have the forecasting job append run-level MAPE to a Delta table,
-- or wire up PredictionLogger, then chart it here.


-- =========================================================================
-- TAB 3: MONITORING - Data Quality, Drift, Anomalies
-- =========================================================================

-- Query 3.1a: Total DQ Checks Run Last 7 Days (Counter)
SELECT
  'Total DQ Runs (Last 7 Days)' AS metric,
  COUNT(*) AS value
FROM chi311.gold.dq_checkpoint_results
WHERE logged_at >= CURRENT_DATE() - INTERVAL 7 DAYS;

-- Query 3.1b: Failed DQ Checks Last 7 Days (Counter)
SELECT
  'Failed DQ Runs (Last 7 Days)' AS metric,
  COUNT(*) AS value
FROM chi311.gold.dq_checkpoint_results
WHERE logged_at >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND success = FALSE;

-- Query 3.1c: Data Quality Pass Rate Last 30 Days (Counter)
SELECT
  'Data Quality Pass Rate' AS metric,
  CONCAT(ROUND(SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1), '%') AS value
FROM chi311.gold.dq_checkpoint_results
WHERE logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS;


-- Query 3.2: Data Quality Pass Rate by Layer Over Time (Line Chart)
-- Replaces the pipeline_run_log-based "pipeline health" chart with something
-- backed by a table that actually exists.
SELECT
  run_date AS date,
  layer,
  ROUND(AVG(pass_rate_pct), 1) AS avg_pass_rate_pct
FROM chi311.gold.dq_checkpoint_results
WHERE logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY run_date, layer
ORDER BY date, layer;


-- Query 3.3: Anomaly Detection Results (Table with Highlighting)
-- Conditional format: highlight rows where anomaly_detected = TRUE.
SELECT
  ds AS date,
  y AS actual_requests,
  anomaly_score,
  is_anomaly AS anomaly_detected,
  zscore_anomaly,
  dod_anomaly,
  forecast_anomaly,
  detection_timestamp
FROM chi311.gold.gold_anomaly_results
WHERE ds >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY ds DESC;


-- Query 3.4: Anomaly Rate Over Time (Line Chart)
-- Share of days flagged anomalous, weekly.
SELECT
  DATE_TRUNC('WEEK', ds) AS week,
  ROUND(SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS anomaly_rate_pct,
  COUNT(*) AS days_evaluated
FROM chi311.gold.gold_anomaly_results
WHERE ds >= CURRENT_DATE() - INTERVAL 180 DAYS
GROUP BY DATE_TRUNC('WEEK', ds)
ORDER BY week;


-- Query 3.5: Forecast Accuracy / Drift (Line Chart with threshold) -- optional
-- Daily absolute percentage error between the latest forecast and actuals.
-- Uses gold_forecasts vs actuals (no pipeline_run_log needed). Add a reference
-- line at 20 for the drift threshold.
SELECT
  f.ds AS date,
  ROUND(ABS(a.total_requests - f.yhat) / a.total_requests * 100, 2) AS mape_percent,
  20.0 AS drift_threshold
FROM chi311.gold.gold_forecasts f
INNER JOIN chi311.gold.gold_daily_service_request_summary a
  ON f.ds = a.request_date
WHERE a.total_requests > 0
ORDER BY f.ds;


-- -------------------------------------------------------------------------
-- DEFERRED (require chi311.gold.pipeline_run_log — not yet created).
-- Enable after wiring up PipelineMetrics in the ingestion/pipeline stages.
-- -------------------------------------------------------------------------
-- Query 3.6: Pipeline Health Over Time (Stacked Area)
-- SELECT DATE(logged_at) AS date, status, COUNT(*) AS run_count
-- FROM chi311.gold.pipeline_run_log
-- WHERE logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS
-- GROUP BY DATE(logged_at), status ORDER BY date, status;
--
-- Query 3.7: Data Volume Trend (Line Chart)
-- SELECT DATE(logged_at) AS date, task_name, SUM(rows_out) AS total_rows_processed
-- FROM chi311.gold.pipeline_run_log
-- WHERE logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS
-- GROUP BY DATE(logged_at), task_name ORDER BY date, task_name;
--
-- Query 3.8: Task Duration Trends (Bar Chart)
-- SELECT task_name, ROUND(AVG(duration_seconds), 1) AS avg_duration_sec,
--        ROUND(MAX(duration_seconds), 1) AS max_duration_sec, COUNT(*) AS run_count
-- FROM chi311.gold.pipeline_run_log
-- WHERE logged_at >= CURRENT_DATE() - INTERVAL 7 DAYS AND status = 'SUCCESS'
-- GROUP BY task_name ORDER BY avg_duration_sec DESC;
--
-- Query 3.9: Recent Errors (Table)
-- SELECT task_name, error_message, logged_at, rows_in, rows_out
-- FROM chi311.gold.pipeline_run_log
-- WHERE status = 'FAILED' AND logged_at >= CURRENT_DATE() - INTERVAL 7 DAYS
-- ORDER BY logged_at DESC LIMIT 20;


-- =========================================================================
-- REFRESH SCHEDULE RECOMMENDATIONS
-- =========================================================================
-- Tab 1 (Overview): Refresh every 1 hour
-- Tab 2 (Forecasts): Refresh every 6 hours (or when ML job completes)
-- Tab 3 (Monitoring): Refresh every 30 minutes
-- =========================================================================
