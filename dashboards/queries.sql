-- =========================================================================
-- Databricks SQL Dashboard Queries for Chi311 ML Platform
-- =========================================================================
-- These queries power the native Databricks dashboard that replaces Streamlit
-- Use these queries when creating visualizations in Databricks SQL Dashboard
-- =========================================================================

-- =========================================================================
-- TAB 1: OVERVIEW - KPIs and Recent Trends
-- =========================================================================

-- Query 1.1: Key Metrics (Single Value Visualizations)
-- Use as: Counter widgets showing current state
SELECT
  'Total Requests (Last 7 Days)' AS metric,
  SUM(y) AS value
FROM chi311.gold.gold_daily_service_request_summary
WHERE ds >= CURRENT_DATE() - INTERVAL 7 DAYS;

SELECT
  'Avg Daily Requests (Last 30 Days)' AS metric,
  ROUND(AVG(y), 0) AS value
FROM chi311.gold.gold_daily_service_request_summary
WHERE ds >= CURRENT_DATE() - INTERVAL 30 DAYS;

SELECT
  'Latest Model MAPE' AS metric,
  CONCAT(ROUND(mape * 100, 1), '%') AS value
FROM (
  SELECT mape
  FROM chi311.gold.gold_prediction_log
  WHERE model_version IS NOT NULL
  ORDER BY logged_at DESC
  LIMIT 1
);

SELECT
  'Active Anomalies (Last 7 Days)' AS metric,
  COUNT(*) AS value
FROM chi311.gold.gold_anomaly_results
WHERE ds >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND is_anomaly = TRUE;


-- Query 1.2: Daily Request Trends (Line Chart)
-- Visualization: Line chart, X-axis: ds, Y-axis: y
SELECT
  ds AS date,
  y AS daily_requests
FROM chi311.gold.gold_daily_service_request_summary
WHERE ds >= CURRENT_DATE() - INTERVAL 90 DAYS
ORDER BY ds;


-- Query 1.3: Day of Week Pattern (Bar Chart)
-- Visualization: Bar chart, X-axis: day_name, Y-axis: avg_requests
SELECT
  CASE DAYOFWEEK(ds)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name,
  DAYOFWEEK(ds) AS day_order,
  ROUND(AVG(y), 0) AS avg_requests
FROM chi311.gold.gold_daily_service_request_summary
WHERE ds >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY DAYOFWEEK(ds)
ORDER BY day_order;


-- Query 1.4: Recent Pipeline Runs Status (Table)
-- Visualization: Table showing recent pipeline execution status
SELECT
  task_name,
  status,
  rows_in,
  rows_out,
  rows_dropped,
  ROUND(duration_seconds, 1) AS duration_sec,
  logged_at
FROM chi311.gold.pipeline_run_log
ORDER BY logged_at DESC
LIMIT 10;


-- =========================================================================
-- TAB 2: FORECASTS - 7-Day Predictions
-- =========================================================================

-- Query 2.1: Latest 7-Day Forecast (Line Chart with Confidence Interval)
-- Visualization: Line chart with multiple series
-- Series 1: Actual (historical), Series 2: Predicted, Series 3-4: Confidence bounds
WITH latest_predictions AS (
  SELECT
    ds,
    yhat,
    yhat_lower,
    yhat_upper,
    model_version,
    logged_at
  FROM chi311.gold.gold_prediction_log
  WHERE logged_at = (SELECT MAX(logged_at) FROM chi311.gold.gold_prediction_log)
),
recent_actuals AS (
  SELECT
    ds,
    y AS actual
  FROM chi311.gold.gold_daily_service_request_summary
  WHERE ds >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT
  COALESCE(a.ds, p.ds) AS date,
  a.actual,
  p.yhat AS predicted,
  p.yhat_lower AS lower_bound,
  p.yhat_upper AS upper_bound,
  CASE
    WHEN a.ds IS NOT NULL THEN 'Historical'
    ELSE 'Forecast'
  END AS data_type
FROM recent_actuals a
FULL OUTER JOIN latest_predictions p ON a.ds = p.ds
ORDER BY date;


-- Query 2.2: Forecast Summary Table
-- Visualization: Table showing forecast details
SELECT
  ds AS forecast_date,
  ROUND(yhat, 0) AS predicted_requests,
  ROUND(yhat_lower, 0) AS lower_bound,
  ROUND(yhat_upper, 0) AS upper_bound,
  ROUND(yhat_upper - yhat_lower, 0) AS uncertainty_range,
  model_version,
  DATE_FORMAT(logged_at, 'yyyy-MM-dd HH:mm:ss') AS generated_at
FROM chi311.gold.gold_prediction_log
WHERE logged_at = (SELECT MAX(logged_at) FROM chi311.gold.gold_prediction_log)
ORDER BY ds;


-- Query 2.3: Model Performance Over Time (Line Chart)
-- Visualization: Line chart showing MAPE trend
SELECT
  DATE(logged_at) AS date,
  AVG(mape) * 100 AS avg_mape_percent,
  model_version
FROM chi311.gold.gold_prediction_log
WHERE mape IS NOT NULL
GROUP BY DATE(logged_at), model_version
ORDER BY date DESC
LIMIT 30;


-- Query 2.4: Prediction vs Actual Comparison (Scatter Plot)
-- Visualization: Scatter plot, X-axis: actual, Y-axis: predicted
WITH predictions_with_actuals AS (
  SELECT
    p.ds,
    p.yhat AS predicted,
    a.y AS actual,
    ABS(a.y - p.yhat) / a.y * 100 AS absolute_pct_error
  FROM chi311.gold.gold_prediction_log p
  INNER JOIN chi311.gold.gold_daily_service_request_summary a ON p.ds = a.ds
  WHERE p.logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT
  actual,
  predicted,
  ROUND(absolute_pct_error, 1) AS error_pct,
  ds AS date
FROM predictions_with_actuals
ORDER BY ds DESC
LIMIT 100;


-- =========================================================================
-- TAB 3: MONITORING - Data Quality, Drift, Anomalies
-- =========================================================================

-- Query 3.1: Data Quality Summary (Counter Widgets)
SELECT
  'Total DQ Checks Run' AS metric,
  COUNT(*) AS value
FROM chi311.gold.dq_checkpoint_results
WHERE run_time >= CURRENT_DATE() - INTERVAL 7 DAYS;

SELECT
  'Failed DQ Checks (Last 7 Days)' AS metric,
  COUNT(*) AS value
FROM chi311.gold.dq_checkpoint_results
WHERE run_time >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND success = FALSE;

SELECT
  'Data Quality Pass Rate' AS metric,
  CONCAT(ROUND(SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1), '%') AS value
FROM chi311.gold.dq_checkpoint_results
WHERE run_time >= CURRENT_DATE() - INTERVAL 30 DAYS;


-- Query 3.2: Pipeline Health Over Time (Area Chart)
-- Visualization: Stacked area chart showing successful vs failed runs
SELECT
  DATE(logged_at) AS date,
  status,
  COUNT(*) AS run_count
FROM chi311.gold.pipeline_run_log
WHERE logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY DATE(logged_at), status
ORDER BY date, status;


-- Query 3.3: Data Volume Trend (Line Chart)
-- Visualization: Line chart showing rows processed over time
SELECT
  DATE(logged_at) AS date,
  task_name,
  SUM(rows_out) AS total_rows_processed
FROM chi311.gold.pipeline_run_log
WHERE logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY DATE(logged_at), task_name
ORDER BY date, task_name;


-- Query 3.4: Anomaly Detection Results (Table with Highlighting)
-- Visualization: Table with conditional formatting on is_anomaly
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


-- Query 3.5: Model Drift Monitoring (Line Chart)
-- Visualization: Line chart with threshold line
WITH drift_by_day AS (
  SELECT
    DATE(p.logged_at) AS date,
    AVG(ABS(a.y - p.yhat) / a.y) AS avg_mape
  FROM chi311.gold.gold_prediction_log p
  INNER JOIN chi311.gold.gold_daily_service_request_summary a ON p.ds = a.ds
  WHERE p.logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS
    AND a.y > 0
  GROUP BY DATE(p.logged_at)
)
SELECT
  date,
  ROUND(avg_mape * 100, 2) AS mape_percent,
  20.0 AS drift_threshold
FROM drift_by_day
ORDER BY date;


-- Query 3.6: Task Duration Trends (Bar Chart)
-- Visualization: Bar chart showing average duration by task
SELECT
  task_name,
  ROUND(AVG(duration_seconds), 1) AS avg_duration_sec,
  ROUND(MAX(duration_seconds), 1) AS max_duration_sec,
  COUNT(*) AS run_count
FROM chi311.gold.pipeline_run_log
WHERE logged_at >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND status = 'SUCCESS'
GROUP BY task_name
ORDER BY avg_duration_sec DESC;


-- Query 3.7: Recent Errors (Table)
-- Visualization: Table showing failed pipeline runs
SELECT
  task_name,
  error_message,
  logged_at,
  rows_in,
  rows_out
FROM chi311.gold.pipeline_run_log
WHERE status = 'FAILED'
  AND logged_at >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY logged_at DESC
LIMIT 20;


-- =========================================================================
-- REFRESH SCHEDULE RECOMMENDATIONS
-- =========================================================================
-- Tab 1 (Overview): Refresh every 1 hour
-- Tab 2 (Forecasts): Refresh every 6 hours (or when ML job completes)
-- Tab 3 (Monitoring): Refresh every 30 minutes
-- =========================================================================
