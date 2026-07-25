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
--   gold_anomaly_results (04_ml/02_anomaly_detection.py) -- CITYWIDE, one row/day
--     ds, y, anomaly_score, is_anomaly, zscore_anomaly, dod_anomaly,
--     forecast_anomaly, detection_timestamp
--   gold_anomaly_results_by_type (04_ml/02_anomaly_detection.py) -- per sr_type/day
--     ds, sr_type, y, z_score, dod_pct_change, zscore_anomaly, dod_anomaly,
--     anomaly_score, is_anomaly, detection_timestamp
--     (z-score + day-over-day only; types averaging >= 20/day; no forecast method)
--   dq_checkpoint_results (03_data_quality/01_data_quality_checks.py)
--     run_date, layer, success, expectations_evaluated, expectations_passed,
--     expectations_failed, pass_rate_pct, logged_at
--   silver_current_311_requests (Lakeflow pipeline — current SCD2 state)
--     sr_number, sr_type, sr_short_code, status, street_address, city,
--     zip_code, ward, community_area, latitude, longitude, created_date,
--     closed_date, last_modified_date, owner_department, is_info_call,
--     is_admin_ward_info_call, valid_from, valid_to
--   gold_request_type_daily_summary (Lakeflow pipeline)
--     request_date, sr_type, sr_short_code, total_requests, completed_count,
--     avg_resolution_hours
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


-- Query 1.3: Day of Week Pattern (Bar Chart) — Monday-first
-- X-axis: day_name, Y-axis: avg_requests.
-- The numeric prefix ("1. Monday" … "7. Sunday") bakes chronological order into
-- the label so alphabetical X-axis sorting renders Mon→Sun regardless of the
-- chart's default string sort. DAYOFWEEK() returns 1=Sunday, so the
-- ((DAYOFWEEK + 5) % 7) + 1 expression remaps to Monday=1 … Sunday=7.
SELECT
  CONCAT(
    ((DAYOFWEEK(request_date) + 5) % 7) + 1,
    '. ',
    DATE_FORMAT(request_date, 'EEEE')
  ) AS day_name,
  ROUND(AVG(total_requests), 0) AS avg_requests
FROM chi311.gold.gold_daily_service_request_summary
WHERE request_date >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY CONCAT(
    ((DAYOFWEEK(request_date) + 5) % 7) + 1,
    '. ',
    DATE_FORMAT(request_date, 'EEEE')
  )
ORDER BY day_name;


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
-- TAB 4: OPERATIONS - Tactical view for ops managers & dispatchers
-- =========================================================================
-- Answers "what do I act on today/this week?" — backlog & aging, forecast
-- vs. normal capacity, and anomaly context. Backlog queries read the current
-- SCD2 state (silver_current_311_requests) so an "open" request is one whose
-- latest version has status = 'Open'. Info calls are excluded throughout.
--
-- ANOMALY DRILL-DOWN: gold_anomaly_results is CITYWIDE (one row/day) — Query 4.6
-- charts its magnitude vs. a trailing baseline. For "which type spiked",
-- 02_anomaly_detection.py now also produces gold_anomaly_results_by_type
-- (per sr_type/day, z-score + day-over-day, types averaging >= 20/day), which
-- Query 4.7 uses for a real per-type drill-down. Ward-level detection is not
-- yet implemented (would be a gold_anomaly_results_by_ward follow-up).


-- Query 4.1a: Open Backlog (Counter)
-- Total currently-open service requests (excludes info calls).
SELECT
  'Open Requests (Backlog)' AS metric,
  COUNT(*) AS value
FROM chi311.silver.silver_current_311_requests
WHERE status = 'Open'
  AND is_info_call = FALSE;

-- Query 4.1b: Aged Backlog >14 Days (Counter)
-- The "escalate today" list — open requests older than 14 days.
SELECT
  'Open >14 Days' AS metric,
  COUNT(*) AS value
FROM chi311.silver.silver_current_311_requests
WHERE status = 'Open'
  AND is_info_call = FALSE
  AND created_date < CURRENT_DATE() - INTERVAL 14 DAYS;

-- Query 4.1c: Oldest Open Request Age in Days (Counter)
SELECT
  'Oldest Open (Days)' AS metric,
  MAX(DATEDIFF(CURRENT_DATE(), DATE(created_date))) AS value
FROM chi311.silver.silver_current_311_requests
WHERE status = 'Open'
  AND is_info_call = FALSE;


-- Query 4.2: Backlog by Age Bucket (Bar Chart) -- #3 backlog/aging
-- X-axis: age_bucket (order baked into the label so it sorts correctly),
-- Y-axis: open_requests. The 15d+ bucket is the tactical escalation queue.
SELECT
  CASE
    WHEN DATEDIFF(CURRENT_DATE(), DATE(created_date)) <= 3  THEN '1. 0-3 days'
    WHEN DATEDIFF(CURRENT_DATE(), DATE(created_date)) <= 7  THEN '2. 4-7 days'
    WHEN DATEDIFF(CURRENT_DATE(), DATE(created_date)) <= 14 THEN '3. 8-14 days'
    ELSE '4. 15+ days'
  END AS age_bucket,
  COUNT(*) AS open_requests
FROM chi311.silver.silver_current_311_requests
WHERE status = 'Open'
  AND is_info_call = FALSE
GROUP BY
  CASE
    WHEN DATEDIFF(CURRENT_DATE(), DATE(created_date)) <= 3  THEN '1. 0-3 days'
    WHEN DATEDIFF(CURRENT_DATE(), DATE(created_date)) <= 7  THEN '2. 4-7 days'
    WHEN DATEDIFF(CURRENT_DATE(), DATE(created_date)) <= 14 THEN '3. 8-14 days'
    ELSE '4. 15+ days'
  END
ORDER BY age_bucket;


-- Query 4.3: Aging Backlog by Request Type (Table with highlighting) -- #3
-- Which categories are falling behind. Conditional-format open_over_14d to
-- flag the worst offenders. Ranked by aged backlog first.
SELECT
  sr_type,
  COUNT(*) AS open_requests,
  SUM(CASE WHEN created_date < CURRENT_DATE() - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS open_over_14d,
  ROUND(AVG(DATEDIFF(CURRENT_DATE(), DATE(created_date))), 1) AS avg_age_days,
  MAX(DATEDIFF(CURRENT_DATE(), DATE(created_date))) AS oldest_age_days
FROM chi311.silver.silver_current_311_requests
WHERE status = 'Open'
  AND is_info_call = FALSE
GROUP BY sr_type
ORDER BY open_over_14d DESC, open_requests DESC
LIMIT 20;


-- Query 4.4: 7-Day Forecast vs. Normal Capacity (Table) -- #1 forecast->staffing
-- The tactical framing: not the curve, but the delta vs. what's typical.
-- Baseline = trailing 28-day average of daily actuals. A staffing flag calls
-- out days that run materially over/under normal.
WITH baseline AS (
  SELECT AVG(total_requests) AS typical_daily
  FROM chi311.gold.gold_daily_service_request_summary
  WHERE request_date >= CURRENT_DATE() - INTERVAL 28 DAYS
),
latest_forecast AS (
  SELECT ds, yhat
  FROM chi311.gold.gold_forecasts
  WHERE prediction_date = (SELECT MAX(prediction_date) FROM chi311.gold.gold_forecasts)
)
SELECT
  f.ds AS forecast_date,
  DATE_FORMAT(f.ds, 'EEEE') AS day_of_week,
  ROUND(f.yhat, 0) AS forecast_requests,
  ROUND(b.typical_daily, 0) AS typical_requests,
  ROUND((f.yhat - b.typical_daily) / b.typical_daily * 100, 1) AS pct_vs_typical,
  CASE
    WHEN (f.yhat - b.typical_daily) / b.typical_daily >  0.15 THEN 'ADD CREWS (+15% or more)'
    WHEN (f.yhat - b.typical_daily) / b.typical_daily < -0.15 THEN 'LIGHT DAY (-15% or more)'
    ELSE 'NORMAL'
  END AS staffing_flag
FROM latest_forecast f
CROSS JOIN baseline b
ORDER BY f.ds;


-- Query 4.5: Request-Type Mix This Week vs Last Week (Table) -- #4 demand shift
-- Ranked top types with week-over-week movement so dispatchers see where
-- demand is shifting. Positive pct_change = rising demand.
WITH this_week AS (
  SELECT sr_type, SUM(total_requests) AS requests
  FROM chi311.gold.gold_request_type_daily_summary
  WHERE request_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  GROUP BY sr_type
),
last_week AS (
  SELECT sr_type, SUM(total_requests) AS requests
  FROM chi311.gold.gold_request_type_daily_summary
  WHERE request_date >= CURRENT_DATE() - INTERVAL 14 DAYS
    AND request_date <  CURRENT_DATE() - INTERVAL 7 DAYS
  GROUP BY sr_type
)
SELECT
  t.sr_type,
  t.requests AS this_week,
  COALESCE(l.requests, 0) AS last_week,
  t.requests - COALESCE(l.requests, 0) AS change,
  CASE
    WHEN COALESCE(l.requests, 0) = 0 THEN NULL
    ELSE ROUND((t.requests - l.requests) * 100.0 / l.requests, 1)
  END AS pct_change
FROM this_week t
LEFT JOIN last_week l ON t.sr_type = l.sr_type
ORDER BY t.requests DESC
LIMIT 15;


-- Query 4.6: Active Anomalies with Magnitude (Table with highlighting) -- #2
-- Recent flagged days, with how far actual volume sat above/below a trailing
-- 28-day baseline as of that day. "methods_agree" = anomaly_score (how many of
-- the 3 detectors fired). Citywide only — see NOTE at top of tab.
WITH scored AS (
  SELECT
    ds,
    y AS actual_requests,
    anomaly_score AS methods_agree,
    zscore_anomaly,
    dod_anomaly,
    forecast_anomaly,
    AVG(y) OVER (
      ORDER BY ds ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
    ) AS trailing_28d_avg
  FROM chi311.gold.gold_anomaly_results
)
SELECT
  ds AS date,
  DATE_FORMAT(ds, 'EEEE') AS day_of_week,
  actual_requests,
  ROUND(trailing_28d_avg, 0) AS typical_requests,
  ROUND((actual_requests - trailing_28d_avg) / trailing_28d_avg * 100, 1) AS pct_vs_typical,
  methods_agree,
  zscore_anomaly,
  dod_anomaly,
  forecast_anomaly
FROM scored
WHERE ds >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND methods_agree >= 2
ORDER BY ds DESC;


-- Query 4.7: Request Types Driving Recent Anomalies (Table with highlighting) -- #2 drill-down
-- Real per-type anomaly signal from gold_anomaly_results_by_type: which request
-- types were individually anomalous in the last 30 days, how far each sat above
-- its own trailing behavior (z-score, day-over-day %), and which detectors
-- fired. This is the genuine "what spiked" view (types averaging >= 20/day).
SELECT
  ds AS date,
  sr_type,
  y AS requests,
  ROUND(z_score, 2) AS z_score,
  ROUND(dod_pct_change * 100, 1) AS dod_pct_change,
  zscore_anomaly,
  dod_anomaly
FROM chi311.gold.gold_anomaly_results_by_type
WHERE is_anomaly = TRUE
  AND ds >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY ds DESC, ABS(z_score) DESC
LIMIT 50;


-- Query 4.8: Data Freshness / Trust Indicator (Counter) -- trust signal
-- Tactically, don't act on stale data. Shows how current the daily summary is.
SELECT
  'Data Through' AS metric,
  MAX(request_date) AS value,
  DATEDIFF(CURRENT_DATE(), MAX(request_date)) AS days_behind
FROM chi311.gold.gold_daily_service_request_summary;


-- =========================================================================
-- REFRESH SCHEDULE RECOMMENDATIONS
-- =========================================================================
-- Tab 1 (Overview):    Refresh every 1 hour
-- Tab 2 (Forecasts):   Refresh every 6 hours (or when ML job completes)
-- Tab 3 (Monitoring):  Refresh every 30 minutes
-- Tab 4 (Operations):  Refresh every 30 minutes (backlog is time-sensitive)
-- =========================================================================
