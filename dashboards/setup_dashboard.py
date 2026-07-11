# Databricks notebook source
# MAGIC %md
# MAGIC # Chi311 Platform - Databricks SQL Dashboard Setup
# MAGIC
# MAGIC This notebook creates the native Databricks SQL Dashboard programmatically.
# MAGIC
# MAGIC **Components Created:**
# MAGIC - 3 Dashboard tabs (Overview, Forecasts, Monitoring)
# MAGIC - 20+ visualizations across all tabs
# MAGIC - Auto-refresh schedules
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks SQL Warehouse created and running
# MAGIC - Gold tables exist in Unity Catalog (chi311.gold.*)
# MAGIC - User has permission to create dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Dashboard configuration
DASHBOARD_NAME = "Chi311 ML Platform - Monitoring Dashboard"
CATALOG = "chi311"
WAREHOUSE_ID = None  # Set to your SQL Warehouse ID or leave None to use default

# SQL Warehouse - detect available warehouse
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
warehouses = list(w.warehouses.list())

if not warehouses:
    raise Exception("No SQL Warehouses found. Please create one first.")

if WAREHOUSE_ID is None:
    # Use first available warehouse
    WAREHOUSE_ID = warehouses[0].id
    print(f"Using SQL Warehouse: {warehouses[0].name} (ID: {WAREHOUSE_ID})")
else:
    print(f"Using specified SQL Warehouse ID: {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dashboard Queries

# COMMAND ----------

def create_query(name, query_text, warehouse_id):
    """Create a Databricks SQL query."""
    from databricks.sdk.service.sql import Query, QueryOptions

    query = w.queries.create(
        name=name,
        query=query_text,
        data_source_id=warehouse_id,
        parent="folders/Shared"
    )
    print(f"Created query: {name} (ID: {query.id})")
    return query.id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tab 1: Overview Queries

# COMMAND ----------

# Query 1.1: Total Requests Last 7 Days
query_1_1 = create_query(
    "Chi311 - Total Requests (7d)",
    """
    SELECT
      'Total Requests (Last 7 Days)' AS metric,
      SUM(y) AS value
    FROM chi311.gold.gold_daily_service_request_summary
    WHERE ds >= CURRENT_DATE() - INTERVAL 7 DAYS
    """,
    WAREHOUSE_ID
)

# Query 1.2: Daily Request Trends
query_1_2 = create_query(
    "Chi311 - Daily Request Trends",
    """
    SELECT
      ds AS date,
      y AS daily_requests
    FROM chi311.gold.gold_daily_service_request_summary
    WHERE ds >= CURRENT_DATE() - INTERVAL 90 DAYS
    ORDER BY ds
    """,
    WAREHOUSE_ID
)

# Query 1.3: Day of Week Pattern
query_1_3 = create_query(
    "Chi311 - Day of Week Pattern",
    """
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
    ORDER BY day_order
    """,
    WAREHOUSE_ID
)

# Query 1.4: Recent Pipeline Runs
query_1_4 = create_query(
    "Chi311 - Recent Pipeline Runs",
    """
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
    LIMIT 10
    """,
    WAREHOUSE_ID
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tab 2: Forecast Queries

# COMMAND ----------

# Query 2.1: Latest 7-Day Forecast
query_2_1 = create_query(
    "Chi311 - Latest Forecast",
    """
    WITH latest_predictions AS (
      SELECT
        ds,
        y_pred,
        y_pred_lower,
        y_pred_upper,
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
      p.y_pred AS predicted,
      p.y_pred_lower AS lower_bound,
      p.y_pred_upper AS upper_bound,
      CASE
        WHEN a.ds IS NOT NULL THEN 'Historical'
        ELSE 'Forecast'
      END AS data_type
    FROM recent_actuals a
    FULL OUTER JOIN latest_predictions p ON a.ds = p.ds
    ORDER BY date
    """,
    WAREHOUSE_ID
)

# Query 2.2: Forecast Summary Table
query_2_2 = create_query(
    "Chi311 - Forecast Summary",
    """
    SELECT
      ds AS forecast_date,
      ROUND(y_pred, 0) AS predicted_requests,
      ROUND(y_pred_lower, 0) AS lower_bound,
      ROUND(y_pred_upper, 0) AS upper_bound,
      model_version,
      DATE_FORMAT(logged_at, 'yyyy-MM-dd HH:mm:ss') AS generated_at
    FROM chi311.gold.gold_prediction_log
    WHERE logged_at = (SELECT MAX(logged_at) FROM chi311.gold.gold_prediction_log)
    ORDER BY ds
    """,
    WAREHOUSE_ID
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tab 3: Monitoring Queries

# COMMAND ----------

# Query 3.1: Pipeline Health Over Time
query_3_1 = create_query(
    "Chi311 - Pipeline Health",
    """
    SELECT
      DATE(logged_at) AS date,
      status,
      COUNT(*) AS run_count
    FROM chi311.gold.pipeline_run_log
    WHERE logged_at >= CURRENT_DATE() - INTERVAL 30 DAYS
    GROUP BY DATE(logged_at), status
    ORDER BY date, status
    """,
    WAREHOUSE_ID
)

# Query 3.2: Anomaly Detection Results
query_3_2 = create_query(
    "Chi311 - Anomalies",
    """
    SELECT
      ds AS date,
      y AS actual_requests,
      ROUND(rolling_mean_30d, 0) AS expected_30d_avg,
      ROUND(z_score, 2) AS z_score,
      is_anomaly AS anomaly_detected,
      anomaly_type
    FROM chi311.gold.gold_anomaly_results
    WHERE ds >= CURRENT_DATE() - INTERVAL 30 DAYS
    ORDER BY ds DESC
    """,
    WAREHOUSE_ID
)

# Query 3.3: Model Drift Monitoring
query_3_3 = create_query(
    "Chi311 - Model Drift",
    """
    WITH drift_by_day AS (
      SELECT
        DATE(p.logged_at) AS date,
        AVG(ABS(a.y - p.y_pred) / a.y) AS avg_mape
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
    ORDER BY date
    """,
    WAREHOUSE_ID
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dashboard
# MAGIC
# MAGIC **Note:** Dashboard creation via API is limited in some Databricks versions.
# MAGIC The queries above have been created and can be added to a dashboard manually.
# MAGIC
# MAGIC See `docs/databricks-dashboard-setup.md` for manual setup instructions.

# COMMAND ----------

print("=" * 70)
print("Dashboard Queries Created Successfully!")
print("=" * 70)
print(f"\nTotal queries created: 9")
print(f"SQL Warehouse: {WAREHOUSE_ID}")
print(f"\nNext Steps:")
print("1. Go to Databricks SQL in your workspace")
print("2. Create a new Dashboard")
print("3. Add the queries created above as visualizations")
print("4. Organize into 3 tabs: Overview, Forecasts, Monitoring")
print("5. Set refresh schedules (see queries.sql for recommendations)")
print("\nFor detailed instructions, see: docs/databricks-dashboard-setup.md")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Run this cell to delete all created queries if you need to start over

# COMMAND ----------

# Uncomment to delete all queries
# query_ids = [query_1_1, query_1_2, query_1_3, query_1_4, query_2_1, query_2_2, query_3_1, query_3_2, query_3_3]
# for query_id in query_ids:
#     try:
#         w.queries.delete(query_id)
#         print(f"Deleted query ID: {query_id}")
#     except Exception as e:
#         print(f"Error deleting query {query_id}: {e}")
