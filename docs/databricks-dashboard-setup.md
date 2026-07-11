# Databricks SQL Dashboard Setup Guide

This guide walks through creating the Chi311 ML Platform monitoring dashboard using Databricks SQL's native dashboard capabilities.

## Prerequisites

- ✅ Databricks SQL Warehouse running (any size)
- ✅ Unity Catalog enabled
- ✅ Gold tables populated (`chi311.gold.*`)
- ✅ Permission to create dashboards in Databricks SQL

## Option 1: Automated Setup (Recommended)

Run the programmatic setup notebook:

```bash
# Upload and run the notebook
databricks workspace import dashboards/setup_dashboard.py /Shared/chi311_dashboard_setup
```

Then navigate to **Databricks SQL** → **Dashboards** to configure visualizations.

## Option 2: Manual Setup

### Step 1: Access Databricks SQL

1. Navigate to your Databricks workspace
2. Click **SQL** in the left sidebar
3. Click **Dashboards** → **Create Dashboard**
4. Name it: `Chi311 ML Platform - Monitoring Dashboard`

### Step 2: Create Queries

For each query in `dashboards/queries.sql`:

1. Click **Queries** → **Create Query**
2. Select your SQL Warehouse
3. Copy the SQL from `queries.sql`
4. Name the query (e.g., "Chi311 - Daily Trends")
5. Click **Save**
6. Click **Run** to test

**Queries to create:**
- Overview tab: 4 queries
- Forecasts tab: 4 queries  
- Monitoring tab: 7 queries

### Step 3: Add Visualizations to Dashboard

#### Tab 1: Overview

1. **KPI Cards** (Top Row)
   - Query: `Chi311 - Total Requests (7d)`
   - Visualization: Counter
   - Label: "Total Requests (7 Days)"

2. **Daily Request Trends** (Full Width)
   - Query: `Chi311 - Daily Request Trends`
   - Visualization: Line Chart
   - X-axis: `date`
   - Y-axis: `daily_requests`
   - Title: "Daily Service Requests (90 Days)"

3. **Day of Week Pattern** (Half Width)
   - Query: `Chi311 - Day of Week Pattern`
   - Visualization: Bar Chart
   - X-axis: `day_name`
   - Y-axis: `avg_requests`
   - Title: "Average Requests by Day of Week"

4. **Recent Pipeline Runs** (Half Width)
   - Query: `Chi311 - Recent Pipeline Runs`
   - Visualization: Table
   - Enable conditional formatting on `status` column:
     - SUCCESS = Green
     - FAILED = Red
   - Title: "Recent Pipeline Executions"

#### Tab 2: Forecasts

1. **Forecast with Confidence Intervals** (Full Width)
   - Query: `Chi311 - Latest Forecast`
   - Visualization: Line Chart with multiple series
   - X-axis: `date`
   - Y-axes: 
     - `actual` (solid line, blue)
     - `predicted` (solid line, orange)
     - `lower_bound` (dashed line, light orange)
     - `upper_bound` (dashed line, light orange)
   - Title: "7-Day Forecast with Confidence Intervals"

2. **Forecast Summary Table** (Full Width)
   - Query: `Chi311 - Forecast Summary`
   - Visualization: Table
   - Format numeric columns to 0 decimal places
   - Title: "Detailed Forecast Breakdown"

3. **Prediction vs Actual** (Half Width)
   - Query: `Chi311 - Prediction vs Actual`
   - Visualization: Scatter Plot
   - X-axis: `actual`
   - Y-axis: `predicted`
   - Add 45° reference line (y=x)
   - Title: "Model Accuracy: Predicted vs Actual"

4. **Model MAPE Trend** (Half Width)
   - Query: `Chi311 - Model Performance`
   - Visualization: Line Chart
   - X-axis: `date`
   - Y-axis: `avg_mape_percent`
   - Title: "Model MAPE Over Time"

#### Tab 3: Monitoring

1. **Data Quality Metrics** (Top Row - 3 Cards)
   - Query: `Chi311 - DQ Checks`, `Chi311 - DQ Failures`, `Chi311 - DQ Pass Rate`
   - Visualization: Counter for each
   - Titles: Self-explanatory from query

2. **Pipeline Health** (Full Width)
   - Query: `Chi311 - Pipeline Health`
   - Visualization: Stacked Area Chart
   - X-axis: `date`
   - Y-axis: `run_count`
   - Stack by: `status`
   - Colors: SUCCESS=Green, FAILED=Red
   - Title: "Pipeline Execution Status (30 Days)"

3. **Anomaly Detection** (Full Width)
   - Query: `Chi311 - Anomalies`
   - Visualization: Table with conditional formatting
   - Highlight rows where `anomaly_detected = TRUE` in red
   - Title: "Recent Anomalies Detected"

4. **Model Drift** (Half Width)
   - Query: `Chi311 - Model Drift`
   - Visualization: Line Chart with threshold
   - X-axis: `date`
   - Y-axes: 
     - `mape_percent` (line)
     - `drift_threshold` (horizontal line, red, dashed)
   - Title: "Model Drift Monitoring (MAPE %)"

5. **Task Duration Trends** (Half Width)
   - Query: `Chi311 - Task Durations`
   - Visualization: Horizontal Bar Chart
   - X-axis: `avg_duration_sec`
   - Y-axis: `task_name`
   - Title: "Average Task Execution Time"

### Step 4: Configure Refresh Schedules

1. Click dashboard **Settings** (gear icon)
2. Enable **Auto-refresh**
3. Set refresh intervals:
   - **Overview Tab**: Every 1 hour
   - **Forecasts Tab**: Every 6 hours
   - **Monitoring Tab**: Every 30 minutes

### Step 5: Set Permissions

1. Click **Share** button
2. Add users/groups who should view the dashboard
3. Set permissions:
   - **Can View**: All users who need to monitor
   - **Can Edit**: Data scientists and engineers
   - **Can Manage**: Platform owners

### Step 6: Add Filters (Optional)

Add global filters for interactive exploration:

1. Click **Add Filter**
2. Common filters:
   - **Date Range**: Filter by time period
   - **Status**: Filter by pipeline status
   - **Task Name**: Filter by specific pipeline task

## Dashboard Layout Recommendations

### Overview Tab Layout
```
+------------------+------------------+------------------+------------------+
| Total Requests   | Avg Daily        | Latest MAPE      | Active Anomalies |
| (Counter)        | (Counter)        | (Counter)        | (Counter)        |
+------------------------------------------------------------------+
|                     Daily Request Trends                          |
|                        (Line Chart)                              |
+----------------------------------+-------------------------------+
|    Day of Week Pattern          |   Recent Pipeline Runs       |
|       (Bar Chart)               |         (Table)              |
+----------------------------------+-------------------------------+
```

### Forecasts Tab Layout
```
+------------------------------------------------------------------+
|            Forecast with Confidence Intervals                    |
|                    (Multi-series Line Chart)                     |
+------------------------------------------------------------------+
|                  Forecast Summary Table                          |
|                         (Table)                                  |
+----------------------------------+-------------------------------+
|    Prediction vs Actual         |   Model MAPE Trend           |
|      (Scatter Plot)             |      (Line Chart)            |
+----------------------------------+-------------------------------+
```

### Monitoring Tab Layout
```
+---------------------+---------------------+---------------------+
| Total DQ Checks    | Failed Checks      | DQ Pass Rate        |
| (Counter)          | (Counter)          | (Counter)           |
+------------------------------------------------------------------+
|                  Pipeline Health Over Time                        |
|                   (Stacked Area Chart)                           |
+------------------------------------------------------------------+
|                     Anomaly Detection                            |
|                         (Table)                                  |
+----------------------------------+-------------------------------+
|        Model Drift              |   Task Duration Trends       |
|      (Line Chart)               |    (Horizontal Bar)          |
+----------------------------------+-------------------------------+
```

## Advanced Features

### Email Subscriptions

1. Click dashboard **Subscriptions**
2. Click **New Subscription**
3. Configure:
   - **Recipients**: Email addresses
   - **Schedule**: Daily at 9 AM
   - **Format**: PDF or PNG
   - **Destination**: Email or Slack

### Dashboard Parameters

Create parameters for dynamic filtering:

```sql
-- Add parameter: date_range (default: 30)
WHERE ds >= CURRENT_DATE() - INTERVAL {{ date_range }} DAYS
```

Users can adjust the parameter in the dashboard UI.

### Alerts

Set up alerts on key metrics:

1. Navigate to a query (e.g., "Failed DQ Checks")
2. Click **Alerts** → **New Alert**
3. Configure:
   - **Condition**: `value > 0`
   - **Notification**: Email, Slack, PagerDuty
   - **Frequency**: Check every 30 minutes

## Troubleshooting

### Query Fails with "Table Not Found"

**Cause**: Gold tables haven't been created yet  
**Solution**: Run the Lakeflow pipeline first to populate tables

### Visualizations Show No Data

**Cause**: No data in the date range  
**Solution**: Adjust date filters or run data ingestion jobs

### Dashboard Won't Refresh

**Cause**: SQL Warehouse is stopped  
**Solution**: Start the warehouse in **SQL** → **Warehouses**

### Permission Denied

**Cause**: User lacks access to Unity Catalog tables  
**Solution**: Grant `SELECT` on `chi311.gold.*` to user

```sql
GRANT SELECT ON SCHEMA chi311.gold TO `user@example.com`;
```

## Comparison: Databricks Dashboard vs Streamlit

| Feature | Databricks SQL Dashboard | Streamlit (Old) |
|---------|-------------------------|-----------------|
| **Deployment** | Native in workspace | Separate hosting required |
| **Cost** | No additional compute | Extra compute cluster |
| **Maintenance** | Minimal | Requires monitoring |
| **Access Control** | Unity Catalog RBAC | Custom implementation |
| **Auto-refresh** | Built-in | Manual polling |
| **SQL Warehouse** | Shared warehouse | Dedicated cluster |
| **Integration** | Native to Databricks | External app |
| **Performance** | Optimized for Delta | Depends on implementation |

## Next Steps

- ✅ Review query performance and add indexes if needed
- ✅ Set up email subscriptions for daily reports
- ✅ Configure alerts for critical metrics
- ✅ Train users on dashboard navigation
- ✅ Document dashboard URLs in team wiki

## References

- [Databricks SQL Dashboards Documentation](https://docs.databricks.com/sql/user/dashboards/index.html)
- [Query Visualization Types](https://docs.databricks.com/sql/user/visualizations/index.html)
- [Dashboard Permissions](https://docs.databricks.com/security/access-control/dashboard-acl.html)
