"""
Chicago 311 Intelligence Platform - Streamlit Dashboard
Connects to Azure Databricks SQL warehouse for live data
"""

import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ================================================================
# Configuration
# ================================================================

st.set_page_config(
    page_title="Chicago 311 Intelligence Platform",
    page_icon="",
    layout="wide",
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "abd-XXXXX.azuredatabricks.net")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/XXXXX")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")


def get_connection():
    """Get Databricks SQL connection."""
    try:
        from databricks import sql

        return sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
    except ImportError:
        st.warning("databricks-sql-connector not installed. Using sample data.")
        return None


def query_databricks(query: str) -> pd.DataFrame:
    """Execute query against Databricks SQL warehouse."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        conn.close()


# ================================================================
# Layout
# ================================================================

st.title("Chicago 311 Intelligence Platform")
st.markdown("**Demand Forecasting & Anomaly Detection** | Azure Databricks")

tab_requests, tab_forecast, tab_monitoring = st.tabs(
    ["📊 Service Requests", "🔮 Forecast & Anomalies", "🔍 Monitoring & Observability"]
)

st.sidebar.header("Controls")
date_range = st.sidebar.selectbox(
    "Date Range",
    ["Last 7 Days", "Last 30 Days", "Last 90 Days", "All Time"],
    index=1,
)

# ================================================================
# TAB 1 — Service Requests
# ================================================================

with tab_requests:
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)

    _q_daily = (
        f"SELECT ds, y, is_weekend, completion_rate"
        f" FROM {CATALOG}.gold.gold_citywide_daily_summary"
        f" ORDER BY ds DESC LIMIT 90"
    )
    try:
        df_daily = query_databricks(_q_daily)
        if df_daily.empty:
            raise ValueError("No data returned")
        df_daily["ds"] = pd.to_datetime(df_daily["ds"])

        with col1:
            st.metric("Avg Daily Requests", f"{df_daily['y'].mean():,.0f}")
        with col2:
            latest = df_daily.iloc[0]["y"]
            prev = df_daily.iloc[1]["y"] if len(df_daily) > 1 else latest
            delta = ((latest - prev) / prev) * 100 if prev > 0 else 0
            st.metric("Latest Day", f"{latest:,.0f}", f"{delta:.1f}%")
        with col3:
            st.metric(
                "Avg Completion Rate",
                f"{df_daily['completion_rate'].mean() * 100:.1f}%",
            )
        with col4:
            st.metric("Pipeline Status", "Healthy")

    except Exception:
        st.info("Showing sample data. Connect to Databricks for live metrics.")
        np.random.seed(42)
        dates = pd.date_range(end=datetime.now(), periods=90, freq="D")
        df_daily = pd.DataFrame(
            {"ds": dates, "y": np.random.normal(3000, 500, 90).clip(1500, 5000)}
        )
        with col1:
            st.metric("Avg Daily Requests", f"{df_daily['y'].mean():,.0f}")
        with col2:
            st.metric("Latest Day", f"{df_daily['y'].iloc[-1]:,.0f}")
        with col3:
            st.metric("Avg Completion Rate", "78.3%")
        with col4:
            st.metric("Pipeline Status", "Sample Mode")

    st.markdown("### Daily Service Request Volume")
    fig = px.line(
        df_daily.sort_values("ds"),
        x="ds",
        y="y",
        title="Daily 311 Service Requests (excl. Info Calls)",
        labels={"ds": "Date", "y": "Request Count"},
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

# ================================================================
# TAB 2 — Forecast & Anomalies
# ================================================================

with tab_forecast:
    st.markdown("### Anomaly Detection")
    _q_anomalies = (
        f"SELECT ds, y, anomaly_score, is_anomaly"
        f" FROM {CATALOG}.gold.gold_anomaly_results"
        f" WHERE is_anomaly = true ORDER BY ds DESC LIMIT 20"
    )
    try:
        df_anomalies = query_databricks(_q_anomalies)
        if not df_anomalies.empty:
            st.dataframe(df_anomalies, use_container_width=True)
        else:
            st.info("No anomalies detected in recent data.")
    except Exception:
        st.info("Run anomaly detection notebook to populate this section.")

    st.markdown("### 7-Day Forecast")
    _q_forecast = (
        f"SELECT ds, yhat, yhat_lower, yhat_upper"
        f" FROM {CATALOG}.gold.gold_forecasts ORDER BY ds DESC LIMIT 7"
    )
    try:
        df_forecast = query_databricks(_q_forecast)
        if not df_forecast.empty:
            fig_fc = go.Figure()
            fig_fc.add_trace(
                go.Scatter(
                    x=df_forecast["ds"],
                    y=df_forecast["yhat"],
                    mode="lines+markers",
                    name="Forecast",
                )
            )
            fig_fc.add_trace(
                go.Scatter(
                    x=df_forecast["ds"],
                    y=df_forecast["yhat_upper"],
                    mode="lines",
                    name="Upper Bound",
                    line={"dash": "dot"},
                )
            )
            fig_fc.add_trace(
                go.Scatter(
                    x=df_forecast["ds"],
                    y=df_forecast["yhat_lower"],
                    mode="lines",
                    name="Lower Bound",
                    line={"dash": "dot"},
                    fill="tonexty",
                )
            )
            fig_fc.update_layout(title="7-Day Demand Forecast", height=400)
            st.plotly_chart(fig_fc, use_container_width=True)
        else:
            st.info("No forecast data. Run the forecasting notebook first.")
    except Exception:
        st.info("Run forecasting notebook to populate this section.")

# ================================================================
# TAB 3 — Monitoring & Observability
# ================================================================

with tab_monitoring:

    # ---- Pipeline Run Health ----------------------------------------
    st.markdown("### Pipeline Run Health")
    _q_runs = (
        f"SELECT task_name, status, rows_in, rows_out, rows_dropped,"
        f" duration_seconds, logged_at"
        f" FROM {CATALOG}.gold.pipeline_run_log"
        f" ORDER BY logged_at DESC LIMIT 50"
    )
    try:
        df_runs = query_databricks(_q_runs)
        if df_runs.empty:
            raise ValueError("no data")

        mc1, mc2, mc3 = st.columns(3)
        total = len(df_runs)
        failed = int((df_runs["status"] == "FAILED").sum())
        success_rate = (total - failed) / total * 100 if total > 0 else 0.0
        with mc1:
            st.metric("Recent Runs", total)
        with mc2:
            st.metric("Failed", failed)
        with mc3:
            st.metric("Success Rate", f"{success_rate:.1f}%")

        def _colour_status(val):
            return (
                "background-color: #d4edda"
                if val == "SUCCESS"
                else "background-color: #f8d7da"
            )

        st.dataframe(
            df_runs.style.applymap(_colour_status, subset=["status"]),
            use_container_width=True,
        )

        st.markdown("#### Row Counts per Task")
        fig_rows = px.bar(
            df_runs.sort_values("logged_at"),
            x="logged_at",
            y=["rows_in", "rows_out", "rows_dropped"],
            barmode="group",
            labels={
                "logged_at": "Run Time",
                "value": "Row Count",
                "variable": "Metric",
            },
            title="Input vs Output vs Dropped Rows by Task Run",
        )
        st.plotly_chart(fig_rows, use_container_width=True)

    except Exception:
        st.info(
            "No pipeline run data yet. Deploy and run jobs to populate this section."
        )

    # ---- Prediction Log ---------------------------------------------
    st.markdown("---")
    st.markdown("### Model Prediction Log")
    _q_preds = (
        f"SELECT ds, model_version, logged_at"
        f" FROM {CATALOG}.gold.gold_prediction_log"
        f" ORDER BY logged_at DESC LIMIT 200"
    )
    try:
        df_preds = query_databricks(_q_preds)
        if df_preds.empty:
            raise ValueError("no data")
        versions = ", ".join(df_preds["model_version"].unique().tolist())
        st.markdown(f"**Model versions logged:** {versions}")
        st.dataframe(df_preds.head(20), use_container_width=True)
    except Exception:
        st.info("No prediction log data yet. Run the ML training job first.")

    # ---- DQ Pass Rates ----------------------------------------------
    st.markdown("---")
    st.markdown("### Data Quality Pass Rates")
    _q_dq = (
        f"SELECT run_date, layer, expectations_evaluated, expectations_passed,"
        f" ROUND(expectations_passed * 100.0 / expectations_evaluated, 1) AS pass_rate_pct"
        f" FROM {CATALOG}.gold.dq_checkpoint_results"
        f" ORDER BY run_date DESC LIMIT 30"
    )
    try:
        df_dq = query_databricks(_q_dq)
        if df_dq.empty:
            raise ValueError("no data")
        fig_dq = px.line(
            df_dq.sort_values("run_date"),
            x="run_date",
            y="pass_rate_pct",
            color="layer",
            title="Data Quality Pass Rate Over Time (%)",
            labels={"run_date": "Date", "pass_rate_pct": "Pass Rate %"},
        )
        fig_dq.add_hline(
            y=95,
            line_dash="dash",
            line_color="red",
            annotation_text="95% threshold",
        )
        st.plotly_chart(fig_dq, use_container_width=True)
    except Exception:
        st.info("No DQ checkpoint results yet. Run the data quality notebook first.")

# ================================================================
# Footer
# ================================================================

st.markdown("---")
st.markdown(
    "**Chicago 311 Intelligence Platform** | "
    "Azure Databricks + Lakeflow + MLflow | "
    f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
)
