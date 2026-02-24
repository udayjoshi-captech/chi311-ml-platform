"""
Chicago 311 Intelligence Platform - Streamlit Dashboard
Connects to Azure Databricks SQL warehouse for live data
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph.objects as go
from datetime import datetime, timedelta
import os

# ================================================================
# Configuration
# ================================================================

st.set_page_config(
    page_title="Chicago 311 Intelligence Platform",
    page_icon="",
    layout="wide"
)

# Databricks SQL Connection (Azure)
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "abd-XXXXX.azuredatabricks.net")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "/sqk/1.0/warehouses/XXXXX")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CATALOG = "workspace"


def get_connection():
    """Get Databricks SQL connection."""
    try:
        from databricks import sql
        return sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
    except ImportError:
        st.warning("databricks-sql-connector not installed. Using sample data.")
        return None


def query_databricks(query: str) -> pd.DataFrame:
    """Execute query against Databricks SQL"""
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
    

# =================================================================================
# Dashboard Layout
# =================================================================================

st.title("Chicago 311 Intelligence Platform")
st.markdown("**Demand Forecasting & Anomaly Detection** | Azure Databricks")

# Sidebar
st.sidebar.header("Controls")
date_range = st.sidebar.selectbox(
    "Date Range",
    ["Last 7 Days", "Last 30 Days", "last 90 Days", "All Time"],
    index=1
)

# =================================================================================
# KPI Cards
# =================================================================================

st.markdown("---")
col1, col2, col3, col4 = st.columns(4)

# Try to load live data, fall back to sample
try:
    df_daily = query_databricks(f"""
        SELECT ds, y, is_weekend, completion_rate
        FROM {CATALOG}.gold.gold_citywide_daily_summary
        ORDER BY ds DESC
        LIMIT 90
""")
    
    if df_daily.empty:
        raise ValueError("No data returned")
    
    df_daily["ds"] = pd.to_datetime(df_daily["ds"])

    with col1:
        avg_daily = df_daily["y"].mean()
        st.metric("Avg Daily Requests", f"{avg_daily:,.0f}")
    
    with col2:
        latest = df_daily.iloc[0]["y"]
        prev = df_daily.iloc[1]["y"] if len(df_daily) > 1 else latest
        delta = ((latest - prev) / prev) * 100 if prev > 0 else 0
        st.metric("Latest Day", f"{latest:,.0f}", f"{delta:.1f}%")
    
    with col3:
        avg_completion = df_daily["completion_rate"].mean() * 100
        st.metric("Avg Completion Rate", f"{avg_completion:.1f}%")
    
    with col4:
        st.metric("Pipeline Status", "Healthy")

except Exception as e:
    # Sample data fallback
    st.info("Showing sample data. Connect to Databricks for live metrics.")

    dates = pd.date_range(end=datetime.now(), period=90, freq="D")
    import numpy as np
    np.random.seed(42)
    sample_y = np.random.normal(3000, 500, 90).clip(1500, 5000)
    df_daily = pd.DataFrame({"ds": dates, "y": sample_y})

    with col1:
        st.metric("Avg Daily Requests", f"{df_daily['y'].mean():,.0f}")
    with col2:
        st.metric("Latest Day", f"{df_daily['y'].iloc[-1]:,.0f}")
    with col3:
        st.metric("Avg Completion Rate", "78.3%")
    with col4:
        st.metric("Pipeline Status", "Sample Mode")
    
# =================================================================================
# Daily Volume Chart
# =================================================================================

st.markdown("### Daily Service Request Volume")

fig = px.line(
    df_daily.sort_values("ds"),
    x="ds", y="y",
    title="Daily 311 Service Request (excl. Info Calls)",
    labels={"ds": "Date", "y": "Request Count"}
)
fig.update_layout(height=400)
st.plotly_chart(fig, use_container_width=True)

# =================================================================================
# Anomaly Detection Results
# =================================================================================

st.markdown("### Anomaly Detection")

try:
    df_anomalies = query_databricks(f"""
        SELECT ds, y, anomaly_score, is_anomaly
        FROM {CATALOG}.gold.gold_anomaly_results
        WHERE is_anomaly = true
        ORDER BY ds DESC
        LIMIT 20
""")
    
    if not df_anomalies.empty:
        st.dataframe(df_anomalies, use_container_width=True)
    else:
        st.info("No anomalies detected in recent data.")
except Exception:
    st.info("Run anomaly detection notebook to populate this section.")

# =================================================================================
# Forecast
# =================================================================================

st.markdown('### 7-Day Forecast')

try:
    df_forecast = query_databricks(f"""
        SELECT ds, yhat, yhat_lower, yhat_upper
        FROM {CATALOG}.gold.gold_forecasts
        ORDER BY ds DESC
        LIMIT 7
""")
    
    if not df_forecast.empty:
        fig_fc = go.Figure()
        fig_fc.add_trace(go.Scatter(x=df_forecast["ds"], y=df_forecast["yhat"],
                                    mode="lines+markers", name="Forecast"))
        fig_fc.add_trace(go.Scatter(x=df_forecast["ds"], y=df_forecast["yhat_upper"],
                                    mode="lines", name="Upper Bound", line=dict(dash="dot")))
        fig_fc.add_trace(go.Scatter(x=df_forecast["ds"], y=df_forecast["yhat_lower"],
                                    mode="lines", name="Lower Bound", line=dict(dash="dot"),
                                    fill="tonexty"))
        fig_fc.update_layout(title="7-Day Demand Forecast", height=400)
        st.plotly_chart(fig_fc, use_container_width=True)
    else:
        st.info("No forecast data. Run the forecasting notebook first.")
except Exception:
    st.info("Run forecasting notebook to populate this section.")

# =================================================================================
# Footer
# =================================================================================

st.markdown("---")
st.markdown(
    "**Chicago 311 Intelligence Platform** | "
    "Azure Databricks + Lakeflow + MLflow |"
    f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
)