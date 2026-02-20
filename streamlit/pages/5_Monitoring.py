"""
SnowPulse — Monitoring Page
Pipeline health, ingestion logs, alert history, and Dynamic Table status.
"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from connection import get_connection

st.set_page_config(page_title="Monitoring | SnowPulse", page_icon="🔧", layout="wide")

conn = get_connection()

st.title("🔧 Pipeline Monitoring")
st.caption("Real-time observability — ingestion logs, alert history, and system health")
st.divider()

# ─────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_pipeline_logs():
    query = """
        SELECT LOG_ID, LOGGED_AT, LEVEL, COMPONENT_NAME, MESSAGE, STACK_TRACE
        FROM COMMON.PIPELINE_LOGS
        ORDER BY LOGGED_AT DESC
        LIMIT 500
    """
    return pd.DataFrame(conn.cursor().execute(query).fetchall(),
                        columns=["LOG_ID", "LOGGED_AT", "LEVEL", "COMPONENT", "MESSAGE", "STACK_TRACE"])

@st.cache_data(ttl=60)
def load_alert_log():
    query = """
        SELECT ALERT_ID, TRIGGERED_AT, ALERT_NAME, TICKER, MESSAGE, METRIC_VALUE
        FROM COMMON.ALERT_LOG
        ORDER BY TRIGGERED_AT DESC
        LIMIT 200
    """
    return pd.DataFrame(conn.cursor().execute(query).fetchall(),
                        columns=["ALERT_ID", "TRIGGERED_AT", "ALERT_NAME", "TICKER", "MESSAGE", "METRIC_VALUE"])

@st.cache_data(ttl=60)
def load_dt_status():
    query = """
        SELECT NAME, SCHEMA_NAME, TARGET_LAG, SCHEDULING_STATE
        FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
        ORDER BY SCHEMA_NAME, NAME
    """
    return pd.DataFrame(conn.cursor().execute(query).fetchall(),
                        columns=["NAME", "SCHEMA", "TARGET_LAG", "STATE"])

@st.cache_data(ttl=60)
def load_row_counts():
    query = """
        SELECT 'RAW_TRADES' AS TBL, COUNT(*) AS CNT FROM RAW.RAW_TRADES
        UNION ALL SELECT 'RAW_AGGREGATES', COUNT(*) FROM RAW.RAW_AGGREGATES
        UNION ALL SELECT 'RAW_NEWS', COUNT(*) FROM RAW.RAW_NEWS
        UNION ALL SELECT 'DAILY_OHLCV', COUNT(*) FROM ANALYTICS.DAILY_OHLCV
        UNION ALL SELECT 'NEWS_SENTIMENT', COUNT(*) FROM ANALYTICS.NEWS_SENTIMENT
        UNION ALL SELECT 'TICKER_SUMMARY', COUNT(*) FROM GOLD.TICKER_SUMMARY
        UNION ALL SELECT 'MACRO_OVERVIEW', COUNT(*) FROM GOLD.MACRO_OVERVIEW
    """
    return pd.DataFrame(conn.cursor().execute(query).fetchall(),
                        columns=["TABLE", "ROW_COUNT"])

try:
    logs = load_pipeline_logs()
    alerts = load_alert_log()
    has_logs = not logs.empty
    has_alerts = not alerts.empty
except Exception:
    logs = pd.DataFrame()
    alerts = pd.DataFrame()
    has_logs = False
    has_alerts = False

try:
    dt_status = load_dt_status()
    has_dt = not dt_status.empty
except Exception:
    dt_status = pd.DataFrame()
    has_dt = False

try:
    row_counts = load_row_counts()
except Exception:
    row_counts = pd.DataFrame()

# ─────────────────────────────────────────────────────────────
# KPI Cards
# ─────────────────────────────────────────────────────────────
st.subheader("📊 System Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_logs = len(logs) if has_logs else 0
    st.metric("Pipeline Logs", f"{total_logs}")

with col2:
    errors = len(logs[logs["LEVEL"] == "ERROR"]) if has_logs else 0
    st.metric("Errors", f"{errors}", delta=None if errors == 0 else f"{errors} errors",
              delta_color="inverse")

with col3:
    total_alerts = len(alerts) if has_alerts else 0
    st.metric("Alerts Triggered", f"{total_alerts}")

with col4:
    if has_logs and len(logs) > 0:
        last_log = logs["LOGGED_AT"].iloc[0]
        st.metric("Last Ingestion", str(last_log)[:16])
    else:
        st.metric("Last Ingestion", "N/A")

st.divider()

# ─────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📋 Pipeline Logs", "🚨 Alert History", "⚡ Dynamic Tables", "📦 Data Volume"])

# ─── Tab 1: Pipeline Logs ────────────────────────────────────
with tab1:
    if has_logs:
        # Filters
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            level_filter = st.multiselect(
                "Filter by Level",
                options=logs["LEVEL"].unique().tolist(),
                default=logs["LEVEL"].unique().tolist()
            )
        with col_f2:
            component_filter = st.multiselect(
                "Filter by Component",
                options=logs["COMPONENT"].dropna().unique().tolist(),
                default=logs["COMPONENT"].dropna().unique().tolist()
            )

        filtered_logs = logs[
            (logs["LEVEL"].isin(level_filter)) &
            (logs["COMPONENT"].isin(component_filter))
        ]

        # Log table
        st.markdown(f"**{len(filtered_logs)}** logs matching filters")
        st.dataframe(
            filtered_logs[["LOGGED_AT", "LEVEL", "COMPONENT", "MESSAGE"]],
            use_container_width=True,
            hide_index=True
        )

        # Expandable log details
        st.markdown("#### Log Details")
        for _, row in filtered_logs.head(50).iterrows():
            level = row["LEVEL"]
            icon = "✅" if level == "INFO" else "⚠️" if level == "WARNING" else "❌"
            timestamp = str(row["LOGGED_AT"])[:19]
            with st.expander(f"{icon} [{timestamp}] **{level}** — {row['COMPONENT']}: {row['MESSAGE'][:100]}"):
                st.text(f"Component: {row['COMPONENT']}")
                st.text(f"Message: {row['MESSAGE']}")
                if row["STACK_TRACE"] and str(row["STACK_TRACE"]) != "None":
                    st.code(row["STACK_TRACE"], language="text")
    else:
        st.info("📋 **No pipeline logs yet.** Logs will appear here once the ingestion script starts running.")

# ─── Tab 2: Alert History ────────────────────────────────────
with tab2:
    if has_alerts:
        # Alert table
        st.dataframe(
            alerts[["TRIGGERED_AT", "ALERT_NAME", "TICKER", "MESSAGE", "METRIC_VALUE"]],
            use_container_width=True,
            hide_index=True
        )

        # Alert feed
        st.markdown("#### Recent Alerts")
        for _, row in alerts.head(30).iterrows():
            name = row["ALERT_NAME"]
            icon = "📈" if "BIG" in name else "🔄" if "TREND" in name else "📊"
            timestamp = str(row["TRIGGERED_AT"])[:19]
            st.markdown(
                f"{icon} **[{timestamp}]** `{name}` — **{row['TICKER']}**: "
                f"{row['MESSAGE']} (value: {row['METRIC_VALUE']:.2f})"
            )
    else:
        st.info(
            "🚨 **No alerts triggered yet.** Alerts are checked every 5 minutes. "
            "They will appear here when market conditions are met."
        )

# ─── Tab 3: Dynamic Tables ──────────────────────────────────
with tab3:
    if has_dt:
        st.markdown("#### Dynamic Table Status")

        for _, row in dt_status.iterrows():
            state = row["STATE"]
            icon = "🟢" if state == "RUNNING" else "🟡" if state == "SUSPENDED" else "🔴"
            st.markdown(
                f"{icon} **{row['SCHEMA']}.{row['NAME']}** — "
                f"Target Lag: `{row['TARGET_LAG']}` — State: `{state}`"
            )
    else:
        st.info("⚡ **No Dynamic Tables found.** Execute the SQL deploy scripts to create them.")

# ─── Tab 4: Data Volume ─────────────────────────────────────
with tab4:
    if not row_counts.empty:
        st.markdown("#### Row Counts per Table")
        st.dataframe(row_counts, use_container_width=True, hide_index=True)
    else:
        st.info("📦 **No data volume information available.**")

# ─────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "🔧 SnowPulse Monitoring | Pipeline Logs: COMMON.PIPELINE_LOGS | "
    "Alert History: COMMON.ALERT_LOG | Auto-refresh: 60s cache"
)
