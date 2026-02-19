"""
SnowPulse - Alerts History Page
Monitor triggered alerts with timeline and statistics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from connection import run_query

st.set_page_config(page_title="Alerts | SnowPulse", page_icon="🚨", layout="wide")

st.title("🚨 Alerts Monitor")
st.caption("Automated market condition alerts — checked every 5 minutes")
st.divider()

# ─── Load Data ──────────────────────────────────────────────
try:
    alerts = run_query("SELECT * FROM COMMON.ALERT_LOG ORDER BY TRIGGERED_AT DESC LIMIT 200")
    has_alerts = not alerts.empty
except Exception:
    alerts = pd.DataFrame()
    has_alerts = False

# ─── Alert Rules Reference ─────────────────────────────────
with st.expander("📋 Alert Rules", expanded=False):
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        ### ⚠️ Big Daily Move
        **Condition:** `|Return| > 3%`
        **Cooldown:** 24h per ticker
        """)
    with col2:
        st.markdown("""
        ### 🔄 Trend Change
        **Condition:** SMA5/SMA20 crossover flips
        **Cooldown:** 24h per ticker
        """)
    with col3:
        st.markdown("""
        ### 📊 High Volume
        **Condition:** Volume > 2x 20-day avg
        **Cooldown:** 24h per ticker
        """)

st.divider()

if not has_alerts:
    st.info(
        "🔔 **No alerts triggered yet.** Alerts are checked every 5 minutes. "
        "They will appear here when market conditions are met "
        "(e.g. a ticker moves > 3%, a trend reversal, or unusual volume)."
    )
    st.stop()

# ─── KPIs ───────────────────────────────────────────────────
total = len(alerts)
types = alerts["ALERT_NAME"].nunique()
tickers_hit = alerts["TICKER"].nunique()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Alerts", total)
c2.metric("Alert Types", types)
c3.metric("Tickers Affected", tickers_hit)
c4.metric("Latest", str(alerts.iloc[0]["TRIGGERED_AT"])[:16] if total > 0 else "N/A")

st.divider()

# ─── Alerts by Type ─────────────────────────────────────────
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("By Alert Type")
    type_counts = alerts["ALERT_NAME"].value_counts().reset_index()
    type_counts.columns = ["Alert", "Count"]
    color_map = {
        "BIG_DAILY_MOVE": "#FFA15A",
        "TREND_CHANGE": "#636EFA",
        "HIGH_VOLUME": "#00CC96",
    }
    fig_type = px.bar(
        type_counts, x="Alert", y="Count",
        color="Alert", color_discrete_map=color_map,
        text="Count",
    )
    fig_type.update_layout(
        template="plotly_dark", height=350, showlegend=False,
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig_type, use_container_width=True)

with col_chart2:
    st.subheader("By Ticker")
    ticker_counts = alerts["TICKER"].value_counts().reset_index()
    ticker_counts.columns = ["Ticker", "Count"]
    fig_ticker = px.pie(
        ticker_counts, values="Count", names="Ticker",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_ticker.update_layout(
        template="plotly_dark", height=350,
        margin=dict(l=20, r=20, t=20, b=40),
    )
    st.plotly_chart(fig_ticker, use_container_width=True)

st.divider()

# ─── Timeline ──────────────────────────────────────────────
st.subheader("📅 Alert Timeline")

alerts["TRIGGERED_AT"] = pd.to_datetime(alerts["TRIGGERED_AT"])
fig_timeline = px.scatter(
    alerts,
    x="TRIGGERED_AT",
    y="TICKER",
    color="ALERT_NAME",
    size="METRIC_VALUE" if "METRIC_VALUE" in alerts.columns else None,
    hover_data=["MESSAGE"],
    color_discrete_map=color_map,
)
fig_timeline.update_layout(
    template="plotly_dark", height=400,
    xaxis_title="Time", yaxis_title="Ticker",
    margin=dict(l=80, r=20, t=20, b=40),
)
st.plotly_chart(fig_timeline, use_container_width=True)

st.divider()

# ─── Alert Feed ─────────────────────────────────────────────
st.subheader("📋 Alert Feed")

# Filters
col_f1, col_f2 = st.columns(2)
with col_f1:
    type_filter = st.selectbox(
        "Filter by Type", ["All"] + sorted(alerts["ALERT_NAME"].unique().tolist())
    )
with col_f2:
    ticker_filter = st.selectbox(
        "Filter by Ticker", ["All"] + sorted(alerts["TICKER"].unique().tolist())
    )

filtered = alerts.copy()
if type_filter != "All":
    filtered = filtered[filtered["ALERT_NAME"] == type_filter]
if ticker_filter != "All":
    filtered = filtered[filtered["TICKER"] == ticker_filter]

for _, row in filtered.head(30).iterrows():
    alert_name = row.get("ALERT_NAME", "")
    if alert_name == "BIG_DAILY_MOVE":
        icon = "⚠️"
    elif alert_name == "TREND_CHANGE":
        icon = "🔄"
    elif alert_name == "HIGH_VOLUME":
        icon = "📊"
    else:
        icon = "ℹ️"

    triggered = str(row.get("TRIGGERED_AT", ""))[:19]
    ticker_val = row.get("TICKER", "")
    message = row.get("MESSAGE", "")
    metric = row.get("METRIC_VALUE", "")

    st.markdown(
        f"{icon} **{triggered}** — `{alert_name}` — **{ticker_val}** — {message}"
    )

st.divider()

# ─── Raw Data ──────────────────────────────────────────────
with st.expander("📄 Raw Alert Data"):
    st.dataframe(filtered, use_container_width=True, hide_index=True)

# ─── Footer ────────────────────────────────────────────────
st.divider()
st.caption("⚡ SnowPulse — Alerts Monitor | Checked every 5 min | Data: Snowflake Alerts")
