"""
SnowPulse - Home Page
Real-Time Market Intelligence Dashboard
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from connection import run_query

# ─── Page Config ────────────────────────────────────────────
st.set_page_config(
    page_title="SnowPulse",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ─────────────────────────────────────────────
st.markdown("""
<style>
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #0f3460;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
    }
    div[data-testid="stMetric"] label {
        color: #a8b2d1 !important;
        font-size: 14px !important;
        font-weight: 600 !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #ccd6f6 !important;
        font-size: 28px !important;
    }
    .main-title {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #00d2ff, #3a7bd5);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0;
    }
    .sub-title {
        color: #8892b0;
        font-size: 1.1rem;
        margin-top: -10px;
    }
</style>
""", unsafe_allow_html=True)

# ─── Sidebar ────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ SnowPulse")
    st.caption("Real-Time Market Intelligence")
    st.divider()

    summary = run_query("SELECT * FROM GOLD.TICKER_SUMMARY ORDER BY TICKER")
    tickers = sorted(summary["TICKER"].unique()) if not summary.empty else []
    selected = st.multiselect("Filter Tickers", tickers, default=tickers)

    st.divider()
    st.markdown(
        "**Tech Stack**\n"
        "- ⚡ Snowpipe Streaming\n"
        "- 🔄 Dynamic Tables\n"
        "- 🚨 Alerts\n"
        "- 🧠 Cortex Sentiment\n"
        "- 📊 Streamlit"
    )
    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ─── Filter ─────────────────────────────────────────────────
summary_f = summary[summary["TICKER"].isin(selected)] if selected else summary

# ─── Header ─────────────────────────────────────────────────
st.markdown('<p class="main-title">SnowPulse Dashboard</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="sub-title">Magnificent Seven — Real-time market intelligence powered by Snowflake</p>',
    unsafe_allow_html=True,
)
st.divider()

# ─── KPI Cards ──────────────────────────────────────────────
if not summary_f.empty:
    cols = st.columns(len(summary_f))
    for i, (_, row) in enumerate(summary_f.iterrows()):
        with cols[i]:
            ret = row.get("LAST_RETURN_PCT", 0) or 0
            trend = row.get("TREND_SIGNAL", "N/A") or "N/A"
            close_val = row.get("LAST_CLOSE")
            st.metric(
                label=row["TICKER"],
                value=f"${close_val:,.2f}" if pd.notna(close_val) else "N/A",
                delta=f"{ret:+.2f}%" if ret else None,
            )
            if trend == "BULLISH":
                st.caption("🟢 BULLISH")
            elif trend == "BEARISH":
                st.caption("🔴 BEARISH")
            else:
                st.caption(f"⚪ {trend}")

st.divider()

# ─── Normalized Performance Chart ───────────────────────────
ohlcv = run_query("SELECT * FROM ANALYTICS.DAILY_OHLCV ORDER BY TICKER, TRADE_DATE")
ohlcv_f = ohlcv[ohlcv["TICKER"].isin(selected)] if selected else ohlcv

if not ohlcv_f.empty:
    st.subheader("📈 Relative Performance (Base 100)")

    perf_data = []
    for ticker in sorted(ohlcv_f["TICKER"].unique()):
        t_df = ohlcv_f[ohlcv_f["TICKER"] == ticker].sort_values("TRADE_DATE").copy()
        if not t_df.empty and t_df.iloc[0]["CLOSE_PRICE"] > 0:
            base = t_df.iloc[0]["CLOSE_PRICE"]
            t_df["PERF"] = ((t_df["CLOSE_PRICE"] / base) - 1) * 100
            perf_data.append(t_df)

    if perf_data:
        perf_df = pd.concat(perf_data)
        fig = go.Figure()
        colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692"]
        for idx, ticker in enumerate(sorted(perf_df["TICKER"].unique())):
            t_data = perf_df[perf_df["TICKER"] == ticker]
            fig.add_trace(go.Scatter(
                x=t_data["TRADE_DATE"], y=t_data["PERF"],
                name=ticker,
                line=dict(color=colors[idx % len(colors)], width=2.5),
                hovertemplate="%{x}<br>%{y:+.2f}%<extra>" + ticker + "</extra>",
            ))
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        fig.update_layout(
            template="plotly_dark",
            height=450,
            hovermode="x unified",
            yaxis_title="Performance (%)",
            xaxis_title="",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
            margin=dict(l=60, r=20, t=40, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)

# ─── Summary Table ──────────────────────────────────────────
if not summary_f.empty:
    st.subheader("📋 Ticker Summary")
    st.dataframe(
        summary_f,
        use_container_width=True,
        hide_index=True,
        column_config={
            "LAST_CLOSE": st.column_config.NumberColumn("Last Close", format="$%.2f"),
            "OPEN_PRICE": st.column_config.NumberColumn("Open", format="$%.2f"),
            "HIGH_PRICE": st.column_config.NumberColumn("High", format="$%.2f"),
            "LOW_PRICE": st.column_config.NumberColumn("Low", format="$%.2f"),
            "SMA_5": st.column_config.NumberColumn("SMA 5", format="$%.2f"),
            "SMA_20": st.column_config.NumberColumn("SMA 20", format="$%.2f"),
            "LAST_RETURN_PCT": st.column_config.NumberColumn("Return %", format="%.2f%%"),
            "LAST_VOLUME": st.column_config.NumberColumn("Volume", format="%d"),
        },
    )

# ─── Beta vs S&P 500 ─────────────────────────────────────────
try:
    beta_data = run_query("""
        SELECT TICKER, TRADE_DATE, BETA_60D, R_SQUARED_60D, RISK_PROFILE
        FROM GOLD.TICKER_BETA
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY TRADE_DATE DESC) = 1
    """)
    has_beta = not beta_data.empty
except Exception:
    beta_data = pd.DataFrame()
    has_beta = False

if has_beta:
    beta_f = beta_data[beta_data["TICKER"].isin(selected)] if selected else beta_data

    if not beta_f.empty:
        st.subheader("📐 Beta vs S&P 500 (SPY)")
        st.caption("Rolling 60-day Beta — Measures stock volatility relative to the market")

        col_chart, col_table = st.columns([2, 1])

        with col_chart:
            beta_sorted = beta_f.sort_values("BETA_60D", ascending=True)
            colors = []
            for b in beta_sorted["BETA_60D"]:
                if b > 1.2:
                    colors.append("#EF553B")
                elif b < 0.8:
                    colors.append("#00CC96")
                else:
                    colors.append("#636EFA")

            fig_beta = go.Figure(go.Bar(
                x=beta_sorted["BETA_60D"],
                y=beta_sorted["TICKER"],
                orientation="h",
                marker_color=colors,
                text=[f"{b:.2f}" for b in beta_sorted["BETA_60D"]],
                textposition="outside",
                hovertemplate="%{y}<br>Beta: %{x:.3f}<extra></extra>",
            ))
            fig_beta.add_vline(x=1.0, line_dash="dash", line_color="white", opacity=0.5,
                               annotation_text="Market (1.0)", annotation_position="top")
            fig_beta.update_layout(
                template="plotly_dark",
                height=350,
                xaxis_title="Beta (60-day)",
                yaxis_title="",
                margin=dict(l=80, r=60, t=30, b=40),
            )
            st.plotly_chart(fig_beta, use_container_width=True)

        with col_table:
            st.markdown("**Risk Profile**")
            for _, row in beta_f.sort_values("BETA_60D", ascending=False).iterrows():
                beta_val = row["BETA_60D"]
                r2 = row.get("R_SQUARED_60D", 0) or 0
                profile = row.get("RISK_PROFILE", "NEUTRAL")

                if profile == "HIGH_RISK":
                    icon = "🔴"
                elif profile == "DEFENSIVE":
                    icon = "🟢"
                else:
                    icon = "⚪"

                st.markdown(
                    f"{icon} **{row['TICKER']}** — Beta: `{beta_val:.2f}` | "
                    f"R²: `{r2:.2f}` | {profile}"
                )

        st.divider()

# ─── Footer ─────────────────────────────────────────────────
st.divider()
st.caption(
    "⚡ SnowPulse | Snowpipe Streaming + Dynamic Tables + Cortex + Alerts | "
    "Data: Massive (Polygon.io) | Auto-refresh: 1 min"
)
