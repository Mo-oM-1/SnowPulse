"""
SnowPulse - Technical Analysis Page
Candlestick charts, Moving Averages, Volume, Returns
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from connection import run_query

st.set_page_config(page_title="Technical Analysis | SnowPulse", page_icon="📊", layout="wide")

# ─── Custom CSS ─────────────────────────────────────────────
st.markdown("""
<style>
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #0f3460;
        border-radius: 12px;
        padding: 12px 16px;
    }
</style>
""", unsafe_allow_html=True)

# ─── Data ───────────────────────────────────────────────────
ohlcv = run_query("SELECT * FROM ANALYTICS.DAILY_OHLCV ORDER BY TICKER, TRADE_DATE")
returns = run_query("SELECT * FROM ANALYTICS.DAILY_RETURNS ORDER BY TICKER, TRADE_DATE")
ma = run_query("SELECT * FROM ANALYTICS.MOVING_AVERAGES ORDER BY TICKER, TRADE_DATE")

try:
    rsi = run_query("SELECT * FROM ANALYTICS.RSI_14 ORDER BY TICKER, TRADE_DATE")
    has_rsi = not rsi.empty
except Exception:
    rsi = pd.DataFrame()
    has_rsi = False

st.title("📊 Technical Analysis")
st.divider()

if ohlcv.empty:
    st.warning("No OHLCV data available.")
    st.stop()

# ─── Ticker Selector ────────────────────────────────────────
tickers = sorted(ohlcv["TICKER"].unique())
col_sel, col_spacer = st.columns([1, 3])
with col_sel:
    ticker = st.selectbox("Select Ticker", tickers, index=0)

t_ohlcv = ohlcv[ohlcv["TICKER"] == ticker].sort_values("TRADE_DATE")
t_returns = returns[returns["TICKER"] == ticker].sort_values("TRADE_DATE")
t_ma = ma[ma["TICKER"] == ticker].sort_values("TRADE_DATE")
t_rsi = rsi[rsi["TICKER"] == ticker].sort_values("TRADE_DATE") if has_rsi else pd.DataFrame()

# ─── KPI Row ────────────────────────────────────────────────
if not t_ohlcv.empty:
    latest = t_ohlcv.iloc[-1]
    prev = t_ohlcv.iloc[-2] if len(t_ohlcv) > 1 else latest
    day_change = latest["CLOSE_PRICE"] - prev["CLOSE_PRICE"]
    day_change_pct = (day_change / prev["CLOSE_PRICE"]) * 100 if prev["CLOSE_PRICE"] else 0
    day_range = latest["HIGH_PRICE"] - latest["LOW_PRICE"]

    # Period performance
    first_close = t_ohlcv.iloc[0]["CLOSE_PRICE"]
    period_pct = ((latest["CLOSE_PRICE"] - first_close) / first_close) * 100 if first_close else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Close", f"${latest['CLOSE_PRICE']:,.2f}", f"{day_change_pct:+.2f}%")
    c2.metric("High", f"${latest['HIGH_PRICE']:,.2f}")
    c3.metric("Low", f"${latest['LOW_PRICE']:,.2f}")
    c4.metric("Volume", f"{latest['VOLUME']:,.0f}")
    c5.metric("30d Perf", f"{period_pct:+.2f}%")

st.divider()

# ─── Candlestick + Volume Chart ─────────────────────────────
st.subheader(f"🕯️ {ticker} — Candlestick Chart")

fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True,
    vertical_spacing=0.03,
    row_heights=[0.7, 0.3],
    subplot_titles=("", "Volume"),
)

# Candlestick
fig.add_trace(go.Candlestick(
    x=t_ohlcv["TRADE_DATE"],
    open=t_ohlcv["OPEN_PRICE"],
    high=t_ohlcv["HIGH_PRICE"],
    low=t_ohlcv["LOW_PRICE"],
    close=t_ohlcv["CLOSE_PRICE"],
    name="OHLC",
    increasing_line_color="#00CC96",
    decreasing_line_color="#EF553B",
), row=1, col=1)

# Moving Averages overlay
if not t_ma.empty:
    fig.add_trace(go.Scatter(
        x=t_ma["TRADE_DATE"], y=t_ma["SMA_5"],
        name="SMA 5", line=dict(color="#FFA15A", width=1.5, dash="dash"),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=t_ma["TRADE_DATE"], y=t_ma["SMA_10"],
        name="SMA 10", line=dict(color="#19D3F3", width=1.5, dash="dash"),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=t_ma["TRADE_DATE"], y=t_ma["SMA_20"],
        name="SMA 20", line=dict(color="#AB63FA", width=1.5, dash="dot"),
    ), row=1, col=1)

# Volume bars (colored by direction)
vol_colors = [
    "#00CC96" if c >= o else "#EF553B"
    for c, o in zip(t_ohlcv["CLOSE_PRICE"], t_ohlcv["OPEN_PRICE"])
]
fig.add_trace(go.Bar(
    x=t_ohlcv["TRADE_DATE"],
    y=t_ohlcv["VOLUME"],
    name="Volume",
    marker_color=vol_colors,
    opacity=0.7,
    showlegend=False,
), row=2, col=1)

fig.update_layout(
    template="plotly_dark",
    height=600,
    xaxis_rangeslider_visible=False,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    margin=dict(l=60, r=20, t=30, b=40),
)
fig.update_yaxes(title_text="Price ($)", row=1, col=1)
fig.update_yaxes(title_text="Volume", row=2, col=1)

st.plotly_chart(fig, use_container_width=True)

# ─── Trend Signal ───────────────────────────────────────────
if not t_ma.empty:
    latest_ma = t_ma.iloc[-1]
    signal = latest_ma.get("TREND_SIGNAL", "N/A")
    sma5 = latest_ma.get("SMA_5", 0)
    sma20 = latest_ma.get("SMA_20", 0)

    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        if signal == "BULLISH":
            st.success(f"🟢 **{ticker} Trend: BULLISH** — SMA 5 (${sma5:,.2f}) > SMA 20 (${sma20:,.2f})")
        else:
            st.error(f"🔴 **{ticker} Trend: BEARISH** — SMA 5 (${sma5:,.2f}) < SMA 20 (${sma20:,.2f})")

st.divider()

# ─── RSI 14-Day ─────────────────────────────────────────────
if not t_rsi.empty:
    st.subheader(f"📐 {ticker} — RSI 14-Day")

    latest_rsi = t_rsi.iloc[-1]
    rsi_val = latest_rsi.get("RSI_14")
    rsi_signal = latest_rsi.get("RSI_SIGNAL", "NEUTRAL")

    # RSI KPI
    col_rsi1, col_rsi2, col_rsi3 = st.columns([1, 1, 2])
    with col_rsi1:
        if rsi_val is not None and pd.notna(rsi_val):
            st.metric("RSI 14", f"{rsi_val:.1f}")
        else:
            st.metric("RSI 14", "N/A")
    with col_rsi2:
        if rsi_signal == "OVERBOUGHT":
            st.error(f"🔴 **OVERBOUGHT** (> 70)")
        elif rsi_signal == "OVERSOLD":
            st.success(f"🟢 **OVERSOLD** (< 30)")
        else:
            st.info(f"⚪ **NEUTRAL** (30-70)")

    # RSI Chart
    fig_rsi = go.Figure()
    fig_rsi.add_trace(go.Scatter(
        x=t_rsi["TRADE_DATE"], y=t_rsi["RSI_14"],
        name="RSI 14",
        line=dict(color="#FFA15A", width=2),
        fill="tozeroy",
        fillcolor="rgba(255,161,90,0.1)",
        hovertemplate="%{x}<br>RSI: %{y:.1f}<extra></extra>",
    ))

    # Overbought / Oversold zones
    fig_rsi.add_hline(y=70, line_dash="dash", line_color="#EF553B", opacity=0.7,
                      annotation_text="Overbought (70)", annotation_position="top left")
    fig_rsi.add_hline(y=30, line_dash="dash", line_color="#00CC96", opacity=0.7,
                      annotation_text="Oversold (30)", annotation_position="bottom left")
    fig_rsi.add_hline(y=50, line_dash="dot", line_color="gray", opacity=0.3)

    # Shade overbought/oversold zones
    fig_rsi.add_hrect(y0=70, y1=100, fillcolor="#EF553B", opacity=0.05, line_width=0)
    fig_rsi.add_hrect(y0=0, y1=30, fillcolor="#00CC96", opacity=0.05, line_width=0)

    fig_rsi.update_layout(
        template="plotly_dark",
        height=300,
        yaxis_title="RSI",
        yaxis_range=[0, 100],
        xaxis_title="",
        margin=dict(l=60, r=20, t=20, b=40),
        hovermode="x unified",
    )
    st.plotly_chart(fig_rsi, use_container_width=True)

    st.divider()

# ─── Daily Returns ──────────────────────────────────────────
st.subheader(f"📉 {ticker} — Daily Returns")

if not t_returns.empty:
    col_r1, col_r2 = st.columns([2, 1])

    with col_r1:
        ret_colors = ["#00CC96" if r >= 0 else "#EF553B" for r in t_returns["DAILY_RETURN_PCT"].fillna(0)]
        fig_ret = go.Figure(go.Bar(
            x=t_returns["TRADE_DATE"],
            y=t_returns["DAILY_RETURN_PCT"],
            marker_color=ret_colors,
            hovertemplate="%{x}<br>%{y:+.2f}%<extra></extra>",
        ))
        fig_ret.update_layout(
            template="plotly_dark",
            height=350,
            yaxis_title="Return (%)",
            xaxis_title="",
            margin=dict(l=60, r=20, t=20, b=40),
        )
        fig_ret.add_hline(y=0, line_color="gray", opacity=0.5)
        st.plotly_chart(fig_ret, use_container_width=True)

    with col_r2:
        valid_returns = t_returns["DAILY_RETURN_PCT"].dropna()
        if not valid_returns.empty:
            st.markdown("**Return Statistics**")
            st.markdown(f"- **Mean**: {valid_returns.mean():+.2f}%")
            st.markdown(f"- **Std Dev**: {valid_returns.std():.2f}%")
            st.markdown(f"- **Max**: {valid_returns.max():+.2f}%")
            st.markdown(f"- **Min**: {valid_returns.min():+.2f}%")
            st.markdown(f"- **Positive Days**: {(valid_returns > 0).sum()}/{len(valid_returns)}")
            st.markdown(f"- **Negative Days**: {(valid_returns < 0).sum()}/{len(valid_returns)}")

st.divider()

# ─── Cross-Ticker Heatmap ──────────────────────────────────
st.subheader("🔥 Returns Heatmap — All Tickers")

pivot = returns.pivot_table(index="TICKER", columns="TRADE_DATE", values="DAILY_RETURN_PCT")
if not pivot.empty:
    fig_heat = px.imshow(
        pivot,
        color_continuous_scale="RdYlGn",
        labels=dict(color="Return %"),
        aspect="auto",
    )
    fig_heat.update_layout(
        template="plotly_dark",
        height=350,
        margin=dict(l=80, r=20, t=20, b=40),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

# ─── Footer ─────────────────────────────────────────────────
st.divider()
st.caption("⚡ SnowPulse — Technical Analysis | Data: Massive (Polygon.io)")
