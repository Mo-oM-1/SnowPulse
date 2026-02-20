"""
SnowPulse — Macro Context Page
Cross-references Magnificent Seven stock prices with macroeconomic indicators
from Snowflake Marketplace (CPI Inflation, Treasury 10Y Yield).
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

st.set_page_config(page_title="Macro Context | SnowPulse", page_icon="🌍", layout="wide")

# ── Import shared connection ──
from connection import get_connection

conn = get_connection()

# ─────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────
st.title("🌍 Macro Context")
st.markdown(
    "Magnificent Seven stock prices enriched with **macroeconomic indicators** "
    "from the **Snowflake Marketplace** — CPI Inflation & Treasury 10Y Yield."
)

st.divider()

# ─────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_macro_overview():
    query = """
        SELECT TICKER, MONTH, AVG_CLOSE, MONTH_HIGH, MONTH_LOW,
               CPI_INDEX, INFLATION_YOY_PCT,
               TREASURY_10Y_INDEX, TREASURY_QOQ_CHANGE
        FROM GOLD.MACRO_OVERVIEW
        ORDER BY MONTH
    """
    return pd.DataFrame(conn.cursor().execute(query).fetchall(),
                        columns=["TICKER", "MONTH", "AVG_CLOSE", "MONTH_HIGH", "MONTH_LOW",
                                 "CPI_INDEX", "INFLATION_YOY_PCT",
                                 "TREASURY_10Y_INDEX", "TREASURY_QOQ_CHANGE"])

@st.cache_data(ttl=3600)
def load_cpi():
    query = """
        SELECT REPORT_DATE, CPI_INDEX, CPI_MOM_CHANGE_PCT, CPI_YOY_CHANGE_PCT
        FROM ANALYTICS.MACRO_CPI
        ORDER BY REPORT_DATE
    """
    return pd.DataFrame(conn.cursor().execute(query).fetchall(),
                        columns=["REPORT_DATE", "CPI_INDEX", "CPI_MOM_CHANGE_PCT", "CPI_YOY_CHANGE_PCT"])

@st.cache_data(ttl=3600)
def load_treasury():
    query = """
        SELECT REPORT_DATE, TREASURY_10Y_INDEX, QOQ_CHANGE
        FROM ANALYTICS.MACRO_TREASURY_10Y
        ORDER BY REPORT_DATE
    """
    return pd.DataFrame(conn.cursor().execute(query).fetchall(),
                        columns=["REPORT_DATE", "TREASURY_10Y_INDEX", "QOQ_CHANGE"])

try:
    df_macro = load_macro_overview()
    df_cpi = load_cpi()
    df_treasury = load_treasury()
    has_data = len(df_macro) > 0
except Exception:
    has_data = False

if not has_data:
    st.info(
        "🌍 **No macro data available yet.** "
        "Make sure you've installed the Snowflake Marketplace dataset "
        "(Snowflake Public Data — Free) and executed "
        "`deploy/06_marketplace/01_macro_enrichment.sql`."
    )
    st.stop()

# ─────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────
tickers = sorted(df_macro["TICKER"].unique())
selected_ticker = st.sidebar.selectbox("Select Ticker", tickers, index=0)

df_ticker = df_macro[df_macro["TICKER"] == selected_ticker].copy()

# ─────────────────────────────────────────────────────────────
# KPI cards
# ─────────────────────────────────────────────────────────────
st.subheader(f"📊 {selected_ticker} — Latest Macro Snapshot")

latest = df_ticker.iloc[-1] if len(df_ticker) > 0 else None

if latest is not None:
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Avg Close (Last Month)",
            f"${latest['AVG_CLOSE']:,.2f}" if pd.notna(latest['AVG_CLOSE']) else "N/A"
        )
    with col2:
        val = latest['INFLATION_YOY_PCT']
        st.metric(
            "CPI Inflation (YoY)",
            f"{val:.1f}%" if pd.notna(val) else "N/A"
        )
    with col3:
        val = latest['TREASURY_10Y_INDEX']
        st.metric(
            "Treasury 10Y Index",
            f"{val:.2f}" if pd.notna(val) else "N/A"
        )
    with col4:
        val = latest['CPI_INDEX']
        st.metric(
            "CPI Index",
            f"{val:.1f}" if pd.notna(val) else "N/A"
        )

st.divider()

# ─────────────────────────────────────────────────────────────
# Chart 1: Stock Price vs CPI Inflation (dual axis)
# ─────────────────────────────────────────────────────────────
st.subheader(f"📈 {selected_ticker} Price vs CPI Inflation (YoY %)")

fig1 = make_subplots(specs=[[{"secondary_y": True}]])

fig1.add_trace(
    go.Scatter(
        x=df_ticker["MONTH"], y=df_ticker["AVG_CLOSE"],
        name=f"{selected_ticker} Avg Close",
        line=dict(color="#00D4AA", width=2)
    ),
    secondary_y=False,
)

fig1.add_trace(
    go.Scatter(
        x=df_ticker["MONTH"], y=df_ticker["INFLATION_YOY_PCT"],
        name="CPI Inflation YoY %",
        line=dict(color="#FF6B6B", width=2, dash="dot")
    ),
    secondary_y=True,
)

fig1.update_layout(
    template="plotly_dark",
    height=450,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
)
fig1.update_yaxes(title_text="Stock Price ($)", secondary_y=False)
fig1.update_yaxes(title_text="Inflation YoY (%)", secondary_y=True)

st.plotly_chart(fig1, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# Chart 2: Stock Price vs Treasury 10Y (dual axis)
# ─────────────────────────────────────────────────────────────
st.subheader(f"📈 {selected_ticker} Price vs Treasury 10Y Yield Index")

fig2 = make_subplots(specs=[[{"secondary_y": True}]])

fig2.add_trace(
    go.Scatter(
        x=df_ticker["MONTH"], y=df_ticker["AVG_CLOSE"],
        name=f"{selected_ticker} Avg Close",
        line=dict(color="#00D4AA", width=2)
    ),
    secondary_y=False,
)

fig2.add_trace(
    go.Scatter(
        x=df_ticker["MONTH"], y=df_ticker["TREASURY_10Y_INDEX"],
        name="Treasury 10Y Index",
        line=dict(color="#FFD93D", width=2, dash="dot")
    ),
    secondary_y=True,
)

fig2.update_layout(
    template="plotly_dark",
    height=450,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
)
fig2.update_yaxes(title_text="Stock Price ($)", secondary_y=False)
fig2.update_yaxes(title_text="Treasury 10Y Index", secondary_y=True)

st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────
# Chart 3: All Mag7 normalized vs CPI
# ─────────────────────────────────────────────────────────────
st.subheader("🏢 Magnificent Seven — Normalized Performance vs Inflation")

fig3 = make_subplots(specs=[[{"secondary_y": True}]])

colors = {
    "AAPL": "#00D4AA", "MSFT": "#4CC9F0", "GOOGL": "#F72585",
    "AMZN": "#FF9E00", "TSLA": "#FF4444", "NVDA": "#7B2FBE", "META": "#3A86FF"
}

for ticker in tickers:
    df_t = df_macro[df_macro["TICKER"] == ticker].copy()
    if len(df_t) > 0:
        base = df_t["AVG_CLOSE"].iloc[0]
        if base and base > 0:
            df_t["NORMALIZED"] = (df_t["AVG_CLOSE"] / base) * 100
            fig3.add_trace(
                go.Scatter(
                    x=df_t["MONTH"], y=df_t["NORMALIZED"],
                    name=ticker,
                    line=dict(color=colors.get(ticker, "#FFFFFF"), width=2)
                ),
                secondary_y=False,
            )

# Add CPI on secondary axis
fig3.add_trace(
    go.Scatter(
        x=df_cpi["REPORT_DATE"], y=df_cpi["CPI_YOY_CHANGE_PCT"],
        name="CPI Inflation YoY %",
        line=dict(color="#FF6B6B", width=3, dash="dash"),
        opacity=0.8
    ),
    secondary_y=True,
)

fig3.update_layout(
    template="plotly_dark",
    height=550,
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
)
fig3.update_yaxes(title_text="Normalized Price (Base 100)", secondary_y=False)
fig3.update_yaxes(title_text="CPI Inflation YoY (%)", secondary_y=True)

st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────
# Chart 4: CPI Index Evolution
# ─────────────────────────────────────────────────────────────
st.subheader("📊 CPI Index & Inflation Rate")

col1, col2 = st.columns(2)

with col1:
    fig_cpi1 = go.Figure()
    fig_cpi1.add_trace(go.Scatter(
        x=df_cpi["REPORT_DATE"], y=df_cpi["CPI_INDEX"],
        fill="tozeroy",
        line=dict(color="#FF6B6B", width=2),
        name="CPI Index"
    ))
    fig_cpi1.update_layout(
        template="plotly_dark", height=350,
        title="CPI Index (Base 1982-84 = 100)",
    )
    st.plotly_chart(fig_cpi1, use_container_width=True)

with col2:
    fig_cpi2 = go.Figure()
    fig_cpi2.add_trace(go.Bar(
        x=df_cpi["REPORT_DATE"], y=df_cpi["CPI_YOY_CHANGE_PCT"],
        marker_color=[
            "#FF4444" if v and v > 3 else "#FFD93D" if v and v > 2 else "#00D4AA"
            for v in df_cpi["CPI_YOY_CHANGE_PCT"]
        ],
        name="YoY Inflation %"
    ))
    fig_cpi2.add_hline(y=2, line_dash="dash", line_color="white",
                       annotation_text="Fed Target (2%)")
    fig_cpi2.update_layout(
        template="plotly_dark", height=350,
        title="Year-over-Year Inflation Rate (%)",
    )
    st.plotly_chart(fig_cpi2, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────
# Chart 5: Treasury 10Y Evolution
# ─────────────────────────────────────────────────────────────
st.subheader("📊 Treasury 10Y Yield Index")

fig_t = go.Figure()
fig_t.add_trace(go.Scatter(
    x=df_treasury["REPORT_DATE"], y=df_treasury["TREASURY_10Y_INDEX"],
    fill="tozeroy",
    line=dict(color="#FFD93D", width=2),
    name="Treasury 10Y Index"
))
fig_t.update_layout(
    template="plotly_dark", height=350,
    title="10-Year Treasury Yield Index (Quarterly)",
)
st.plotly_chart(fig_t, use_container_width=True)

st.divider()

# ─────────────────────────────────────────────────────────────
# Data Table
# ─────────────────────────────────────────────────────────────
st.subheader(f"📋 {selected_ticker} — Monthly Data with Macro Indicators")

display_df = df_ticker[["MONTH", "AVG_CLOSE", "MONTH_HIGH", "MONTH_LOW",
                         "CPI_INDEX", "INFLATION_YOY_PCT",
                         "TREASURY_10Y_INDEX"]].copy()
display_df.columns = ["Month", "Avg Close ($)", "High ($)", "Low ($)",
                       "CPI Index", "Inflation YoY (%)", "Treasury 10Y"]
display_df = display_df.sort_values("Month", ascending=False)

st.dataframe(display_df, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "🌍 Macro data from Snowflake Marketplace — Snowflake Public Data (Free) | "
    "CPI: Bureau of Labor Statistics | Treasury: Federal Reserve | "
    "Stock prices: Nasdaq via Cybersyn"
)
