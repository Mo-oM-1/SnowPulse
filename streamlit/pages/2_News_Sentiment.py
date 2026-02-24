"""
SnowPulse - News & Sentiment Page
Cortex-powered sentiment analysis on financial news
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from connection import run_query

st.set_page_config(page_title="News & Sentiment | SnowPulse", page_icon="🧠", layout="wide")

st.title("🧠 News & Sentiment Analysis")
st.caption("Powered by Snowflake Cortex LLM")
st.divider()

# ─── Load Data (gracefully handle missing tables) ──────────
try:
    sentiment = run_query(
        "SELECT * FROM ANALYTICS.NEWS_SENTIMENT ORDER BY PUBLISHED_AT DESC LIMIT 200"
    )
    has_sentiment = not sentiment.empty
except Exception:
    sentiment = pd.DataFrame()
    has_sentiment = False

try:
    sentiment_summary = run_query("SELECT * FROM GOLD.SENTIMENT_SUMMARY ORDER BY TICKER")
    has_summary = not sentiment_summary.empty
except Exception:
    sentiment_summary = pd.DataFrame()
    has_summary = False

try:
    news_flat = run_query(
        "SELECT * FROM ANALYTICS.NEWS_FLATTENED ORDER BY PUBLISHED_AT DESC LIMIT 100"
    )
    has_news = not news_flat.empty
except Exception:
    news_flat = pd.DataFrame()
    has_news = False

try:
    momentum = run_query(
        "SELECT * FROM GOLD.SENTIMENT_MOMENTUM ORDER BY TICKER, SENTIMENT_DATE"
    )
    has_momentum = not momentum.empty
except Exception:
    momentum = pd.DataFrame()
    has_momentum = False

# ─── No Data State ──────────────────────────────────────────
if not has_sentiment and not has_news:
    st.info(
        "🔄 **No news data yet.** Run the streaming script to ingest news articles, "
        "then execute `deploy/05_cortex/01_cortex_sentiment.sql` in Snowsight to enable Cortex analysis."
    )
    st.markdown("""
    **Steps to activate:**
    1. Run the streaming script for a few minutes to collect news
    2. Execute the Cortex SQL file in Snowsight
    3. Come back here — data will appear automatically

    ```bash
    cd /Users/moom/DataProjects/snowpulse
    source .venv/bin/activate
    python streaming/stream_to_snowflake.py
    ```
    """)
    st.stop()

# ─── Sentiment Summary KPIs ────────────────────────────────
if has_summary:
    st.subheader("📊 Sentiment Overview by Ticker")

    cols = st.columns(len(sentiment_summary))
    for i, (_, row) in enumerate(sentiment_summary.iterrows()):
        with cols[i]:
            avg_s = row.get("AVG_SENTIMENT", 0) or 0
            total = row.get("TOTAL_ARTICLES", 0)
            pos_pct = row.get("POSITIVE_PCT", 0) or 0

            if avg_s > 0.1:
                emoji = "🟢"
                label = "Positive"
            elif avg_s < -0.1:
                emoji = "🔴"
                label = "Negative"
            else:
                emoji = "⚪"
                label = "Neutral"

            st.metric(row["TICKER"], f"{avg_s:+.3f}", f"{total} articles")
            st.caption(f"{emoji} {label} | {pos_pct:.0f}% positive")

    st.divider()

    # Sentiment bar chart
    fig_bar = go.Figure()
    colors = [
        "#00CC96" if s > 0.1 else "#EF553B" if s < -0.1 else "#636EFA"
        for s in sentiment_summary["AVG_SENTIMENT"]
    ]
    fig_bar.add_trace(go.Bar(
        x=sentiment_summary["TICKER"],
        y=sentiment_summary["AVG_SENTIMENT"],
        marker_color=colors,
        text=[f"{s:+.3f}" for s in sentiment_summary["AVG_SENTIMENT"]],
        textposition="outside",
        hovertemplate="%{x}<br>Avg Sentiment: %{y:+.3f}<extra></extra>",
    ))
    fig_bar.add_hline(y=0, line_color="gray", opacity=0.5)
    fig_bar.update_layout(
        template="plotly_dark",
        height=400,
        yaxis_title="Average Sentiment Score",
        xaxis_title="",
        margin=dict(l=60, r=20, t=30, b=40),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # Sentiment distribution
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Sentiment Distribution")
        dist_data = sentiment_summary[["TICKER", "POSITIVE_COUNT", "NEUTRAL_COUNT", "NEGATIVE_COUNT"]].copy()
        dist_melted = dist_data.melt(id_vars="TICKER", var_name="Sentiment", value_name="Count")
        dist_melted["Sentiment"] = dist_melted["Sentiment"].map({
            "POSITIVE_COUNT": "Positive",
            "NEUTRAL_COUNT": "Neutral",
            "NEGATIVE_COUNT": "Negative",
        })
        fig_dist = px.bar(
            dist_melted, x="TICKER", y="Count", color="Sentiment",
            color_discrete_map={"Positive": "#00CC96", "Neutral": "#636EFA", "Negative": "#EF553B"},
            barmode="stack",
        )
        fig_dist.update_layout(
            template="plotly_dark", height=350,
            margin=dict(l=60, r=20, t=20, b=40),
        )
        st.plotly_chart(fig_dist, use_container_width=True)

    with col2:
        st.subheader("📈 Articles per Ticker")
        fig_articles = px.pie(
            sentiment_summary, values="TOTAL_ARTICLES", names="TICKER",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_articles.update_layout(
            template="plotly_dark", height=350,
            margin=dict(l=20, r=20, t=20, b=40),
        )
        st.plotly_chart(fig_articles, use_container_width=True)

    st.divider()

# ─── Sentiment Momentum ─────────────────────────────────────
if has_momentum:
    st.subheader("📈 Sentiment Momentum")
    st.caption("3-day vs 7-day moving average — Detects hype preceding price movements")

    mom_tickers = sorted(momentum["TICKER"].unique())
    sel_mom_ticker = st.selectbox("Ticker", mom_tickers, index=0, key="mom_ticker")
    t_mom = momentum[momentum["TICKER"] == sel_mom_ticker].sort_values("SENTIMENT_DATE")

    if not t_mom.empty:
        # Latest signal KPI
        latest_mom = t_mom.iloc[-1]
        signal = latest_mom.get("MOMENTUM_SIGNAL", "STABLE")
        ma3 = latest_mom.get("SENTIMENT_MA_3D", 0) or 0
        ma7 = latest_mom.get("SENTIMENT_MA_7D", 0) or 0

        col_m1, col_m2, col_m3 = st.columns(3)
        with col_m1:
            st.metric("MA 3-Day", f"{ma3:+.4f}")
        with col_m2:
            st.metric("MA 7-Day", f"{ma7:+.4f}")
        with col_m3:
            if signal == "HYPE_RISING":
                st.success(f"🚀 **HYPE RISING**")
            elif signal == "HYPE_FALLING":
                st.error(f"📉 **HYPE FALLING**")
            else:
                st.info(f"➡️ **STABLE**")

        # Momentum chart: MA 3D vs MA 7D
        fig_mom = go.Figure()
        fig_mom.add_trace(go.Scatter(
            x=t_mom["SENTIMENT_DATE"], y=t_mom["SENTIMENT_MA_3D"],
            name="MA 3-Day (Short)",
            line=dict(color="#FFA15A", width=2.5),
            hovertemplate="%{x}<br>MA 3D: %{y:.4f}<extra></extra>",
        ))
        fig_mom.add_trace(go.Scatter(
            x=t_mom["SENTIMENT_DATE"], y=t_mom["SENTIMENT_MA_7D"],
            name="MA 7-Day (Long)",
            line=dict(color="#636EFA", width=2.5),
            hovertemplate="%{x}<br>MA 7D: %{y:.4f}<extra></extra>",
        ))

        # Daily sentiment dots
        fig_mom.add_trace(go.Scatter(
            x=t_mom["SENTIMENT_DATE"], y=t_mom["AVG_DAILY_SENTIMENT"],
            name="Daily Avg",
            mode="markers",
            marker=dict(size=5, color="#AB63FA", opacity=0.5),
            hovertemplate="%{x}<br>Daily: %{y:.4f}<extra></extra>",
        ))

        fig_mom.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.4)
        fig_mom.update_layout(
            template="plotly_dark",
            height=400,
            yaxis_title="Sentiment Score",
            xaxis_title="",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
            margin=dict(l=60, r=20, t=40, b=40),
        )
        st.plotly_chart(fig_mom, use_container_width=True)

        # Momentum signal timeline
        signal_colors = {
            "HYPE_RISING": "#00CC96",
            "HYPE_FALLING": "#EF553B",
            "STABLE": "#636EFA",
        }
        t_mom_signals = t_mom[t_mom["MOMENTUM_SIGNAL"] != "STABLE"].tail(10)
        if not t_mom_signals.empty:
            st.markdown("**Recent Signals**")
            for _, row in t_mom_signals.iterrows():
                sig = row["MOMENTUM_SIGNAL"]
                date = row["SENTIMENT_DATE"]
                icon = "🚀" if sig == "HYPE_RISING" else "📉"
                st.caption(f"{icon} `{date}` — **{sig}** (Articles: {int(row.get('ARTICLE_COUNT', 0))})")

    st.divider()

# ─── Recent Articles with Sentiment ────────────────────────
if has_sentiment:
    st.subheader("📰 Recent Articles")

    # Filter
    ticker_filter = st.selectbox(
        "Filter by Ticker", ["All"] + sorted(sentiment["TICKER"].unique().tolist())
    )
    if ticker_filter != "All":
        display_news = sentiment[sentiment["TICKER"] == ticker_filter]
    else:
        display_news = sentiment

    for _, article in display_news.head(20).iterrows():
        score = article.get("SENTIMENT_SCORE", 0) or 0
        label = article.get("SENTIMENT_LABEL", "NEUTRAL")
        title = article.get("TITLE", "No title")
        publisher = article.get("PUBLISHER_NAME", "")
        published = article.get("PUBLISHED_AT", "")
        ai_summary = article.get("AI_SUMMARY", "")
        url = article.get("ARTICLE_URL", "")
        ticker_val = article.get("TICKER", "")

        if label == "POSITIVE":
            color = "🟢"
        elif label == "NEGATIVE":
            color = "🔴"
        else:
            color = "⚪"

        with st.container():
            col_info, col_score = st.columns([4, 1])
            with col_info:
                st.markdown(f"**{title}**")
                st.caption(f"{publisher} • {published} • `{ticker_val}`")
                if ai_summary:
                    st.markdown(f"*{ai_summary}*")
                if url:
                    st.markdown(f"[Read article →]({url})")
            with col_score:
                st.markdown(f"### {color} {score:+.2f}")
                st.caption(label)
            st.divider()

elif has_news:
    # Show raw news without sentiment (Cortex not yet set up)
    st.subheader("📰 Recent Articles (Sentiment pending)")
    st.info("Execute `deploy/05_cortex/01_cortex_sentiment.sql` to enable Cortex sentiment analysis.")

    for _, article in news_flat.head(15).iterrows():
        title = article.get("TITLE", "No title")
        publisher = article.get("PUBLISHER_NAME", "")
        published = article.get("PUBLISHED_AT", "")
        st.markdown(f"**{title}**")
        st.caption(f"{publisher} • {published}")
        st.divider()

# ─── Footer ─────────────────────────────────────────────────
st.divider()
st.caption("⚡ SnowPulse — News & Sentiment | Powered by Snowflake Cortex | Data: Massive (Polygon.io)")
