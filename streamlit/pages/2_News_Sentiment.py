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
