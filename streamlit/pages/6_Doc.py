"""
SnowPulse - Documentation Page
"""

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Doc | SnowPulse", page_icon="📖", layout="wide")

st.title("📖 Documentation")

# ─────────────────────────────────────────────────────────────
# Architecture Diagram
# ─────────────────────────────────────────────────────────────
st.header("📊 Data Pipeline Flow")

components.html("""
<svg viewBox="0 0 900 680" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:900px;margin:0 auto;display:block;background:transparent;">
  <defs>
    <marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#64B5F6"/>
    </marker>
    <linearGradient id="gSource" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#5C6BC0"/><stop offset="100%" stop-color="#7E57C2"/>
    </linearGradient>
    <linearGradient id="gRaw" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#6D4C41"/><stop offset="100%" stop-color="#8D6E63"/>
    </linearGradient>
    <linearGradient id="gAnalytics" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#455A64"/><stop offset="100%" stop-color="#607D8B"/>
    </linearGradient>
    <linearGradient id="gGold" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#F57F17"/><stop offset="100%" stop-color="#FFB300"/>
    </linearGradient>
    <linearGradient id="gCommon" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#37474F"/><stop offset="100%" stop-color="#546E7A"/>
    </linearGradient>
    <linearGradient id="gDash" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#00897B"/><stop offset="100%" stop-color="#26A69A"/>
    </linearGradient>
  </defs>

  <rect x="80" y="10" width="320" height="52" rx="10" fill="url(#gSource)" opacity="0.9"/>
  <text x="240" y="32" fill="white" font-size="13" font-family="sans-serif" text-anchor="middle" font-weight="bold">Massive (Polygon.io) REST API</text>
  <text x="240" y="50" fill="#B0BEC5" font-size="11" font-family="sans-serif" text-anchor="middle">Magnificent Seven — 5 req/min</text>

  <rect x="500" y="10" width="320" height="52" rx="10" fill="url(#gSource)" opacity="0.9"/>
  <text x="660" y="32" fill="white" font-size="13" font-family="sans-serif" text-anchor="middle" font-weight="bold">Snowflake Marketplace</text>
  <text x="660" y="50" fill="#B0BEC5" font-size="11" font-family="sans-serif" text-anchor="middle">CPI, Treasury 10Y, Stock Prices</text>

  <line x1="240" y1="62" x2="240" y2="95" stroke="#64B5F6" stroke-width="2" marker-end="url(#arrow)"/>
  <text x="295" y="83" fill="#90CAF9" font-size="10" font-family="sans-serif">Python REST Polling</text>

  <rect x="120" y="98" width="240" height="42" rx="8" fill="#1A237E" opacity="0.85" stroke="#42A5F5" stroke-width="1"/>
  <text x="240" y="124" fill="#64B5F6" font-size="12" font-family="sans-serif" text-anchor="middle" font-weight="bold">Snowpipe Streaming SDK</text>

  <line x1="240" y1="140" x2="240" y2="175" stroke="#64B5F6" stroke-width="2" marker-end="url(#arrow)"/>
  <text x="295" y="163" fill="#90CAF9" font-size="10" font-family="sans-serif">append_rows() → VARIANT</text>

  <rect x="50" y="178" width="800" height="65" rx="12" fill="url(#gRaw)" opacity="0.85"/>
  <text x="100" y="200" fill="white" font-size="14" font-family="sans-serif" font-weight="bold">RAW Layer</text>
  <text x="100" y="218" fill="#BCAAA4" font-size="11" font-family="sans-serif">RAW_TRADES</text>
  <text x="250" y="218" fill="#BCAAA4" font-size="11" font-family="sans-serif">RAW_AGGREGATES</text>
  <text x="420" y="218" fill="#BCAAA4" font-size="11" font-family="sans-serif">RAW_NEWS</text>
  <text x="100" y="235" fill="#A1887F" font-size="10" font-family="sans-serif">+ Streams (CDC — append-only change tracking)</text>

  <line x1="350" y1="243" x2="350" y2="278" stroke="#64B5F6" stroke-width="2" marker-end="url(#arrow)"/>
  <text x="400" y="266" fill="#90CAF9" font-size="10" font-family="sans-serif">Dynamic Tables — 1 min lag</text>

  <line x1="660" y1="62" x2="660" y2="278" stroke="#64B5F6" stroke-width="2" stroke-dasharray="6,3" marker-end="url(#arrow)"/>
  <text x="670" y="175" fill="#90CAF9" font-size="10" font-family="sans-serif">Secure Data Sharing</text>

  <rect x="50" y="281" width="800" height="100" rx="12" fill="url(#gAnalytics)" opacity="0.85"/>
  <text x="100" y="305" fill="white" font-size="14" font-family="sans-serif" font-weight="bold">ANALYTICS Layer</text>
  <text x="100" y="325" fill="#B0BEC5" font-size="10.5" font-family="sans-serif">DAILY_OHLCV · DAILY_RETURNS · MOVING_AVERAGES · RSI_14</text>
  <text x="100" y="343" fill="#B0BEC5" font-size="10.5" font-family="sans-serif">NEWS_FLATTENED · NEWS_SENTIMENT (Cortex CTE)</text>
  <text x="100" y="361" fill="#B0BEC5" font-size="10.5" font-family="sans-serif">MACRO_CPI · MACRO_TREASURY_10Y · MARKETPLACE_STOCK_PRICES (Mag7+SPY) · MACRO_STOCK_MONTHLY</text>

  <rect x="660" y="298" width="170" height="35" rx="6" fill="#263238" stroke="#4DB6AC" stroke-width="1"/>
  <text x="745" y="312" fill="#4DB6AC" font-size="10" font-family="sans-serif" text-anchor="middle">DMFs + Cortex LLM</text>
  <text x="745" y="326" fill="#80CBC4" font-size="9" font-family="sans-serif" text-anchor="middle">Quality Metrics + Sentiment</text>

  <line x1="350" y1="381" x2="350" y2="416" stroke="#64B5F6" stroke-width="2" marker-end="url(#arrow)"/>
  <text x="400" y="404" fill="#90CAF9" font-size="10" font-family="sans-serif">Dynamic Tables — 1 min to 1 day</text>

  <rect x="50" y="419" width="800" height="75" rx="12" fill="url(#gGold)" opacity="0.85"/>
  <text x="100" y="443" fill="white" font-size="14" font-family="sans-serif" font-weight="bold">GOLD Layer</text>
  <text x="100" y="463" fill="#FFF8E1" font-size="10.5" font-family="sans-serif">TICKER_SUMMARY · TICKER_BETA (vs SPY) · SENTIMENT_SUMMARY · SENTIMENT_MOMENTUM · MACRO_OVERVIEW</text>
  <text x="100" y="481" fill="#FFE082" font-size="10" font-family="sans-serif">Dashboard-ready: 1 table = 1 query</text>

  <rect x="50" y="528" width="800" height="55" rx="12" fill="url(#gCommon)" opacity="0.85"/>
  <text x="100" y="550" fill="white" font-size="14" font-family="sans-serif" font-weight="bold">COMMON Layer (Monitoring & Logs)</text>
  <text x="100" y="570" fill="#B0BEC5" font-size="10.5" font-family="sans-serif">Pipeline Logs · Alert Log · Data Quality (SP + Task + DT + Alert) · Tags (Governance)</text>

  <line x1="650" y1="494" x2="650" y2="618" stroke="#64B5F6" stroke-width="2" marker-end="url(#arrow)"/>

  <line x1="250" y1="583" x2="250" y2="618" stroke="#64B5F6" stroke-width="2" marker-end="url(#arrow)"/>

  <rect x="120" y="621" width="660" height="48" rx="10" fill="url(#gDash)" opacity="0.9"/>
  <text x="450" y="643" fill="white" font-size="14" font-family="sans-serif" text-anchor="middle" font-weight="bold">Streamlit Dashboard (7 Pages)</text>
  <text x="450" y="660" fill="#B2DFDB" font-size="11" font-family="sans-serif" text-anchor="middle">Home · Technical Analysis · News & Sentiment · Alerts · Macro · Monitoring · Doc</text>
</svg>
""", height=700)

st.divider()

# ─────────────────────────────────────────────────────────────
# Data Layers
# ─────────────────────────────────────────────────────────────
st.header("📂 Data Layers")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🟤 RAW Layer (Bronze)")
    st.markdown("""
    **Format:** VARIANT (JSON)

    **Tables:**
    - `RAW_TRADES` (daily OHLCV bars)
    - `RAW_AGGREGATES` (previous close)
    - `RAW_NEWS` (market news)

    **Ingestion:** Snowpipe Streaming SDK
    - RSA key-pair authentication
    - Python `append_rows()`
    - Real-time via REST polling
    """)

with col2:
    st.markdown("### ⚪ ANALYTICS Layer (Silver)")
    st.markdown("""
    **Format:** Typed columns (FLOAT, DATE...)

    **Dynamic Tables:**
    - `DAILY_OHLCV` (deduplicated with QUALIFY)
    - `DAILY_RETURNS`
    - `MOVING_AVERAGES`
    - `RSI_14` (Relative Strength Index)
    - `NEWS_FLATTENED` / `NEWS_SENTIMENT` (Cortex CTE-optimized)
    - `MACRO_CPI` / `MACRO_TREASURY_10Y`
    - `MARKETPLACE_STOCK_PRICES` (Mag7 + SPY)
    - `MACRO_STOCK_MONTHLY`

    **Refresh:** Automatic (TARGET_LAG = 1 min to 1 day)
    """)

with col3:
    st.markdown("### 🟡 GOLD Layer")
    st.markdown("""
    **Format:** Aggregations

    **Dynamic Tables:**
    - `TICKER_SUMMARY` — latest price, return, trend
    - `TICKER_BETA` — rolling 60-day Beta vs S&P 500 (REGR_SLOPE)
    - `SENTIMENT_SUMMARY` — aggregated sentiment per ticker
    - `SENTIMENT_MOMENTUM` — 7-day sentiment MA (hype detection)
    - `MACRO_OVERVIEW` — stock prices + CPI + Treasury 10Y

    **Features:**
    - SMA 5/20, RSI 14, Beta vs SPY
    - Sentiment momentum (3d vs 7d crossover)
    - Dashboard-ready (1 table = 1 query)

    **Refresh:** Automatic (TARGET_LAG = 1 min to 1 day)
    """)

st.divider()

# ─────────────────────────────────────────────────────────────
# Snowpipe Streaming
# ─────────────────────────────────────────────────────────────
st.header("🚀 Snowpipe Streaming")

st.markdown("""
### What is Snowpipe Streaming?

**Snowpipe Streaming** is a real-time ingestion service from Snowflake that allows sending data directly into tables via the Python SDK, **without staging files**.

### Snowpipe Streaming vs Classic Snowpipe

| | **Classic Snowpipe** | **Snowpipe Streaming** |
|---|---|---|
| **Mechanism** | File → Stage → COPY INTO | Python SDK → `append_rows()` |
| **Latency** | ~1 minute (micro-batch) | **Seconds** (row-level) |
| **Trigger** | S3/Azure/GCS notification | Direct call from code |
| **Use case** | Batch / CSV / JSON files | Streaming / real-time APIs |
| **Pipe object** | CREATE PIPE required | Auto-created by the SDK |
""")

st.subheader("📌 Code Examples")

with st.expander("Connect to Snowpipe Streaming SDK"):
    st.code("""
from snowpipe_streaming import StreamingIngestClient

# profile.json contains: account, user, url, private_key_file, role
client = StreamingIngestClient("SNOWPULSE_CLIENT", "path/to/profile.json")

# Open a channel on a target table
channel = client.open_channel(
    "my_channel",
    "SNOWPULSE_DB",
    "RAW",
    "RAW_TRADES"
)
    """, language="python")

with st.expander("Insert data (append_rows)"):
    st.code("""
# IMPORTANT: Pass Python dicts, NOT json.dumps()
# json.dumps() creates a STRING inside the VARIANT column
# A Python dict is natively converted to a VARIANT object

rows = [
    {
        "RECORD_METADATA": {"source": "massive", "ticker": "AAPL"},
        "RECORD_CONTENT": {
            "ticker": "AAPL",
            "o": 185.50,    # open
            "h": 187.20,    # high
            "l": 184.80,    # low
            "c": 186.90,    # close
            "v": 52341000,  # volume
            "t": 1706140800000  # timestamp (Unix ms)
        }
    }
]

channel.append_rows(rows)
    """, language="python")

with st.expander("Verify ingestion in Snowflake"):
    st.code("""
USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- Count records
SELECT COUNT(*) FROM RAW.RAW_TRADES;

-- Sample with VARIANT extraction
SELECT
    RECORD_CONTENT:ticker::STRING AS TICKER,
    RECORD_CONTENT:c::FLOAT AS CLOSE_PRICE,
    RECORD_CONTENT:v::FLOAT AS VOLUME
FROM RAW.RAW_TRADES
LIMIT 10;
    """, language="sql")

st.divider()

# ─────────────────────────────────────────────────────────────
# Dynamic Tables
# ─────────────────────────────────────────────────────────────
st.header("⚡ Dynamic Tables")

st.markdown("""
### What is a Dynamic Table?

**Dynamic Tables** replace the classic **Tasks + Streams + MERGE** pattern with a **declarative** approach. You define the desired result (a SQL query), and Snowflake automatically handles the refresh.

### Dynamic Tables vs Tasks

| | **Tasks + Streams** | **Dynamic Tables** |
|---|---|---|
| **Approach** | Imperative (MERGE, INSERT) | Declarative (SELECT) |
| **CDC** | Manual Streams | Automatic |
| **Orchestration** | DAG of Tasks | Automatic cascade |
| **Maintenance** | Complex SQL code | Simple SQL |
| **Refresh** | CRON or dependencies | TARGET_LAG (automatic) |

### Cascade Architecture
""")

components.html("""
<svg viewBox="0 0 920 520" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:920px;margin:0 auto;display:block;background:transparent;">
  <defs>
    <marker id="arr2" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#64B5F6"/>
    </marker>
  </defs>

  <!-- ROW 1: RAW Sources -->
  <rect x="30" y="10" width="200" height="40" rx="8" fill="#5D4037" opacity="0.9"/>
  <text x="130" y="35" fill="white" font-size="11" font-family="sans-serif" text-anchor="middle" font-weight="bold">RAW_TRADES (VARIANT)</text>

  <rect x="350" y="10" width="200" height="40" rx="8" fill="#5D4037" opacity="0.9"/>
  <text x="450" y="35" fill="white" font-size="11" font-family="sans-serif" text-anchor="middle" font-weight="bold">RAW_NEWS (VARIANT)</text>

  <rect x="670" y="10" width="220" height="40" rx="8" fill="#4527A0" opacity="0.85"/>
  <text x="780" y="28" fill="white" font-size="10.5" font-family="sans-serif" text-anchor="middle" font-weight="bold">Snowflake Marketplace</text>
  <text x="780" y="44" fill="#B39DDB" font-size="9.5" font-family="sans-serif" text-anchor="middle">Mag7 + SPY + CPI + Treasury</text>

  <!-- Arrows ROW 1 -> ROW 2 -->
  <line x1="130" y1="50" x2="130" y2="82" stroke="#64B5F6" stroke-width="1.5" marker-end="url(#arr2)"/>
  <line x1="450" y1="50" x2="450" y2="82" stroke="#64B5F6" stroke-width="1.5" marker-end="url(#arr2)"/>
  <line x1="780" y1="50" x2="780" y2="82" stroke="#64B5F6" stroke-width="1.5" stroke-dasharray="5,3" marker-end="url(#arr2)"/>

  <!-- LAG labels -->
  <text x="160" y="72" fill="#90CAF9" font-size="9" font-family="sans-serif">1 min</text>
  <text x="480" y="72" fill="#90CAF9" font-size="9" font-family="sans-serif">1 min</text>
  <text x="810" y="72" fill="#90CAF9" font-size="9" font-family="sans-serif">1 day</text>

  <!-- ROW 2: First Analytics -->
  <rect x="30" y="85" width="200" height="36" rx="7" fill="#455A64" opacity="0.9"/>
  <text x="130" y="108" fill="#E0E0E0" font-size="11" font-family="sans-serif" text-anchor="middle" font-weight="bold">DAILY_OHLCV</text>

  <rect x="350" y="85" width="200" height="36" rx="7" fill="#455A64" opacity="0.9"/>
  <text x="450" y="108" fill="#E0E0E0" font-size="11" font-family="sans-serif" text-anchor="middle" font-weight="bold">NEWS_FLATTENED</text>

  <rect x="670" y="85" width="105" height="36" rx="7" fill="#455A64" opacity="0.9"/>
  <text x="722" y="108" fill="#E0E0E0" font-size="10" font-family="sans-serif" text-anchor="middle">MACRO_CPI</text>

  <rect x="785" y="85" width="105" height="36" rx="7" fill="#455A64" opacity="0.9"/>
  <text x="837" y="108" fill="#E0E0E0" font-size="10" font-family="sans-serif" text-anchor="middle">TREASURY_10Y</text>

  <!-- Arrows ROW 2 -> ROW 3 -->
  <line x1="70" y1="121" x2="70" y2="165" stroke="#64B5F6" stroke-width="1.5" marker-end="url(#arr2)"/>
  <line x1="130" y1="121" x2="130" y2="165" stroke="#64B5F6" stroke-width="1.5" marker-end="url(#arr2)"/>
  <line x1="190" y1="121" x2="190" y2="165" stroke="#64B5F6" stroke-width="1.5" marker-end="url(#arr2)"/>
  <line x1="450" y1="121" x2="450" y2="165" stroke="#64B5F6" stroke-width="1.5" marker-end="url(#arr2)"/>
  <text x="470" y="148" fill="#90CAF9" font-size="9" font-family="sans-serif">5 min</text>

  <!-- ROW 3: Second Analytics -->
  <rect x="10" y="168" width="120" height="36" rx="7" fill="#546E7A" opacity="0.9"/>
  <text x="70" y="191" fill="#E0E0E0" font-size="10" font-family="sans-serif" text-anchor="middle">DAILY_RETURNS</text>

  <rect x="140" y="168" width="75" height="36" rx="7" fill="#546E7A" opacity="0.9"/>
  <text x="177" y="191" fill="#E0E0E0" font-size="10" font-family="sans-serif" text-anchor="middle">MA</text>

  <rect x="225" y="168" width="75" height="36" rx="7" fill="#546E7A" opacity="0.9"/>
  <text x="262" y="191" fill="#E0E0E0" font-size="10" font-family="sans-serif" text-anchor="middle">RSI_14</text>

  <rect x="370" y="168" width="160" height="36" rx="7" fill="#546E7A" opacity="0.9" stroke="#7E57C2" stroke-width="1"/>
  <text x="450" y="185" fill="#CE93D8" font-size="10" font-family="sans-serif" text-anchor="middle" font-weight="bold">NEWS_SENTIMENT</text>
  <text x="450" y="198" fill="#B39DDB" font-size="8.5" font-family="sans-serif" text-anchor="middle">Cortex CTE</text>

  <rect x="670" y="168" width="105" height="36" rx="7" fill="#546E7A" opacity="0.9"/>
  <text x="722" y="185" fill="#E0E0E0" font-size="9.5" font-family="sans-serif" text-anchor="middle">STOCK_PRICES</text>
  <text x="722" y="198" fill="#B0BEC5" font-size="8.5" font-family="sans-serif" text-anchor="middle">Mag7 + SPY</text>

  <rect x="785" y="168" width="105" height="36" rx="7" fill="#546E7A" opacity="0.9"/>
  <text x="837" y="191" fill="#E0E0E0" font-size="9.5" font-family="sans-serif" text-anchor="middle">STOCK_MONTHLY</text>

  <!-- Arrows ROW 3 -> ROW 4 (Gold) -->
  <!-- DAILY_RETURNS + MA -> TICKER_SUMMARY -->
  <line x1="70" y1="204" x2="70" y2="250" stroke="#64B5F6" stroke-width="1.2"/>
  <line x1="70" y1="250" x2="130" y2="280" stroke="#64B5F6" stroke-width="1.2" marker-end="url(#arr2)"/>
  <line x1="177" y1="204" x2="177" y2="250" stroke="#64B5F6" stroke-width="1.2"/>
  <line x1="177" y1="250" x2="130" y2="280" stroke="#64B5F6" stroke-width="1.2"/>

  <!-- NEWS_SENTIMENT -> SENTIMENT_SUMMARY + SENTIMENT_MOMENTUM -->
  <line x1="410" y1="204" x2="380" y2="280" stroke="#64B5F6" stroke-width="1.2" marker-end="url(#arr2)"/>
  <line x1="490" y1="204" x2="530" y2="280" stroke="#64B5F6" stroke-width="1.2" marker-end="url(#arr2)"/>

  <!-- STOCK_PRICES -> TICKER_BETA -->
  <line x1="722" y1="204" x2="722" y2="248" stroke="#64B5F6" stroke-width="1.2"/>
  <line x1="130" y1="204" x2="130" y2="248" stroke="#64B5F6" stroke-width="1.2" stroke-dasharray="4,3"/>
  <line x1="130" y1="248" x2="722" y2="248" stroke="#64B5F6" stroke-width="0.8" stroke-dasharray="4,3" opacity="0.4"/>
  <line x1="722" y1="248" x2="722" y2="280" stroke="#64B5F6" stroke-width="1.2" marker-end="url(#arr2)"/>

  <!-- STOCK_MONTHLY + Macro -> MACRO_OVERVIEW -->
  <line x1="837" y1="204" x2="837" y2="280" stroke="#64B5F6" stroke-width="1.2" marker-end="url(#arr2)"/>

  <!-- ROW 4: GOLD Layer -->
  <rect x="30" y="275" width="870" height="10" rx="3" fill="#F57F17" opacity="0.2"/>

  <rect x="50" y="283" width="160" height="40" rx="8" fill="#F57F17" opacity="0.85"/>
  <text x="130" y="308" fill="white" font-size="11" font-family="sans-serif" text-anchor="middle" font-weight="bold">TICKER_SUMMARY</text>

  <rect x="310" y="283" width="160" height="40" rx="8" fill="#F57F17" opacity="0.85"/>
  <text x="390" y="302" fill="white" font-size="10" font-family="sans-serif" text-anchor="middle" font-weight="bold">SENTIMENT_SUMMARY</text>

  <rect x="480" y="283" width="170" height="40" rx="8" fill="#F57F17" opacity="0.85"/>
  <text x="565" y="302" fill="white" font-size="10" font-family="sans-serif" text-anchor="middle" font-weight="bold">SENTIMENT_MOMENTUM</text>
  <text x="565" y="316" fill="#FFF8E1" font-size="8.5" font-family="sans-serif" text-anchor="middle">7d hype detection</text>

  <rect x="660" y="283" width="105" height="40" rx="8" fill="#F57F17" opacity="0.85"/>
  <text x="712" y="302" fill="white" font-size="10" font-family="sans-serif" text-anchor="middle" font-weight="bold">TICKER_BETA</text>
  <text x="712" y="316" fill="#FFF8E1" font-size="8.5" font-family="sans-serif" text-anchor="middle">vs SPY (60d)</text>

  <rect x="775" y="283" width="120" height="40" rx="8" fill="#F57F17" opacity="0.85"/>
  <text x="835" y="302" fill="white" font-size="10" font-family="sans-serif" text-anchor="middle" font-weight="bold">MACRO_OVERVIEW</text>
  <text x="835" y="316" fill="#FFF8E1" font-size="8.5" font-family="sans-serif" text-anchor="middle">CPI + Treasury</text>

  <!-- GOLD label -->
  <text x="30" y="350" fill="#FFB300" font-size="12" font-family="sans-serif" font-weight="bold">GOLD LAYER</text>
  <text x="160" y="350" fill="#FFE082" font-size="10" font-family="sans-serif">Dashboard-ready — auto-refresh cascade</text>

  <!-- Legend -->
  <rect x="30" y="380" width="860" height="130" rx="10" fill="#1a1a2e" opacity="0.6" stroke="#2a2a4e" stroke-width="1"/>
  <text x="50" y="400" fill="#B0BEC5" font-size="11" font-family="sans-serif" font-weight="bold">Legend</text>

  <rect x="50" y="412" width="14" height="14" rx="3" fill="#5D4037"/>
  <text x="72" y="424" fill="#BCAAA4" font-size="10" font-family="sans-serif">RAW (VARIANT JSON)</text>

  <rect x="50" y="434" width="14" height="14" rx="3" fill="#455A64"/>
  <text x="72" y="446" fill="#B0BEC5" font-size="10" font-family="sans-serif">ANALYTICS (Typed + Computed)</text>

  <rect x="50" y="456" width="14" height="14" rx="3" fill="#F57F17"/>
  <text x="72" y="468" fill="#FFE082" font-size="10" font-family="sans-serif">GOLD (Dashboard-Ready)</text>

  <rect x="50" y="478" width="14" height="14" rx="3" fill="#4527A0"/>
  <text x="72" y="490" fill="#B39DDB" font-size="10" font-family="sans-serif">Snowflake Marketplace (Secure Data Sharing)</text>

  <line x1="350" y1="419" x2="400" y2="419" stroke="#64B5F6" stroke-width="1.5" marker-end="url(#arr2)"/>
  <text x="410" y="424" fill="#90CAF9" font-size="10" font-family="sans-serif">Dynamic Table dependency</text>

  <line x1="350" y1="441" x2="400" y2="441" stroke="#64B5F6" stroke-width="1.5" stroke-dasharray="5,3" marker-end="url(#arr2)"/>
  <text x="410" y="446" fill="#90CAF9" font-size="10" font-family="sans-serif">Cross-layer join (DAILY_OHLCV + SPY → BETA)</text>

  <rect x="348" y="458" width="16" height="12" rx="2" fill="none" stroke="#7E57C2" stroke-width="1"/>
  <text x="372" y="468" fill="#CE93D8" font-size="10" font-family="sans-serif">Cortex LLM (SENTIMENT + SUMMARIZE)</text>
</svg>
""", height=530)

st.markdown("""
Snowflake automatically detects dependencies and refreshes downstream tables in cascade.
""")

st.subheader("📌 SQL Examples")

with st.expander("Create a Dynamic Table (DAILY_OHLCV — with deduplication)"):
    st.code("""
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DAILY_OHLCV
    WAREHOUSE = SNOWPULSE_WH
    TARGET_LAG = '1 minute'
AS
SELECT
    RECORD_CONTENT:ticker::STRING           AS TICKER,
    TO_DATE(TO_TIMESTAMP(RECORD_CONTENT:t::NUMBER / 1000)) AS TRADE_DATE,
    RECORD_CONTENT:o::FLOAT                 AS OPEN_PRICE,
    RECORD_CONTENT:h::FLOAT                 AS HIGH_PRICE,
    RECORD_CONTENT:l::FLOAT                 AS LOW_PRICE,
    RECORD_CONTENT:c::FLOAT                 AS CLOSE_PRICE,
    RECORD_CONTENT:v::FLOAT                 AS VOLUME,
    RECORD_CONTENT:vw::FLOAT                AS VWAP,
    RECORD_CONTENT:n::NUMBER                AS NUM_TRANSACTIONS,
    RECORD_METADATA:ingested_at::TIMESTAMP_NTZ AS INGESTED_AT
FROM RAW.RAW_TRADES
-- Deduplication: keep only the latest ingestion per (TICKER, TRADE_DATE)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TICKER, TRADE_DATE
    ORDER BY INGESTED_AT DESC
) = 1;
    """, language="sql")

with st.expander("Check Dynamic Table status"):
    st.code("""
USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- List all Dynamic Tables and their state
SHOW DYNAMIC TABLES IN DATABASE SNOWPULSE_DB;

-- Check current lag
SELECT
    NAME,
    SCHEMA_NAME,
    TARGET_LAG,
    SCHEDULING_STATE
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES());
    """, language="sql")

with st.expander("Manually refresh a Dynamic Table"):
    st.code("""
-- Force an immediate refresh
ALTER DYNAMIC TABLE ANALYTICS.DAILY_OHLCV REFRESH;

-- Suspend / Resume
ALTER DYNAMIC TABLE ANALYTICS.DAILY_OHLCV SUSPEND;
ALTER DYNAMIC TABLE ANALYTICS.DAILY_OHLCV RESUME;
    """, language="sql")

st.divider()

# ─────────────────────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────────────────────
st.header("🚨 Snowflake Alerts")

st.markdown("""
### What is a Snowflake Alert?

**Alerts** are Snowflake objects that evaluate a SQL condition at regular intervals and execute an action when the condition is met.

### Our 4 Alerts

| Alert | Condition | Frequency |
|---|---|---|
| `ALERT_BIG_DAILY_MOVE` | Any ticker moves > 3% in a day | Every 5 min |
| `ALERT_TREND_CHANGE` | SMA5/SMA20 signal flips (BULLISH ↔ BEARISH) | Every 5 min |
| `ALERT_HIGH_VOLUME` | Volume exceeds 2x the 20-day average | Every 5 min |
| `ALERT_DATA_QUALITY_FAIL` | Any quality check returns FAIL status | Every 60 min |

### Deduplication
Each alert checks that no identical alert was triggered for the same ticker in the **last 24 hours** via the `COMMON.ALERT_LOG` table.
""")

st.subheader("📌 SQL Examples")

with st.expander("Create an Alert (BIG_DAILY_MOVE)"):
    st.code("""
CREATE OR REPLACE ALERT COMMON.ALERT_BIG_DAILY_MOVE
    WAREHOUSE = SNOWPULSE_WH
    SCHEDULE = '5 MINUTE'
IF (EXISTS (
    SELECT 1
    FROM ANALYTICS.DAILY_RETURNS
    WHERE ABS(DAILY_RETURN_PCT) > 3
      AND TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.DAILY_RETURNS)
      AND TICKER NOT IN (
          SELECT TICKER FROM COMMON.ALERT_LOG
          WHERE ALERT_NAME = 'BIG_DAILY_MOVE'
            AND TRIGGERED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
      )
))
THEN
    INSERT INTO COMMON.ALERT_LOG (ALERT_NAME, TICKER, MESSAGE, METRIC_VALUE)
    SELECT 'BIG_DAILY_MOVE', TICKER,
        TICKER || ' moved ' || DAILY_RETURN_PCT || '% on ' || TRADE_DATE,
        DAILY_RETURN_PCT
    FROM ANALYTICS.DAILY_RETURNS
    WHERE ABS(DAILY_RETURN_PCT) > 3
      AND TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.DAILY_RETURNS);
    """, language="sql")

with st.expander("Query triggered alerts"):
    st.code("""
USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- Latest alerts
SELECT * FROM COMMON.ALERT_LOG
ORDER BY TRIGGERED_AT DESC
LIMIT 20;

-- Alerts by type
SELECT ALERT_NAME, COUNT(*) AS NB
FROM COMMON.ALERT_LOG
GROUP BY ALERT_NAME;

-- Check alert status
SHOW ALERTS IN SCHEMA COMMON;
    """, language="sql")

with st.expander("Manage alerts (suspend / resume)"):
    st.code("""
-- Suspend an alert
ALTER ALERT COMMON.ALERT_BIG_DAILY_MOVE SUSPEND;

-- Resume an alert
ALTER ALERT COMMON.ALERT_BIG_DAILY_MOVE RESUME;

-- Drop an alert
DROP ALERT COMMON.ALERT_BIG_DAILY_MOVE;
    """, language="sql")

st.divider()

# ─────────────────────────────────────────────────────────────
# Logging & Observability
# ─────────────────────────────────────────────────────────────
st.header("🛡️ Logging & Observability")

st.markdown("""
### Pipeline Monitoring

The Python streaming script logs every operation to `COMMON.PIPELINE_LOGS`:
- **INFO**: Successful ingestion (e.g. "Ingested 21 daily bars for AAPL")
- **WARNING**: Partial success (rate limiting, missing data)
- **ERROR**: API call failure or insertion error

### Alert Monitoring

The `COMMON.ALERT_LOG` table centralizes all triggered alerts with:
- Timestamp, alert name, affected ticker
- Descriptive message and metric value
""")

col1, col2 = st.columns(2)
with col1:
    st.subheader("💡 Resilience")
    st.write("""
    The pipeline uses a **shared rate limiter** across threads (12s between each API call).
    If a call fails, the script logs the error and continues with the remaining tickers.
    """)
with col2:
    st.subheader("🚀 Performance")
    st.write("""
    - **Batch insert** via Snowpipe Streaming (multiple rows per `append_rows()`)
    - **Dynamic Tables** auto-refresh (no CRON to manage)
    - **XS Warehouse** with 60s auto-suspend
    """)

st.divider()

# ─────────────────────────────────────────────────────────────
# Technical Stack
# ─────────────────────────────────────────────────────────────
st.header("⚙️ Technical Stack")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Data Layer")
    st.markdown("""
    - **Ingestion**: Snowpipe Streaming SDK (Python)
    - **Storage**: Snowflake Tables (VARIANT JSON)
    - **Transformation**: Dynamic Tables (declarative SQL)
    - **AI**: Cortex LLM (SENTIMENT + SUMMARIZE)
    - **Quality**: Tags, DMFs, Streams, SP, Task, Alert, DT
    - **Monitoring**: Snowflake Alerts (market + quality)
    - **Auth**: RSA key-pair authentication
    """)

with col2:
    st.markdown("### Infrastructure")
    st.markdown("""
    - **Warehouse**: SNOWPULSE_WH (XS, auto-suspend 60s)
    - **API**: Massive / Polygon.io REST (5 req/min free tier)
    - **Tickers**: AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META
    - **Enrichment**: Snowflake Marketplace (CPI, Treasury 10Y)
    - **Security**: External Access Integration + Secrets
    - **Compute**: AWS EC2 (t2.micro, systemd service)
    - **Frontend**: Streamlit (6 pages)
    - **Version Control**: Git / GitHub
    """)

st.divider()

# ─────────────────────────────────────────────────────────────
# Cost Optimization
# ─────────────────────────────────────────────────────────────
st.header("💰 Cost Optimization")

st.markdown("""
### Key Optimizations

1. **Dynamic Tables** 🎯
   - No manual Streams or complex MERGE statements
   - Snowflake automatically optimizes incremental refresh
   - Less code = less maintenance = fewer bugs

2. **Warehouse auto-suspend**: 60 seconds
   - Warehouse shuts down as soon as queries stop

3. **Cortex CTE optimization** 🧠
   - NEWS_SENTIMENT uses a CTE to call `SENTIMENT()` only once per row
   - The label (POSITIVE/NEGATIVE/NEUTRAL) is derived from the pre-computed score
   - Reduces Cortex LLM calls by 66% (1 call instead of 3)

4. **TARGET_LAG = 1 minute**
   - Near real-time without overconsumption
   - Snowflake batches updates intelligently

5. **Free Tier API**
   - Massive (Polygon.io): 5 req/min (sufficient for 7 tickers)
   - Python-side rate limiting to respect API limits

6. **XS Warehouse**
   - Smallest size available, sufficient for this data volume
""")

st.divider()

# ─────────────────────────────────────────────────────────────
# Security
# ─────────────────────────────────────────────────────────────
st.header("🔐 Security")

st.markdown("""
### Access Control

- **Role-Based Access Control (RBAC)**
  - Role: `SNOWPULSE_ROLE`
  - Warehouse: `SNOWPULSE_WH`
  - Database: `SNOWPULSE_DB`

### Authentication

- **RSA Key-pair** for Snowpipe Streaming
  - Private key stored locally (`~/.ssh/snowflake_key.p8`)
  - Public key registered on the Snowflake user
  - No password in code

### API Security

- **External Access Integration** for Massive (Polygon.io)
  - Network Rule restricted to `api.polygon.io`
  - Snowflake Secret for the API key
  - `.env` + `.gitignore` for local credentials

### Best Practices

- No secrets in versioned code
- `.env.example` as a template (placeholder values only)
- `profile.json` and `.streamlit/secrets.toml` in `.gitignore`
- Least privilege principle (`SNOWPULSE_ROLE`)
""")

st.divider()

# ─────────────────────────────────────────────────────────────
# Snowflake Marketplace
# ─────────────────────────────────────────────────────────────
st.header("🛒 Snowflake Marketplace — Data Enrichment")

st.markdown("""
### What is Snowflake Marketplace?

**Snowflake Marketplace** allows you to access live, ready-to-query datasets shared by data providers — directly in your Snowflake account, **with zero ingestion or ETL**. The data stays in the provider's account and is accessed via secure data sharing.

### Dataset Used: Snowflake Public Data (Free)

We use the **Snowflake Public Data (Free)** dataset (90+ public domain sources) to enrich our Magnificent Seven stock data with macroeconomic indicators:

| Indicator | Source | Frequency | Usage |
|---|---|---|---|
| **CPI (Consumer Price Index)** | Bureau of Labor Statistics | Monthly | Inflation tracking — correlate with stock performance |
| **Treasury 10Y Yield** | Federal Reserve | Quarterly | Interest rate environment — impact on tech valuations |
| **Stock Prices (Nasdaq)** | Cybersyn / Databento | Daily | Extended price history + validation of ingested data |

### Why This Matters

- **Data Enrichment**: Cross-referencing real-time stock data with macro indicators
- **No ETL needed**: Marketplace data is available instantly via shared views
- **Zero storage cost**: Data lives in the provider's account
- **Portfolio value**: Demonstrates ability to integrate multiple data sources
""")

st.subheader("📌 Dynamic Tables")

st.markdown("""
| Dynamic Table | Layer | Description |
|---|---|---|
| `MACRO_CPI` | Analytics | Monthly CPI index with MoM and YoY change (%) |
| `MACRO_TREASURY_10Y` | Analytics | Quarterly 10-Year Treasury yield with QoQ change |
| `MARKETPLACE_STOCK_PRICES` | Analytics | Mag7 daily close, high, low, volume from Nasdaq |
| `MACRO_STOCK_MONTHLY` | Analytics | Monthly average close per ticker (for JOIN with CPI) |
| `MACRO_OVERVIEW` | Gold | Final enriched table — stock prices + CPI + Treasury 10Y |
""")

st.subheader("📊 Macro Context Dashboard")

st.markdown("""
The **Macro Context** page provides the following visualizations:

| Chart | Description |
|---|---|
| **Stock Price vs CPI Inflation** | Dual-axis chart comparing a ticker's monthly avg close price with CPI Year-over-Year inflation rate. Highlights how inflation trends correlate with stock movements. |
| **Stock Price vs Treasury 10Y** | Dual-axis chart overlaying a ticker's price with the 10-Year Treasury yield index. Shows the impact of interest rate environment on tech valuations. |
| **Mag7 Normalized vs Inflation** | All 7 tickers normalized to base 100, overlaid with CPI YoY inflation. Compares relative performance across the Magnificent Seven against the macro backdrop. |
| **CPI Index & Inflation Rate** | Dedicated CPI panel — index evolution (area chart) and YoY inflation rate (bar chart with Fed 2% target line). Color-coded: green (<2%), yellow (2-3%), red (>3%). |
| **Treasury 10Y Yield Index** | Area chart showing the quarterly evolution of the 10-Year Treasury yield. |
| **Monthly Data Table** | Detailed table per ticker: monthly avg close, high, low, CPI index, inflation YoY %, Treasury 10Y. |
""")

with st.expander("Installation"):
    st.markdown("""
1. Go to **Snowflake Marketplace** (left menu in Snowsight)
2. Search for **"Snowflake Public Data"** (provider: Snowflake Public Data Products)
3. Click **Get** → name the database `Snowflake_FINANCE__ECONOMICS`
4. Grant access to `SNOWPULSE_ROLE`
5. Execute `deploy/06_marketplace/01_macro_enrichment.sql`
""")

st.divider()

# ─────────────────────────────────────────────────────────────
# Data Quality
# ─────────────────────────────────────────────────────────────
st.header("🔍 Data Quality & Governance")

st.markdown("""
### 7 Snowflake Features for Data Quality

SnowPulse uses a comprehensive data quality layer built on **7 native Snowflake features**:

| Feature | Object | Purpose |
|---|---|---|
| **Tags** | `DATA_DOMAIN`, `DATA_LAYER`, `FRESHNESS_SLA` | Governance — classify every table by domain, layer, and freshness SLA |
| **Data Metric Functions (DMFs)** | `NEGATIVE_PRICE_COUNT`, `OHLCV_VIOLATION_COUNT`, `MISSING_TICKER_COUNT` | Automated quality metrics attached to DAILY_OHLCV — computed every 60 minutes |
| **Streams** | `STREAM_RAW_TRADES`, `STREAM_RAW_AGGREGATES`, `STREAM_RAW_NEWS` | CDC (Change Data Capture) — append-only tracking of new ingestions |
| **Stored Procedure** | `SP_DATA_QUALITY_CHECK()` | Runs 10 quality checks: freshness, completeness, validity, consistency, duplicates, volume |
| **Task** | `TASK_DATA_QUALITY_CHECK` | Schedules the SP every 60 minutes automatically |
| **Alert** | `ALERT_DATA_QUALITY_FAIL` | Fires when any quality check fails — writes to ALERT_LOG |
| **Dynamic Table** | `DATA_QUALITY_SUMMARY` | Latest quality status per check — dashboard-ready |

### Quality Checks (10 checks)

| # | Check | Category | What It Verifies |
|---|---|---|---|
| 1 | `freshness_raw_trades` | Freshness | RAW_TRADES has data from the last 24 hours |
| 2 | `freshness_raw_aggregates` | Freshness | RAW_AGGREGATES has data from the last 24 hours |
| 3 | `freshness_raw_news` | Freshness | RAW_NEWS has data from the last 48 hours |
| 4 | `completeness_tickers` | Completeness | All 7 Mag7 tickers are present in DAILY_OHLCV |
| 5 | `validity_prices` | Validity | No negative prices in DAILY_OHLCV |
| 6 | `consistency_ohlcv` | Consistency | HIGH >= LOW for every bar |
| 7 | `duplicates_daily_ohlcv` | Duplicates | No duplicate (TICKER, TRADE_DATE) pairs |
| 8 | `volume_raw_trades` | Volume | New rows detected in RAW_TRADES stream |
| 9 | `volume_raw_aggregates` | Volume | New rows detected in RAW_AGGREGATES stream |
| 10 | `volume_raw_news` | Volume | New rows detected in RAW_NEWS stream |

### Deduplication Strategy

The `DAILY_OHLCV` Dynamic Table uses `QUALIFY ROW_NUMBER()` to automatically keep only the most recent ingestion per (TICKER, TRADE_DATE) pair — even if the source RAW_TRADES contains duplicates from multiple backfills.

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TICKER, TRADE_DATE
    ORDER BY INGESTED_AT DESC
) = 1
```
""")

st.subheader("📌 SQL Examples")

with st.expander("Run quality checks manually"):
    st.code("""
USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- Run all 10 quality checks
CALL COMMON.SP_DATA_QUALITY_CHECK();

-- View latest results (Dynamic Table)
SELECT * FROM COMMON.DATA_QUALITY_SUMMARY
ORDER BY CASE STATUS WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END;
    """, language="sql")

with st.expander("View Data Metric Function results"):
    st.code("""
-- DMF results are stored in Snowflake's internal table
SELECT
    MEASUREMENT_TIME,
    TABLE_SCHEMA || '.' || TABLE_NAME AS TABLE_NAME,
    METRIC_NAME,
    VALUE
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE TABLE_DATABASE = 'SNOWPULSE_DB'
ORDER BY MEASUREMENT_TIME DESC;
    """, language="sql")

with st.expander("View governance tags"):
    st.code("""
-- See all tags applied to tables
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'SNOWPULSE_DB.RAW.RAW_TRADES', 'TABLE'
));

-- Query by tag value
SELECT SYSTEM$GET_TAG('COMMON.DATA_LAYER', 'RAW.RAW_TRADES', 'TABLE');
    """, language="sql")

with st.expander("Manage the quality task"):
    st.code("""
-- Check task status
SHOW TASKS IN SCHEMA COMMON;

-- Suspend / Resume
ALTER TASK COMMON.TASK_DATA_QUALITY_CHECK SUSPEND;
ALTER TASK COMMON.TASK_DATA_QUALITY_CHECK RESUME;

-- Run once immediately
EXECUTE TASK COMMON.TASK_DATA_QUALITY_CHECK;
    """, language="sql")

st.divider()

# ─────────────────────────────────────────────────────────────
# Glossary
# ─────────────────────────────────────────────────────────────
st.header("📚 Glossary — Financial Terms")

st.markdown("""
| Term | Definition |
|---|---|
| **Ticker** | A unique abbreviation used to identify a publicly traded stock on an exchange. For example, `AAPL` stands for Apple Inc. and `NVDA` for NVIDIA Corporation. |
| **OHLCV** | Open, High, Low, Close, Volume — the five standard data points that describe a stock's price action over a given period (usually one trading day). |
| **Open** | The price at which a stock begins trading when the market opens for the day. |
| **High** | The highest price reached by a stock during a trading session. |
| **Low** | The lowest price reached by a stock during a trading session. |
| **Close** | The last price at which a stock trades when the market closes. This is the most commonly referenced price. |
| **Volume** | The total number of shares traded during a given period. High volume often signals strong investor interest or significant news. |
| **VWAP** | Volume-Weighted Average Price. The average price of a stock weighted by the volume traded at each price level during the day. More representative than a simple average because it accounts for where most trading actually occurred. |
| **Daily Return (%)** | The percentage change in closing price from one trading day to the next. Calculated as: `((Today's Close - Yesterday's Close) / Yesterday's Close) × 100`. |
| **SMA (Simple Moving Average)** | The average closing price over a rolling window of N trading days. SMA 5 = average of the last 5 days, SMA 20 = average of the last 20 days. Used to smooth out short-term fluctuations and identify trends. |
| **Bullish** | A market signal indicating an upward trend. In this project, the trend is BULLISH when SMA 5 (short-term) is above SMA 20 (long-term), meaning recent prices are higher than the longer-term average. |
| **Bearish** | A market signal indicating a downward trend. The trend is BEARISH when SMA 5 is below SMA 20, meaning recent prices are falling below the longer-term average. |
| **SMA Crossover** | The moment when a short-term moving average crosses above or below a long-term moving average. A bullish crossover (SMA 5 crosses above SMA 20) is often seen as a buy signal; a bearish crossover as a sell signal. |
| **Market Cap** | Market Capitalization — the total market value of a company's outstanding shares. Calculated as `Share Price × Total Shares Outstanding`. The Magnificent Seven are the largest US tech companies by market cap. |
| **Magnificent Seven** | The seven largest US technology stocks: Apple (AAPL), Microsoft (MSFT), Alphabet (GOOGL), Amazon (AMZN), Tesla (TSLA), NVIDIA (NVDA), and Meta (META). They represent a dominant share of major US indices. |
| **Trading Day** | A day when the stock market is open for trading (Monday to Friday, excluding US public holidays). Weekends and holidays have no price data. |
| **Previous Close** | The closing price of the most recent completed trading session. Used as a reference point to calculate the current day's performance. |
| **Aggregate Bar** | A summary of all trades within a specific time window (e.g. 1 day). Contains the OHLCV data for that period. This project uses daily aggregate bars. |
| **CPI (Consumer Price Index)** | A measure of the average change in prices paid by consumers for a basket of goods and services. Used as the primary indicator of inflation. Published monthly by the Bureau of Labor Statistics. |
| **YoY (Year-over-Year)** | A comparison of a statistic from one period to the same period in the previous year. For example, CPI YoY measures inflation by comparing this month's CPI to the same month last year. |
| **Treasury 10Y Yield** | The return on the US government 10-year bond. A key benchmark for interest rates — when it rises, borrowing costs increase, which typically puts pressure on growth/tech stock valuations. |
| **Federal Reserve (Fed)** | The central bank of the United States. Sets monetary policy including the Federal Funds Rate. Its decisions on interest rates directly impact stock markets, especially tech stocks. |
| **Inflation** | The rate at which the general level of prices for goods and services rises, eroding purchasing power. The Fed targets approximately 2% annual inflation. |
| **RSI (Relative Strength Index)** | A momentum oscillator ranging from 0 to 100. Calculated over 14 days using average gains vs average losses. RSI > 70 = overbought (potential pullback), RSI < 30 = oversold (potential rebound). |
| **Beta** | A measure of a stock's volatility relative to the overall market (S&P 500 / SPY). Beta > 1 = more volatile than the market, Beta < 1 = less volatile. Calculated using REGR_SLOPE on daily returns. |
| **SPY** | The SPDR S&P 500 ETF — an exchange-traded fund that tracks the S&P 500 index. Used as the market benchmark for Beta calculation. |
| **Sentiment Momentum** | A 7-day moving average of daily sentiment scores (from Cortex LLM). When the 3-day MA rises above the 7-day MA, it signals rising hype that may precede price movements. |
| **REGR_SLOPE** | A Snowflake aggregate function that computes the slope of the linear regression line between two variables. Used here to calculate Beta (stock returns vs market returns). |
""")

st.divider()

# ─────────────────────────────────────────────────────────────
# EC2 Deployment
# ─────────────────────────────────────────────────────────────
st.header("☁️ Production Deployment — AWS EC2")

st.markdown("""
### Why EC2?

The Snowpipe Streaming SDK is a **client-side** library — it runs on a machine, not inside Snowflake.
To keep the pipeline running 24/7 without relying on a local computer, we deploy the ingestion script on an **AWS EC2** instance.

### Architecture

```
AWS EC2 (t2.micro)
├── Python 3.11
├── streaming/stream_to_snowflake.py   ← runs as systemd service
├── .env                               ← API key
├── streaming/profile.json             ← Snowflake connection
└── ~/.ssh/snowflake_key.p8            ← RSA private key
```

### Setup Procedure
""")

with st.expander("1 — Launch an EC2 Instance"):
    st.markdown("""
- **AMI**: Amazon Linux 2023
- **Instance type**: `t2.micro` (free tier eligible)
- **Security group**: Allow SSH (port 22) from your IP
- **Key pair**: Create or use an existing `.pem` key
- Save the `.pem` file locally (e.g. `~/.ssh/snowpulse-key.pem`)
""")
    st.code("""
# Set proper permissions on the key
chmod 400 ~/.ssh/snowpulse-key.pem

# Connect via SSH
ssh -i ~/.ssh/snowpulse-key.pem ec2-user@<EC2_PUBLIC_IP>
    """, language="bash")

with st.expander("2 — Install Dependencies"):
    st.code("""
# Update packages
sudo dnf update -y

# Install Python 3.11 and pip
sudo dnf install -y python3.11 python3.11-pip git

# Clone the repository
git clone https://github.com/<your-user>/SnowPulse.git
cd SnowPulse

# Install Python dependencies
pip3.11 install -r streaming/requirements.txt
    """, language="bash")

with st.expander("3 — Copy Secrets to EC2"):
    st.markdown("From your **local machine**, copy the required secret files:")
    st.code("""
# Copy .env (API key)
scp -i ~/.ssh/snowpulse-key.pem .env ec2-user@<EC2_PUBLIC_IP>:~/SnowPulse/.env

# Copy Snowflake profile
scp -i ~/.ssh/snowpulse-key.pem streaming/profile.json ec2-user@<EC2_PUBLIC_IP>:~/SnowPulse/streaming/profile.json

# Copy RSA private key
scp -i ~/.ssh/snowpulse-key.pem ~/.ssh/snowflake_key.p8 ec2-user@<EC2_PUBLIC_IP>:~/.ssh/snowflake_key.p8
    """, language="bash")
    st.markdown("""
**Important:** Update `profile.json` on EC2 so that `private_key_file` points to the EC2 path:
```json
"private_key_file": "/home/ec2-user/.ssh/snowflake_key.p8"
```
""")

with st.expander("4 — Create a systemd Service"):
    st.markdown("Create a systemd unit file so the script starts automatically and restarts on failure:")
    st.code("""
sudo tee /etc/systemd/system/snowpulse.service > /dev/null <<'EOF'
[Unit]
Description=SnowPulse - Real-Time Market Data Streamer
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/SnowPulse
ExecStart=/usr/bin/python3.11 streaming/stream_to_snowflake.py
Restart=always
RestartSec=30
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd, enable and start
sudo systemctl daemon-reload
sudo systemctl enable snowpulse
sudo systemctl start snowpulse
    """, language="bash")

with st.expander("5 — Monitor & Manage the Service"):
    st.code("""
# Check service status
sudo systemctl status snowpulse

# View live logs
sudo journalctl -u snowpulse -f

# View last 50 log lines
sudo journalctl -u snowpulse -n 50

# Restart after a code update
cd ~/SnowPulse && git pull
sudo systemctl restart snowpulse

# Stop the service
sudo systemctl stop snowpulse
    """, language="bash")

st.markdown("""
### Key Features

| Feature | Detail |
|---|---|
| **Auto-restart** | `Restart=always` + `RestartSec=30` — if the script crashes, systemd restarts it after 30 seconds |
| **Boot persistence** | `systemctl enable` — the service starts automatically when the EC2 reboots |
| **Log management** | All logs go to journald — queryable with `journalctl` |
| **Cost** | `t2.micro` is free tier eligible (750 hrs/month for the first 12 months) |
""")

st.divider()

# ─────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────
st.caption(
    "📖 SnowPulse Documentation | Magnificent Seven | "
    "Snowpipe Streaming + Dynamic Tables + Cortex LLM + Data Quality + Alerts + RSI + Beta + Sentiment Momentum | "
    "Data: Massive (Polygon.io) + Snowflake Marketplace"
)
