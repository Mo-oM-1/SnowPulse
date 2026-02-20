"""
SnowPulse - Documentation Page
"""

import streamlit as st

st.set_page_config(page_title="Doc | SnowPulse", page_icon="📖", layout="wide")

st.title("📖 Documentation")

# ─────────────────────────────────────────────────────────────
# Architecture Diagram
# ─────────────────────────────────────────────────────────────
st.header("📊 Data Pipeline Flow")

st.markdown("""
```
Massive (Polygon.io) REST API — Magnificent Seven
        |
        ↓ (Python — REST Polling)
Snowpipe Streaming SDK
        |
        ↓ (append_rows → VARIANT)
RAW Layer (VARIANT JSON)
        |
        ↓ (Dynamic Tables — 1 min lag)
ANALYTICS Layer (Typed & Computed)
        |
        ↓ (Dynamic Tables — 1 min lag)
GOLD Layer (Dashboard-Ready)
        |
        ↓
Streamlit Dashboard + Snowflake Alerts
```
""")

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
    - `DAILY_OHLCV`
    - `DAILY_RETURNS`
    - `MOVING_AVERAGES`

    **Refresh:** Automatic (TARGET_LAG = 1 min)
    """)

with col3:
    st.markdown("### 🟡 GOLD Layer")
    st.markdown("""
    **Format:** Aggregations

    **Dynamic Tables:**
    - `TICKER_SUMMARY`

    **Features:**
    - Latest price, return %, trend
    - SMA 5/20, BULLISH/BEARISH signal
    - Dashboard-ready (1 table = 1 query)

    **Refresh:** Automatic (TARGET_LAG = 1 min)
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

```
RAW.RAW_TRADES (VARIANT)
    ↓ TARGET_LAG = 1 min
ANALYTICS.DAILY_OHLCV (typed columns)
    ↓ TARGET_LAG = 1 min          ↓ TARGET_LAG = 1 min
ANALYTICS.DAILY_RETURNS      ANALYTICS.MOVING_AVERAGES
    ↓                              ↓
    └──────── JOIN ────────────────┘
                    ↓ TARGET_LAG = 1 min
            GOLD.TICKER_SUMMARY
```

Snowflake automatically detects dependencies and refreshes downstream tables in cascade.
""")

st.subheader("📌 SQL Examples")

with st.expander("Create a Dynamic Table (DAILY_OHLCV)"):
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
FROM RAW.RAW_TRADES;
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

### Our 3 Alerts

| Alert | Condition | Frequency |
|---|---|---|
| `ALERT_BIG_DAILY_MOVE` | Any ticker moves > 3% in a day | Every 5 min |
| `ALERT_TREND_CHANGE` | SMA5/SMA20 signal flips (BULLISH ↔ BEARISH) | Every 5 min |
| `ALERT_HIGH_VOLUME` | Volume exceeds 2x the 20-day average | Every 5 min |

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
    - **Monitoring**: Snowflake Alerts (condition-based)
    - **Auth**: RSA key-pair authentication
    """)

with col2:
    st.markdown("### Infrastructure")
    st.markdown("""
    - **Warehouse**: SNOWPULSE_WH (XS, auto-suspend 60s)
    - **API**: Massive / Polygon.io REST (5 req/min free tier)
    - **Tickers**: AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META
    - **Security**: External Access Integration + Secrets
    - **Frontend**: Streamlit
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

3. **TARGET_LAG = 1 minute**
   - Near real-time without overconsumption
   - Snowflake batches updates intelligently

4. **Free Tier API**
   - Massive (Polygon.io): 5 req/min (sufficient for 7 tickers)
   - Python-side rate limiting to respect API limits

5. **XS Warehouse**
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
    "Snowpipe Streaming + Dynamic Tables + Alerts | Data: Massive (Polygon.io)"
)
