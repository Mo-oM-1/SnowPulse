# ⚡ SnowPulse — Real-Time Market Intelligence

> Real-time market intelligence platform for the **Magnificent Seven** (AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META) built 100% on **Snowflake native features**.

## 🎯 What This Project Demonstrates

| Snowflake Feature | Usage |
|---|---|
| **Snowpipe Streaming** | Real-time ingestion from Massive (Polygon.io) REST API via Python SDK |
| **Dynamic Tables** | Declarative transformations with automatic cascade refresh (1 min lag) |
| **Snowflake Alerts** | Automated market condition monitoring (big moves, trend changes, high volume) |
| **Cortex LLM** | AI-powered sentiment analysis on financial news (SENTIMENT + SUMMARIZE) |
| **VARIANT** | Schema-on-read for semi-structured JSON market data |
| **External Access Integration** | Secure outbound API calls with Network Rules + Secrets |
| **RSA Key-pair Auth** | Passwordless authentication for Snowpipe Streaming SDK |

## 🏗️ Architecture

```
┌─────────────────────────┐
│  Massive (Polygon.io)   │
│      REST API           │
└───────────┬─────────────┘
            │ Python (REST polling)
            ▼
┌─────────────────────────┐
│  Snowpipe Streaming SDK │
│  append_rows() → VARIANT│
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│                    SNOWPULSE_DB                              │
│                                                              │
│  ┌─── RAW ──────────────┐                                   │
│  │ RAW_TRADES           │                                    │
│  │ RAW_AGGREGATES       │──── Dynamic Tables (1 min lag)     │
│  │ RAW_NEWS             │           │                        │
│  └──────────────────────┘           ▼                        │
│                           ┌─── ANALYTICS ──────────┐         │
│                           │ DAILY_OHLCV            │         │
│                           │ DAILY_RETURNS          │         │
│                           │ MOVING_AVERAGES        │         │
│                           │ NEWS_FLATTENED         │         │
│                           │ NEWS_TICKERS           │         │
│                           │ NEWS_SENTIMENT (Cortex)│         │
│                           └─────────┬──────────────┘         │
│                                     │                        │
│                                     ▼                        │
│                           ┌─── GOLD ──────────────┐          │
│                           │ TICKER_SUMMARY        │          │
│                           │ SENTIMENT_SUMMARY     │          │
│                           └─────────┬─────────────┘          │
│                                     │                        │
│  ┌─── COMMON ──────────┐           │                        │
│  │ ALERT_LOG           │◄── Alerts (5 min schedule)          │
│  │ PIPELINE_LOGS       │                                     │
│  └─────────────────────┘                                     │
└──────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────┐
│   Streamlit Dashboard   │
│  Home │ Analysis │ News │
│  Alerts │ Doc           │
└─────────────────────────┘
```

## 📊 Dashboard Pages

| Page | Description |
|---|---|
| **Home** | KPI cards, relative performance chart (base 100), ticker summary table |
| **Technical Analysis** | Candlestick charts with SMA overlay, volume bars, daily returns, heatmap |
| **News & Sentiment** | Cortex-powered sentiment scores, distribution charts, article feed |
| **Alerts** | Alert timeline, statistics by type/ticker, real-time alert feed |
| **Documentation** | Pipeline architecture, code examples, financial glossary |

## 🚀 Quick Start

### Prerequisites

- Snowflake account with `ACCOUNTADMIN` access
- Python 3.9+
- RSA key-pair generated (`~/.ssh/snowflake_key.p8`)
- Massive (Polygon.io) free API key

### 1. Clone & Setup

```bash
git clone https://github.com/YOUR_USERNAME/snowpulse.git
cd snowpulse
python -m venv .venv
source .venv/bin/activate
pip install -r streaming/requirements.txt
pip install -r streamlit/requirements.txt
```

### 2. Configure Secrets

```bash
cp .env.example .env
# Edit .env with your Polygon API key and Snowflake details
```

Create `streaming/profile.json`:
```json
{
    "account": "YOUR_ACCOUNT_ID",
    "user": "YOUR_USER",
    "url": "https://YOUR_ACCOUNT_ID.snowflakecomputing.com:443",
    "private_key_file": "/path/to/.ssh/snowflake_key.p8",
    "role": "SNOWPULSE_ROLE"
}
```

Create `.streamlit/secrets.toml`:
```toml
[snowflake]
account = "YOUR_ACCOUNT_ID"
user = "YOUR_USER"
private_key_path = "/path/to/.ssh/snowflake_key.p8"
role = "SNOWPULSE_ROLE"
warehouse = "SNOWPULSE_WH"
database = "SNOWPULSE_DB"
```

### 3. Deploy Snowflake Objects

Execute SQL files **in order** in Snowsight:

```
deploy/01_setup/01_setup.sql          # Database, schemas, warehouse, role
deploy/02_raw/01_tables.sql           # RAW tables (VARIANT)
deploy/03_dynamic_tables/01_dynamic_tables.sql  # Analytics + Gold tables
deploy/04_alerts/01_alerts.sql        # Alert rules + log table
deploy/05_cortex/01_cortex_sentiment.sql  # Cortex sentiment analysis
```

Then manually create the API secret:
```sql
USE ROLE SNOWPULSE_ROLE;
USE SCHEMA SNOWPULSE_DB.COMMON;
CREATE OR REPLACE SECRET POLYGON_API_KEY
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_API_KEY>';
```

### 4. Ingest Data

```bash
cd snowpulse
source .venv/bin/activate
python streaming/stream_to_snowflake.py
```

### 5. Launch Dashboard

```bash
streamlit run streamlit/app.py
```

Open http://localhost:8501

## 📁 Project Structure

```
snowpulse/
├── deploy/
│   ├── 01_setup/01_setup.sql              # Infrastructure
│   ├── 02_raw/01_tables.sql               # RAW tables
│   ├── 03_dynamic_tables/01_dynamic_tables.sql  # ANALYTICS + GOLD
│   ├── 04_alerts/01_alerts.sql            # Alert rules
│   └── 05_cortex/01_cortex_sentiment.sql  # Cortex LLM
├── streaming/
│   ├── stream_to_snowflake.py             # Ingestion script
│   ├── profile.json                       # Snowflake auth (gitignored)
│   └── requirements.txt
├── streamlit/
│   ├── app.py                             # Home page
│   ├── connection.py                      # Shared SF connection
│   ├── requirements.txt
│   └── pages/
│       ├── 1_Doc.py                       # Documentation
│       ├── 2_Technical_Analysis.py        # Candlestick + SMA
│       ├── 3_News_Sentiment.py            # Cortex sentiment
│       └── 4_Alerts.py                    # Alert monitor
├── .env.example                           # Environment template
├── .gitignore
└── README.md
```

## 🔐 Security

- **No secrets in code** — all credentials via `.env`, `profile.json`, `secrets.toml` (gitignored)
- **RSA key-pair auth** — passwordless Snowpipe Streaming connection
- **External Access Integration** — Snowflake controls outbound API calls
- **RBAC** — dedicated `SNOWPULSE_ROLE` with least privilege
- **Network Rules** — egress restricted to `api.polygon.io`

## ⚡ Key Technical Decisions

| Decision | Why |
|---|---|
| REST polling instead of WebSocket | Free tier compatible (5 req/min) |
| Python dicts for VARIANT (not json.dumps) | Direct VARIANT object storage, not string |
| Dynamic Tables instead of Tasks | Declarative, less code, automatic cascade |
| TARGET_LAG = 1 minute | Near real-time without overconsumption |
| Cortex SENTIMENT() + SUMMARIZE() | Native AI, no external ML pipeline needed |

## 📜 License

This project is built for educational and portfolio purposes.

---

Built with ❄️ Snowflake + 🐍 Python + 📊 Streamlit
