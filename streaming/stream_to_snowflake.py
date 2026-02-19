"""
SnowPulse - Real-Time Market Data Streamer
Connects to Polygon.io REST API (free tier compatible)
Ingests into Snowflake via Snowpipe Streaming SDK
"""

import os
import json
import time
import signal
import logging
import threading
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from snowflake.ingest.streaming import StreamingIngestClient

# ============================================
# Configuration
# ============================================

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]

POLYGON_REST_URL = "https://api.polygon.io"

# Free tier: 5 requests/min → stagger calls
TRADE_POLL_INTERVAL = 60       # Fetch last trades every 60s
AGG_POLL_INTERVAL = 60         # Fetch aggregates every 60s
NEWS_POLL_INTERVAL = 300       # Fetch news every 5 minutes
API_CALL_DELAY = 12            # 12s between API calls = 5 req/min max

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("snowpulse")

# Graceful shutdown
shutdown_event = threading.Event()


def signal_handler(sig, frame):
    log.info("Shutdown signal received, stopping...")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================
# Snowpipe Streaming Client
# ============================================

PROFILE_JSON_PATH = os.path.join(os.path.dirname(__file__), "profile.json")


def create_streaming_client(client_name, table_name):
    """Create a Snowpipe Streaming client using the default pipe (<TABLE>-STREAMING)"""
    return StreamingIngestClient(
        client_name=client_name,
        db_name="SNOWPULSE_DB",
        schema_name="RAW",
        pipe_name=f"{table_name}-STREAMING",
        profile_json=PROFILE_JSON_PATH
    )


# ============================================
# Rate-limited API caller
# ============================================

api_lock = threading.Lock()
last_api_call = 0


def api_get(url, params):
    """Rate-limited GET request to Polygon.io (free tier: 5 req/min)"""
    global last_api_call
    with api_lock:
        elapsed = time.time() - last_api_call
        if elapsed < API_CALL_DELAY:
            time.sleep(API_CALL_DELAY - elapsed)
        params["apiKey"] = POLYGON_API_KEY
        resp = requests.get(url, params=params, timeout=15)
        last_api_call = time.time()
        resp.raise_for_status()
        return resp.json()


# ============================================
# Historical Aggregates Poller (REST → Snowpipe)
# Load 30 days of daily bars on startup
# ============================================

class HistoricalAggPoller:
    """Loads historical daily OHLCV bars (30 days) into RAW_TRADES table"""

    def __init__(self):
        self.client = create_streaming_client("trades_client", "RAW_TRADES")
        self.channel, _ = self.client.open_channel(channel_name="trades_channel")
        self.total_ingested = 0
        self.offset = 0
        self.loaded = False
        log.info(f"HistoricalAggPoller initialized | Tickers: {', '.join(TICKERS)}")

    def fetch_and_ingest(self):
        """Fetch 30 days of daily bars for each ticker"""
        if self.loaded:
            return

        from datetime import timedelta
        today = datetime.now().strftime("%Y-%m-%d")
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = []

        for ticker in TICKERS:
            if shutdown_event.is_set():
                break
            try:
                data = api_get(
                    f"{POLYGON_REST_URL}/v2/aggs/ticker/{ticker}/range/1/day/{thirty_days_ago}/{today}",
                    {"adjusted": "true"}
                )
                for bar in data.get("results", []):
                    bar["ticker"] = ticker
                    rows.append({
                        "RECORD_CONTENT": bar,
                        "RECORD_METADATA": {
                            "ingested_at": datetime.now(timezone.utc).isoformat(),
                            "source": "polygon_rest_daily_bars",
                            "ticker": ticker
                        }
                    })
            except Exception as e:
                log.error(f"HIST | Error fetching {ticker}: {e}")

        if rows:
            try:
                self.offset += len(rows)
                self.channel.append_rows(
                    rows,
                    start_offset_token=str(self.offset - len(rows)),
                    end_offset_token=str(self.offset)
                )
                self.total_ingested += len(rows)
                self.loaded = True
                log.info(f"HIST | Ingested {len(rows)} daily bars (30 days) | Total: {self.total_ingested}")
            except Exception as e:
                log.error(f"HIST | Ingest error: {e}")

    def run(self):
        """Load once, then idle"""
        self.fetch_and_ingest()
        # Stay alive but don't re-fetch
        while not shutdown_event.is_set():
            shutdown_event.wait(3600)

    def close(self):
        self.channel.close()
        self.client.close()
        log.info(f"HistoricalAggPoller closed | Total ingested: {self.total_ingested}")


# ============================================
# Aggregates Poller (REST → Snowpipe)
# ============================================

class AggregatePoller:
    """Polls Polygon.io REST API for previous day OHLCV bars"""

    def __init__(self):
        self.client = create_streaming_client("agg_client", "RAW_AGGREGATES")
        self.channel, _ = self.client.open_channel(channel_name="agg_channel")
        self.total_ingested = 0
        self.offset = 0
        log.info("AggregatePoller initialized")

    def fetch_and_ingest(self):
        """Fetch previous close bars for all tickers"""
        rows = []

        for ticker in TICKERS:
            if shutdown_event.is_set():
                break
            try:
                data = api_get(
                    f"{POLYGON_REST_URL}/v2/aggs/ticker/{ticker}/prev",
                    {"adjusted": "true"}
                )
                for result in data.get("results", []):
                    result["ticker"] = ticker
                    rows.append({
                        "RECORD_CONTENT": result,
                        "RECORD_METADATA": {
                            "ingested_at": datetime.now(timezone.utc).isoformat(),
                            "source": "polygon_rest_aggs",
                            "ticker": ticker
                        }
                    })
            except Exception as e:
                log.error(f"AGG | Error fetching {ticker}: {e}")

        if rows:
            try:
                self.offset += len(rows)
                self.channel.append_rows(
                    rows,
                    start_offset_token=str(self.offset - len(rows)),
                    end_offset_token=str(self.offset)
                )
                self.total_ingested += len(rows)
                log.info(f"AGG | Ingested {len(rows)} bars | Total: {self.total_ingested}")
            except Exception as e:
                log.error(f"AGG | Ingest error: {e}")

    def run(self):
        """Polling loop"""
        while not shutdown_event.is_set():
            self.fetch_and_ingest()
            shutdown_event.wait(AGG_POLL_INTERVAL)

    def close(self):
        self.channel.close()
        self.client.close()
        log.info(f"AggregatePoller closed | Total ingested: {self.total_ingested}")


# ============================================
# News Poller (REST → Snowpipe)
# ============================================

class NewsPoller:
    """Polls Polygon.io REST API for financial news"""

    def __init__(self):
        self.client = create_streaming_client("news_client", "RAW_NEWS")
        self.channel, _ = self.client.open_channel(channel_name="news_channel")
        self.total_ingested = 0
        self.seen_ids = set()
        self.offset = 0
        log.info("NewsPoller initialized")

    def fetch_and_ingest(self):
        """Fetch latest news for tracked tickers"""
        rows = []

        try:
            data = api_get(
                f"{POLYGON_REST_URL}/v2/reference/news",
                {
                    "ticker": ",".join(TICKERS),
                    "limit": 50,
                    "sort": "published_utc",
                    "order": "desc"
                }
            )

            for article in data.get("results", []):
                article_id = article.get("id")
                if article_id and article_id not in self.seen_ids:
                    self.seen_ids.add(article_id)
                    rows.append({
                        "RECORD_CONTENT": article,
                        "RECORD_METADATA": {
                            "ingested_at": datetime.now(timezone.utc).isoformat(),
                            "source": "polygon_rest_news"
                        }
                    })

            # Keep seen_ids from growing unbounded
            if len(self.seen_ids) > 5000:
                self.seen_ids = set(list(self.seen_ids)[-2500:])

        except Exception as e:
            log.error(f"NEWS | Error fetching: {e}")

        if rows:
            try:
                self.offset += len(rows)
                self.channel.append_rows(
                    rows,
                    start_offset_token=str(self.offset - len(rows)),
                    end_offset_token=str(self.offset)
                )
                self.total_ingested += len(rows)
                log.info(f"NEWS | Ingested {len(rows)} articles | Total: {self.total_ingested}")
            except Exception as e:
                log.error(f"NEWS | Ingest error: {e}")

    def run(self):
        """Polling loop"""
        while not shutdown_event.is_set():
            self.fetch_and_ingest()
            shutdown_event.wait(NEWS_POLL_INTERVAL)

    def close(self):
        self.channel.close()
        self.client.close()
        log.info(f"NewsPoller closed | Total ingested: {self.total_ingested}")


# ============================================
# Main
# ============================================

def main():
    log.info("=" * 50)
    log.info("SnowPulse - Real-Time Market Data Streamer")
    log.info(f"Tickers: {', '.join(TICKERS)}")
    log.info(f"Mode: REST API polling (free tier)")
    log.info("=" * 50)

    # Validate config
    missing = []
    if not POLYGON_API_KEY:
        missing.append("POLYGON_API_KEY")
    if missing:
        log.error(f"Missing env vars: {', '.join(missing)}")
        log.error("Copy .env.example to .env and fill in your values")
        return

    # Initialize pollers
    hist_poller = HistoricalAggPoller()
    agg_poller = AggregatePoller()
    news_poller = NewsPoller()

    # Start background threads
    hist_thread = threading.Thread(target=hist_poller.run, daemon=True, name="historical")
    hist_thread.start()

    agg_thread = threading.Thread(target=agg_poller.run, daemon=True, name="aggregates")
    agg_thread.start()

    news_thread = threading.Thread(target=news_poller.run, daemon=True, name="news")
    news_thread.start()

    log.info("All pollers started. Press Ctrl+C to stop.")

    # Keep main thread alive
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(1)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("Shutting down all pollers...")
        shutdown_event.set()
        hist_poller.close()
        agg_poller.close()
        news_poller.close()
        log.info("SnowPulse stopped.")


if __name__ == "__main__":
    main()
