-- ============================================
-- SNOWPULSE - RAW Tables
-- Target for Snowpipe Streaming ingestion
-- ============================================

USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE SCHEMA RAW;
USE WAREHOUSE SNOWPULSE_WH;

-- ----------------------------------------
-- RAW_TRADES
-- Source: Polygon.io WebSocket (T events)
-- Fields: ev, sym, x, i, z, p, s, c, t, q, trfi, trft
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS RAW.RAW_TRADES (
    RECORD_METADATA    VARIANT    COMMENT 'Snowpipe Streaming metadata (offset, partition, timestamp)',
    RECORD_CONTENT     VARIANT    COMMENT 'Raw trade event from Polygon.io WebSocket',
    -- ev  : event type (always "T")
    -- sym : ticker symbol (AAPL, MSFT, etc.)
    -- p   : price
    -- s   : trade size (shares)
    -- x   : exchange ID
    -- i   : trade ID
    -- z   : tape (1=NYSE, 2=AMEX, 3=Nasdaq)
    -- c   : trade conditions (array)
    -- t   : SIP timestamp (Unix ms)
    -- q   : sequence number
    INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw trades from Polygon.io WebSocket via Snowpipe Streaming';

-- ----------------------------------------
-- RAW_AGGREGATES
-- Source: Polygon.io REST API (aggregates/custom-bars)
-- Fields: ticker, o, h, l, c, v, vw, n, t
-- Fetched every minute via Python script
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS RAW.RAW_AGGREGATES (
    RECORD_METADATA    VARIANT    COMMENT 'Snowpipe Streaming metadata',
    RECORD_CONTENT     VARIANT    COMMENT 'Raw aggregate bar from Polygon.io REST API',
    -- ticker : stock symbol
    -- o      : open price
    -- h      : high price
    -- l      : low price
    -- c      : close price
    -- v      : volume
    -- vw     : volume-weighted average price (VWAP)
    -- n      : number of transactions
    -- t      : Unix ms timestamp (start of bar)
    INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw OHLCV aggregates from Polygon.io REST API via Snowpipe Streaming';

-- ----------------------------------------
-- RAW_NEWS
-- Source: Polygon.io REST API (/v2/reference/news)
-- Used for Cortex sentiment analysis
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS RAW.RAW_NEWS (
    RECORD_METADATA    VARIANT    COMMENT 'Snowpipe Streaming metadata',
    RECORD_CONTENT     VARIANT    COMMENT 'Raw news article from Polygon.io REST API',
    -- id           : unique article ID
    -- publisher    : { name, homepage_url, logo_url }
    -- title        : article title
    -- author       : author name
    -- published_utc: ISO 8601 timestamp
    -- article_url  : link to article
    -- tickers      : array of related tickers
    -- description  : article summary/body
    -- keywords     : array of keywords
    INGESTED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw financial news from Polygon.io - used for Cortex sentiment analysis';

-- ----------------------------------------
-- PIPELINE_LOGS (shared)
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS COMMON.PIPELINE_LOGS (
    LOG_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    LOGGED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LEVEL           VARCHAR(10),    -- INFO, WARNING, ERROR
    COMPONENT_NAME  VARCHAR(100),
    MESSAGE         VARCHAR(4000),
    STACK_TRACE     VARCHAR(16000)
)
COMMENT = 'Centralized pipeline logs for SnowPulse';

-- ----------------------------------------
-- Verify
-- ----------------------------------------
SHOW TABLES IN SCHEMA RAW;
SHOW TABLES IN SCHEMA COMMON;
