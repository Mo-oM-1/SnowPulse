-- ============================================
-- SNOWPULSE - Dynamic Tables
-- Auto-refresh when upstream RAW data changes
-- No Tasks needed — Snowflake handles it!
-- ============================================

USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- ============================================
-- 1. DAILY OHLCV (flattened from RAW_TRADES)
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DAILY_OHLCV
    TARGET_LAG = '1 minute'
    WAREHOUSE = SNOWPULSE_WH
    COMMENT = 'Daily OHLCV bars - flattened from RAW_TRADES via Snowpipe Streaming'
AS
SELECT
    RECORD_CONTENT:ticker::STRING           AS TICKER,
    TO_DATE(TO_TIMESTAMP_NTZ(
        RECORD_CONTENT:t::NUMBER / 1000
    ))                                      AS TRADE_DATE,
    RECORD_CONTENT:o::FLOAT                 AS OPEN_PRICE,
    RECORD_CONTENT:h::FLOAT                 AS HIGH_PRICE,
    RECORD_CONTENT:l::FLOAT                 AS LOW_PRICE,
    RECORD_CONTENT:c::FLOAT                 AS CLOSE_PRICE,
    RECORD_CONTENT:v::FLOAT                 AS VOLUME,
    RECORD_CONTENT:vw::FLOAT                AS VWAP,
    RECORD_CONTENT:n::NUMBER                AS NUM_TRANSACTIONS,
    RECORD_METADATA:ingested_at::TIMESTAMP_NTZ AS INGESTED_AT
FROM RAW.RAW_TRADES;

-- ============================================
-- 2. DAILY RETURNS (% change day over day)
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DAILY_RETURNS
    TARGET_LAG = '1 minute'
    WAREHOUSE = SNOWPULSE_WH
    COMMENT = 'Daily returns calculated from DAILY_OHLCV - auto-refreshes when upstream changes'
AS
SELECT
    TICKER,
    TRADE_DATE,
    CLOSE_PRICE,
    LAG(CLOSE_PRICE) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE) AS PREV_CLOSE,
    ROUND(
        (CLOSE_PRICE - LAG(CLOSE_PRICE) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE))
        / NULLIF(LAG(CLOSE_PRICE) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE), 0)
        * 100, 2
    ) AS DAILY_RETURN_PCT,
    VOLUME,
    VWAP
FROM ANALYTICS.DAILY_OHLCV;

-- ============================================
-- 3. MOVING AVERAGES (SMA 5, 10, 20 days)
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MOVING_AVERAGES
    TARGET_LAG = '1 minute'
    WAREHOUSE = SNOWPULSE_WH
    COMMENT = 'Simple Moving Averages (5/10/20 days) - key technical indicator'
AS
SELECT
    TICKER,
    TRADE_DATE,
    CLOSE_PRICE,
    ROUND(AVG(CLOSE_PRICE) OVER (
        PARTITION BY TICKER ORDER BY TRADE_DATE ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 2) AS SMA_5,
    ROUND(AVG(CLOSE_PRICE) OVER (
        PARTITION BY TICKER ORDER BY TRADE_DATE ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ), 2) AS SMA_10,
    ROUND(AVG(CLOSE_PRICE) OVER (
        PARTITION BY TICKER ORDER BY TRADE_DATE ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 2) AS SMA_20,
    -- Signal: SMA5 crosses above SMA20 = bullish
    CASE
        WHEN AVG(CLOSE_PRICE) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
           > AVG(CLOSE_PRICE) OVER (PARTITION BY TICKER ORDER BY TRADE_DATE ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        THEN 'BULLISH'
        ELSE 'BEARISH'
    END AS TREND_SIGNAL
FROM ANALYTICS.DAILY_OHLCV;

-- ============================================
-- 4. TICKER SUMMARY (latest stats per ticker)
-- Gold layer — ready for Streamlit dashboard
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.TICKER_SUMMARY
    TARGET_LAG = '1 minute'
    WAREHOUSE = SNOWPULSE_WH
    COMMENT = 'Latest summary per ticker - dashboard ready'
AS
WITH latest AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY TRADE_DATE DESC) AS rn
    FROM ANALYTICS.DAILY_OHLCV
),
returns AS (
    SELECT TICKER, DAILY_RETURN_PCT,
        ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY TRADE_DATE DESC) AS rn
    FROM ANALYTICS.DAILY_RETURNS
    WHERE DAILY_RETURN_PCT IS NOT NULL
),
ma AS (
    SELECT TICKER, SMA_5, SMA_20, TREND_SIGNAL,
        ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY TRADE_DATE DESC) AS rn
    FROM ANALYTICS.MOVING_AVERAGES
)
SELECT
    l.TICKER,
    l.TRADE_DATE           AS LAST_TRADE_DATE,
    l.OPEN_PRICE,
    l.HIGH_PRICE,
    l.LOW_PRICE,
    l.CLOSE_PRICE          AS LAST_CLOSE,
    l.VOLUME               AS LAST_VOLUME,
    l.VWAP                 AS LAST_VWAP,
    r.DAILY_RETURN_PCT     AS LAST_RETURN_PCT,
    m.SMA_5,
    m.SMA_20,
    m.TREND_SIGNAL
FROM latest l
LEFT JOIN returns r ON l.TICKER = r.TICKER AND r.rn = 1
LEFT JOIN ma m ON l.TICKER = m.TICKER AND m.rn = 1
WHERE l.rn = 1;

-- ============================================
-- Verify
-- ============================================
SHOW DYNAMIC TABLES IN DATABASE SNOWPULSE_DB;
