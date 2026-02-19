-- ============================================
-- SNOWPULSE - Cortex Sentiment Analysis
-- Uses Snowflake Cortex LLM to analyze news sentiment
-- ============================================

USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- ============================================
-- 1. NEWS_FLATTENED - Parse raw news + explode tickers
-- One row per (article, ticker) pair
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.NEWS_FLATTENED
    TARGET_LAG = '1 minute'
    WAREHOUSE = SNOWPULSE_WH
    COMMENT = 'News articles flattened by ticker from RAW_NEWS'
AS
SELECT
    RECORD_CONTENT:id::STRING                   AS ARTICLE_ID,
    RECORD_CONTENT:title::STRING                AS TITLE,
    RECORD_CONTENT:description::STRING          AS DESCRIPTION,
    RECORD_CONTENT:author::STRING               AS AUTHOR,
    RECORD_CONTENT:published_utc::TIMESTAMP_NTZ AS PUBLISHED_AT,
    RECORD_CONTENT:article_url::STRING          AS ARTICLE_URL,
    RECORD_CONTENT:publisher:name::STRING       AS PUBLISHER_NAME,
    RECORD_CONTENT:publisher:logo_url::STRING   AS PUBLISHER_LOGO,
    RECORD_METADATA:ingested_at::TIMESTAMP_NTZ  AS INGESTED_AT,
    t.VALUE::STRING                             AS TICKER
FROM RAW.RAW_NEWS,
    LATERAL FLATTEN(input => RECORD_CONTENT:tickers) t
WHERE RECORD_CONTENT:title IS NOT NULL
  AND t.VALUE::STRING IN ('AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META');

-- ============================================
-- 2. NEWS_SENTIMENT - Cortex LLM analysis
-- Uses SNOWFLAKE.CORTEX.SENTIMENT() for scoring
-- and SNOWFLAKE.CORTEX.SUMMARIZE() for summaries
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.NEWS_SENTIMENT
    TARGET_LAG = '5 minutes'
    WAREHOUSE = SNOWPULSE_WH
    COMMENT = 'Cortex-powered sentiment analysis on news articles'
AS
SELECT
    ARTICLE_ID,
    TITLE,
    DESCRIPTION,
    TICKER,
    PUBLISHED_AT,
    PUBLISHER_NAME,
    ARTICLE_URL,

    -- Sentiment score (-1 to +1) via Cortex
    SNOWFLAKE.CORTEX.SENTIMENT(
        COALESCE(TITLE, '') || '. ' || COALESCE(DESCRIPTION, '')
    ) AS SENTIMENT_SCORE,

    -- Classify sentiment
    CASE
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(COALESCE(TITLE, '') || '. ' || COALESCE(DESCRIPTION, '')) > 0.3 THEN 'POSITIVE'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(COALESCE(TITLE, '') || '. ' || COALESCE(DESCRIPTION, '')) < -0.3 THEN 'NEGATIVE'
        ELSE 'NEUTRAL'
    END AS SENTIMENT_LABEL,

    -- One-line summary via Cortex LLM
    SNOWFLAKE.CORTEX.SUMMARIZE(
        COALESCE(TITLE, '') || '. ' || COALESCE(DESCRIPTION, '')
    ) AS AI_SUMMARY

FROM ANALYTICS.NEWS_FLATTENED;

-- ============================================
-- 3. GOLD.SENTIMENT_SUMMARY - Aggregated per ticker
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE GOLD.SENTIMENT_SUMMARY
    TARGET_LAG = '5 minutes'
    WAREHOUSE = SNOWPULSE_WH
    COMMENT = 'Aggregated sentiment per ticker - dashboard ready'
AS
SELECT
    TICKER,
    COUNT(*) AS TOTAL_ARTICLES,
    ROUND(AVG(SENTIMENT_SCORE), 3) AS AVG_SENTIMENT,
    SUM(CASE WHEN SENTIMENT_LABEL = 'POSITIVE' THEN 1 ELSE 0 END) AS POSITIVE_COUNT,
    SUM(CASE WHEN SENTIMENT_LABEL = 'NEUTRAL' THEN 1 ELSE 0 END)  AS NEUTRAL_COUNT,
    SUM(CASE WHEN SENTIMENT_LABEL = 'NEGATIVE' THEN 1 ELSE 0 END) AS NEGATIVE_COUNT,
    ROUND(
        SUM(CASE WHEN SENTIMENT_LABEL = 'POSITIVE' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1
    ) AS POSITIVE_PCT,
    MIN(PUBLISHED_AT) AS EARLIEST_ARTICLE,
    MAX(PUBLISHED_AT) AS LATEST_ARTICLE
FROM ANALYTICS.NEWS_SENTIMENT
GROUP BY TICKER;

-- ============================================
-- Drop old unused table if exists
-- ============================================
DROP DYNAMIC TABLE IF EXISTS ANALYTICS.NEWS_TICKERS;

-- ============================================
-- Verify
-- ============================================
SHOW DYNAMIC TABLES IN DATABASE SNOWPULSE_DB;
