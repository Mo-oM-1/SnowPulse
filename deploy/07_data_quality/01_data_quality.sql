-- ============================================================
-- SnowPulse — Data Quality Layer
-- Snowflake Features: Tags · DMFs · Streams · Stored Procedure · Task · Alert · Dynamic Table
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- 0. ACCOUNTADMIN GRANTS (run once)
-- ─────────────────────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;

GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE SNOWPULSE_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SNOWPULSE_ROLE;

USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- ─────────────────────────────────────────────────────────────
-- 1. DATA QUALITY LOG TABLE
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS COMMON.DATA_QUALITY_LOG (
    CHECK_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    CHECKED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CHECK_NAME      VARCHAR(100),
    TABLE_NAME      VARCHAR(200),
    STATUS          VARCHAR(10),    -- PASS, WARN, FAIL
    METRIC_VALUE    FLOAT,
    THRESHOLD       FLOAT,
    MESSAGE         VARCHAR(2000)
)
COMMENT = 'Data quality check results — populated by scheduled Task';

ALTER TABLE COMMON.DATA_QUALITY_LOG SET CHANGE_TRACKING = TRUE;

-- ─────────────────────────────────────────────────────────────
-- 2. TAGS — Data Governance & Classification
-- ─────────────────────────────────────────────────────────────
CREATE TAG IF NOT EXISTS COMMON.DATA_DOMAIN
    ALLOWED_VALUES 'MARKET_DATA', 'NEWS', 'MACRO', 'OPERATIONAL'
    COMMENT = 'Business domain classification';

CREATE TAG IF NOT EXISTS COMMON.DATA_LAYER
    ALLOWED_VALUES 'RAW', 'ANALYTICS', 'GOLD'
    COMMENT = 'Medallion architecture layer';

CREATE TAG IF NOT EXISTS COMMON.FRESHNESS_SLA
    ALLOWED_VALUES '1_MINUTE', '5_MINUTES', '1_HOUR', '1_DAY'
    COMMENT = 'Expected data freshness SLA';

-- Apply tags to RAW tables
ALTER TABLE RAW.RAW_TRADES SET TAG
    COMMON.DATA_DOMAIN = 'MARKET_DATA',
    COMMON.DATA_LAYER = 'RAW',
    COMMON.FRESHNESS_SLA = '1_MINUTE';

ALTER TABLE RAW.RAW_AGGREGATES SET TAG
    COMMON.DATA_DOMAIN = 'MARKET_DATA',
    COMMON.DATA_LAYER = 'RAW',
    COMMON.FRESHNESS_SLA = '1_MINUTE';

ALTER TABLE RAW.RAW_NEWS SET TAG
    COMMON.DATA_DOMAIN = 'NEWS',
    COMMON.DATA_LAYER = 'RAW',
    COMMON.FRESHNESS_SLA = '5_MINUTES';

-- Apply tags to Analytics Dynamic Tables
ALTER TABLE ANALYTICS.DAILY_OHLCV SET TAG
    COMMON.DATA_DOMAIN = 'MARKET_DATA',
    COMMON.DATA_LAYER = 'ANALYTICS';

ALTER TABLE ANALYTICS.DAILY_RETURNS SET TAG
    COMMON.DATA_DOMAIN = 'MARKET_DATA',
    COMMON.DATA_LAYER = 'ANALYTICS';

ALTER TABLE ANALYTICS.MOVING_AVERAGES SET TAG
    COMMON.DATA_DOMAIN = 'MARKET_DATA',
    COMMON.DATA_LAYER = 'ANALYTICS';

ALTER TABLE ANALYTICS.NEWS_FLATTENED SET TAG
    COMMON.DATA_DOMAIN = 'NEWS',
    COMMON.DATA_LAYER = 'ANALYTICS';

ALTER TABLE ANALYTICS.NEWS_SENTIMENT SET TAG
    COMMON.DATA_DOMAIN = 'NEWS',
    COMMON.DATA_LAYER = 'ANALYTICS';

-- Apply tags to Gold Dynamic Tables
ALTER TABLE GOLD.TICKER_SUMMARY SET TAG
    COMMON.DATA_DOMAIN = 'MARKET_DATA',
    COMMON.DATA_LAYER = 'GOLD';

ALTER TABLE GOLD.SENTIMENT_SUMMARY SET TAG
    COMMON.DATA_DOMAIN = 'NEWS',
    COMMON.DATA_LAYER = 'GOLD';

ALTER TABLE GOLD.MACRO_OVERVIEW SET TAG
    COMMON.DATA_DOMAIN = 'MACRO',
    COMMON.DATA_LAYER = 'GOLD';

-- Apply tags to Operational tables
ALTER TABLE COMMON.PIPELINE_LOGS SET TAG
    COMMON.DATA_DOMAIN = 'OPERATIONAL',
    COMMON.DATA_LAYER = 'RAW';

ALTER TABLE COMMON.ALERT_LOG SET TAG
    COMMON.DATA_DOMAIN = 'OPERATIONAL',
    COMMON.DATA_LAYER = 'RAW';

ALTER TABLE COMMON.DATA_QUALITY_LOG SET TAG
    COMMON.DATA_DOMAIN = 'OPERATIONAL',
    COMMON.DATA_LAYER = 'RAW';

-- ─────────────────────────────────────────────────────────────
-- 3. DATA METRIC FUNCTIONS (DMFs)
--    Automated quality metrics attached directly to tables
--    Results visible in SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
-- ─────────────────────────────────────────────────────────────

-- DMF: Count negative prices (should always be 0)
CREATE OR REPLACE DATA METRIC FUNCTION COMMON.NEGATIVE_PRICE_COUNT(
    ARG_T TABLE(ARG_C FLOAT)
)
RETURNS NUMBER
COMMENT = 'Count of negative prices — expected: 0'
AS $$
    SELECT COUNT(*) FROM ARG_T WHERE ARG_C < 0
$$;

-- DMF: OHLCV consistency violations (Low should be <= High)
CREATE OR REPLACE DATA METRIC FUNCTION COMMON.OHLCV_VIOLATION_COUNT(
    ARG_T TABLE(LOW_COL FLOAT, HIGH_COL FLOAT)
)
RETURNS NUMBER
COMMENT = 'Rows where Low > High — expected: 0'
AS $$
    SELECT COUNT(*) FROM ARG_T WHERE LOW_COL > HIGH_COL
$$;

-- DMF: Missing Magnificent Seven tickers
CREATE OR REPLACE DATA METRIC FUNCTION COMMON.MISSING_TICKER_COUNT(
    ARG_T TABLE(TICKER_COL VARCHAR)
)
RETURNS NUMBER
COMMENT = 'Missing Mag7 tickers — expected: 0'
AS $$
    SELECT 7 - COUNT(DISTINCT TICKER_COL) FROM ARG_T
    WHERE TICKER_COL IN ('AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','META')
$$;

-- DMF: Data freshness in hours
CREATE OR REPLACE DATA METRIC FUNCTION COMMON.FRESHNESS_HOURS(
    ARG_T TABLE(TS_COL TIMESTAMP_NTZ)
)
RETURNS NUMBER
COMMENT = 'Hours since latest record — measures data staleness'
AS $$
    SELECT COALESCE(
        ROUND(TIMESTAMPDIFF('HOUR', MAX(TS_COL), CURRENT_TIMESTAMP()), 1),
        -1
    ) FROM ARG_T
$$;

-- Attach DMFs to DAILY_OHLCV (runs every 60 min)
ALTER TABLE ANALYTICS.DAILY_OHLCV SET DATA_METRIC_SCHEDULE = '60 MINUTE';

ALTER TABLE ANALYTICS.DAILY_OHLCV ADD DATA METRIC FUNCTION
    COMMON.NEGATIVE_PRICE_COUNT ON (CLOSE_PRICE);

ALTER TABLE ANALYTICS.DAILY_OHLCV ADD DATA METRIC FUNCTION
    COMMON.OHLCV_VIOLATION_COUNT ON (LOW_PRICE, HIGH_PRICE);

ALTER TABLE ANALYTICS.DAILY_OHLCV ADD DATA METRIC FUNCTION
    COMMON.MISSING_TICKER_COUNT ON (TICKER);

ALTER TABLE ANALYTICS.DAILY_OHLCV ADD DATA METRIC FUNCTION
    COMMON.FRESHNESS_HOURS ON (INGESTED_AT);

-- Attach freshness DMF to RAW tables
ALTER TABLE RAW.RAW_TRADES SET DATA_METRIC_SCHEDULE = '60 MINUTE';
ALTER TABLE RAW.RAW_TRADES ADD DATA METRIC FUNCTION
    COMMON.FRESHNESS_HOURS ON (INGESTED_AT);

ALTER TABLE RAW.RAW_AGGREGATES SET DATA_METRIC_SCHEDULE = '60 MINUTE';
ALTER TABLE RAW.RAW_AGGREGATES ADD DATA METRIC FUNCTION
    COMMON.FRESHNESS_HOURS ON (INGESTED_AT);

ALTER TABLE RAW.RAW_NEWS SET DATA_METRIC_SCHEDULE = '60 MINUTE';
ALTER TABLE RAW.RAW_NEWS ADD DATA METRIC FUNCTION
    COMMON.FRESHNESS_HOURS ON (INGESTED_AT);

-- ─────────────────────────────────────────────────────────────
-- 4. STREAMS — Change Data Capture (CDC) on RAW tables
--    Tracks new rows ingested since last quality check
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE STREAM COMMON.STREAM_RAW_TRADES
    ON TABLE RAW.RAW_TRADES
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream — tracks new trade ingestions';

CREATE OR REPLACE STREAM COMMON.STREAM_RAW_AGGREGATES
    ON TABLE RAW.RAW_AGGREGATES
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream — tracks new aggregate ingestions';

CREATE OR REPLACE STREAM COMMON.STREAM_RAW_NEWS
    ON TABLE RAW.RAW_NEWS
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream — tracks new news ingestions';

-- ─────────────────────────────────────────────────────────────
-- 5. STORED PROCEDURE — Quality check logic (Snowflake Scripting)
--    10 checks: freshness, completeness, validity,
--    consistency, duplicates, volume (via streams)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE COMMON.SP_DATA_QUALITY_CHECK()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Runs 10 data quality checks and logs results to DATA_QUALITY_LOG'
AS
BEGIN
    -- Check 1: Freshness — RAW_TRADES (threshold: 10 min)
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'FRESHNESS', 'RAW.RAW_TRADES',
        CASE WHEN TIMESTAMPDIFF('MINUTE', MAX(INGESTED_AT), CURRENT_TIMESTAMP()) > 10
             THEN 'FAIL' ELSE 'PASS' END,
        TIMESTAMPDIFF('MINUTE', MAX(INGESTED_AT), CURRENT_TIMESTAMP()),
        10,
        'Last ingestion: ' || COALESCE(TO_VARCHAR(MAX(INGESTED_AT), 'YYYY-MM-DD HH24:MI:SS'), 'never')
    FROM RAW.RAW_TRADES;

    -- Check 2: Freshness — RAW_AGGREGATES (threshold: 10 min)
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'FRESHNESS', 'RAW.RAW_AGGREGATES',
        CASE WHEN TIMESTAMPDIFF('MINUTE', MAX(INGESTED_AT), CURRENT_TIMESTAMP()) > 10
             THEN 'FAIL' ELSE 'PASS' END,
        TIMESTAMPDIFF('MINUTE', MAX(INGESTED_AT), CURRENT_TIMESTAMP()),
        10,
        'Last ingestion: ' || COALESCE(TO_VARCHAR(MAX(INGESTED_AT), 'YYYY-MM-DD HH24:MI:SS'), 'never')
    FROM RAW.RAW_AGGREGATES;

    -- Check 3: Freshness — RAW_NEWS (threshold: 30 min)
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'FRESHNESS', 'RAW.RAW_NEWS',
        CASE WHEN TIMESTAMPDIFF('MINUTE', MAX(INGESTED_AT), CURRENT_TIMESTAMP()) > 30
             THEN 'FAIL' ELSE 'PASS' END,
        TIMESTAMPDIFF('MINUTE', MAX(INGESTED_AT), CURRENT_TIMESTAMP()),
        30,
        'Last ingestion: ' || COALESCE(TO_VARCHAR(MAX(INGESTED_AT), 'YYYY-MM-DD HH24:MI:SS'), 'never')
    FROM RAW.RAW_NEWS;

    -- Check 4: Completeness — All 7 Mag7 tickers in DAILY_OHLCV
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'COMPLETENESS', 'ANALYTICS.DAILY_OHLCV',
        CASE WHEN COUNT(DISTINCT TICKER) < 7 THEN 'FAIL' ELSE 'PASS' END,
        COUNT(DISTINCT TICKER),
        7,
        'Tickers: ' || LISTAGG(DISTINCT TICKER, ', ') WITHIN GROUP (ORDER BY TICKER)
    FROM ANALYTICS.DAILY_OHLCV
    WHERE TICKER IN ('AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','META');

    -- Check 5: Validity — No negative prices
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'VALIDITY', 'ANALYTICS.DAILY_OHLCV',
        CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
        COUNT(*),
        0,
        CASE WHEN COUNT(*) > 0
             THEN COUNT(*) || ' rows with negative prices'
             ELSE 'All prices valid' END
    FROM ANALYTICS.DAILY_OHLCV
    WHERE CLOSE_PRICE < 0 OR OPEN_PRICE < 0 OR HIGH_PRICE < 0 OR LOW_PRICE < 0;

    -- Check 6: Consistency — OHLCV (Low <= High)
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'CONSISTENCY', 'ANALYTICS.DAILY_OHLCV',
        CASE WHEN COUNT(*) > 0 THEN 'WARN' ELSE 'PASS' END,
        COUNT(*),
        0,
        CASE WHEN COUNT(*) > 0
             THEN COUNT(*) || ' rows where Low > High'
             ELSE 'OHLCV values consistent' END
    FROM ANALYTICS.DAILY_OHLCV
    WHERE LOW_PRICE > HIGH_PRICE;

    -- Check 7: Duplicates — Same ticker+date should not repeat
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'DUPLICATES', 'ANALYTICS.DAILY_OHLCV',
        CASE WHEN COALESCE(SUM(CNT - 1), 0) > 0 THEN 'WARN' ELSE 'PASS' END,
        COALESCE(SUM(CNT - 1), 0),
        0,
        CASE WHEN COALESCE(SUM(CNT - 1), 0) > 0
             THEN COALESCE(SUM(CNT - 1), 0) || ' duplicate rows'
             ELSE 'No duplicates' END
    FROM (
        SELECT TICKER, TRADE_DATE, COUNT(*) AS CNT
        FROM ANALYTICS.DAILY_OHLCV
        GROUP BY TICKER, TRADE_DATE
        HAVING COUNT(*) > 1
    );

    -- Check 8: Volume — New trades via Stream (CDC)
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'VOLUME', 'RAW.RAW_TRADES',
        CASE WHEN COUNT(*) = 0 THEN 'WARN' ELSE 'PASS' END,
        COUNT(*),
        1,
        COUNT(*) || ' new rows since last check'
    FROM COMMON.STREAM_RAW_TRADES;

    -- Check 9: Volume — New aggregates via Stream (CDC)
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'VOLUME', 'RAW.RAW_AGGREGATES',
        CASE WHEN COUNT(*) = 0 THEN 'WARN' ELSE 'PASS' END,
        COUNT(*),
        1,
        COUNT(*) || ' new rows since last check'
    FROM COMMON.STREAM_RAW_AGGREGATES;

    -- Check 10: Volume — New news via Stream (CDC)
    INSERT INTO COMMON.DATA_QUALITY_LOG (CHECK_NAME, TABLE_NAME, STATUS, METRIC_VALUE, THRESHOLD, MESSAGE)
    SELECT
        'VOLUME', 'RAW.RAW_NEWS',
        CASE WHEN COUNT(*) = 0 THEN 'WARN' ELSE 'PASS' END,
        COUNT(*),
        1,
        COUNT(*) || ' new rows since last check'
    FROM COMMON.STREAM_RAW_NEWS;

    -- Cleanup: keep only last 7 days of quality logs
    DELETE FROM COMMON.DATA_QUALITY_LOG
    WHERE CHECKED_AT < DATEADD('DAY', -7, CURRENT_TIMESTAMP());

    RETURN 'Quality checks completed: 10 checks executed';
END;

-- ─────────────────────────────────────────────────────────────
-- 6. TASK — Scheduled execution (every hour)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TASK COMMON.TASK_DATA_QUALITY_CHECK
    WAREHOUSE = SNOWPULSE_WH
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly data quality checks — freshness, completeness, validity, consistency, volume'
AS
CALL COMMON.SP_DATA_QUALITY_CHECK();

ALTER TASK COMMON.TASK_DATA_QUALITY_CHECK RESUME;

-- ─────────────────────────────────────────────────────────────
-- 7. ALERT — Quality degradation notification
--    Fires when any quality check fails, logs to ALERT_LOG
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE ALERT COMMON.ALERT_DATA_QUALITY_FAIL
    WAREHOUSE = SNOWPULSE_WH
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Fires when data quality checks fail — writes to ALERT_LOG'
IF (EXISTS (
    SELECT 1 FROM COMMON.DATA_QUALITY_LOG
    WHERE STATUS = 'FAIL'
      AND CHECKED_AT > DATEADD('MINUTE', -65, CURRENT_TIMESTAMP())
))
THEN
    INSERT INTO COMMON.ALERT_LOG (ALERT_NAME, TICKER, MESSAGE, METRIC_VALUE)
    SELECT
        'DATA_QUALITY_FAIL',
        '--',
        CHECK_NAME || ' failed on ' || TABLE_NAME || ': ' || MESSAGE,
        METRIC_VALUE
    FROM COMMON.DATA_QUALITY_LOG
    WHERE STATUS = 'FAIL'
      AND CHECKED_AT > DATEADD('MINUTE', -65, CURRENT_TIMESTAMP());

ALTER ALERT COMMON.ALERT_DATA_QUALITY_FAIL RESUME;

-- ─────────────────────────────────────────────────────────────
-- 8. DYNAMIC TABLE — Quality summary (latest status per check)
--    Auto-refreshes when DATA_QUALITY_LOG gets new rows
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE COMMON.DATA_QUALITY_SUMMARY
    WAREHOUSE = SNOWPULSE_WH
    TARGET_LAG = '1 HOUR'
    COMMENT = 'Latest quality check result per check — dashboard-ready'
AS
SELECT
    CHECK_ID,
    CHECKED_AT,
    CHECK_NAME,
    TABLE_NAME,
    STATUS,
    METRIC_VALUE,
    THRESHOLD,
    MESSAGE
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CHECK_NAME, TABLE_NAME ORDER BY CHECKED_AT DESC) AS RN
    FROM COMMON.DATA_QUALITY_LOG
)
WHERE RN = 1;

-- ─────────────────────────────────────────────────────────────
-- Verify
-- ─────────────────────────────────────────────────────────────
SHOW TAGS IN SCHEMA COMMON;
SHOW STREAMS IN SCHEMA COMMON;
SHOW TASKS IN SCHEMA COMMON;
SHOW DATA METRIC FUNCTIONS IN SCHEMA COMMON;
SHOW DYNAMIC TABLES IN DATABASE SNOWPULSE_DB;
