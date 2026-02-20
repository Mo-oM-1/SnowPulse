-- ============================================================
-- SnowPulse — Macro Enrichment from Snowflake Marketplace
-- Source: Snowflake Public Data (Free) → Snowflake_FINANCE__ECONOMICS
-- ============================================================

USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- ─────────────────────────────────────────────────────────────
-- Grant access to the Marketplace database
-- (run as SYSADMIN or ACCOUNTADMIN if SNOWPULSE_ROLE lacks access)
-- ─────────────────────────────────────────────────────────────
-- GRANT IMPORTED PRIVILEGES ON DATABASE Snowflake_FINANCE__ECONOMICS TO ROLE SNOWPULSE_ROLE;

-- ─────────────────────────────────────────────────────────────
-- 1. MACRO_CPI — Monthly Consumer Price Index (Inflation)
--    Source: Bureau of Labor Statistics via FINANCIAL_ECONOMIC_INDICATORS
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MACRO_CPI
    WAREHOUSE = SNOWPULSE_WH
    TARGET_LAG = '1 day'
AS
SELECT
    DATE_TRUNC('MONTH', DATE)::DATE     AS REPORT_DATE,
    VALUE                               AS CPI_INDEX,
    LAG(VALUE) OVER (ORDER BY DATE)     AS PREV_CPI,
    ROUND(((VALUE - LAG(VALUE) OVER (ORDER BY DATE))
           / NULLIF(LAG(VALUE) OVER (ORDER BY DATE), 0)) * 100, 2)
                                        AS CPI_MOM_CHANGE_PCT,
    ROUND(((VALUE - LAG(VALUE, 12) OVER (ORDER BY DATE))
           / NULLIF(LAG(VALUE, 12) OVER (ORDER BY DATE), 0)) * 100, 2)
                                        AS CPI_YOY_CHANGE_PCT
FROM Snowflake_FINANCE__ECONOMICS.PUBLIC_DATA_FREE.FINANCIAL_ECONOMIC_INDICATORS_TIMESERIES
WHERE VARIABLE_NAME = 'CPI: All items, Monthly, 1982-84 Index Date (Not seasonally adjusted)'
  AND DATE >= '2020-01-01';

-- ─────────────────────────────────────────────────────────────
-- 2. MACRO_TREASURY_10Y — 10-Year Treasury Yield (Quarterly)
--    Source: Federal Reserve
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MACRO_TREASURY_10Y
    WAREHOUSE = SNOWPULSE_WH
    TARGET_LAG = '1 day'
AS
SELECT
    DATE_TRUNC('QUARTER', DATE)::DATE   AS REPORT_DATE,
    VALUE                               AS TREASURY_10Y_INDEX,
    LAG(VALUE) OVER (ORDER BY DATE)     AS PREV_VALUE,
    ROUND(VALUE - LAG(VALUE) OVER (ORDER BY DATE), 2)
                                        AS QOQ_CHANGE
FROM Snowflake_FINANCE__ECONOMICS.PUBLIC_DATA_FREE.FEDERAL_RESERVE_TIMESERIES
WHERE VARIABLE_NAME = 'Level, not seasonally adjusted (NSA): Interest rates and price indexes; 10-year Treasury yield - Input series with zero seasonal factor, Quarterly'
  AND DATE >= '2020-01-01';

-- ─────────────────────────────────────────────────────────────
-- 3. MARKETPLACE_STOCK_PRICES — Mag7 stock prices from Marketplace
--    Used to compare/validate our ingested data + extend history
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MARKETPLACE_STOCK_PRICES
    WAREHOUSE = SNOWPULSE_WH
    TARGET_LAG = '1 day'
AS
SELECT
    TICKER,
    DATE                                AS TRADE_DATE,
    VARIABLE_NAME,
    VALUE
FROM Snowflake_FINANCE__ECONOMICS.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
WHERE TICKER IN ('AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','META')
  AND VARIABLE_NAME IN ('Post-Market Close', 'Nasdaq Volume', 'All-Day High', 'All-Day Low')
  AND DATE >= '2020-01-01';

-- ─────────────────────────────────────────────────────────────
-- 4. MACRO_STOCK_MONTHLY — Monthly avg close per Mag7 ticker
--    For correlation with CPI (monthly) and Treasury (quarterly)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MACRO_STOCK_MONTHLY
    WAREHOUSE = SNOWPULSE_WH
    TARGET_LAG = '1 day'
AS
SELECT
    TICKER,
    DATE_TRUNC('MONTH', DATE)::DATE     AS MONTH,
    ROUND(AVG(VALUE), 2)                AS AVG_CLOSE,
    ROUND(MAX(VALUE), 2)                AS MONTH_HIGH,
    ROUND(MIN(VALUE), 2)                AS MONTH_LOW
FROM Snowflake_FINANCE__ECONOMICS.PUBLIC_DATA_FREE.STOCK_PRICE_TIMESERIES
WHERE TICKER IN ('AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','META')
  AND VARIABLE_NAME = 'Post-Market Close'
  AND DATE >= '2020-01-01'
GROUP BY TICKER, DATE_TRUNC('MONTH', DATE);

-- ─────────────────────────────────────────────────────────────
-- 5. GOLD.MACRO_OVERVIEW — Final enriched view for the dashboard
--    Joins monthly stock prices with CPI and Treasury 10Y
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE DYNAMIC TABLE GOLD.MACRO_OVERVIEW
    WAREHOUSE = SNOWPULSE_WH
    TARGET_LAG = '1 day'
AS
SELECT
    s.TICKER,
    s.MONTH,
    s.AVG_CLOSE,
    s.MONTH_HIGH,
    s.MONTH_LOW,
    c.CPI_INDEX,
    c.CPI_YOY_CHANGE_PCT                AS INFLATION_YOY_PCT,
    t.TREASURY_10Y_INDEX,
    t.QOQ_CHANGE                         AS TREASURY_QOQ_CHANGE
FROM ANALYTICS.MACRO_STOCK_MONTHLY s
LEFT JOIN ANALYTICS.MACRO_CPI c
    ON s.MONTH = c.REPORT_DATE
LEFT JOIN ANALYTICS.MACRO_TREASURY_10Y t
    ON DATE_TRUNC('QUARTER', s.MONTH)::DATE = t.REPORT_DATE
;
