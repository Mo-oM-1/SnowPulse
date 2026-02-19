-- ============================================
-- SNOWPULSE - Alerts
-- Automated notifications on market conditions
-- ============================================

USE ROLE SNOWPULSE_ROLE;
USE DATABASE SNOWPULSE_DB;
USE WAREHOUSE SNOWPULSE_WH;

-- ============================================
-- Alert Log Table (stores triggered alerts)
-- ============================================
CREATE TABLE IF NOT EXISTS COMMON.ALERT_LOG (
    ALERT_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    TRIGGERED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ALERT_NAME      VARCHAR(100),
    TICKER          VARCHAR(10),
    MESSAGE         VARCHAR(2000),
    METRIC_VALUE    FLOAT
)
COMMENT = 'Log of all triggered alerts';

-- ============================================
-- 1. ALERT: Big Daily Move (> 3% up or down)
-- Checks every 5 minutes
-- ============================================
CREATE OR REPLACE ALERT COMMON.ALERT_BIG_DAILY_MOVE
    WAREHOUSE = SNOWPULSE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Fires when any ticker moves more than 3% in a day'
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
    SELECT
        'BIG_DAILY_MOVE',
        TICKER,
        TICKER || ' moved ' || DAILY_RETURN_PCT || '% on ' || TRADE_DATE,
        DAILY_RETURN_PCT
    FROM ANALYTICS.DAILY_RETURNS
    WHERE ABS(DAILY_RETURN_PCT) > 3
      AND TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.DAILY_RETURNS)
      AND TICKER NOT IN (
          SELECT TICKER FROM COMMON.ALERT_LOG
          WHERE ALERT_NAME = 'BIG_DAILY_MOVE'
            AND TRIGGERED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
      );

-- ============================================
-- 2. ALERT: Trend Change (signal flipped)
-- Checks every 5 minutes
-- ============================================
CREATE OR REPLACE ALERT COMMON.ALERT_TREND_CHANGE
    WAREHOUSE = SNOWPULSE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Fires when SMA5/SMA20 crossover changes trend signal'
IF (EXISTS (
    SELECT 1
    FROM ANALYTICS.MOVING_AVERAGES ma
    WHERE ma.TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.MOVING_AVERAGES)
      AND ma.TICKER NOT IN (
          SELECT TICKER FROM COMMON.ALERT_LOG
          WHERE ALERT_NAME = 'TREND_CHANGE'
            AND TRIGGERED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
      )
      AND ma.TREND_SIGNAL != (
          SELECT ma2.TREND_SIGNAL
          FROM ANALYTICS.MOVING_AVERAGES ma2
          WHERE ma2.TICKER = ma.TICKER
            AND ma2.TRADE_DATE = (
                SELECT MAX(TRADE_DATE) FROM ANALYTICS.MOVING_AVERAGES
                WHERE TRADE_DATE < (SELECT MAX(TRADE_DATE) FROM ANALYTICS.MOVING_AVERAGES)
                  AND TICKER = ma.TICKER
            )
      )
))
THEN
    INSERT INTO COMMON.ALERT_LOG (ALERT_NAME, TICKER, MESSAGE, METRIC_VALUE)
    SELECT
        'TREND_CHANGE',
        ma.TICKER,
        ma.TICKER || ' trend changed to ' || ma.TREND_SIGNAL || ' (SMA5=' || ma.SMA_5 || ', SMA20=' || ma.SMA_20 || ')',
        ma.SMA_5
    FROM ANALYTICS.MOVING_AVERAGES ma
    WHERE ma.TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.MOVING_AVERAGES)
      AND ma.TICKER NOT IN (
          SELECT TICKER FROM COMMON.ALERT_LOG
          WHERE ALERT_NAME = 'TREND_CHANGE'
            AND TRIGGERED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
      )
      AND ma.TREND_SIGNAL != (
          SELECT ma2.TREND_SIGNAL
          FROM ANALYTICS.MOVING_AVERAGES ma2
          WHERE ma2.TICKER = ma.TICKER
            AND ma2.TRADE_DATE = (
                SELECT MAX(TRADE_DATE) FROM ANALYTICS.MOVING_AVERAGES
                WHERE TRADE_DATE < (SELECT MAX(TRADE_DATE) FROM ANALYTICS.MOVING_AVERAGES)
                  AND TICKER = ma.TICKER
            )
      );

-- ============================================
-- 3. ALERT: High Volume (> 2x average volume)
-- Checks every 5 minutes
-- ============================================
CREATE OR REPLACE ALERT COMMON.ALERT_HIGH_VOLUME
    WAREHOUSE = SNOWPULSE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Fires when daily volume exceeds 2x the 20-day average'
IF (EXISTS (
    SELECT 1
    FROM ANALYTICS.DAILY_OHLCV latest
    WHERE latest.TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.DAILY_OHLCV)
      AND latest.VOLUME > 2 * (
          SELECT AVG(hist.VOLUME)
          FROM ANALYTICS.DAILY_OHLCV hist
          WHERE hist.TICKER = latest.TICKER
            AND hist.TRADE_DATE < latest.TRADE_DATE
      )
      AND latest.TICKER NOT IN (
          SELECT TICKER FROM COMMON.ALERT_LOG
          WHERE ALERT_NAME = 'HIGH_VOLUME'
            AND TRIGGERED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
      )
))
THEN
    INSERT INTO COMMON.ALERT_LOG (ALERT_NAME, TICKER, MESSAGE, METRIC_VALUE)
    SELECT
        'HIGH_VOLUME',
        latest.TICKER,
        latest.TICKER || ' volume ' || ROUND(latest.VOLUME/1000000, 1) || 'M vs avg ' ||
            ROUND((SELECT AVG(hist.VOLUME) FROM ANALYTICS.DAILY_OHLCV hist
                   WHERE hist.TICKER = latest.TICKER AND hist.TRADE_DATE < latest.TRADE_DATE) / 1000000, 1) || 'M',
        latest.VOLUME
    FROM ANALYTICS.DAILY_OHLCV latest
    WHERE latest.TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.DAILY_OHLCV)
      AND latest.VOLUME > 2 * (
          SELECT AVG(hist.VOLUME)
          FROM ANALYTICS.DAILY_OHLCV hist
          WHERE hist.TICKER = latest.TICKER
            AND hist.TRADE_DATE < latest.TRADE_DATE
      )
      AND latest.TICKER NOT IN (
          SELECT TICKER FROM COMMON.ALERT_LOG
          WHERE ALERT_NAME = 'HIGH_VOLUME'
            AND TRIGGERED_AT > DATEADD('hour', -24, CURRENT_TIMESTAMP())
      );

-- ============================================
-- Resume alerts (they start suspended)
-- ============================================
ALTER ALERT COMMON.ALERT_BIG_DAILY_MOVE RESUME;
ALTER ALERT COMMON.ALERT_TREND_CHANGE RESUME;
ALTER ALERT COMMON.ALERT_HIGH_VOLUME RESUME;

-- ============================================
-- Verify
-- ============================================
SHOW ALERTS IN SCHEMA COMMON;
