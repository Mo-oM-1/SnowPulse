-- ============================================
-- SNOWPULSE - Setup (Database, Schemas, Warehouse, Role)
-- ============================================

USE ROLE ACCOUNTADMIN;

-- ----------------------------------------
-- Warehouse
-- ----------------------------------------
CREATE WAREHOUSE IF NOT EXISTS SNOWPULSE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'SnowPulse warehouse - auto suspend after 60s';

-- ----------------------------------------
-- Database & Schemas
-- ----------------------------------------
CREATE DATABASE IF NOT EXISTS SNOWPULSE_DB
    COMMENT = 'SnowPulse - Real-Time Market Intelligence';

CREATE SCHEMA IF NOT EXISTS SNOWPULSE_DB.RAW
    COMMENT = 'Raw market data ingested via Snowpipe Streaming';

CREATE SCHEMA IF NOT EXISTS SNOWPULSE_DB.ANALYTICS
    COMMENT = 'Dynamic Tables - computed metrics and indicators';

CREATE SCHEMA IF NOT EXISTS SNOWPULSE_DB.GOLD
    COMMENT = 'Final analytics tables for Streamlit dashboard';

CREATE SCHEMA IF NOT EXISTS SNOWPULSE_DB.COMMON
    COMMENT = 'Shared objects: secrets, integrations, logs';

-- ----------------------------------------
-- Role
-- ----------------------------------------
CREATE ROLE IF NOT EXISTS SNOWPULSE_ROLE;

GRANT USAGE ON WAREHOUSE SNOWPULSE_WH TO ROLE SNOWPULSE_ROLE;
GRANT ALL ON DATABASE SNOWPULSE_DB TO ROLE SNOWPULSE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE SNOWPULSE_DB TO ROLE SNOWPULSE_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE SNOWPULSE_DB TO ROLE SNOWPULSE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE SNOWPULSE_DB TO ROLE SNOWPULSE_ROLE;
GRANT ALL ON FUTURE DYNAMIC TABLES IN DATABASE SNOWPULSE_DB TO ROLE SNOWPULSE_ROLE;

-- Grant role to user
GRANT ROLE SNOWPULSE_ROLE TO USER FISHER;

-- ----------------------------------------
-- Network Rule + External Access (Polygon.io API)
-- ----------------------------------------
USE SCHEMA SNOWPULSE_DB.COMMON;

CREATE OR REPLACE NETWORK RULE POLYGON_API_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.polygon.io')
    COMMENT = 'Allow outbound access to Polygon.io API';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION POLYGON_API_ACCESS
    ALLOWED_NETWORK_RULES = (POLYGON_API_RULE)
    ENABLED = TRUE
    COMMENT = 'External access integration for Polygon.io';

-- ----------------------------------------
-- Secret (API Key)
-- ----------------------------------------
-- /!\ Ne jamais mettre la vraie clé ici - ce fichier est versionné sur GitHub
-- Exécuter manuellement dans Snowflake avec ta vraie clé :
-- CREATE OR REPLACE SECRET POLYGON_API_KEY
--     TYPE = GENERIC_STRING
--     SECRET_STRING = '<TA_CLE_API_ICI>'
--     COMMENT = 'Polygon.io API key';

-- ----------------------------------------
-- Verify
-- ----------------------------------------
SHOW WAREHOUSES LIKE 'SNOWPULSE%';
SHOW DATABASES LIKE 'SNOWPULSE%';
SHOW SCHEMAS IN DATABASE SNOWPULSE_DB;
