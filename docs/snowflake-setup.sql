-- Snowflake setup for sql2iceberg Iceberg tests (made-up data only).
-- Account: <your-account>  (https://<your-account>.snowflakecomputing.com)
-- Run in Snowsight, step by step, as a role that can create databases (e.g. SYSADMIN).
-- UNVERIFIED: written from Snowflake's docs, not yet run on a live account. Check each step's result.

-- 0. Who am I and what can I do? (free: no warehouse needed for these)
SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_ROLE(), CURRENT_VERSION();

-- 1. Small warehouse that suspends fast, so tests spend as few credits as possible.
CREATE WAREHOUSE IF NOT EXISTS SF2ICE_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE SF2ICE_WH;

-- 2. Sandbox database and schema.
CREATE DATABASE IF NOT EXISTS SF2ICE_DB;
CREATE SCHEMA IF NOT EXISTS SF2ICE_DB.TESTS;
USE SCHEMA SF2ICE_DB.TESTS;

-- 3. Cap spend: stop the warehouse after 2 credits per month (needs ACCOUNTADMIN).
-- USE ROLE ACCOUNTADMIN;
-- CREATE RESOURCE MONITOR SF2ICE_RM WITH CREDIT_QUOTA = 2
--   TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;
-- ALTER WAREHOUSE SF2ICE_WH SET RESOURCE_MONITOR = SF2ICE_RM;

-- 4. Snowflake-managed Iceberg table (storage is managed by Snowflake: no S3/Azure/GCS needed).
--    If this errors on EXTERNAL_VOLUME or ICEBERG_VERSION, your account/region/edition may not support it yet;
--    send me the error text.
CREATE OR REPLACE ICEBERG TABLE SF2ICE_DB.TESTS.ORDERS_ICE (
  ID  INT,
  AMT DOUBLE
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
  ICEBERG_VERSION = 3;

INSERT INTO SF2ICE_DB.TESTS.ORDERS_ICE VALUES (1, 10.0), (2, 20.0);
SELECT * FROM SF2ICE_DB.TESTS.ORDERS_ICE;
SHOW ICEBERG TABLES IN SCHEMA SF2ICE_DB.TESTS;

-- 5. Cleanup when done.
-- DROP DATABASE SF2ICE_DB;
-- DROP WAREHOUSE SF2ICE_WH;
