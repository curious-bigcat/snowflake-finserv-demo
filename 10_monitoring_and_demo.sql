/*=============================================================================
  HOL SNOWPARK DEMO — Step 10: Monitoring, Validation & Demo Queries
  Run after all other scripts to verify the pipeline is working.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

-- ============================================================
-- 1. PIPELINE INVENTORY: All objects created
-- ============================================================

-- Tables
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'HOL_DB'
  AND TABLE_SCHEMA IN ('RAW', 'CURATED', 'CONSUMPTION')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Dynamic Tables
SHOW DYNAMIC TABLES IN DATABASE HOL_DB;

-- Materialized Views
SHOW MATERIALIZED VIEWS IN DATABASE HOL_DB;

-- Streams
SHOW STREAMS IN DATABASE HOL_DB;

-- Tasks
SHOW TASKS IN DATABASE HOL_DB;

-- Stored Procedures
SHOW PROCEDURES IN DATABASE HOL_DB;

-- User-Defined Functions
SHOW USER FUNCTIONS IN DATABASE HOL_DB;

-- ============================================================
-- 2. DATA FLOW VALIDATION: Row counts across layers
-- ============================================================
SELECT 'RAW'         AS LAYER, 'CUSTOMERS'              AS OBJECT, COUNT(*) AS ROWS FROM RAW.CUSTOMERS
UNION ALL
SELECT 'RAW',                  'PRODUCTS',                         COUNT(*)         FROM RAW.PRODUCTS
UNION ALL
SELECT 'RAW',                  'ORDERS',                           COUNT(*)         FROM RAW.ORDERS
UNION ALL
SELECT 'RAW',                  'WEBSITE_EVENTS',                   COUNT(*)         FROM RAW.WEBSITE_EVENTS
UNION ALL
SELECT 'CURATED',              'MV_CUSTOMER_DIRECTORY',            COUNT(*)         FROM CURATED.MV_CUSTOMER_DIRECTORY
UNION ALL
SELECT 'CURATED',              'DT_CUSTOMER_SUMMARY',              COUNT(*)         FROM CURATED.DT_CUSTOMER_SUMMARY
UNION ALL
SELECT 'CURATED',              'DT_ORDER_ENRICHED',                COUNT(*)         FROM CURATED.DT_ORDER_ENRICHED
UNION ALL
SELECT 'CURATED',              'DT_EVENT_PARSED',                  COUNT(*)         FROM CURATED.DT_EVENT_PARSED
UNION ALL
SELECT 'CONSUMPTION',          'DT_DAILY_SALES',                   COUNT(*)         FROM CONSUMPTION.DT_DAILY_SALES
UNION ALL
SELECT 'CONSUMPTION',          'MV_PRODUCT_CATALOG',               COUNT(*)         FROM CONSUMPTION.MV_PRODUCT_CATALOG
UNION ALL
SELECT 'CONSUMPTION',          'DT_PRODUCT_PERFORMANCE',           COUNT(*)         FROM CONSUMPTION.DT_PRODUCT_PERFORMANCE
UNION ALL
SELECT 'CONSUMPTION',          'DT_CUSTOMER_360',                  COUNT(*)         FROM CONSUMPTION.DT_CUSTOMER_360
UNION ALL
SELECT 'CONSUMPTION',          'DT_CATEGORY_TRENDS',               COUNT(*)         FROM CONSUMPTION.DT_CATEGORY_TRENDS
ORDER BY LAYER, OBJECT;

-- ============================================================
-- 3. STREAM STATUS
-- ============================================================
SELECT
    'CUSTOMERS_STREAM' AS STREAM, SYSTEM$STREAM_HAS_DATA('RAW.CUSTOMERS_STREAM') AS HAS_DATA
UNION ALL
SELECT 'ORDERS_STREAM',        SYSTEM$STREAM_HAS_DATA('RAW.ORDERS_STREAM')
UNION ALL
SELECT 'EVENTS_STREAM',        SYSTEM$STREAM_HAS_DATA('RAW.EVENTS_STREAM');

-- ============================================================
-- 4. DYNAMIC TABLE REFRESH STATUS
-- ============================================================
SELECT
    NAME,
    SCHEMA_NAME,
    TARGET_LAG,
    REFRESH_MODE,
    SCHEDULING_STATE
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME_PREFIX => 'HOL_DB'
))
QUALIFY ROW_NUMBER() OVER (PARTITION BY NAME ORDER BY DATA_TIMESTAMP DESC) = 1
ORDER BY SCHEMA_NAME, NAME;

-- Alternative: simpler view
SELECT
    NAME,
    SCHEMA_NAME,
    STATE,
    TARGET_LAG,
    REFRESH_MODE
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
WHERE TABLE_CATALOG = 'HOL_DB'
ORDER BY SCHEMA_NAME, NAME;

-- ============================================================
-- 5. TASK DAG STATUS & HISTORY
-- ============================================================

-- Task DAG dependencies
SELECT
    NAME,
    SCHEMA_NAME,
    STATE,
    SCHEDULE,
    PREDECESSORS,
    CONDITION,
    COMMENT
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'HOL_DB.RAW.TASK_ROOT_SCHEDULER',
    RECURSIVE => TRUE
));

-- Recent task execution history
SELECT
    NAME,
    SCHEMA_NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    TIMESTAMPDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS DURATION_SEC,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -2, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 30
))
ORDER BY SCHEDULED_TIME DESC;

-- ============================================================
-- 6. MATERIALIZED VIEW vs REGULAR VIEW PERFORMANCE
-- ============================================================

-- Create a regular view equivalent for comparison
CREATE OR REPLACE VIEW CONSUMPTION.V_PRODUCT_PERFORMANCE AS
SELECT
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.SUB_CATEGORY,
    p.UNIT_PRICE                                   AS LIST_PRICE,
    COUNT(DISTINCT oe.ORDER_ID)                    AS TIMES_ORDERED,
    SUM(oe.LINE_QUANTITY)                          AS TOTAL_UNITS_SOLD,
    SUM(oe.LINE_TOTAL)                             AS TOTAL_REVENUE,
    AVG(oe.LINE_TOTAL)                             AS AVG_LINE_VALUE,
    SUM(oe.LINE_DISCOUNT)                          AS TOTAL_DISCOUNT_GIVEN,
    COUNT(DISTINCT oe.CUSTOMER_ID)                 AS UNIQUE_BUYERS
FROM RAW.PRODUCTS p
LEFT JOIN CURATED.DT_ORDER_ENRICHED oe
    ON p.PRODUCT_ID = oe.LINE_PRODUCT_ID
GROUP BY p.PRODUCT_ID, p.PRODUCT_NAME, p.CATEGORY, p.SUB_CATEGORY, p.UNIT_PRICE;

-- Query the dynamic table (pre-computed — fast!)
SELECT * FROM CONSUMPTION.DT_PRODUCT_PERFORMANCE ORDER BY TOTAL_REVENUE DESC;

-- Query the regular view (computed on-the-fly)
SELECT * FROM CONSUMPTION.V_PRODUCT_PERFORMANCE ORDER BY TOTAL_REVENUE DESC;

-- Compare query profiles in Snowsight Query History
-- The MV query should show "MaterializedViewResult" scan type

-- ============================================================
-- 7. DEMO SCENARIOS: Simulate new data & observe pipeline
-- ============================================================

-- A) Insert a new customer
INSERT INTO RAW.CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE, COUNTRY)
VALUES ('Demo', 'User', 'demo.user@example.com', 'San Francisco', 'CA', 'USA');

-- B) Insert a new order with semi-structured details
INSERT INTO RAW.ORDERS (CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, ORDER_DETAILS)
SELECT
    CUSTOMER_ID,
    CURRENT_TIMESTAMP(),
    'PROCESSING',
    159.98,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 1, "product_name": "Wireless Mouse", "quantity": 2, "unit_price": 29.99, "discount": 0.0},
            {"product_id": 2, "product_name": "Mechanical Keyboard", "quantity": 1, "unit_price": 89.99, "discount": 0.0}
        ],
        "shipping": {"method": "express", "cost": 10.00, "address": {"street": "1 Market St", "city": "San Francisco", "zip": "94105"}},
        "payment": {"method": "credit_card", "card_type": "visa", "last_four": "0000"}
    }')
FROM RAW.CUSTOMERS
WHERE EMAIL = 'demo.user@example.com';

-- C) Insert new website events
INSERT INTO RAW.WEBSITE_EVENTS (EVENT_TIME, CUSTOMER_ID, EVENT_TYPE, EVENT_DATA)
SELECT
    CURRENT_TIMESTAMP(),
    CUSTOMER_ID,
    'PAGE_VIEW',
    PARSE_JSON('{"page": "/products/wireless-mouse", "referrer": "google.com", "device": {"type": "desktop", "browser": "Chrome", "os": "macOS"}, "session_id": "sess-demo-001", "duration_sec": 30}')
FROM RAW.CUSTOMERS
WHERE EMAIL = 'demo.user@example.com';

-- D) Check streams immediately
SELECT 'CUSTOMERS_STREAM' AS STREAM, SYSTEM$STREAM_HAS_DATA('RAW.CUSTOMERS_STREAM') AS HAS_DATA
UNION ALL
SELECT 'ORDERS_STREAM',        SYSTEM$STREAM_HAS_DATA('RAW.ORDERS_STREAM')
UNION ALL
SELECT 'EVENTS_STREAM',        SYSTEM$STREAM_HAS_DATA('RAW.EVENTS_STREAM');

-- E) Trigger the task DAG manually
EXECUTE TASK RAW.TASK_ROOT_SCHEDULER;

-- F) Wait ~30 seconds, then check updated row counts
-- (Dynamic tables will refresh automatically based on target lag)

-- ============================================================
-- 8. CLEANUP (run only when done with demo)
-- ============================================================
/*
-- Suspend tasks first (bottom-up, all in RAW schema)
ALTER TASK RAW.TASK_REFRESH_METRICS SUSPEND;
ALTER TASK RAW.TASK_PROCESS_EVENTS  SUSPEND;
ALTER TASK RAW.TASK_PROCESS_ORDERS  SUSPEND;
ALTER TASK RAW.TASK_ROOT_SCHEDULER  SUSPEND;

-- Drop everything
DROP DATABASE IF EXISTS HOL_DB;
DROP WAREHOUSE IF EXISTS HOL_WH;
*/
