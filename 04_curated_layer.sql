/*=============================================================================
  HOL SNOWPARK DEMO — Step 4: Curated Layer (RAW → CURATED)
  Dynamic Tables, Materialized Views, and stream-consuming tables.

  NOTE: Snowflake Materialized Views only support single-table queries.
  Multi-table joins use Dynamic Tables instead.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

------------------------------------------------------
-- MATERIALIZED VIEW: Customer Directory (single table)
-- Fast lookup of customer details
------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW CURATED.MV_CUSTOMER_DIRECTORY AS
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    CITY,
    STATE,
    COUNTRY,
    SIGNUP_DATE
FROM RAW.CUSTOMERS;

------------------------------------------------------
-- DYNAMIC TABLE: Customer Summary (multi-table join)
-- Aggregates order metrics per customer
------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_CUSTOMER_SUMMARY
    TARGET_LAG = '1 minute'
    WAREHOUSE  = HOL_WH
AS
SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,
    c.CITY,
    c.COUNTRY,
    COUNT(o.ORDER_ID)                           AS TOTAL_ORDERS,
    COALESCE(SUM(o.TOTAL_AMOUNT), 0)            AS TOTAL_SPEND,
    COALESCE(AVG(o.TOTAL_AMOUNT), 0)            AS AVG_ORDER_VALUE,
    MIN(o.ORDER_DATE)                            AS FIRST_ORDER_DATE,
    MAX(o.ORDER_DATE)                            AS LAST_ORDER_DATE,
    DATEDIFF('day', MIN(o.ORDER_DATE), MAX(o.ORDER_DATE)) AS CUSTOMER_TENURE_DAYS
FROM RAW.CUSTOMERS c
LEFT JOIN RAW.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.EMAIL, c.CITY, c.COUNTRY;

------------------------------------------------------
-- DYNAMIC TABLE: Enriched Orders (flattened VARIANT)
-- Joins orders with customers and flattens line items
------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_ORDER_ENRICHED
    TARGET_LAG = '1 minute'
    WAREHOUSE  = HOL_WH
AS
SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME         AS CUSTOMER_NAME,
    c.CITY                                       AS CUSTOMER_CITY,
    c.COUNTRY                                    AS CUSTOMER_COUNTRY,
    o.ORDER_DATE,
    o.STATUS,
    o.TOTAL_AMOUNT,
    -- Flattened line items from VARIANT
    li.VALUE:product_id::INT                     AS LINE_PRODUCT_ID,
    li.VALUE:product_name::VARCHAR               AS LINE_PRODUCT_NAME,
    li.VALUE:quantity::INT                        AS LINE_QUANTITY,
    li.VALUE:unit_price::NUMBER(10,2)            AS LINE_UNIT_PRICE,
    li.VALUE:discount::NUMBER(10,2)              AS LINE_DISCOUNT,
    (li.VALUE:quantity::INT * li.VALUE:unit_price::NUMBER(10,2))
        - li.VALUE:discount::NUMBER(10,2)        AS LINE_TOTAL,
    -- Shipping info
    o.ORDER_DETAILS:shipping.method::VARCHAR     AS SHIPPING_METHOD,
    o.ORDER_DETAILS:shipping.cost::NUMBER(10,2)  AS SHIPPING_COST,
    o.ORDER_DETAILS:shipping.address.city::VARCHAR AS SHIPPING_CITY,
    -- Payment info
    o.ORDER_DETAILS:payment.method::VARCHAR      AS PAYMENT_METHOD
FROM RAW.ORDERS o
JOIN RAW.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID,
LATERAL FLATTEN(INPUT => o.ORDER_DETAILS:line_items) li;

------------------------------------------------------
-- DYNAMIC TABLE: Parsed Website Events
-- Extracts structured fields from event VARIANT data
------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_EVENT_PARSED
    TARGET_LAG = '1 minute'
    WAREHOUSE  = HOL_WH
AS
SELECT
    e.EVENT_ID,
    e.EVENT_TIME,
    e.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME         AS CUSTOMER_NAME,
    e.EVENT_TYPE,
    -- Parsed event fields
    e.EVENT_DATA:page::VARCHAR                   AS PAGE_URL,
    e.EVENT_DATA:referrer::VARCHAR               AS REFERRER,
    e.EVENT_DATA:session_id::VARCHAR             AS SESSION_ID,
    e.EVENT_DATA:duration_sec::INT               AS PAGE_DURATION_SEC,
    -- Device info
    e.EVENT_DATA:device.type::VARCHAR            AS DEVICE_TYPE,
    e.EVENT_DATA:device.browser::VARCHAR         AS BROWSER,
    e.EVENT_DATA:device.os::VARCHAR              AS OS,
    -- Cart/checkout specifics
    e.EVENT_DATA:product_id::INT                 AS PRODUCT_ID,
    e.EVENT_DATA:product_name::VARCHAR           AS PRODUCT_NAME,
    e.EVENT_DATA:quantity::INT                   AS QUANTITY,
    e.EVENT_DATA:cart_total::NUMBER(12,2)        AS CART_TOTAL,
    e.EVENT_DATA:items_count::INT                AS ITEMS_COUNT,
    e.EVENT_DATA:coupon_applied::VARCHAR         AS COUPON_APPLIED,
    -- Search specifics
    e.EVENT_DATA:query::VARCHAR                  AS SEARCH_QUERY,
    e.EVENT_DATA:results_count::INT              AS SEARCH_RESULTS_COUNT
FROM RAW.WEBSITE_EVENTS e
LEFT JOIN RAW.CUSTOMERS c ON e.CUSTOMER_ID = c.CUSTOMER_ID;

------------------------------------------------------
-- STREAM-FED TABLE: Orders processed via stream
-- This table will be populated by a task (see 06_tasks_and_dag.sql)
------------------------------------------------------
CREATE OR REPLACE TABLE CURATED.ORDERS_FROM_STREAM (
    ORDER_ID          INT,
    CUSTOMER_ID       INT,
    CUSTOMER_NAME     VARCHAR(100),
    ORDER_DATE        TIMESTAMP_NTZ,
    STATUS            VARCHAR(20),
    TOTAL_AMOUNT      NUMBER(12,2),
    ITEM_COUNT        INT,
    SHIPPING_METHOD   VARCHAR(30),
    PAYMENT_METHOD    VARCHAR(30),
    PROCESSED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STREAM_ACTION     VARCHAR(10)  -- METADATA$ACTION from stream
);

------------------------------------------------------
-- STREAM-FED TABLE: Events processed via stream
------------------------------------------------------
CREATE OR REPLACE TABLE CURATED.EVENTS_FROM_STREAM (
    EVENT_ID        INT,
    EVENT_TIME      TIMESTAMP_NTZ,
    CUSTOMER_ID     INT,
    CUSTOMER_NAME   VARCHAR(100),
    EVENT_TYPE      VARCHAR(30),
    PAGE_URL        VARCHAR,
    DEVICE_TYPE     VARCHAR(20),
    BROWSER         VARCHAR(30),
    SESSION_ID      VARCHAR(50),
    PROCESSED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

------------------------------------------------------
-- Verify curated objects
------------------------------------------------------
SHOW MATERIALIZED VIEWS IN SCHEMA CURATED;
SHOW DYNAMIC TABLES IN SCHEMA CURATED;

-- Sample the materialized view
SELECT * FROM CURATED.MV_CUSTOMER_DIRECTORY LIMIT 5;

-- Sample the customer summary dynamic table
SELECT * FROM CURATED.DT_CUSTOMER_SUMMARY ORDER BY TOTAL_SPEND DESC;

-- Sample the enriched orders dynamic table
SELECT * FROM CURATED.DT_ORDER_ENRICHED LIMIT 10;

-- Sample parsed events
SELECT * FROM CURATED.DT_EVENT_PARSED LIMIT 10;
