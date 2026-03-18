/*=============================================================================
  HOL SNOWPARK DEMO — Step 5: Consumption Layer (CURATED → CONSUMPTION)
  Business-ready aggregates using Dynamic Tables and Materialized Views.

  NOTE: Snowflake Materialized Views only support single-table queries.
  Multi-table joins use Dynamic Tables instead.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

------------------------------------------------------
-- DYNAMIC TABLE: Daily Sales Dashboard
-- Aggregates sales by date with running totals
------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_DAILY_SALES
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = HOL_WH
AS
SELECT
    DATE_TRUNC('day', ORDER_DATE)::DATE          AS SALE_DATE,
    COUNT(DISTINCT ORDER_ID)                      AS NUM_ORDERS,
    COUNT(DISTINCT CUSTOMER_ID)                   AS UNIQUE_CUSTOMERS,
    SUM(LINE_QUANTITY)                             AS TOTAL_ITEMS_SOLD,
    SUM(LINE_TOTAL)                                AS GROSS_REVENUE,
    SUM(LINE_DISCOUNT)                             AS TOTAL_DISCOUNTS,
    SUM(SHIPPING_COST) / NULLIF(COUNT(DISTINCT ORDER_ID), 0) AS AVG_SHIPPING_COST,
    -- Breakdown by payment method
    COUNT_IF(PAYMENT_METHOD = 'credit_card')       AS CREDIT_CARD_ORDERS,
    COUNT_IF(PAYMENT_METHOD != 'credit_card')      AS ALTERNATIVE_PAYMENT_ORDERS
FROM CURATED.DT_ORDER_ENRICHED
GROUP BY DATE_TRUNC('day', ORDER_DATE)::DATE;

------------------------------------------------------
-- MATERIALIZED VIEW: Product Catalog (single table)
-- Fast lookup of product details
------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW CONSUMPTION.MV_PRODUCT_CATALOG AS
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    SUB_CATEGORY,
    UNIT_PRICE,
    CREATED_AT
FROM RAW.PRODUCTS;

------------------------------------------------------
-- DYNAMIC TABLE: Product Performance (multi-table join)
-- Revenue and volume metrics per product
------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_PRODUCT_PERFORMANCE
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = HOL_WH
AS
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

------------------------------------------------------
-- DYNAMIC TABLE: Customer 360 View
-- Full customer profile with order + event metrics
------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_CUSTOMER_360
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = HOL_WH
AS
WITH order_metrics AS (
    SELECT
        CUSTOMER_ID,
        COUNT(DISTINCT ORDER_ID)                   AS TOTAL_ORDERS,
        SUM(TOTAL_AMOUNT)                          AS LIFETIME_VALUE,
        AVG(TOTAL_AMOUNT)                          AS AVG_ORDER_VALUE,
        MIN(ORDER_DATE)                            AS FIRST_ORDER,
        MAX(ORDER_DATE)                            AS LAST_ORDER,
        LISTAGG(DISTINCT STATUS, ', ')
            WITHIN GROUP (ORDER BY STATUS)          AS ORDER_STATUSES
    FROM RAW.ORDERS
    GROUP BY CUSTOMER_ID
),
event_metrics AS (
    SELECT
        CUSTOMER_ID,
        COUNT(*)                                    AS TOTAL_EVENTS,
        COUNT_IF(EVENT_TYPE = 'PAGE_VIEW')          AS PAGE_VIEWS,
        COUNT_IF(EVENT_TYPE = 'ADD_TO_CART')         AS ADD_TO_CARTS,
        COUNT_IF(EVENT_TYPE = 'CHECKOUT')            AS CHECKOUTS,
        COUNT_IF(EVENT_TYPE = 'SEARCH')              AS SEARCHES,
        COUNT_IF(EVENT_TYPE = 'CART_ABANDON')        AS CART_ABANDONS,
        COUNT(DISTINCT EVENT_DATA:session_id::VARCHAR) AS UNIQUE_SESSIONS,
        MIN(EVENT_TIME)                              AS FIRST_EVENT,
        MAX(EVENT_TIME)                              AS LAST_EVENT
    FROM RAW.WEBSITE_EVENTS
    GROUP BY CUSTOMER_ID
)
SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,
    c.CITY,
    c.COUNTRY,
    c.SIGNUP_DATE,
    -- Order metrics
    COALESCE(om.TOTAL_ORDERS, 0)                    AS TOTAL_ORDERS,
    COALESCE(om.LIFETIME_VALUE, 0)                  AS LIFETIME_VALUE,
    COALESCE(om.AVG_ORDER_VALUE, 0)                 AS AVG_ORDER_VALUE,
    om.FIRST_ORDER,
    om.LAST_ORDER,
    om.ORDER_STATUSES,
    -- Event metrics
    COALESCE(em.TOTAL_EVENTS, 0)                    AS TOTAL_EVENTS,
    COALESCE(em.PAGE_VIEWS, 0)                      AS PAGE_VIEWS,
    COALESCE(em.ADD_TO_CARTS, 0)                    AS ADD_TO_CARTS,
    COALESCE(em.CHECKOUTS, 0)                       AS CHECKOUTS,
    COALESCE(em.SEARCHES, 0)                        AS SEARCHES,
    COALESCE(em.CART_ABANDONS, 0)                   AS CART_ABANDONS,
    COALESCE(em.UNIQUE_SESSIONS, 0)                 AS UNIQUE_SESSIONS,
    -- Derived: Engagement score
    CASE
        WHEN COALESCE(om.LIFETIME_VALUE, 0) >= 500 AND COALESCE(om.TOTAL_ORDERS, 0) >= 2
            THEN 'HIGH_VALUE'
        WHEN COALESCE(om.LIFETIME_VALUE, 0) >= 200
            THEN 'MEDIUM_VALUE'
        WHEN COALESCE(om.TOTAL_ORDERS, 0) >= 1
            THEN 'LOW_VALUE'
        ELSE 'PROSPECT'
    END                                              AS CUSTOMER_SEGMENT,
    -- Derived: Conversion rate
    CASE
        WHEN COALESCE(em.ADD_TO_CARTS, 0) > 0
            THEN ROUND(COALESCE(em.CHECKOUTS, 0) / em.ADD_TO_CARTS * 100, 1)
        ELSE 0
    END                                              AS CART_CONVERSION_RATE
FROM RAW.CUSTOMERS c
LEFT JOIN order_metrics om ON c.CUSTOMER_ID = om.CUSTOMER_ID
LEFT JOIN event_metrics em ON c.CUSTOMER_ID = em.CUSTOMER_ID;

------------------------------------------------------
-- DYNAMIC TABLE: Category Revenue Trends
-- Monthly revenue by product category
------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_CATEGORY_TRENDS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = HOL_WH
AS
SELECT
    DATE_TRUNC('month', oe.ORDER_DATE)::DATE       AS MONTH,
    p.CATEGORY,
    p.SUB_CATEGORY,
    COUNT(DISTINCT oe.ORDER_ID)                     AS NUM_ORDERS,
    SUM(oe.LINE_QUANTITY)                            AS UNITS_SOLD,
    SUM(oe.LINE_TOTAL)                               AS REVENUE,
    COUNT(DISTINCT oe.CUSTOMER_ID)                   AS UNIQUE_CUSTOMERS
FROM CURATED.DT_ORDER_ENRICHED oe
JOIN RAW.PRODUCTS p ON oe.LINE_PRODUCT_ID = p.PRODUCT_ID
GROUP BY DATE_TRUNC('month', oe.ORDER_DATE)::DATE, p.CATEGORY, p.SUB_CATEGORY;

------------------------------------------------------
-- Verify consumption objects
------------------------------------------------------
SHOW DYNAMIC TABLES IN SCHEMA CONSUMPTION;
SHOW MATERIALIZED VIEWS IN SCHEMA CONSUMPTION;

-- Sample daily sales
SELECT * FROM CONSUMPTION.DT_DAILY_SALES ORDER BY SALE_DATE;

-- Sample product performance
SELECT * FROM CONSUMPTION.DT_PRODUCT_PERFORMANCE ORDER BY TOTAL_REVENUE DESC;

-- Sample customer 360
SELECT * FROM CONSUMPTION.DT_CUSTOMER_360 ORDER BY LIFETIME_VALUE DESC;

-- Sample category trends
SELECT * FROM CONSUMPTION.DT_CATEGORY_TRENDS ORDER BY MONTH, CATEGORY;
