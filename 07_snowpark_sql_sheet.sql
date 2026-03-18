/*=============================================================================
  HOL SNOWPARK DEMO — Step 7: Snowpark SQL Sheet
  Advanced SQL operations on structured + semi-structured data.
  Run this in a Snowsight SQL Worksheet.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

-- ============================================================
-- 1. FLATTEN: Explode VARIANT line items into rows
-- ============================================================
SELECT
    o.ORDER_ID,
    o.ORDER_DATE,
    o.STATUS,
    li.INDEX                                     AS LINE_NUMBER,
    li.VALUE:product_name::VARCHAR               AS PRODUCT,
    li.VALUE:quantity::INT                        AS QTY,
    li.VALUE:unit_price::NUMBER(10,2)            AS PRICE,
    li.VALUE:discount::NUMBER(10,2)              AS DISCOUNT,
    (li.VALUE:quantity * li.VALUE:unit_price) - li.VALUE:discount AS LINE_TOTAL
FROM RAW.ORDERS o,
LATERAL FLATTEN(INPUT => o.ORDER_DETAILS:line_items) li
ORDER BY o.ORDER_ID, li.INDEX;

-- ============================================================
-- 2. LATERAL FLATTEN on nested shipping address
-- ============================================================
SELECT
    ORDER_ID,
    ORDER_DETAILS:shipping.method::VARCHAR        AS SHIP_METHOD,
    ORDER_DETAILS:shipping.cost::NUMBER(10,2)     AS SHIP_COST,
    ORDER_DETAILS:shipping.address.street::VARCHAR AS STREET,
    ORDER_DETAILS:shipping.address.city::VARCHAR   AS CITY,
    ORDER_DETAILS:shipping.address.zip::VARCHAR    AS ZIP
FROM RAW.ORDERS
ORDER BY ORDER_ID;

-- ============================================================
-- 3. WINDOW FUNCTIONS: Running total & rank per customer
-- ============================================================
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    TOTAL_AMOUNT,
    SUM(TOTAL_AMOUNT) OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY ORDER_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                              AS RUNNING_TOTAL,
    ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY ORDER_DATE
    )                                              AS ORDER_SEQUENCE,
    LAG(TOTAL_AMOUNT) OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY ORDER_DATE
    )                                              AS PREV_ORDER_AMOUNT,
    TOTAL_AMOUNT - COALESCE(
        LAG(TOTAL_AMOUNT) OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY ORDER_DATE
        ), 0
    )                                              AS AMOUNT_CHANGE
FROM RAW.ORDERS
ORDER BY CUSTOMER_ID, ORDER_DATE;

-- ============================================================
-- 4. PIVOT: Revenue by payment method per month
-- ============================================================
SELECT *
FROM (
    SELECT
        DATE_TRUNC('month', ORDER_DATE)::DATE      AS ORDER_MONTH,
        ORDER_DETAILS:payment.method::VARCHAR       AS PAY_METHOD,
        TOTAL_AMOUNT
    FROM RAW.ORDERS
)
PIVOT (
    SUM(TOTAL_AMOUNT) FOR PAY_METHOD IN (
        'credit_card', 'bank_transfer', 'upi', 'alipay', 'gcash', 'promptpay'
    )
) AS pvt
ORDER BY ORDER_MONTH;

-- ============================================================
-- 5. UNPIVOT: Turn customer columns into rows for profiling
-- ============================================================
SELECT *
FROM (
    SELECT
        CUSTOMER_ID,
        FIRST_NAME   AS NAME_FIRST,
        LAST_NAME    AS NAME_LAST,
        CITY,
        COUNTRY
    FROM RAW.CUSTOMERS
)
UNPIVOT (
    FIELD_VALUE FOR FIELD_NAME IN (NAME_FIRST, NAME_LAST, CITY, COUNTRY)
)
ORDER BY CUSTOMER_ID, FIELD_NAME;

-- ============================================================
-- 6. Semi-structured: Event funnel analysis
-- ============================================================
WITH session_events AS (
    SELECT
        EVENT_DATA:session_id::VARCHAR              AS SESSION_ID,
        CUSTOMER_ID,
        EVENT_TYPE,
        EVENT_TIME
    FROM RAW.WEBSITE_EVENTS
),
funnel AS (
    SELECT
        SESSION_ID,
        CUSTOMER_ID,
        MAX(CASE WHEN EVENT_TYPE = 'PAGE_VIEW'    THEN 1 ELSE 0 END) AS VIEWED,
        MAX(CASE WHEN EVENT_TYPE = 'SEARCH'        THEN 1 ELSE 0 END) AS SEARCHED,
        MAX(CASE WHEN EVENT_TYPE = 'ADD_TO_CART'   THEN 1 ELSE 0 END) AS ADDED_TO_CART,
        MAX(CASE WHEN EVENT_TYPE = 'CHECKOUT'      THEN 1 ELSE 0 END) AS CHECKED_OUT,
        MAX(CASE WHEN EVENT_TYPE = 'CART_ABANDON'  THEN 1 ELSE 0 END) AS ABANDONED
    FROM session_events
    GROUP BY SESSION_ID, CUSTOMER_ID
)
SELECT
    COUNT(*)                                        AS TOTAL_SESSIONS,
    SUM(VIEWED)                                     AS SESSIONS_WITH_VIEWS,
    SUM(SEARCHED)                                   AS SESSIONS_WITH_SEARCH,
    SUM(ADDED_TO_CART)                              AS SESSIONS_WITH_CART,
    SUM(CHECKED_OUT)                                AS SESSIONS_WITH_CHECKOUT,
    SUM(ABANDONED)                                  AS SESSIONS_WITH_ABANDON,
    ROUND(SUM(CHECKED_OUT) / NULLIF(SUM(ADDED_TO_CART), 0) * 100, 1)
                                                    AS CHECKOUT_RATE_PCT
FROM funnel;

-- ============================================================
-- 7. OBJECT_CONSTRUCT: Build JSON from relational data
-- ============================================================
SELECT
    CUSTOMER_ID,
    OBJECT_CONSTRUCT(
        'name', FIRST_NAME || ' ' || LAST_NAME,
        'email', EMAIL,
        'location', OBJECT_CONSTRUCT(
            'city', CITY,
            'state', STATE,
            'country', COUNTRY
        ),
        'signup_date', SIGNUP_DATE
    ) AS CUSTOMER_JSON
FROM RAW.CUSTOMERS
LIMIT 5;

-- ============================================================
-- 8. Query Dynamic Tables & Materialized Views
-- ============================================================

-- Top customers by lifetime value (from dynamic table)
SELECT
    CUSTOMER_ID,
    FIRST_NAME || ' ' || LAST_NAME AS CUSTOMER,
    CUSTOMER_SEGMENT,
    LIFETIME_VALUE,
    TOTAL_ORDERS,
    TOTAL_EVENTS,
    CART_CONVERSION_RATE
FROM CONSUMPTION.DT_CUSTOMER_360
ORDER BY LIFETIME_VALUE DESC;

-- Product ranking (from dynamic table)
SELECT
    PRODUCT_NAME,
    CATEGORY,
    TOTAL_REVENUE,
    TOTAL_UNITS_SOLD,
    UNIQUE_BUYERS,
    RANK() OVER (ORDER BY TOTAL_REVENUE DESC) AS REVENUE_RANK
FROM CONSUMPTION.DT_PRODUCT_PERFORMANCE
ORDER BY REVENUE_RANK;

-- Daily sales with 7-day moving average (from dynamic table)
SELECT
    SALE_DATE,
    NUM_ORDERS,
    GROSS_REVENUE,
    AVG(GROSS_REVENUE) OVER (
        ORDER BY SALE_DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                              AS REVENUE_7DAY_AVG
FROM CONSUMPTION.DT_DAILY_SALES
ORDER BY SALE_DATE;

-- ============================================================
-- 9. Stream metadata queries
-- ============================================================
SELECT
    'CUSTOMERS_STREAM' AS STREAM_NAME,
    SYSTEM$STREAM_HAS_DATA('RAW.CUSTOMERS_STREAM') AS HAS_DATA
UNION ALL
SELECT
    'ORDERS_STREAM',
    SYSTEM$STREAM_HAS_DATA('RAW.ORDERS_STREAM')
UNION ALL
SELECT
    'EVENTS_STREAM',
    SYSTEM$STREAM_HAS_DATA('RAW.EVENTS_STREAM');

-- ============================================================
-- 10. ARRAY_AGG + OBJECT_AGG: Build nested aggregations
-- ============================================================
SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER,
    ARRAY_AGG(DISTINCT o.STATUS) WITHIN GROUP (ORDER BY o.STATUS)
                                                    AS ORDER_STATUSES,
    OBJECT_AGG(
        o.ORDER_ID::VARCHAR,
        OBJECT_CONSTRUCT(
            'date', o.ORDER_DATE,
            'amount', o.TOTAL_AMOUNT,
            'status', o.STATUS
        )
    )                                               AS ORDERS_JSON
FROM RAW.CUSTOMERS c
JOIN RAW.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME
ORDER BY c.CUSTOMER_ID;
