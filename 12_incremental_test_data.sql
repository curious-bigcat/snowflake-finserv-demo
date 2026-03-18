-- =============================================================================
-- 12_incremental_test_data.sql
-- HOL Snowpark Demo — Incremental Data Test Script
-- =============================================================================
-- Purpose: Insert new data into RAW tables to test the end-to-end pipeline.
--   1. Capture BEFORE counts
--   2. Insert new customers, products, orders, and website events
--   3. Check stream status (should show HAS_DATA = TRUE)
--   4. Trigger the task DAG manually
--   5. Wait for dynamic tables to refresh
--   6. Capture AFTER counts and compare
--
-- Run this in a Snowsight SQL Worksheet after deploying scripts 01-06.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

-- =============================================================================
-- SECTION A: Capture BEFORE Counts
-- =============================================================================

-- Save baseline row counts so we can compare after the pipeline runs
SELECT '>>> BEFORE COUNTS <<<' AS SECTION;

SELECT 'RAW.CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.CUSTOMERS
UNION ALL SELECT 'RAW.PRODUCTS', COUNT(*) FROM RAW.PRODUCTS
UNION ALL SELECT 'RAW.ORDERS', COUNT(*) FROM RAW.ORDERS
UNION ALL SELECT 'RAW.WEBSITE_EVENTS', COUNT(*) FROM RAW.WEBSITE_EVENTS
UNION ALL SELECT 'CURATED.DT_CUSTOMER_SUMMARY', COUNT(*) FROM CURATED.DT_CUSTOMER_SUMMARY
UNION ALL SELECT 'CURATED.DT_ORDER_ENRICHED', COUNT(*) FROM CURATED.DT_ORDER_ENRICHED
UNION ALL SELECT 'CURATED.DT_EVENT_PARSED', COUNT(*) FROM CURATED.DT_EVENT_PARSED
UNION ALL SELECT 'CONSUMPTION.DT_DAILY_SALES', COUNT(*) FROM CONSUMPTION.DT_DAILY_SALES
UNION ALL SELECT 'CONSUMPTION.DT_PRODUCT_PERFORMANCE', COUNT(*) FROM CONSUMPTION.DT_PRODUCT_PERFORMANCE
UNION ALL SELECT 'CONSUMPTION.DT_CUSTOMER_360', COUNT(*) FROM CONSUMPTION.DT_CUSTOMER_360
UNION ALL SELECT 'CONSUMPTION.DT_CATEGORY_TRENDS', COUNT(*) FROM CONSUMPTION.DT_CATEGORY_TRENDS
UNION ALL SELECT 'CONSUMPTION.PIPELINE_METRICS', COUNT(*) FROM CONSUMPTION.PIPELINE_METRICS
ORDER BY TABLE_NAME;


-- =============================================================================
-- SECTION B: Insert New Customers
-- =============================================================================
-- IDs are AUTOINCREMENT so we don't specify CUSTOMER_ID

SELECT '>>> INSERTING NEW CUSTOMERS <<<' AS SECTION;

INSERT INTO RAW.CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE, COUNTRY, SIGNUP_DATE)
VALUES
    ('Maya', 'Rodriguez', 'maya.r@example.com', 'Miami', 'Florida', 'USA', '2025-08-20 10:00:00'::TIMESTAMP_NTZ),
    ('Raj', 'Patel', 'raj.p@example.com', 'Bangalore', 'Karnataka', 'India', '2025-08-22 14:30:00'::TIMESTAMP_NTZ),
    ('Sophie', 'Müller', 'sophie.m@example.com', 'Berlin', 'Berlin', 'Germany', '2025-08-25 09:00:00'::TIMESTAMP_NTZ);

SELECT 'Inserted 3 new customers' AS STATUS, COUNT(*) AS TOTAL_CUSTOMERS FROM RAW.CUSTOMERS;


-- =============================================================================
-- SECTION C: Insert New Products
-- =============================================================================

SELECT '>>> INSERTING NEW PRODUCTS <<<' AS SECTION;

INSERT INTO RAW.PRODUCTS (PRODUCT_NAME, CATEGORY, SUB_CATEGORY, UNIT_PRICE, CREATED_AT)
VALUES
    ('Monitor Arm', 'Accessories', 'Mounts', 89.99, '2025-05-01 00:00:00'::TIMESTAMP_NTZ),
    ('Wireless Charger', 'Electronics', 'Accessories', 35.00, '2025-05-01 00:00:00'::TIMESTAMP_NTZ);

SELECT 'Inserted 2 new products' AS STATUS, COUNT(*) AS TOTAL_PRODUCTS FROM RAW.PRODUCTS;


-- =============================================================================
-- SECTION D: Insert New Orders (with VARIANT data)
-- =============================================================================
-- NOTE: PARSE_JSON() cannot be used inside a VALUES clause in Snowflake.
-- We use SELECT ... UNION ALL pattern instead.

SELECT '>>> INSERTING NEW ORDERS <<<' AS SECTION;

-- Order from new customer Maya (customer_id = 13) — 2 items
-- Order from new customer Raj (customer_id = 14) — 3 items including new product
-- Order from existing customer Alice (customer_id = 1) — repeat purchase
-- Order from new customer Sophie (customer_id = 15) — 2 items
-- Order from existing customer Bob (customer_id = 2) — high value order

INSERT INTO RAW.ORDERS (CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, ORDER_DETAILS)
SELECT 13, '2025-09-01 11:00:00'::TIMESTAMP_NTZ, 'PROCESSING', 479.98,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "unit_price": 399.99, "discount": 10.00},
            {"product_id": 13, "product_name": "Monitor Arm", "quantity": 1, "unit_price": 89.99, "discount": 0.00}
        ],
        "shipping": {"method": "express", "cost": 25.00, "address": {"street": "100 Ocean Drive", "city": "Miami", "zip": "33139"}},
        "payment": {"method": "credit_card", "card_type": "visa", "last_four": "2222"}
    }')
UNION ALL
SELECT 14, '2025-09-02 09:30:00'::TIMESTAMP_NTZ, 'SHIPPED', 354.98,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 1, "product_name": "Wireless Mouse", "quantity": 2, "unit_price": 29.99, "discount": 0.00},
            {"product_id": 10, "product_name": "Noise-Cancel Headset", "quantity": 1, "unit_price": 199.99, "discount": 0.00},
            {"product_id": 14, "product_name": "Wireless Charger", "quantity": 1, "unit_price": 35.00, "discount": 0.00}
        ],
        "shipping": {"method": "standard", "cost": 15.00, "address": {"street": "42 MG Road", "city": "Bangalore", "zip": "560001"}},
        "payment": {"method": "paypal"}
    }')
UNION ALL
SELECT 1, '2025-09-03 16:45:00'::TIMESTAMP_NTZ, 'DELIVERED', 614.00,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "unit_price": 549.00, "discount": 0.00},
            {"product_id": 7, "product_name": "Desk Lamp", "quantity": 1, "unit_price": 65.00, "discount": 0.00}
        ],
        "shipping": {"method": "standard", "cost": 30.00, "address": {"street": "42 Harbour St", "city": "Sydney", "zip": "2000"}},
        "payment": {"method": "credit_card", "card_type": "mastercard", "last_four": "4242"}
    }')
UNION ALL
SELECT 15, '2025-09-05 13:15:00'::TIMESTAMP_NTZ, 'PROCESSING', 234.98,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 2, "product_name": "Mechanical Keyboard", "quantity": 1, "unit_price": 89.99, "discount": 0.00},
            {"product_id": 9, "product_name": "Webcam HD", "quantity": 1, "unit_price": 79.99, "discount": 0.00},
            {"product_id": 7, "product_name": "Desk Lamp", "quantity": 1, "unit_price": 65.00, "discount": 0.00}
        ],
        "shipping": {"method": "express", "cost": 40.00, "address": {"street": "15 Berliner Str", "city": "Berlin", "zip": "10117"}},
        "payment": {"method": "credit_card", "card_type": "amex", "last_four": "5555"}
    }')
UNION ALL
SELECT 2, '2025-09-07 10:00:00'::TIMESTAMP_NTZ, 'SHIPPED', 1148.00,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "unit_price": 549.00, "discount": 50.00},
            {"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "unit_price": 399.99, "discount": 0.00},
            {"product_id": 13, "product_name": "Monitor Arm", "quantity": 1, "unit_price": 89.99, "discount": 0.00}
        ],
        "shipping": {"method": "express", "cost": 0.00, "address": {"street": "15 Lonsdale St", "city": "Melbourne", "zip": "3000"}},
        "payment": {"method": "credit_card", "card_type": "visa", "last_four": "7777"}
    }');

SELECT 'Inserted 5 new orders' AS STATUS, COUNT(*) AS TOTAL_ORDERS FROM RAW.ORDERS;


-- =============================================================================
-- SECTION E: Insert New Website Events (with VARIANT data)
-- =============================================================================

SELECT '>>> INSERTING NEW WEBSITE EVENTS <<<' AS SECTION;

INSERT INTO RAW.WEBSITE_EVENTS (EVENT_TIME, CUSTOMER_ID, EVENT_TYPE, EVENT_DATA)
-- Maya's browsing session
SELECT '2025-09-01 10:30:00'::TIMESTAMP_NTZ, 13, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/products/monitors", "referrer": "google.com", "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-201", "duration_sec": 35}')
UNION ALL
SELECT '2025-09-01 10:35:00'::TIMESTAMP_NTZ, 13, 'ADD_TO_CART',
    PARSE_JSON('{"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-201"}')
UNION ALL
SELECT '2025-09-01 10:36:00'::TIMESTAMP_NTZ, 13, 'ADD_TO_CART',
    PARSE_JSON('{"product_id": 13, "product_name": "Monitor Arm", "quantity": 1, "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-201"}')
UNION ALL
SELECT '2025-09-01 10:45:00'::TIMESTAMP_NTZ, 13, 'CHECKOUT',
    PARSE_JSON('{"cart_total": 479.98, "items_count": 2, "coupon_applied": null, "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-201"}')
UNION ALL
-- Raj's browsing session
SELECT '2025-09-02 08:00:00'::TIMESTAMP_NTZ, 14, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/categories/electronics", "referrer": "direct", "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-202", "duration_sec": 50}')
UNION ALL
SELECT '2025-09-02 08:10:00'::TIMESTAMP_NTZ, 14, 'SEARCH',
    PARSE_JSON('{"query": "wireless accessories", "results_count": 5, "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-202"}')
UNION ALL
SELECT '2025-09-02 08:20:00'::TIMESTAMP_NTZ, 14, 'ADD_TO_CART',
    PARSE_JSON('{"product_id": 1, "product_name": "Wireless Mouse", "quantity": 2, "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-202"}')
UNION ALL
SELECT '2025-09-02 08:30:00'::TIMESTAMP_NTZ, 14, 'CHECKOUT',
    PARSE_JSON('{"cart_total": 354.98, "items_count": 4, "coupon_applied": null, "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-202"}')
UNION ALL
-- Sophie's browsing session (with a cart abandon)
SELECT '2025-09-05 12:00:00'::TIMESTAMP_NTZ, 15, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/", "referrer": "email_campaign", "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-203", "duration_sec": 20}')
UNION ALL
SELECT '2025-09-05 12:10:00'::TIMESTAMP_NTZ, 15, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/products/keyboards", "referrer": "/", "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-203", "duration_sec": 45}')
UNION ALL
SELECT '2025-09-05 12:15:00'::TIMESTAMP_NTZ, 15, 'ADD_TO_CART',
    PARSE_JSON('{"product_id": 2, "product_name": "Mechanical Keyboard", "quantity": 1, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-203"}')
UNION ALL
SELECT '2025-09-05 12:20:00'::TIMESTAMP_NTZ, 15, 'ADD_TO_CART',
    PARSE_JSON('{"product_id": 9, "product_name": "Webcam HD", "quantity": 1, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-203"}')
UNION ALL
SELECT '2025-09-05 12:25:00'::TIMESTAMP_NTZ, 15, 'CART_ABANDON',
    PARSE_JSON('{"cart_total": 169.98, "items_count": 2, "time_in_cart_sec": 300, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-203"}')
UNION ALL
-- Sophie returns and completes purchase
SELECT '2025-09-05 13:00:00'::TIMESTAMP_NTZ, 15, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/cart", "referrer": "direct", "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-204", "duration_sec": 30}')
UNION ALL
SELECT '2025-09-05 13:10:00'::TIMESTAMP_NTZ, 15, 'ADD_TO_CART',
    PARSE_JSON('{"product_id": 7, "product_name": "Desk Lamp", "quantity": 1, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-204"}')
UNION ALL
SELECT '2025-09-05 13:15:00'::TIMESTAMP_NTZ, 15, 'CHECKOUT',
    PARSE_JSON('{"cart_total": 234.98, "items_count": 3, "coupon_applied": null, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-204"}');

SELECT 'Inserted 16 new website events' AS STATUS, COUNT(*) AS TOTAL_EVENTS FROM RAW.WEBSITE_EVENTS;


-- =============================================================================
-- SECTION F: Verify Streams Have Data
-- =============================================================================

SELECT '>>> STREAM STATUS (should all be TRUE) <<<' AS SECTION;

SELECT 'CUSTOMERS_STREAM' AS STREAM_NAME,
       SYSTEM$STREAM_HAS_DATA('RAW.CUSTOMERS_STREAM') AS HAS_DATA
UNION ALL
SELECT 'ORDERS_STREAM',
       SYSTEM$STREAM_HAS_DATA('RAW.ORDERS_STREAM')
UNION ALL
SELECT 'EVENTS_STREAM',
       SYSTEM$STREAM_HAS_DATA('RAW.EVENTS_STREAM');


-- =============================================================================
-- SECTION G: Trigger the Task DAG Manually
-- =============================================================================
-- The root task runs on a 5-minute CRON schedule. To see results immediately,
-- trigger it manually. Dynamic Tables will also auto-refresh (1-5 min lag).

SELECT '>>> TRIGGERING TASK DAG <<<' AS SECTION;

-- Execute the root task manually
EXECUTE TASK RAW.TASK_ROOT_SCHEDULER;

-- Wait a moment for tasks to pick up stream data
-- (In Snowsight, you can check Task History in the Activity tab)
SELECT 'Task DAG triggered. Dynamic Tables will refresh within 1-5 minutes.' AS STATUS;
SELECT 'Check Activity > Task History in Snowsight to monitor progress.' AS INFO;


-- =============================================================================
-- SECTION H: Verify AFTER Counts (run after ~2-5 minutes)
-- =============================================================================
-- Wait 2-5 minutes for Dynamic Tables to refresh, then run this section.

SELECT '>>> AFTER COUNTS (run after 2-5 min wait) <<<' AS SECTION;

SELECT 'RAW.CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.CUSTOMERS
UNION ALL SELECT 'RAW.PRODUCTS', COUNT(*) FROM RAW.PRODUCTS
UNION ALL SELECT 'RAW.ORDERS', COUNT(*) FROM RAW.ORDERS
UNION ALL SELECT 'RAW.WEBSITE_EVENTS', COUNT(*) FROM RAW.WEBSITE_EVENTS
UNION ALL SELECT 'CURATED.DT_CUSTOMER_SUMMARY', COUNT(*) FROM CURATED.DT_CUSTOMER_SUMMARY
UNION ALL SELECT 'CURATED.DT_ORDER_ENRICHED', COUNT(*) FROM CURATED.DT_ORDER_ENRICHED
UNION ALL SELECT 'CURATED.DT_EVENT_PARSED', COUNT(*) FROM CURATED.DT_EVENT_PARSED
UNION ALL SELECT 'CONSUMPTION.DT_DAILY_SALES', COUNT(*) FROM CONSUMPTION.DT_DAILY_SALES
UNION ALL SELECT 'CONSUMPTION.DT_PRODUCT_PERFORMANCE', COUNT(*) FROM CONSUMPTION.DT_PRODUCT_PERFORMANCE
UNION ALL SELECT 'CONSUMPTION.DT_CUSTOMER_360', COUNT(*) FROM CONSUMPTION.DT_CUSTOMER_360
UNION ALL SELECT 'CONSUMPTION.DT_CATEGORY_TRENDS', COUNT(*) FROM CONSUMPTION.DT_CATEGORY_TRENDS
UNION ALL SELECT 'CONSUMPTION.PIPELINE_METRICS', COUNT(*) FROM CONSUMPTION.PIPELINE_METRICS
ORDER BY TABLE_NAME;


-- =============================================================================
-- SECTION I: Expected Changes Summary
-- =============================================================================
-- After the pipeline runs, you should see:
--
-- TABLE                              BEFORE → AFTER  (CHANGE)
-- ─────────────────────────────────────────────────────────────
-- RAW.CUSTOMERS                        12  →  15     (+3 new customers)
-- RAW.PRODUCTS                         12  →  14     (+2 new products)
-- RAW.ORDERS                           15  →  20     (+5 new orders)
-- RAW.WEBSITE_EVENTS                   31  →  47     (+16 new events)
-- CURATED.DT_CUSTOMER_SUMMARY          12  →  15     (+3 new customer summaries)
-- CURATED.DT_ORDER_ENRICHED            37  →  ~50    (+line items from 5 orders)
-- CURATED.DT_EVENT_PARSED              31  →  47     (+16 parsed events)
-- CONSUMPTION.DT_DAILY_SALES           15  →  ~19    (+4 new September dates)
-- CONSUMPTION.DT_PRODUCT_PERFORMANCE   12  →  14     (+2 new products appear)
-- CONSUMPTION.DT_CUSTOMER_360          12  →  15     (+3 new customers)
-- CONSUMPTION.DT_CATEGORY_TRENDS       24  →  ~30    (+September category entries)
-- CONSUMPTION.PIPELINE_METRICS          1  →   1     (values updated by task)
--
-- The Streamlit dashboard (11_streamlit_dashboard.py) will show:
--   - Updated KPI cards (higher revenue, more orders/customers)
--   - New September dates in daily sales chart
--   - New products in product performance chart
--   - 3 new customers in customer insights
--   - September entries in category trends
--   - Refreshed pipeline health metrics


-- =============================================================================
-- SECTION J: Spot-Check New Data in Consumption
-- =============================================================================

SELECT '>>> SPOT-CHECK: New Customers in Customer 360 <<<' AS SECTION;

SELECT FIRST_NAME, LAST_NAME, CITY, COUNTRY, TOTAL_ORDERS, LIFETIME_VALUE, CUSTOMER_SEGMENT
FROM CONSUMPTION.DT_CUSTOMER_360
WHERE FIRST_NAME IN ('Maya', 'Raj', 'Sophie')
ORDER BY FIRST_NAME;

SELECT '>>> SPOT-CHECK: New Products in Product Performance <<<' AS SECTION;

SELECT PRODUCT_NAME, CATEGORY, TIMES_ORDERED, TOTAL_UNITS_SOLD, TOTAL_REVENUE
FROM CONSUMPTION.DT_PRODUCT_PERFORMANCE
WHERE PRODUCT_NAME IN ('Monitor Arm', 'Wireless Charger')
ORDER BY PRODUCT_NAME;

SELECT '>>> SPOT-CHECK: September Daily Sales <<<' AS SECTION;

SELECT SALE_DATE, NUM_ORDERS, UNIQUE_CUSTOMERS, GROSS_REVENUE
FROM CONSUMPTION.DT_DAILY_SALES
WHERE SALE_DATE >= '2025-09-01'
ORDER BY SALE_DATE;

SELECT '>>> SPOT-CHECK: Updated Pipeline Metrics <<<' AS SECTION;

SELECT * FROM CONSUMPTION.PIPELINE_METRICS;

SELECT '>>> FIRST BATCH INCREMENTAL TEST COMPLETE <<<' AS SECTION;


-- =============================================================================
-- SECTION K: Second Incremental Batch (for testing repeated pipeline runs)
-- =============================================================================
-- Purpose: Insert a second round of data to verify the pipeline handles
-- MULTIPLE incremental loads correctly. This proves the pipeline is truly
-- incremental and not just a one-time load.
--
-- Run this section AFTER Sections A-J have completed successfully.
-- =============================================================================

SELECT '>>> SECOND BATCH: INSERTING MORE DATA <<<' AS SECTION;

-- 2 more customers (from new regions to test geographic expansion)
INSERT INTO RAW.CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE, COUNTRY, SIGNUP_DATE)
VALUES
    ('Liam', 'O''Brien', 'liam.o@example.com', 'Dublin', 'Leinster', 'Ireland', '2025-09-10 11:00:00'::TIMESTAMP_NTZ),
    ('Yuki', 'Yamamoto', 'yuki.y@example.com', 'Osaka', 'Osaka', 'Japan', '2025-09-12 08:30:00'::TIMESTAMP_NTZ);

SELECT 'Inserted 2 more customers (Liam, Yuki)' AS STATUS, COUNT(*) AS TOTAL_CUSTOMERS FROM RAW.CUSTOMERS;

-- 3 more orders (including a repeat purchase from new customer Maya and existing customer Hannah)
INSERT INTO RAW.ORDERS (CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, ORDER_DETAILS)
SELECT 13, '2025-09-10 14:00:00'::TIMESTAMP_NTZ, 'SHIPPED', 154.98,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 2, "product_name": "Mechanical Keyboard", "quantity": 1, "unit_price": 89.99, "discount": 0.00},
            {"product_id": 7, "product_name": "Desk Lamp", "quantity": 1, "unit_price": 65.00, "discount": 0.00}
        ],
        "shipping": {"method": "standard", "cost": 10.00, "address": {"street": "100 Ocean Drive", "city": "Miami", "zip": "33139"}},
        "payment": {"method": "credit_card", "card_type": "visa", "last_four": "2222"}
    }')
UNION ALL
SELECT 8, '2025-09-12 09:00:00'::TIMESTAMP_NTZ, 'PROCESSING', 289.98,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 10, "product_name": "Noise-Cancel Headset", "quantity": 1, "unit_price": 199.99, "discount": 0.00},
            {"product_id": 2, "product_name": "Mechanical Keyboard", "quantity": 1, "unit_price": 89.99, "discount": 0.00}
        ],
        "shipping": {"method": "express", "cost": 20.00, "address": {"street": "22 Queen St", "city": "Auckland", "zip": "1010"}},
        "payment": {"method": "credit_card", "card_type": "mastercard", "last_four": "8888"}
    }')
UNION ALL
SELECT 16, '2025-09-15 16:30:00'::TIMESTAMP_NTZ, 'PROCESSING', 584.99,
    PARSE_JSON('{
        "line_items": [
            {"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "unit_price": 549.00, "discount": 25.00},
            {"product_id": 14, "product_name": "Wireless Charger", "quantity": 1, "unit_price": 35.00, "discount": 0.00}
        ],
        "shipping": {"method": "freight", "cost": 25.00, "address": {"street": "8 Grafton St", "city": "Dublin", "zip": "D02"}},
        "payment": {"method": "bank_transfer", "reference": "BT-20250915-001"}
    }');

SELECT 'Inserted 3 more orders' AS STATUS, COUNT(*) AS TOTAL_ORDERS FROM RAW.ORDERS;

-- 8 more website events (Liam's complete shopping journey)
INSERT INTO RAW.WEBSITE_EVENTS (EVENT_TIME, CUSTOMER_ID, EVENT_TYPE, EVENT_DATA)
SELECT '2025-09-15 15:30:00'::TIMESTAMP_NTZ, 16, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/", "referrer": "linkedin.com", "device": {"type": "desktop", "browser": "Firefox", "os": "macOS"}, "session_id": "sess-301", "duration_sec": 25}')
UNION ALL
SELECT '2025-09-15 15:40:00'::TIMESTAMP_NTZ, 16, 'SEARCH',
    PARSE_JSON('{"query": "standing desk", "results_count": 3, "device": {"type": "desktop", "browser": "Firefox", "os": "macOS"}, "session_id": "sess-301"}')
UNION ALL
SELECT '2025-09-15 15:45:00'::TIMESTAMP_NTZ, 16, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/products/standing-desk", "referrer": "/search", "device": {"type": "desktop", "browser": "Firefox", "os": "macOS"}, "session_id": "sess-301", "duration_sec": 60}')
UNION ALL
SELECT '2025-09-15 15:50:00'::TIMESTAMP_NTZ, 16, 'ADD_TO_CART',
    PARSE_JSON('{"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "device": {"type": "desktop", "browser": "Firefox", "os": "macOS"}, "session_id": "sess-301"}')
UNION ALL
SELECT '2025-09-15 15:55:00'::TIMESTAMP_NTZ, 16, 'ADD_TO_CART',
    PARSE_JSON('{"product_id": 14, "product_name": "Wireless Charger", "quantity": 1, "device": {"type": "desktop", "browser": "Firefox", "os": "macOS"}, "session_id": "sess-301"}')
UNION ALL
SELECT '2025-09-15 16:30:00'::TIMESTAMP_NTZ, 16, 'CHECKOUT',
    PARSE_JSON('{"cart_total": 584.99, "items_count": 2, "coupon_applied": null, "device": {"type": "desktop", "browser": "Firefox", "os": "macOS"}, "session_id": "sess-301"}')
UNION ALL
-- Yuki browsing but NOT purchasing (prospect behavior)
SELECT '2025-09-12 07:00:00'::TIMESTAMP_NTZ, 17, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/categories/electronics", "referrer": "google.co.jp", "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-302", "duration_sec": 40}')
UNION ALL
SELECT '2025-09-12 07:10:00'::TIMESTAMP_NTZ, 17, 'PAGE_VIEW',
    PARSE_JSON('{"page": "/products/noise-cancel-headset", "referrer": "/categories/electronics", "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-302", "duration_sec": 55}');

SELECT 'Inserted 8 more events' AS STATUS, COUNT(*) AS TOTAL_EVENTS FROM RAW.WEBSITE_EVENTS;


-- =============================================================================
-- SECTION L: Trigger DAG for Second Batch
-- =============================================================================

SELECT '>>> TRIGGERING DAG FOR SECOND BATCH <<<' AS SECTION;

EXECUTE TASK RAW.TASK_ROOT_SCHEDULER;

SELECT 'Second batch DAG triggered. Wait 2-5 minutes, then run Section M.' AS STATUS;


-- =============================================================================
-- SECTION M: Verify Second Batch (run after 2-5 min wait)
-- =============================================================================

SELECT '>>> SECOND BATCH VERIFICATION <<<' AS SECTION;

SELECT 'RAW.CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.CUSTOMERS
UNION ALL SELECT 'RAW.PRODUCTS', COUNT(*) FROM RAW.PRODUCTS
UNION ALL SELECT 'RAW.ORDERS', COUNT(*) FROM RAW.ORDERS
UNION ALL SELECT 'RAW.WEBSITE_EVENTS', COUNT(*) FROM RAW.WEBSITE_EVENTS
UNION ALL SELECT 'CONSUMPTION.DT_CUSTOMER_360', COUNT(*) FROM CONSUMPTION.DT_CUSTOMER_360
UNION ALL SELECT 'CONSUMPTION.DT_DAILY_SALES', COUNT(*) FROM CONSUMPTION.DT_DAILY_SALES
UNION ALL SELECT 'CONSUMPTION.DT_PRODUCT_PERFORMANCE', COUNT(*) FROM CONSUMPTION.DT_PRODUCT_PERFORMANCE
UNION ALL SELECT 'CONSUMPTION.DT_CATEGORY_TRENDS', COUNT(*) FROM CONSUMPTION.DT_CATEGORY_TRENDS
ORDER BY TABLE_NAME;


-- =============================================================================
-- SECTION N: Second Batch Expected Changes
-- =============================================================================
-- After the second batch pipeline runs, cumulative totals should be:
--
-- TABLE                              AFTER BATCH 1 → AFTER BATCH 2  (CHANGE)
-- ────────────────────────────────────────────────────────────────────────────
-- RAW.CUSTOMERS                        15  →  17     (+2: Liam, Yuki)
-- RAW.ORDERS                           20  →  23     (+3: Maya repeat, Hannah, Liam)
-- RAW.WEBSITE_EVENTS                   47  →  55     (+8: Liam session, Yuki browsing)
-- CONSUMPTION.DT_CUSTOMER_360          15  →  17     (+2: Liam, Yuki profiles)
-- CONSUMPTION.DT_DAILY_SALES           ~19 →  ~22    (+3 more September dates)
-- CONSUMPTION.DT_PRODUCT_PERFORMANCE   14  →  14     (no new products, but updated metrics)
--
-- Dashboard changes to observe:
--   - Overview: Revenue and order counts increase again
--   - Products: Standing Desk and Mechanical Keyboard revenue increases
--   - Customers: Liam appears as a new HIGH_VALUE customer (>$500 order)
--                Yuki appears as PROSPECT (browsed but no purchase)
--                Maya's segment may upgrade with 2nd order
--   - Category Trends: September totals grow further
--   - Pipeline Health: Row counts increase, new refresh history entries


-- =============================================================================
-- SECTION O: Spot-Check Second Batch Data
-- =============================================================================

SELECT '>>> SPOT-CHECK: New Customers from Second Batch <<<' AS SECTION;

SELECT FIRST_NAME, LAST_NAME, CITY, COUNTRY, TOTAL_ORDERS, LIFETIME_VALUE, CUSTOMER_SEGMENT
FROM CONSUMPTION.DT_CUSTOMER_360
WHERE FIRST_NAME IN ('Liam', 'Yuki')
ORDER BY FIRST_NAME;

SELECT '>>> SPOT-CHECK: Maya repeat purchase (should show 2 orders now) <<<' AS SECTION;

SELECT FIRST_NAME, TOTAL_ORDERS, LIFETIME_VALUE, CUSTOMER_SEGMENT
FROM CONSUMPTION.DT_CUSTOMER_360
WHERE FIRST_NAME = 'Maya';

SELECT '>>> SPOT-CHECK: September daily sales (should have more dates) <<<' AS SECTION;

SELECT SALE_DATE, NUM_ORDERS, UNIQUE_CUSTOMERS, GROSS_REVENUE
FROM CONSUMPTION.DT_DAILY_SALES
WHERE SALE_DATE >= '2025-09-01'
ORDER BY SALE_DATE;

SELECT '>>> SPOT-CHECK: Updated Pipeline Metrics <<<' AS SECTION;

SELECT * FROM CONSUMPTION.PIPELINE_METRICS;

SELECT '>>> ALL INCREMENTAL TESTS COMPLETE <<<' AS SECTION;
SELECT 'Open the Streamlit dashboard and click Refresh Data to see all changes.' AS NEXT_STEP;
