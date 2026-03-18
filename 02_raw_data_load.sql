/*=============================================================================
  HOL SNOWPARK DEMO — Step 2: Raw Tables & Sample Data (Structured + Semi-Structured)
  Creates tables in the RAW schema and inserts realistic e-commerce data.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA HOL_DB.RAW;

------------------------------------------------------
-- STRUCTURED TABLE: CUSTOMERS
------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID   INT AUTOINCREMENT START 1 INCREMENT 1,
    FIRST_NAME    VARCHAR(50),
    LAST_NAME     VARCHAR(50),
    EMAIL         VARCHAR(100),
    CITY          VARCHAR(50),
    STATE         VARCHAR(50),
    COUNTRY       VARCHAR(50),
    SIGNUP_DATE   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CUSTOMER_ID)
);

INSERT INTO CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE, COUNTRY, SIGNUP_DATE)
VALUES
    ('Alice',   'Johnson',  'alice.j@example.com',    'Sydney',       'NSW',          'Australia',   '2025-01-15 10:30:00'),
    ('Bob',     'Smith',    'bob.smith@example.com',   'Melbourne',    'VIC',          'Australia',   '2025-02-20 14:15:00'),
    ('Charlie', 'Lee',      'charlie.l@example.com',   'Singapore',    'Central',      'Singapore',   '2025-03-10 09:00:00'),
    ('Diana',   'Kumar',    'diana.k@example.com',     'Mumbai',       'Maharashtra',  'India',       '2025-03-22 11:45:00'),
    ('Ethan',   'Tanaka',   'ethan.t@example.com',     'Tokyo',        'Kanto',        'Japan',       '2025-04-05 16:20:00'),
    ('Fiona',   'Chen',     'fiona.c@example.com',     'Shanghai',     'Shanghai',     'China',       '2025-04-18 08:10:00'),
    ('George',  'Nguyen',   'george.n@example.com',    'Ho Chi Minh',  'HCMC',         'Vietnam',     '2025-05-02 13:30:00'),
    ('Hannah',  'Park',     'hannah.p@example.com',    'Seoul',        'Seoul',        'South Korea', '2025-05-15 10:00:00'),
    ('Ivan',    'Santos',   'ivan.s@example.com',      'Manila',       'NCR',          'Philippines', '2025-06-01 07:45:00'),
    ('Julia',   'Williams', 'julia.w@example.com',     'Auckland',     'Auckland',     'New Zealand', '2025-06-20 15:30:00'),
    ('Kevin',   'Brown',    'kevin.b@example.com',     'Perth',        'WA',           'Australia',   '2025-07-04 09:15:00'),
    ('Luna',    'Garcia',   'luna.g@example.com',      'Bangkok',      'Bangkok',      'Thailand',    '2025-07-19 12:00:00');

------------------------------------------------------
-- STRUCTURED TABLE: PRODUCTS
------------------------------------------------------
CREATE OR REPLACE TABLE PRODUCTS (
    PRODUCT_ID    INT AUTOINCREMENT START 1 INCREMENT 1,
    PRODUCT_NAME  VARCHAR(100),
    CATEGORY      VARCHAR(50),
    SUB_CATEGORY  VARCHAR(50),
    UNIT_PRICE    NUMBER(10,2),
    CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (PRODUCT_ID)
);

INSERT INTO PRODUCTS (PRODUCT_NAME, CATEGORY, SUB_CATEGORY, UNIT_PRICE, CREATED_AT)
VALUES
    ('Wireless Mouse',       'Electronics',  'Accessories',  29.99,   '2025-01-01 00:00:00'),
    ('Mechanical Keyboard',  'Electronics',  'Accessories',  89.99,   '2025-01-01 00:00:00'),
    ('USB-C Hub',            'Electronics',  'Accessories',  45.00,   '2025-01-01 00:00:00'),
    ('4K Monitor',           'Electronics',  'Displays',     399.99,  '2025-01-01 00:00:00'),
    ('Standing Desk',        'Furniture',    'Desks',        549.00,  '2025-02-01 00:00:00'),
    ('Ergonomic Chair',      'Furniture',    'Chairs',       349.00,  '2025-02-01 00:00:00'),
    ('Desk Lamp',            'Furniture',    'Lighting',     65.00,   '2025-02-01 00:00:00'),
    ('Laptop Stand',         'Accessories',  'Stands',       39.99,   '2025-03-01 00:00:00'),
    ('Webcam HD',            'Electronics',  'Cameras',      79.99,   '2025-03-01 00:00:00'),
    ('Noise-Cancel Headset', 'Electronics',  'Audio',        199.99,  '2025-03-01 00:00:00'),
    ('Whiteboard',           'Office',       'Boards',       120.00,  '2025-04-01 00:00:00'),
    ('Cable Management Kit', 'Accessories',  'Organization', 24.99,   '2025-04-01 00:00:00');

------------------------------------------------------
-- SEMI-STRUCTURED TABLE: ORDERS (with VARIANT column)
------------------------------------------------------
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID      INT AUTOINCREMENT START 1001 INCREMENT 1,
    CUSTOMER_ID   INT,
    ORDER_DATE    TIMESTAMP_NTZ,
    STATUS        VARCHAR(20),
    TOTAL_AMOUNT  NUMBER(12,2),
    ORDER_DETAILS VARIANT,  -- Semi-structured: line items, shipping, payment
    PRIMARY KEY (ORDER_ID)
);

INSERT INTO ORDERS (CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, ORDER_DETAILS)
-- NOTE: PARSE_JSON() cannot be used inside a VALUES clause in Snowflake.
-- Use SELECT ... UNION ALL SELECT ... pattern instead.
SELECT 1, '2025-06-01 10:00:00'::TIMESTAMP_NTZ, 'DELIVERED', 164.98, PARSE_JSON('{
    "line_items": [
        {"product_id": 1, "product_name": "Wireless Mouse", "quantity": 2, "unit_price": 29.99, "discount": 0.0},
        {"product_id": 2, "product_name": "Mechanical Keyboard", "quantity": 1, "unit_price": 89.99, "discount": 5.0}
    ],
    "shipping": {"method": "express", "cost": 15.00, "address": {"street": "42 Harbour St", "city": "Sydney", "zip": "2000"}},
    "payment": {"method": "credit_card", "card_type": "visa", "last_four": "4242"}
}')
UNION ALL
SELECT 2, '2025-06-05 14:30:00'::TIMESTAMP_NTZ, 'DELIVERED', 898.00, PARSE_JSON('{
    "line_items": [
        {"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "unit_price": 549.00, "discount": 0.0},
        {"product_id": 6, "product_name": "Ergonomic Chair", "quantity": 1, "unit_price": 349.00, "discount": 0.0}
    ],
    "shipping": {"method": "freight", "cost": 50.00, "address": {"street": "15 Lonsdale St", "city": "Melbourne", "zip": "3000"}},
    "payment": {"method": "bank_transfer", "reference": "BT-20250605-001"}
}')
UNION ALL
SELECT 3, '2025-06-10 09:15:00'::TIMESTAMP_NTZ, 'SHIPPED', 524.97, PARSE_JSON('{
    "line_items": [
        {"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "unit_price": 399.99, "discount": 10.0},
        {"product_id": 3, "product_name": "USB-C Hub", "quantity": 1, "unit_price": 45.00, "discount": 0.0},
        {"product_id": 9, "product_name": "Webcam HD", "quantity": 1, "unit_price": 79.99, "discount": 0.0}
    ],
    "shipping": {"method": "standard", "cost": 10.00, "address": {"street": "8 Raffles Pl", "city": "Singapore", "zip": "048619"}},
    "payment": {"method": "credit_card", "card_type": "mastercard", "last_four": "8888"}
}')
UNION ALL
SELECT 4, '2025-06-15 11:00:00'::TIMESTAMP_NTZ, 'DELIVERED', 224.98, PARSE_JSON('{
    "line_items": [
        {"product_id": 10, "product_name": "Noise-Cancel Headset", "quantity": 1, "unit_price": 199.99, "discount": 0.0},
        {"product_id": 12, "product_name": "Cable Management Kit", "quantity": 1, "unit_price": 24.99, "discount": 0.0}
    ],
    "shipping": {"method": "express", "cost": 20.00, "address": {"street": "22 MG Road", "city": "Mumbai", "zip": "400001"}},
    "payment": {"method": "upi", "upi_id": "diana@upi"}
}')
UNION ALL
SELECT 5, '2025-06-20 16:00:00'::TIMESTAMP_NTZ, 'PROCESSING', 554.98, PARSE_JSON('{
    "line_items": [
        {"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "unit_price": 549.00, "discount": 50.0},
        {"product_id": 7, "product_name": "Desk Lamp", "quantity": 1, "unit_price": 65.00, "discount": 10.0}
    ],
    "shipping": {"method": "standard", "cost": 0.00, "address": {"street": "3-1 Shibuya", "city": "Tokyo", "zip": "150-0002"}},
    "payment": {"method": "credit_card", "card_type": "amex", "last_four": "1234"}
}')
UNION ALL
SELECT 6, '2025-07-01 08:30:00'::TIMESTAMP_NTZ, 'DELIVERED', 114.98, PARSE_JSON('{
    "line_items": [
        {"product_id": 1, "product_name": "Wireless Mouse", "quantity": 1, "unit_price": 29.99, "discount": 0.0},
        {"product_id": 8, "product_name": "Laptop Stand", "quantity": 1, "unit_price": 39.99, "discount": 0.0},
        {"product_id": 3, "product_name": "USB-C Hub", "quantity": 1, "unit_price": 45.00, "discount": 0.0}
    ],
    "shipping": {"method": "standard", "cost": 5.00, "address": {"street": "100 Nanjing Rd", "city": "Shanghai", "zip": "200000"}},
    "payment": {"method": "alipay", "transaction_id": "ALI-20250701-006"}
}')
UNION ALL
SELECT 7, '2025-07-05 13:00:00'::TIMESTAMP_NTZ, 'SHIPPED', 184.99, PARSE_JSON('{
    "line_items": [
        {"product_id": 11, "product_name": "Whiteboard", "quantity": 1, "unit_price": 120.00, "discount": 0.0},
        {"product_id": 7, "product_name": "Desk Lamp", "quantity": 1, "unit_price": 65.00, "discount": 0.0}
    ],
    "shipping": {"method": "express", "cost": 12.00, "address": {"street": "5 Le Loi", "city": "Ho Chi Minh", "zip": "700000"}},
    "payment": {"method": "credit_card", "card_type": "visa", "last_four": "5678"}
}')
UNION ALL
SELECT 8, '2025-07-10 10:00:00'::TIMESTAMP_NTZ, 'DELIVERED', 959.97, PARSE_JSON('{
    "line_items": [
        {"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "unit_price": 399.99, "discount": 20.0},
        {"product_id": 6, "product_name": "Ergonomic Chair", "quantity": 1, "unit_price": 349.00, "discount": 0.0},
        {"product_id": 10, "product_name": "Noise-Cancel Headset", "quantity": 1, "unit_price": 199.99, "discount": 0.0}
    ],
    "shipping": {"method": "freight", "cost": 30.00, "address": {"street": "12 Gangnam-daero", "city": "Seoul", "zip": "06130"}},
    "payment": {"method": "credit_card", "card_type": "visa", "last_four": "9999"}
}')
UNION ALL
SELECT 1, '2025-07-15 11:30:00'::TIMESTAMP_NTZ, 'PROCESSING', 279.98, PARSE_JSON('{
    "line_items": [
        {"product_id": 10, "product_name": "Noise-Cancel Headset", "quantity": 1, "unit_price": 199.99, "discount": 0.0},
        {"product_id": 9, "product_name": "Webcam HD", "quantity": 1, "unit_price": 79.99, "discount": 0.0}
    ],
    "shipping": {"method": "express", "cost": 15.00, "address": {"street": "42 Harbour St", "city": "Sydney", "zip": "2000"}},
    "payment": {"method": "credit_card", "card_type": "visa", "last_four": "4242"}
}')
UNION ALL
SELECT 9, '2025-07-20 07:00:00'::TIMESTAMP_NTZ, 'DELIVERED', 329.94, PARSE_JSON('{
    "line_items": [
        {"product_id": 1, "product_name": "Wireless Mouse", "quantity": 5, "unit_price": 29.99, "discount": 5.0},
        {"product_id": 12, "product_name": "Cable Management Kit", "quantity": 5, "unit_price": 24.99, "discount": 5.0}
    ],
    "shipping": {"method": "freight", "cost": 25.00, "address": {"street": "88 EDSA", "city": "Manila", "zip": "1600"}},
    "payment": {"method": "gcash", "mobile": "+63-917-XXX-XXXX"}
}')
UNION ALL
SELECT 11, '2025-07-25 09:00:00'::TIMESTAMP_NTZ, 'SHIPPED', 469.98, PARSE_JSON('{
    "line_items": [
        {"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "unit_price": 399.99, "discount": 0.0},
        {"product_id": 8, "product_name": "Laptop Stand", "quantity": 1, "unit_price": 39.99, "discount": 0.0},
        {"product_id": 1, "product_name": "Wireless Mouse", "quantity": 1, "unit_price": 29.99, "discount": 0.0}
    ],
    "shipping": {"method": "standard", "cost": 8.00, "address": {"street": "7 St Georges Tce", "city": "Perth", "zip": "6000"}},
    "payment": {"method": "credit_card", "card_type": "mastercard", "last_four": "3333"}
}')
UNION ALL
SELECT 12, '2025-08-01 12:00:00'::TIMESTAMP_NTZ, 'PROCESSING', 1148.98, PARSE_JSON('{
    "line_items": [
        {"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "unit_price": 549.00, "discount": 0.0},
        {"product_id": 6, "product_name": "Ergonomic Chair", "quantity": 1, "unit_price": 349.00, "discount": 0.0},
        {"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "unit_price": 399.99, "discount": 0.0}
    ],
    "shipping": {"method": "freight", "cost": 45.00, "address": {"street": "99 Sukhumvit Rd", "city": "Bangkok", "zip": "10110"}},
    "payment": {"method": "promptpay", "reference": "PP-20250801-012"}
}')
UNION ALL
SELECT 2, '2025-08-05 15:00:00'::TIMESTAMP_NTZ, 'DELIVERED', 134.97, PARSE_JSON('{
    "line_items": [
        {"product_id": 8, "product_name": "Laptop Stand", "quantity": 1, "unit_price": 39.99, "discount": 0.0},
        {"product_id": 2, "product_name": "Mechanical Keyboard", "quantity": 1, "unit_price": 89.99, "discount": 5.0},
        {"product_id": 12, "product_name": "Cable Management Kit", "quantity": 1, "unit_price": 24.99, "discount": 15.0}
    ],
    "shipping": {"method": "standard", "cost": 0.00, "address": {"street": "15 Lonsdale St", "city": "Melbourne", "zip": "3000"}},
    "payment": {"method": "credit_card", "card_type": "visa", "last_four": "7777"}
}')
UNION ALL
SELECT 10, '2025-08-10 15:30:00'::TIMESTAMP_NTZ, 'SHIPPED', 614.99, PARSE_JSON('{
    "line_items": [
        {"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "unit_price": 549.00, "discount": 0.0},
        {"product_id": 7, "product_name": "Desk Lamp", "quantity": 1, "unit_price": 65.00, "discount": 0.0}
    ],
    "shipping": {"method": "standard", "cost": 12.00, "address": {"street": "20 Queen St", "city": "Auckland", "zip": "1010"}},
    "payment": {"method": "credit_card", "card_type": "visa", "last_four": "6666"}
}')
UNION ALL
SELECT 4, '2025-08-15 10:00:00'::TIMESTAMP_NTZ, 'DELIVERED', 494.98, PARSE_JSON('{
    "line_items": [
        {"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "unit_price": 399.99, "discount": 0.0},
        {"product_id": 3, "product_name": "USB-C Hub", "quantity": 1, "unit_price": 45.00, "discount": 0.0},
        {"product_id": 1, "product_name": "Wireless Mouse", "quantity": 1, "unit_price": 29.99, "discount": 0.0}
    ],
    "shipping": {"method": "express", "cost": 20.00, "address": {"street": "22 MG Road", "city": "Mumbai", "zip": "400001"}},
    "payment": {"method": "credit_card", "card_type": "rupay", "last_four": "1111"}
}');

------------------------------------------------------
-- SEMI-STRUCTURED TABLE: WEBSITE_EVENTS (clickstream)
------------------------------------------------------
CREATE OR REPLACE TABLE WEBSITE_EVENTS (
    EVENT_ID      INT AUTOINCREMENT START 1 INCREMENT 1,
    EVENT_TIME    TIMESTAMP_NTZ,
    CUSTOMER_ID   INT,
    EVENT_TYPE    VARCHAR(30),
    EVENT_DATA    VARIANT,  -- Semi-structured: page info, device, session context
    PRIMARY KEY (EVENT_ID)
);

INSERT INTO WEBSITE_EVENTS (EVENT_TIME, CUSTOMER_ID, EVENT_TYPE, EVENT_DATA)
-- NOTE: PARSE_JSON() cannot be used inside a VALUES clause in Snowflake.
-- Page views: Alice browsing then buying
SELECT '2025-06-01 09:50:00'::TIMESTAMP_NTZ, 1, 'PAGE_VIEW', PARSE_JSON('{"page": "/products/wireless-mouse", "referrer": "google.com", "device": {"type": "desktop", "browser": "Chrome", "os": "macOS"}, "session_id": "sess-001", "duration_sec": 45}')
UNION ALL SELECT '2025-06-01 09:52:00'::TIMESTAMP_NTZ, 1, 'PAGE_VIEW', PARSE_JSON('{"page": "/products/mechanical-keyboard", "referrer": "/products/wireless-mouse", "device": {"type": "desktop", "browser": "Chrome", "os": "macOS"}, "session_id": "sess-001", "duration_sec": 30}')
UNION ALL SELECT '2025-06-01 09:55:00'::TIMESTAMP_NTZ, 1, 'ADD_TO_CART', PARSE_JSON('{"product_id": 1, "product_name": "Wireless Mouse", "quantity": 2, "device": {"type": "desktop", "browser": "Chrome", "os": "macOS"}, "session_id": "sess-001"}')
UNION ALL SELECT '2025-06-01 09:56:00'::TIMESTAMP_NTZ, 1, 'ADD_TO_CART', PARSE_JSON('{"product_id": 2, "product_name": "Mechanical Keyboard", "quantity": 1, "device": {"type": "desktop", "browser": "Chrome", "os": "macOS"}, "session_id": "sess-001"}')
UNION ALL SELECT '2025-06-01 10:00:00'::TIMESTAMP_NTZ, 1, 'CHECKOUT', PARSE_JSON('{"cart_total": 164.98, "items_count": 3, "coupon_applied": null, "device": {"type": "desktop", "browser": "Chrome", "os": "macOS"}, "session_id": "sess-001"}')
-- Bob browsing then buying
UNION ALL SELECT '2025-06-05 14:00:00'::TIMESTAMP_NTZ, 2, 'PAGE_VIEW', PARSE_JSON('{"page": "/category/furniture", "referrer": "direct", "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-002", "duration_sec": 60}')
UNION ALL SELECT '2025-06-05 14:10:00'::TIMESTAMP_NTZ, 2, 'PAGE_VIEW', PARSE_JSON('{"page": "/products/standing-desk", "referrer": "/category/furniture", "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-002", "duration_sec": 120}')
UNION ALL SELECT '2025-06-05 14:15:00'::TIMESTAMP_NTZ, 2, 'ADD_TO_CART', PARSE_JSON('{"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-002"}')
UNION ALL SELECT '2025-06-05 14:20:00'::TIMESTAMP_NTZ, 2, 'ADD_TO_CART', PARSE_JSON('{"product_id": 6, "product_name": "Ergonomic Chair", "quantity": 1, "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-002"}')
UNION ALL SELECT '2025-06-05 14:30:00'::TIMESTAMP_NTZ, 2, 'CHECKOUT', PARSE_JSON('{"cart_total": 898.00, "items_count": 2, "coupon_applied": null, "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-002"}')
-- Charlie browsing with search
UNION ALL SELECT '2025-06-10 08:45:00'::TIMESTAMP_NTZ, 3, 'SEARCH', PARSE_JSON('{"query": "4k monitor", "results_count": 3, "device": {"type": "desktop", "browser": "Firefox", "os": "Windows"}, "session_id": "sess-003"}')
UNION ALL SELECT '2025-06-10 08:50:00'::TIMESTAMP_NTZ, 3, 'PAGE_VIEW', PARSE_JSON('{"page": "/products/4k-monitor", "referrer": "/search?q=4k+monitor", "device": {"type": "desktop", "browser": "Firefox", "os": "Windows"}, "session_id": "sess-003", "duration_sec": 90}')
UNION ALL SELECT '2025-06-10 09:00:00'::TIMESTAMP_NTZ, 3, 'ADD_TO_CART', PARSE_JSON('{"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "device": {"type": "desktop", "browser": "Firefox", "os": "Windows"}, "session_id": "sess-003"}')
UNION ALL SELECT '2025-06-10 09:10:00'::TIMESTAMP_NTZ, 3, 'ADD_TO_CART', PARSE_JSON('{"product_id": 3, "product_name": "USB-C Hub", "quantity": 1, "device": {"type": "desktop", "browser": "Firefox", "os": "Windows"}, "session_id": "sess-003"}')
UNION ALL SELECT '2025-06-10 09:15:00'::TIMESTAMP_NTZ, 3, 'CHECKOUT', PARSE_JSON('{"cart_total": 524.97, "items_count": 3, "coupon_applied": "WELCOME10", "device": {"type": "desktop", "browser": "Firefox", "os": "Windows"}, "session_id": "sess-003"}')
-- Hannah - extensive browsing, comparison shopping
UNION ALL SELECT '2025-07-10 08:00:00'::TIMESTAMP_NTZ, 8, 'PAGE_VIEW', PARSE_JSON('{"page": "/", "referrer": "google.com", "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-004", "duration_sec": 15}')
UNION ALL SELECT '2025-07-10 08:05:00'::TIMESTAMP_NTZ, 8, 'SEARCH', PARSE_JSON('{"query": "ergonomic office setup", "results_count": 8, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-004"}')
UNION ALL SELECT '2025-07-10 08:10:00'::TIMESTAMP_NTZ, 8, 'PAGE_VIEW', PARSE_JSON('{"page": "/products/ergonomic-chair", "referrer": "/search", "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-004", "duration_sec": 180}')
UNION ALL SELECT '2025-07-10 08:15:00'::TIMESTAMP_NTZ, 8, 'PAGE_VIEW', PARSE_JSON('{"page": "/products/4k-monitor", "referrer": "/products/ergonomic-chair", "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-004", "duration_sec": 120}')
UNION ALL SELECT '2025-07-10 09:00:00'::TIMESTAMP_NTZ, 8, 'ADD_TO_CART', PARSE_JSON('{"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-004"}')
UNION ALL SELECT '2025-07-10 09:30:00'::TIMESTAMP_NTZ, 8, 'ADD_TO_CART', PARSE_JSON('{"product_id": 6, "product_name": "Ergonomic Chair", "quantity": 1, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-004"}')
UNION ALL SELECT '2025-07-10 09:45:00'::TIMESTAMP_NTZ, 8, 'ADD_TO_CART', PARSE_JSON('{"product_id": 10, "product_name": "Noise-Cancel Headset", "quantity": 1, "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-004"}')
UNION ALL SELECT '2025-07-10 10:00:00'::TIMESTAMP_NTZ, 8, 'CHECKOUT', PARSE_JSON('{"cart_total": 959.97, "items_count": 3, "coupon_applied": "SUMMER20", "device": {"type": "tablet", "browser": "Chrome", "os": "Android"}, "session_id": "sess-004"}')
-- Luna - abandoned cart then completed
UNION ALL SELECT '2025-08-01 10:00:00'::TIMESTAMP_NTZ, 12, 'PAGE_VIEW', PARSE_JSON('{"page": "/category/furniture", "referrer": "instagram.com", "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-005", "duration_sec": 40}')
UNION ALL SELECT '2025-08-01 10:10:00'::TIMESTAMP_NTZ, 12, 'ADD_TO_CART', PARSE_JSON('{"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-005"}')
UNION ALL SELECT '2025-08-01 10:15:00'::TIMESTAMP_NTZ, 12, 'CART_ABANDON', PARSE_JSON('{"cart_total": 549.00, "items_count": 1, "time_in_cart_sec": 300, "device": {"type": "mobile", "browser": "Safari", "os": "iOS"}, "session_id": "sess-005"}')
UNION ALL SELECT '2025-08-01 11:30:00'::TIMESTAMP_NTZ, 12, 'PAGE_VIEW', PARSE_JSON('{"page": "/products/standing-desk", "referrer": "email_campaign", "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-006", "duration_sec": 60}')
UNION ALL SELECT '2025-08-01 11:45:00'::TIMESTAMP_NTZ, 12, 'ADD_TO_CART', PARSE_JSON('{"product_id": 5, "product_name": "Standing Desk", "quantity": 1, "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-006"}')
UNION ALL SELECT '2025-08-01 11:50:00'::TIMESTAMP_NTZ, 12, 'ADD_TO_CART', PARSE_JSON('{"product_id": 6, "product_name": "Ergonomic Chair", "quantity": 1, "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-006"}')
UNION ALL SELECT '2025-08-01 11:55:00'::TIMESTAMP_NTZ, 12, 'ADD_TO_CART', PARSE_JSON('{"product_id": 4, "product_name": "4K Monitor", "quantity": 1, "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-006"}')
UNION ALL SELECT '2025-08-01 12:00:00'::TIMESTAMP_NTZ, 12, 'CHECKOUT', PARSE_JSON('{"cart_total": 1148.98, "items_count": 3, "coupon_applied": null, "device": {"type": "desktop", "browser": "Chrome", "os": "Windows"}, "session_id": "sess-006"}');

-- Verify counts
SELECT 'CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CUSTOMERS
UNION ALL
SELECT 'PRODUCTS', COUNT(*) FROM PRODUCTS
UNION ALL
SELECT 'ORDERS', COUNT(*) FROM ORDERS
UNION ALL
SELECT 'WEBSITE_EVENTS', COUNT(*) FROM WEBSITE_EVENTS;
