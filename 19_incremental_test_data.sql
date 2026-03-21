/*=============================================================================
  FINSERV DEMO — Step 19: Incremental Test Data
  Two-batch incremental test to validate the end-to-end pipeline.
  Run after scripts 01-08 are deployed and stable.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- =============================================================================
-- SECTION A: Capture BEFORE Counts
-- =============================================================================

SELECT '>>> BEFORE COUNTS <<<' AS SECTION;

SELECT 'BASE.CUSTOMERS'        AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BASE.CUSTOMERS
UNION ALL SELECT 'BASE.ACCOUNTS',        COUNT(*) FROM BASE.ACCOUNTS
UNION ALL SELECT 'BASE.TRANSACTIONS',    COUNT(*) FROM BASE.TRANSACTIONS
UNION ALL SELECT 'BASE.SUPPORT_TICKETS', COUNT(*) FROM BASE.SUPPORT_TICKETS
UNION ALL SELECT 'RAW.TRANSACTION_ALERTS',  COUNT(*) FROM RAW.TRANSACTION_ALERTS
UNION ALL SELECT 'RAW.TICKET_ESCALATIONS',  COUNT(*) FROM RAW.TICKET_ESCALATIONS
UNION ALL SELECT 'CURATED.DT_CUSTOMER_PROFILE',     COUNT(*) FROM CURATED.DT_CUSTOMER_PROFILE
UNION ALL SELECT 'CURATED.DT_TRANSACTION_ENRICHED',  COUNT(*) FROM CURATED.DT_TRANSACTION_ENRICHED
UNION ALL SELECT 'CONSUMPTION.DT_CUSTOMER_360',      COUNT(*) FROM CONSUMPTION.DT_CUSTOMER_360
UNION ALL SELECT 'CONSUMPTION.DT_DAILY_FINANCIAL_METRICS', COUNT(*) FROM CONSUMPTION.DT_DAILY_FINANCIAL_METRICS
UNION ALL SELECT 'CONSUMPTION.DT_CHURN_FEATURES',    COUNT(*) FROM CONSUMPTION.DT_CHURN_FEATURES
ORDER BY TABLE_NAME;


-- =============================================================================
-- SECTION B: Insert New Customers (Batch 1)
-- =============================================================================

SELECT '>>> BATCH 1: NEW CUSTOMERS <<<' AS SECTION;

INSERT INTO BASE.CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, PHONE, DATE_OF_BIRTH, CITY, STATE, COUNTRY, ANNUAL_INCOME, EMPLOYMENT_STATUS, CREDIT_SCORE, SIGNUP_DATE)
VALUES
    ('Aisha', 'Rahman',   'aisha.r@example.com',   '+971-50-123-4567', '1988-06-15', 'Dubai',     'Dubai',     'UAE',         180000.00, 'EMPLOYED',      780, '2025-10-01 09:00:00'::TIMESTAMP_NTZ),
    ('Marco', 'Rossi',    'marco.r@example.com',   '+39-02-1234-5678', '1975-11-22', 'Milan',     'Lombardy',  'Italy',       120000.00, 'SELF_EMPLOYED',  710, '2025-10-05 14:30:00'::TIMESTAMP_NTZ),
    ('Yuna',  'Kim',      'yuna.k@example.com',    '+82-2-1234-5678',  '1992-03-08', 'Seoul',     'Seoul',     'South Korea', 95000.00,  'EMPLOYED',      750, '2025-10-10 10:15:00'::TIMESTAMP_NTZ);

SELECT 'Inserted 3 new customers' AS STATUS, COUNT(*) AS TOTAL FROM BASE.CUSTOMERS;


-- =============================================================================
-- SECTION C: Insert New Accounts
-- =============================================================================

SELECT '>>> BATCH 1: NEW ACCOUNTS <<<' AS SECTION;

-- Get the customer IDs for the new customers
-- (They will be the last 3 AUTOINCREMENT values)
INSERT INTO BASE.ACCOUNTS (CUSTOMER_ID, ACCOUNT_TYPE, BALANCE, CREDIT_LIMIT, INTEREST_RATE, OPENED_DATE, STATUS, BRANCH_CODE)
SELECT c.CUSTOMER_ID, 'CHECKING', 45000.00, 0, 0.0050, '2025-10-01', 'ACTIVE', 'DXB-001'
FROM BASE.CUSTOMERS c WHERE c.EMAIL = 'aisha.r@example.com'
UNION ALL
SELECT c.CUSTOMER_ID, 'CREDIT_CARD', 2500.00, 30000.00, 0.1899, '2025-10-01', 'ACTIVE', 'DXB-001'
FROM BASE.CUSTOMERS c WHERE c.EMAIL = 'aisha.r@example.com'
UNION ALL
SELECT c.CUSTOMER_ID, 'SAVINGS', 85000.00, 0, 0.0325, '2025-10-05', 'ACTIVE', 'MIL-001'
FROM BASE.CUSTOMERS c WHERE c.EMAIL = 'marco.r@example.com'
UNION ALL
SELECT c.CUSTOMER_ID, 'CHECKING', 12000.00, 0, 0.0015, '2025-10-10', 'ACTIVE', 'SEO-001'
FROM BASE.CUSTOMERS c WHERE c.EMAIL = 'yuna.k@example.com';

SELECT 'Inserted 4 new accounts' AS STATUS, COUNT(*) AS TOTAL FROM BASE.ACCOUNTS;


-- =============================================================================
-- SECTION D: Insert New Transactions
-- =============================================================================

SELECT '>>> BATCH 1: NEW TRANSACTIONS <<<' AS SECTION;

INSERT INTO BASE.TRANSACTIONS (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT, MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED)
SELECT a.ACCOUNT_ID, '2025-10-02 10:30:00'::TIMESTAMP_NTZ, 'DEBIT', 250.00, 'Emirates Mall', 'SHOPPING', 'POS', FALSE
FROM BASE.ACCOUNTS a JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.EMAIL = 'aisha.r@example.com' AND a.ACCOUNT_TYPE = 'CHECKING'
UNION ALL
SELECT a.ACCOUNT_ID, '2025-10-02 15:00:00'::TIMESTAMP_NTZ, 'DEBIT', 89.99, 'Netflix Premium', 'ENTERTAINMENT', 'ONLINE', FALSE
FROM BASE.ACCOUNTS a JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.EMAIL = 'aisha.r@example.com' AND a.ACCOUNT_TYPE = 'CREDIT_CARD'
UNION ALL
SELECT a.ACCOUNT_ID, '2025-10-06 09:00:00'::TIMESTAMP_NTZ, 'CREDIT', 15000.00, 'Freelance Payment', 'INVESTMENT', 'ONLINE', FALSE
FROM BASE.ACCOUNTS a JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.EMAIL = 'marco.r@example.com' AND a.ACCOUNT_TYPE = 'SAVINGS'
UNION ALL
SELECT a.ACCOUNT_ID, '2025-10-11 12:30:00'::TIMESTAMP_NTZ, 'DEBIT', 45.00, 'Starbucks Gangnam', 'DINING', 'POS', FALSE
FROM BASE.ACCOUNTS a JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.EMAIL = 'yuna.k@example.com' AND a.ACCOUNT_TYPE = 'CHECKING'
UNION ALL
SELECT a.ACCOUNT_ID, '2025-10-12 08:00:00'::TIMESTAMP_NTZ, 'DEBIT', 5200.00, 'Unknown Offshore', 'TRANSFER', 'ONLINE', TRUE
FROM BASE.ACCOUNTS a JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.EMAIL = 'yuna.k@example.com' AND a.ACCOUNT_TYPE = 'CHECKING';

SELECT 'Inserted 5 transactions (1 flagged)' AS STATUS, COUNT(*) AS TOTAL FROM BASE.TRANSACTIONS;


-- =============================================================================
-- SECTION E: Insert New Support Tickets
-- =============================================================================

SELECT '>>> BATCH 1: NEW TICKETS <<<' AS SECTION;

INSERT INTO BASE.SUPPORT_TICKETS (CUSTOMER_ID, CREATED_AT, SUBJECT, PRIORITY, BODY, RESOLUTION_STATUS, ASSIGNED_TO)
SELECT c.CUSTOMER_ID, '2025-10-12 09:00:00'::TIMESTAMP_NTZ,
    'Suspicious transaction on my account', 'HIGH',
    'I noticed a transaction of $5,200 to an unknown offshore account that I did not authorize. Please freeze my account immediately and investigate this transaction. I have not shared my credentials with anyone.',
    'OPEN', 'Fraud Team'
FROM BASE.CUSTOMERS c WHERE c.EMAIL = 'yuna.k@example.com'
UNION ALL
SELECT c.CUSTOMER_ID, '2025-10-03 11:00:00'::TIMESTAMP_NTZ,
    'Credit card limit increase request', 'MEDIUM',
    'I would like to request an increase to my credit card limit from $30,000 to $50,000. My income has increased recently and I have maintained a good payment history.',
    'IN_PROGRESS', 'Card Services'
FROM BASE.CUSTOMERS c WHERE c.EMAIL = 'aisha.r@example.com';

SELECT 'Inserted 2 support tickets' AS STATUS, COUNT(*) AS TOTAL FROM BASE.SUPPORT_TICKETS;


-- =============================================================================
-- SECTION F: Verify Streams
-- =============================================================================

SELECT '>>> STREAM STATUS (should be TRUE) <<<' AS SECTION;

SELECT 'TRANSACTIONS_STREAM'     AS STREAM, SYSTEM$STREAM_HAS_DATA('RAW.TRANSACTIONS_STREAM')     AS HAS_DATA
UNION ALL SELECT 'SUPPORT_TICKETS_STREAM',  SYSTEM$STREAM_HAS_DATA('RAW.SUPPORT_TICKETS_STREAM');


-- =============================================================================
-- SECTION G: Trigger Task DAG
-- =============================================================================

SELECT '>>> TRIGGERING TASK DAG <<<' AS SECTION;

EXECUTE TASK RAW.TASK_ROOT_SCHEDULER;

SELECT 'Task DAG triggered. DTs will refresh within 1-5 min.' AS STATUS;
SELECT 'Check Activity > Task History in Snowsight.' AS INFO;


-- =============================================================================
-- SECTION H: After Counts (run after 2-5 min)
-- =============================================================================

SELECT '>>> AFTER COUNTS (wait 2-5 min) <<<' AS SECTION;

SELECT 'BASE.CUSTOMERS'        AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BASE.CUSTOMERS
UNION ALL SELECT 'BASE.ACCOUNTS',        COUNT(*) FROM BASE.ACCOUNTS
UNION ALL SELECT 'BASE.TRANSACTIONS',    COUNT(*) FROM BASE.TRANSACTIONS
UNION ALL SELECT 'BASE.SUPPORT_TICKETS', COUNT(*) FROM BASE.SUPPORT_TICKETS
UNION ALL SELECT 'RAW.TRANSACTION_ALERTS',  COUNT(*) FROM RAW.TRANSACTION_ALERTS
UNION ALL SELECT 'RAW.TICKET_ESCALATIONS',  COUNT(*) FROM RAW.TICKET_ESCALATIONS
UNION ALL SELECT 'CURATED.DT_CUSTOMER_PROFILE',     COUNT(*) FROM CURATED.DT_CUSTOMER_PROFILE
UNION ALL SELECT 'CURATED.DT_TRANSACTION_ENRICHED',  COUNT(*) FROM CURATED.DT_TRANSACTION_ENRICHED
UNION ALL SELECT 'CONSUMPTION.DT_CUSTOMER_360',      COUNT(*) FROM CONSUMPTION.DT_CUSTOMER_360
UNION ALL SELECT 'CONSUMPTION.DT_DAILY_FINANCIAL_METRICS', COUNT(*) FROM CONSUMPTION.DT_DAILY_FINANCIAL_METRICS
UNION ALL SELECT 'CONSUMPTION.DT_CHURN_FEATURES',    COUNT(*) FROM CONSUMPTION.DT_CHURN_FEATURES
ORDER BY TABLE_NAME;


-- =============================================================================
-- SECTION I: Expected Changes
-- =============================================================================
-- TABLE                              BEFORE → AFTER  (CHANGE)
-- ──────────────────────────────────────────────────────────────
-- BASE.CUSTOMERS                       ~2000  → +3   (Aisha, Marco, Yuna)
-- BASE.ACCOUNTS                        ~3000  → +4   (2 for Aisha, 1 Marco, 1 Yuna)
-- BASE.TRANSACTIONS                   ~10000  → +5   (including 1 flagged)
-- BASE.SUPPORT_TICKETS                 ~1000  → +2   (fraud alert + limit request)
-- RAW.TRANSACTION_ALERTS                   0  → +1   (1 flagged txn detected by task)
-- RAW.TICKET_ESCALATIONS                   0  → +1   (1 HIGH-priority ticket escalated)
-- CURATED.DT_CUSTOMER_PROFILE          ~2000  → +3   (new customer profiles)
-- CURATED.DT_TRANSACTION_ENRICHED     ~10000  → +5   (enriched new transactions)
-- CONSUMPTION.DT_CUSTOMER_360          ~2000  → +3   (Aisha, Marco, Yuna)
-- CONSUMPTION.DT_DAILY_FINANCIAL_METRICS   → +new Oct dates
-- CONSUMPTION.DT_CHURN_FEATURES        ~2000  → +3   (new churn features)


-- =============================================================================
-- SECTION J: Spot-Check New Data
-- =============================================================================

SELECT '>>> SPOT-CHECK: New Customers <<<' AS SECTION;

SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, CITY, COUNTRY,
       CUSTOMER_SEGMENT, TOTAL_BALANCE, TOTAL_TRANSACTIONS, LATEST_RISK_SCORE
FROM CONSUMPTION.DT_CUSTOMER_360
WHERE FIRST_NAME IN ('Aisha', 'Marco', 'Yuna')
ORDER BY FIRST_NAME;

SELECT '>>> SPOT-CHECK: Flagged Transaction <<<' AS SECTION;

SELECT TXN_ID, CUSTOMER_NAME, AMOUNT, MERCHANT_NAME, IS_FLAGGED, CHANNEL
FROM CURATED.DT_TRANSACTION_ENRICHED
WHERE MERCHANT_NAME = 'Unknown Offshore';

SELECT '>>> SPOT-CHECK: October Daily Metrics <<<' AS SECTION;

SELECT METRIC_DATE, TOTAL_TRANSACTIONS, TOTAL_VOLUME, FLAGGED_COUNT
FROM CONSUMPTION.DT_DAILY_FINANCIAL_METRICS
WHERE METRIC_DATE >= '2025-10-01'
ORDER BY METRIC_DATE;

SELECT '>>> SPOT-CHECK: Transaction Alerts (from task DAG) <<<' AS SECTION;

SELECT ALERT_ID, TXN_ID, ACCOUNT_ID, AMOUNT, MERCHANT_NAME, ALERT_REASON, DETECTED_AT
FROM RAW.TRANSACTION_ALERTS
ORDER BY DETECTED_AT DESC
LIMIT 10;

SELECT '>>> SPOT-CHECK: Ticket Escalations (from task DAG) <<<' AS SECTION;

SELECT ESCALATION_ID, TICKET_ID, CUSTOMER_ID, SUBJECT, PRIORITY, ESCALATION_REASON, ESCALATED_AT
FROM RAW.TICKET_ESCALATIONS
ORDER BY ESCALATED_AT DESC
LIMIT 10;

SELECT '>>> BATCH 1 INCREMENTAL TEST COMPLETE <<<' AS SECTION;


-- =============================================================================
-- SECTION K: Batch 2 (repeat test)
-- =============================================================================

SELECT '>>> BATCH 2: MORE DATA <<<' AS SECTION;

-- 2 more customers
INSERT INTO BASE.CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, PHONE, DATE_OF_BIRTH, CITY, STATE, COUNTRY, ANNUAL_INCOME, EMPLOYMENT_STATUS, CREDIT_SCORE, SIGNUP_DATE)
VALUES
    ('Lucas', 'Santos', 'lucas.s@example.com', '+55-11-9876-5432', '1985-09-12', 'Sao Paulo', 'SP', 'Brazil', 150000.00, 'EMPLOYED', 720, '2025-10-15 08:00:00'::TIMESTAMP_NTZ),
    ('Emma',  'Wilson', 'emma.w@example.com',  '+1-416-555-0199',  '1990-01-25', 'Toronto',   'ON', 'Canada', 110000.00, 'EMPLOYED', 790, '2025-10-18 16:00:00'::TIMESTAMP_NTZ);

SELECT 'Batch 2: Inserted 2 customers' AS STATUS;

-- 3 more transactions
INSERT INTO BASE.TRANSACTIONS (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT, MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED)
SELECT a.ACCOUNT_ID, '2025-10-15 14:00:00'::TIMESTAMP_NTZ, 'DEBIT', 1200.00, 'Electronics Store', 'SHOPPING', 'POS', FALSE
FROM BASE.ACCOUNTS a JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.EMAIL = 'marco.r@example.com' AND a.ACCOUNT_TYPE = 'SAVINGS'
UNION ALL
SELECT a.ACCOUNT_ID, '2025-10-16 10:00:00'::TIMESTAMP_NTZ, 'DEBIT', 350.00, 'Luxury Brands Dubai', 'SHOPPING', 'POS', FALSE
FROM BASE.ACCOUNTS a JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.EMAIL = 'aisha.r@example.com' AND a.ACCOUNT_TYPE = 'CREDIT_CARD'
UNION ALL
SELECT a.ACCOUNT_ID, '2025-10-18 09:30:00'::TIMESTAMP_NTZ, 'DEBIT', 75.50, 'Local Grocery', 'GROCERIES', 'MOBILE', FALSE
FROM BASE.ACCOUNTS a JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.EMAIL = 'yuna.k@example.com' AND a.ACCOUNT_TYPE = 'CHECKING';

SELECT 'Batch 2: Inserted 3 transactions' AS STATUS;

-- Trigger DAG
EXECUTE TASK RAW.TASK_ROOT_SCHEDULER;
SELECT 'Batch 2 DAG triggered. Wait 2-5 min for refresh.' AS STATUS;


-- =============================================================================
-- SECTION L: Batch 2 Verification (run after 2-5 min)
-- =============================================================================

SELECT '>>> BATCH 2 VERIFICATION <<<' AS SECTION;

SELECT 'BASE.CUSTOMERS'        AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BASE.CUSTOMERS
UNION ALL SELECT 'BASE.TRANSACTIONS',    COUNT(*) FROM BASE.TRANSACTIONS
UNION ALL SELECT 'RAW.TRANSACTION_ALERTS',  COUNT(*) FROM RAW.TRANSACTION_ALERTS
UNION ALL SELECT 'RAW.TICKET_ESCALATIONS',  COUNT(*) FROM RAW.TICKET_ESCALATIONS
UNION ALL SELECT 'CONSUMPTION.DT_CUSTOMER_360', COUNT(*) FROM CONSUMPTION.DT_CUSTOMER_360
UNION ALL SELECT 'CONSUMPTION.DT_CHURN_FEATURES', COUNT(*) FROM CONSUMPTION.DT_CHURN_FEATURES
ORDER BY TABLE_NAME;

SELECT '>>> INCREMENTAL TEST COMPLETE <<<' AS SECTION;
