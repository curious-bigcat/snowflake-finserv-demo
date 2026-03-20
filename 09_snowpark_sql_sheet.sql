/*=============================================================================
  FINSERV DEMO — Step 9: Advanced SQL Patterns (Snowpark SQL Sheet)
  Demonstrates window functions, LATERAL FLATTEN, PIVOT, CTEs, and more
  against the financial services data model.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. RUNNING BALANCE PER ACCOUNT (Window Functions)
--    Compute cumulative balance over time for each account.
-- ============================================================

SELECT
    t.ACCOUNT_ID,
    t.TXN_DATE,
    t.TXN_TYPE,
    t.AMOUNT,
    SUM(
        CASE WHEN t.TXN_TYPE IN ('CREDIT', 'TRANSFER') THEN t.AMOUNT
             WHEN t.TXN_TYPE IN ('DEBIT', 'FEE', 'PAYMENT') THEN -t.AMOUNT
             ELSE 0
        END
    ) OVER (
        PARTITION BY t.ACCOUNT_ID
        ORDER BY t.TXN_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS RUNNING_BALANCE,
    ROW_NUMBER() OVER (PARTITION BY t.ACCOUNT_ID ORDER BY t.TXN_DATE) AS TXN_SEQUENCE
FROM BASE.TRANSACTIONS t
WHERE t.ACCOUNT_ID <= 5
ORDER BY t.ACCOUNT_ID, t.TXN_DATE
LIMIT 50;


-- ============================================================
-- 2. TRANSACTION VELOCITY (Anomaly Detection via Window)
--    Detect customers with transaction bursts (> 5 txns in 1 hour).
-- ============================================================

WITH txn_velocity AS (
    SELECT
        te.CUSTOMER_ID,
        te.CUSTOMER_NAME,
        te.TXN_ID,
        te.TXN_DATE,
        te.AMOUNT,
        te.MERCHANT_NAME,
        COUNT(*) OVER (
            PARTITION BY te.CUSTOMER_ID
            ORDER BY te.TXN_DATE
            RANGE BETWEEN INTERVAL '1 HOUR' PRECEDING AND CURRENT ROW
        ) AS TXNS_LAST_HOUR,
        SUM(te.AMOUNT) OVER (
            PARTITION BY te.CUSTOMER_ID
            ORDER BY te.TXN_DATE
            RANGE BETWEEN INTERVAL '1 HOUR' PRECEDING AND CURRENT ROW
        ) AS AMOUNT_LAST_HOUR
    FROM CURATED.DT_TRANSACTION_ENRICHED te
)
SELECT *
FROM txn_velocity
WHERE TXNS_LAST_HOUR >= 5
ORDER BY TXNS_LAST_HOUR DESC
LIMIT 20;


-- ============================================================
-- 3. LATERAL FLATTEN: Parse Risk Factors from VARIANT
--    Extract individual risk factors per customer.
-- ============================================================

SELECT
    r.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
    r.ASSESSMENT_DATE,
    r.RISK_DATA:risk_score::INT        AS RISK_SCORE,
    r.RISK_DATA:risk_level::VARCHAR    AS RISK_LEVEL,
    f.INDEX                            AS FACTOR_INDEX,
    f.VALUE::VARCHAR                   AS RISK_FACTOR,
    -- Credit history nested access
    r.RISK_DATA:credit_history.missed_payments::INT AS MISSED_PAYMENTS,
    r.RISK_DATA:credit_history.credit_age_months::INT AS CREDIT_AGE_MONTHS,
    r.RISK_DATA:debt_to_income::FLOAT  AS DEBT_TO_INCOME
FROM BASE.RISK_ASSESSMENTS r,
     LATERAL FLATTEN(INPUT => r.RISK_DATA:factors) f
JOIN BASE.CUSTOMERS c ON r.CUSTOMER_ID = c.CUSTOMER_ID
WHERE r.RISK_DATA:risk_level::VARCHAR IN ('HIGH', 'CRITICAL')
ORDER BY RISK_SCORE DESC
LIMIT 30;


-- ============================================================
-- 4. PIVOT: Transaction Volume by Channel per Month
-- ============================================================

SELECT *
FROM (
    SELECT
        DATE_TRUNC('MONTH', TXN_DATE)::DATE AS TXN_MONTH,
        CHANNEL,
        AMOUNT
    FROM BASE.TRANSACTIONS
    WHERE TXN_DATE >= '2025-01-01'
)
PIVOT (
    SUM(AMOUNT)
    FOR CHANNEL IN ('ONLINE', 'BRANCH', 'ATM', 'MOBILE', 'POS')
) AS p (TXN_MONTH, ONLINE, BRANCH, ATM, MOBILE, POS)
ORDER BY TXN_MONTH;


-- ============================================================
-- 5. UNPIVOT: Normalize Account Balances for Comparison
-- ============================================================

SELECT *
FROM (
    SELECT
        a.ACCOUNT_ID,
        a.ACCOUNT_TYPE,
        a.BALANCE                AS CURRENT_BALANCE,
        a.CREDIT_LIMIT           AS CREDIT_LIMIT_AMT
    FROM BASE.ACCOUNTS a
    WHERE a.ACCOUNT_TYPE = 'CREDIT_CARD'
    LIMIT 20
)
UNPIVOT (
    VALUE FOR METRIC IN (CURRENT_BALANCE, CREDIT_LIMIT_AMT)
)
ORDER BY ACCOUNT_ID, METRIC;


-- ============================================================
-- 6. MOVING AVERAGES on Market Data (7-day and 30-day)
-- ============================================================

SELECT
    TICKER,
    TRADE_DATE,
    MARKET_DATA:close::FLOAT AS CLOSE_PRICE,
    AVG(MARKET_DATA:close::FLOAT) OVER (
        PARTITION BY TICKER
        ORDER BY TRADE_DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS MA_7D,
    AVG(MARKET_DATA:close::FLOAT) OVER (
        PARTITION BY TICKER
        ORDER BY TRADE_DATE
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS MA_30D,
    MARKET_DATA:volume::INT AS VOLUME,
    MARKET_DATA:indicators.rsi::FLOAT AS RSI
FROM BASE.MARKET_DATA
WHERE TICKER = 'AAPL'
ORDER BY TRADE_DATE DESC
LIMIT 60;


-- ============================================================
-- 7. RECURSIVE CTE: Customer Account Hierarchy
--    Build a report showing customer → accounts → top transactions.
-- ============================================================

WITH customer_accounts AS (
    SELECT
        c.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
        c.CITY,
        c.COUNTRY,
        a.ACCOUNT_ID,
        a.ACCOUNT_TYPE,
        a.BALANCE,
        a.STATUS,
        RANK() OVER (PARTITION BY c.CUSTOMER_ID ORDER BY a.BALANCE DESC) AS ACCOUNT_RANK
    FROM BASE.CUSTOMERS c
    JOIN BASE.ACCOUNTS a ON c.CUSTOMER_ID = a.CUSTOMER_ID
    WHERE a.STATUS = 'ACTIVE'
),
top_transactions AS (
    SELECT
        ACCOUNT_ID,
        TXN_DATE,
        AMOUNT,
        CATEGORY,
        ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY AMOUNT DESC) AS TXN_RANK
    FROM BASE.TRANSACTIONS
)
SELECT
    ca.CUSTOMER_NAME,
    ca.CITY,
    ca.ACCOUNT_TYPE,
    ca.BALANCE,
    tt.TXN_DATE     AS TOP_TXN_DATE,
    tt.AMOUNT        AS TOP_TXN_AMOUNT,
    tt.CATEGORY      AS TOP_TXN_CATEGORY
FROM customer_accounts ca
JOIN top_transactions tt ON ca.ACCOUNT_ID = tt.ACCOUNT_ID
WHERE ca.ACCOUNT_RANK <= 2
  AND tt.TXN_RANK <= 3
ORDER BY ca.CUSTOMER_NAME, ca.ACCOUNT_RANK, tt.TXN_RANK
LIMIT 50;


-- ============================================================
-- 8. SUPPORT TICKET RESOLUTION FUNNEL
--    How many tickets move through each status.
-- ============================================================

SELECT
    RESOLUTION_STATUS,
    PRIORITY,
    COUNT(*)                                                   AS TICKET_COUNT,
    AVG(DATEDIFF('hour', CREATED_AT, CURRENT_TIMESTAMP()))    AS AVG_AGE_HOURS,
    MIN(CREATED_AT)                                            AS OLDEST_TICKET,
    MAX(CREATED_AT)                                            AS NEWEST_TICKET
FROM BASE.SUPPORT_TICKETS
GROUP BY ROLLUP(RESOLUTION_STATUS, PRIORITY)
ORDER BY RESOLUTION_STATUS NULLS LAST, PRIORITY NULLS LAST;


-- ============================================================
-- 9. PERCENTILE ANALYSIS: Transaction Amounts by Category
-- ============================================================

SELECT
    CATEGORY,
    COUNT(*)                                        AS TXN_COUNT,
    ROUND(AVG(AMOUNT), 2)                           AS AVG_AMOUNT,
    ROUND(MEDIAN(AMOUNT), 2)                        AS MEDIAN_AMOUNT,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY AMOUNT), 2) AS P25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY AMOUNT), 2) AS P75,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY AMOUNT), 2) AS P95,
    ROUND(MAX(AMOUNT), 2)                           AS MAX_AMOUNT
FROM BASE.TRANSACTIONS
GROUP BY CATEGORY
ORDER BY AVG_AMOUNT DESC;


-- ============================================================
-- 10. FRAUD PATTERN DETECTION
--     Find customers with suspicious transaction patterns.
-- ============================================================

WITH fraud_indicators AS (
    SELECT
        te.CUSTOMER_ID,
        te.CUSTOMER_NAME,
        COUNT(*)                                          AS TOTAL_TXNS,
        SUM(CASE WHEN IS_FLAGGED THEN 1 ELSE 0 END)    AS FLAGGED_TXNS,
        ROUND(SUM(CASE WHEN IS_FLAGGED THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2)                    AS FLAG_RATE,
        COUNT(DISTINCT CHANNEL)                          AS CHANNELS_USED,
        COUNT(DISTINCT CATEGORY)                         AS CATEGORIES_USED,
        COUNT(DISTINCT MERCHANT_NAME)                    AS MERCHANTS_USED,
        STDDEV(AMOUNT)                                   AS AMOUNT_STDDEV,
        MAX(AMOUNT) - MIN(AMOUNT)                        AS AMOUNT_RANGE
    FROM CURATED.DT_TRANSACTION_ENRICHED te
    GROUP BY te.CUSTOMER_ID, te.CUSTOMER_NAME
)
SELECT *
FROM fraud_indicators
WHERE FLAGGED_TXNS > 0
ORDER BY FLAG_RATE DESC, FLAGGED_TXNS DESC
LIMIT 20;
