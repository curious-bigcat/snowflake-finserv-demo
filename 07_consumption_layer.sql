/*=============================================================================
  FINSERV DEMO — Step 7: Consumption Layer
  Business-ready aggregates and KPI tables for dashboards, Cortex, and ML.
  Dynamic tables with DOWNSTREAM or 5-MINUTE lag from curated layer.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. DT_CUSTOMER_360
--    Complete customer view: balances, tx metrics, risk, support.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_CUSTOMER_360
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Full customer 360 view with all metrics'
AS
SELECT
    cp.CUSTOMER_ID,
    cp.FIRST_NAME,
    cp.LAST_NAME,
    cp.EMAIL,
    cp.CITY,
    cp.COUNTRY,
    cp.ANNUAL_INCOME,
    cp.EMPLOYMENT_STATUS,
    cp.CREDIT_SCORE,
    cp.SIGNUP_DATE,
    cp.TOTAL_ACCOUNTS,
    cp.ACTIVE_ACCOUNTS,
    cp.TOTAL_BALANCE,
    cp.TOTAL_CREDIT_LIMIT,
    cp.LATEST_RISK_SCORE,
    cp.LATEST_RISK_LEVEL,
    cp.DEBT_TO_INCOME,
    -- Transaction metrics
    COALESCE(tx.TOTAL_TRANSACTIONS, 0)       AS TOTAL_TRANSACTIONS,
    COALESCE(tx.TOTAL_SPENT, 0)              AS TOTAL_SPENT,
    COALESCE(tx.AVG_TRANSACTION, 0)          AS AVG_TRANSACTION,
    COALESCE(tx.FLAGGED_TRANSACTIONS, 0)     AS FLAGGED_TRANSACTIONS,
    COALESCE(tx.DAYS_SINCE_LAST_TXN, 999)   AS DAYS_SINCE_LAST_TXN,
    COALESCE(tx.UNIQUE_MERCHANTS, 0)         AS UNIQUE_MERCHANTS,
    COALESCE(tx.UNIQUE_CATEGORIES, 0)        AS UNIQUE_CATEGORIES,
    -- Support metrics
    COALESCE(sp.TOTAL_TICKETS, 0)            AS TOTAL_TICKETS,
    COALESCE(sp.OPEN_TICKETS, 0)             AS OPEN_TICKETS,
    COALESCE(sp.HIGH_PRIORITY_TICKETS, 0)    AS HIGH_PRIORITY_TICKETS,
    -- Customer segment
    CASE
        WHEN cp.TOTAL_BALANCE >= 200000 AND COALESCE(tx.TOTAL_TRANSACTIONS, 0) >= 50
            THEN 'PLATINUM'
        WHEN cp.TOTAL_BALANCE >= 100000 AND COALESCE(tx.TOTAL_TRANSACTIONS, 0) >= 25
            THEN 'GOLD'
        WHEN cp.TOTAL_BALANCE >= 25000 AND COALESCE(tx.TOTAL_TRANSACTIONS, 0) >= 10
            THEN 'SILVER'
        ELSE 'STANDARD'
    END AS CUSTOMER_SEGMENT
FROM CURATED.DT_CUSTOMER_PROFILE cp
LEFT JOIN (
    SELECT
        CUSTOMER_ID,
        COUNT(*)                                         AS TOTAL_TRANSACTIONS,
        SUM(CASE WHEN TXN_TYPE = 'DEBIT' THEN AMOUNT ELSE 0 END) AS TOTAL_SPENT,
        AVG(AMOUNT)                                      AS AVG_TRANSACTION,
        SUM(CASE WHEN IS_FLAGGED THEN 1 ELSE 0 END)    AS FLAGGED_TRANSACTIONS,
        DATEDIFF('day', MAX(TXN_DATE), CURRENT_TIMESTAMP()) AS DAYS_SINCE_LAST_TXN,
        COUNT(DISTINCT MERCHANT_NAME)                    AS UNIQUE_MERCHANTS,
        COUNT(DISTINCT CATEGORY)                         AS UNIQUE_CATEGORIES
    FROM CURATED.DT_TRANSACTION_ENRICHED
    GROUP BY CUSTOMER_ID
) tx ON cp.CUSTOMER_ID = tx.CUSTOMER_ID
LEFT JOIN (
    SELECT
        CUSTOMER_ID,
        COUNT(*)                                                          AS TOTAL_TICKETS,
        SUM(CASE WHEN RESOLUTION_STATUS IN ('OPEN','IN_PROGRESS') THEN 1 ELSE 0 END) AS OPEN_TICKETS,
        SUM(CASE WHEN PRIORITY IN ('HIGH','CRITICAL') THEN 1 ELSE 0 END) AS HIGH_PRIORITY_TICKETS
    FROM CURATED.DT_SUPPORT_ENRICHED
    GROUP BY CUSTOMER_ID
) sp ON cp.CUSTOMER_ID = sp.CUSTOMER_ID;


-- ============================================================
-- 2. DT_DAILY_FINANCIAL_METRICS
--    Daily aggregated financial KPIs.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_DAILY_FINANCIAL_METRICS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Daily financial KPIs: volume, revenue, customers'
AS
SELECT
    TXN_DATE::DATE                                         AS METRIC_DATE,
    COUNT(*)                                               AS TOTAL_TRANSACTIONS,
    COUNT(DISTINCT CUSTOMER_ID)                            AS UNIQUE_CUSTOMERS,
    SUM(AMOUNT)                                            AS TOTAL_VOLUME,
    SUM(CASE WHEN TXN_TYPE = 'DEBIT' THEN AMOUNT ELSE 0 END) AS TOTAL_DEBITS,
    SUM(CASE WHEN TXN_TYPE = 'CREDIT' THEN AMOUNT ELSE 0 END) AS TOTAL_CREDITS,
    AVG(AMOUNT)                                            AS AVG_TRANSACTION_AMOUNT,
    MAX(AMOUNT)                                            AS MAX_TRANSACTION,
    SUM(CASE WHEN IS_FLAGGED THEN 1 ELSE 0 END)          AS FLAGGED_COUNT,
    ROUND(SUM(CASE WHEN IS_FLAGGED THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 2)                          AS FLAGGED_RATE_PCT
FROM CURATED.DT_TRANSACTION_ENRICHED
GROUP BY TXN_DATE::DATE;


-- ============================================================
-- 3. DT_RISK_DASHBOARD
--    Risk distribution and high-risk customer summary.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_RISK_DASHBOARD
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Risk distribution and high-risk customer overview'
AS
SELECT
    cp.CUSTOMER_ID,
    cp.FIRST_NAME || ' ' || cp.LAST_NAME  AS CUSTOMER_NAME,
    cp.CITY,
    cp.COUNTRY,
    cp.CREDIT_SCORE,
    cp.TOTAL_BALANCE,
    cp.LATEST_RISK_SCORE,
    cp.LATEST_RISK_LEVEL,
    cp.DEBT_TO_INCOME,
    cp.TOTAL_ACCOUNTS,
    -- Risk tier for dashboarding
    CASE
        WHEN cp.LATEST_RISK_SCORE >= 80 THEN 'CRITICAL'
        WHEN cp.LATEST_RISK_SCORE >= 60 THEN 'HIGH'
        WHEN cp.LATEST_RISK_SCORE >= 40 THEN 'MEDIUM'
        WHEN cp.LATEST_RISK_SCORE >= 20 THEN 'LOW'
        ELSE 'MINIMAL'
    END AS RISK_TIER
FROM CURATED.DT_CUSTOMER_PROFILE cp
WHERE cp.LATEST_RISK_SCORE IS NOT NULL;


-- ============================================================
-- 4. DT_CHANNEL_PERFORMANCE
--    Transaction metrics segmented by channel.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_CHANNEL_PERFORMANCE
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Transaction metrics by channel'
AS
SELECT
    CHANNEL,
    TXN_DATE::DATE                                         AS METRIC_DATE,
    COUNT(*)                                               AS TRANSACTION_COUNT,
    SUM(AMOUNT)                                            AS TOTAL_VOLUME,
    AVG(AMOUNT)                                            AS AVG_AMOUNT,
    COUNT(DISTINCT CUSTOMER_ID)                            AS UNIQUE_CUSTOMERS,
    SUM(CASE WHEN IS_FLAGGED THEN 1 ELSE 0 END)          AS FLAGGED_COUNT,
    COUNT(DISTINCT CATEGORY)                               AS CATEGORIES_USED
FROM CURATED.DT_TRANSACTION_ENRICHED
GROUP BY CHANNEL, TXN_DATE::DATE;


-- ============================================================
-- 5. DT_CHURN_FEATURES
--    ML feature table for customer churn prediction.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_CHURN_FEATURES
    TARGET_LAG = '5 MINUTE'
    WAREHOUSE = FINSERV_WH
    COMMENT = 'ML feature table for churn classification'
AS
SELECT
    c360.CUSTOMER_ID,
    -- Demographics
    c360.ANNUAL_INCOME,
    c360.CREDIT_SCORE,
    DATEDIFF('day', c360.SIGNUP_DATE, CURRENT_TIMESTAMP())    AS TENURE_DAYS,
    -- Account features
    c360.TOTAL_ACCOUNTS,
    c360.ACTIVE_ACCOUNTS,
    c360.TOTAL_BALANCE,
    c360.TOTAL_CREDIT_LIMIT,
    CASE WHEN c360.TOTAL_CREDIT_LIMIT > 0
        THEN ROUND(c360.TOTAL_BALANCE / c360.TOTAL_CREDIT_LIMIT, 4)
        ELSE 0
    END AS CREDIT_UTILIZATION,
    -- Transaction features
    c360.TOTAL_TRANSACTIONS,
    c360.TOTAL_SPENT,
    c360.AVG_TRANSACTION,
    c360.DAYS_SINCE_LAST_TXN,
    c360.UNIQUE_MERCHANTS,
    c360.UNIQUE_CATEGORIES,
    c360.FLAGGED_TRANSACTIONS,
    -- Risk features
    COALESCE(c360.LATEST_RISK_SCORE, 50)     AS RISK_SCORE,
    COALESCE(c360.DEBT_TO_INCOME, 0.5)       AS DEBT_TO_INCOME,
    -- Support features
    c360.TOTAL_TICKETS,
    c360.OPEN_TICKETS,
    c360.HIGH_PRIORITY_TICKETS,
    -- Churn label (derived: inactive > 90 days OR closed all accounts)
    CASE
        WHEN c360.DAYS_SINCE_LAST_TXN > 90 THEN 1
        WHEN c360.ACTIVE_ACCOUNTS = 0 THEN 1
        ELSE 0
    END AS IS_CHURNED
FROM CONSUMPTION.DT_CUSTOMER_360 c360;


-- ============================================================
-- 6. DT_MONTHLY_REVENUE
--    Monthly aggregates for revenue regression model.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_MONTHLY_REVENUE
    TARGET_LAG = '5 MINUTE'
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Monthly revenue aggregates for regression model'
AS
SELECT
    DATE_TRUNC('MONTH', METRIC_DATE)::DATE             AS REVENUE_MONTH,
    SUM(TOTAL_TRANSACTIONS)                            AS MONTHLY_TRANSACTIONS,
    SUM(UNIQUE_CUSTOMERS)                              AS MONTHLY_ACTIVE_CUSTOMERS,
    SUM(TOTAL_VOLUME)                                  AS MONTHLY_VOLUME,
    SUM(TOTAL_DEBITS)                                  AS MONTHLY_DEBITS,
    SUM(TOTAL_CREDITS)                                 AS MONTHLY_CREDITS,
    AVG(AVG_TRANSACTION_AMOUNT)                        AS AVG_DAILY_TXN_AMOUNT,
    MAX(MAX_TRANSACTION)                               AS PEAK_TRANSACTION,
    SUM(FLAGGED_COUNT)                                 AS MONTHLY_FLAGS,
    AVG(FLAGGED_RATE_PCT)                              AS AVG_FLAG_RATE,
    COUNT(DISTINCT METRIC_DATE)                        AS TRADING_DAYS
FROM CONSUMPTION.DT_DAILY_FINANCIAL_METRICS
GROUP BY DATE_TRUNC('MONTH', METRIC_DATE);


-- ============================================================
-- 7. DT_MARKET_OVERVIEW
--    Market performance summary from curated market data.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_MARKET_OVERVIEW
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Market overview: latest price, volume, and technical signals per ticker'
AS
SELECT
    TICKER,
    TRADE_DATE,
    CLOSE_PRICE,
    OPEN_PRICE,
    HIGH_PRICE,
    LOW_PRICE,
    VOLUME,
    ROUND((CLOSE_PRICE - OPEN_PRICE) / NULLIF(OPEN_PRICE, 0) * 100, 2) AS DAILY_RETURN_PCT,
    RSI,
    MACD,
    SMA_20,
    CASE
        WHEN RSI > 70 THEN 'OVERBOUGHT'
        WHEN RSI < 30 THEN 'OVERSOLD'
        ELSE 'NEUTRAL'
    END AS RSI_SIGNAL,
    CASE
        WHEN MACD > 0 THEN 'BULLISH'
        ELSE 'BEARISH'
    END AS MACD_SIGNAL
FROM CURATED.MV_MARKET_LATEST;


-- ============================================================
-- 8. DT_COMPLIANCE_SUMMARY
--    Compliance document aggregates by type, regulatory body, status.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_COMPLIANCE_SUMMARY
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Compliance document summary by type, body, and status'
AS
SELECT
    DOC_TYPE,
    REGULATORY_BODY,
    DOC_STATUS,
    CLASSIFICATION,
    REVIEW_CYCLE,
    COUNT(*)                          AS DOC_COUNT,
    AVG(CONTENT_LENGTH)               AS AVG_CONTENT_LENGTH,
    MIN(CREATED_AT)                   AS EARLIEST_DOC,
    MAX(CREATED_AT)                   AS LATEST_DOC
FROM CURATED.DT_COMPLIANCE_ENRICHED
GROUP BY DOC_TYPE, REGULATORY_BODY, DOC_STATUS, CLASSIFICATION, REVIEW_CYCLE;


-- ============================================================
-- 9. DT_RISK_FACTOR_SUMMARY
--    Aggregated risk factor analysis from parsed risk assessments.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CONSUMPTION.DT_RISK_FACTOR_SUMMARY
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Risk factor aggregates: avg scores, distributions by factor type'
AS
SELECT
    RISK_FACTOR,
    RISK_LEVEL,
    COUNT(*)                          AS ASSESSMENT_COUNT,
    AVG(RISK_SCORE)                   AS AVG_RISK_SCORE,
    AVG(DEBT_TO_INCOME)               AS AVG_DEBT_TO_INCOME,
    MIN(RISK_SCORE)                   AS MIN_RISK_SCORE,
    MAX(RISK_SCORE)                   AS MAX_RISK_SCORE
FROM CURATED.DT_RISK_FACTORS_PARSED
GROUP BY RISK_FACTOR, RISK_LEVEL;


-- ============================================================
-- 10. PIPELINE_METRICS TABLE (for task DAG)
-- ============================================================

CREATE OR REPLACE TABLE CONSUMPTION.PIPELINE_METRICS (
    METRIC_DATE        DATE,
    TOTAL_CUSTOMERS    INT,
    TOTAL_ACCOUNTS     INT,
    TOTAL_TRANSACTIONS INT,
    TOTAL_VOLUME       NUMBER(14,2),
    AUM                NUMBER(14,2),
    TOTAL_TICKETS      INT,
    REFRESHED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================
-- 11. VERIFICATION
-- ============================================================

SELECT 'DT_CUSTOMER_360'           AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CONSUMPTION.DT_CUSTOMER_360
UNION ALL
SELECT 'DT_DAILY_FINANCIAL_METRICS', COUNT(*) FROM CONSUMPTION.DT_DAILY_FINANCIAL_METRICS
UNION ALL
SELECT 'DT_RISK_DASHBOARD',         COUNT(*) FROM CONSUMPTION.DT_RISK_DASHBOARD
UNION ALL
SELECT 'DT_CHANNEL_PERFORMANCE',    COUNT(*) FROM CONSUMPTION.DT_CHANNEL_PERFORMANCE
UNION ALL
SELECT 'DT_CHURN_FEATURES',         COUNT(*) FROM CONSUMPTION.DT_CHURN_FEATURES
UNION ALL
SELECT 'DT_MONTHLY_REVENUE',        COUNT(*) FROM CONSUMPTION.DT_MONTHLY_REVENUE
UNION ALL
SELECT 'DT_MARKET_OVERVIEW',        COUNT(*) FROM CONSUMPTION.DT_MARKET_OVERVIEW
UNION ALL
SELECT 'DT_COMPLIANCE_SUMMARY',     COUNT(*) FROM CONSUMPTION.DT_COMPLIANCE_SUMMARY
UNION ALL
SELECT 'DT_RISK_FACTOR_SUMMARY',    COUNT(*) FROM CONSUMPTION.DT_RISK_FACTOR_SUMMARY
ORDER BY TABLE_NAME;
