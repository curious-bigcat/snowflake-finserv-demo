/*=============================================================================
  FINSERV DEMO — Step 6: Curated Layer
  Dynamic tables and materialized views that enrich and join BASE data.
  TARGET_LAG = 1 MINUTE for near-real-time processing.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. DT_CUSTOMER_PROFILE
--    Customers enriched with account summary + latest risk score.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_CUSTOMER_PROFILE
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Customer profiles with account summary and risk score'
AS
SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,
    c.PHONE,
    c.DATE_OF_BIRTH,
    c.CITY,
    c.STATE,
    c.COUNTRY,
    c.ANNUAL_INCOME,
    c.EMPLOYMENT_STATUS,
    c.CREDIT_SCORE,
    c.SIGNUP_DATE,
    -- Account summary
    COUNT(DISTINCT a.ACCOUNT_ID)                                  AS TOTAL_ACCOUNTS,
    SUM(CASE WHEN a.STATUS = 'ACTIVE' THEN 1 ELSE 0 END)        AS ACTIVE_ACCOUNTS,
    COALESCE(SUM(a.BALANCE), 0)                                   AS TOTAL_BALANCE,
    COALESCE(SUM(a.CREDIT_LIMIT), 0)                              AS TOTAL_CREDIT_LIMIT,
    -- Latest risk assessment
    MAX(r.RISK_DATA:risk_score::INT)                              AS LATEST_RISK_SCORE,
    MAX(r.RISK_DATA:credit_history::VARCHAR)                      AS LATEST_CREDIT_HISTORY,
    MAX(r.RISK_DATA:debt_to_income::FLOAT)                        AS DEBT_TO_INCOME,
    MAX(r.ASSESSED_AT)                                            AS LAST_ASSESSMENT_DATE
FROM BASE.CUSTOMERS c
LEFT JOIN BASE.ACCOUNTS a
    ON c.CUSTOMER_ID = a.CUSTOMER_ID
LEFT JOIN (
    -- Get latest risk assessment per customer
    SELECT *
    FROM BASE.RISK_ASSESSMENTS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY ASSESSED_AT DESC) = 1
) r ON c.CUSTOMER_ID = r.CUSTOMER_ID
GROUP BY
    c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.EMAIL, c.PHONE,
    c.DATE_OF_BIRTH, c.CITY, c.STATE, c.COUNTRY, c.ANNUAL_INCOME,
    c.EMPLOYMENT_STATUS, c.CREDIT_SCORE, c.SIGNUP_DATE,
    r.RISK_DATA, r.ASSESSED_AT;


-- ============================================================
-- 2. DT_TRANSACTION_ENRICHED
--    Transactions joined with customer and account context.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_TRANSACTION_ENRICHED
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Transactions enriched with customer and account details'
AS
SELECT
    t.TXN_ID,
    t.ACCOUNT_ID,
    t.TXN_DATE,
    t.TXN_TYPE,
    t.AMOUNT,
    t.MERCHANT_NAME,
    t.CATEGORY,
    t.CHANNEL,
    t.IS_FLAGGED,
    -- Account context
    a.ACCOUNT_TYPE,
    a.BALANCE       AS ACCOUNT_BALANCE,
    a.STATUS        AS ACCOUNT_STATUS,
    a.BRANCH_CODE,
    -- Customer context
    a.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME  AS CUSTOMER_NAME,
    c.CITY          AS CUSTOMER_CITY,
    c.COUNTRY       AS CUSTOMER_COUNTRY,
    c.CREDIT_SCORE,
    c.ANNUAL_INCOME
FROM BASE.TRANSACTIONS t
JOIN BASE.ACCOUNTS a ON t.ACCOUNT_ID = a.ACCOUNT_ID
JOIN BASE.CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID;


-- ============================================================
-- 3. DT_SUPPORT_ENRICHED
--    Support tickets with customer context for analysis.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_SUPPORT_ENRICHED
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Support tickets enriched with customer profile'
AS
SELECT
    st.TICKET_ID,
    st.CUSTOMER_ID,
    st.CREATED_AT,
    st.SUBJECT,
    st.PRIORITY,
    st.BODY,
    st.RESOLUTION_STATUS,
    st.ASSIGNED_TO,
    -- Customer context
    c.FIRST_NAME || ' ' || c.LAST_NAME  AS CUSTOMER_NAME,
    c.CITY,
    c.COUNTRY,
    c.CREDIT_SCORE,
    c.ANNUAL_INCOME,
    -- Account summary
    (SELECT COUNT(*) FROM BASE.ACCOUNTS a WHERE a.CUSTOMER_ID = st.CUSTOMER_ID) AS CUSTOMER_ACCOUNTS,
    (SELECT SUM(BALANCE) FROM BASE.ACCOUNTS a WHERE a.CUSTOMER_ID = st.CUSTOMER_ID) AS CUSTOMER_BALANCE,
    -- Ticket age
    DATEDIFF('hour', st.CREATED_AT, CURRENT_TIMESTAMP()) AS TICKET_AGE_HOURS
FROM BASE.SUPPORT_TICKETS st
LEFT JOIN BASE.CUSTOMERS c ON st.CUSTOMER_ID = c.CUSTOMER_ID;


-- ============================================================
-- 4. DT_MARKET_LATEST
--    Latest market data per ticker (dynamic table).
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_MARKET_LATEST
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Latest market data per ticker symbol'
AS
SELECT
    m.TICKER,
    m.TRADE_DATE,
    m.MARKET_DATA:open::FLOAT           AS OPEN_PRICE,
    m.MARKET_DATA:high::FLOAT           AS HIGH_PRICE,
    m.MARKET_DATA:low::FLOAT            AS LOW_PRICE,
    m.MARKET_DATA:close::FLOAT          AS CLOSE_PRICE,
    m.MARKET_DATA:volume::INT           AS VOLUME,
    m.MARKET_DATA:indicators.moving_avg_50::FLOAT AS MOVING_AVG_50,
    m.MARKET_DATA:indicators.rsi::FLOAT          AS RSI,
    m.MARKET_DATA:indicators.macd::FLOAT         AS MACD
FROM BASE.MARKET_DATA m
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.TICKER ORDER BY m.TRADE_DATE DESC) = 1;


-- ============================================================
-- 5. DT_RISK_FACTORS_PARSED
--    Flattened risk factors from semi-structured assessments.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_RISK_FACTORS_PARSED
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Flattened risk factors from VARIANT risk assessments'
AS
SELECT
    r.ASSESSMENT_ID,
    r.CUSTOMER_ID,
    r.ASSESSED_AT,
    r.RISK_DATA:risk_score::INT                     AS RISK_SCORE,
    r.RISK_DATA:credit_history::VARCHAR             AS CREDIT_HISTORY,
    r.RISK_DATA:debt_to_income::FLOAT               AS DEBT_TO_INCOME,
    r.RISK_DATA:assessment_type::VARCHAR             AS ASSESSMENT_TYPE,
    r.RISK_DATA:model_version::VARCHAR               AS MODEL_VERSION,
    f.VALUE:factor::VARCHAR                          AS RISK_FACTOR,
    f.VALUE:score::INT                               AS FACTOR_SCORE
FROM BASE.RISK_ASSESSMENTS r,
     LATERAL FLATTEN(INPUT => r.RISK_DATA:risk_factors, OUTER => TRUE) f;


-- ============================================================
-- 6. DT_COMPLIANCE_ENRICHED
--    Compliance documents with parsed METADATA for reporting.
-- ============================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.DT_COMPLIANCE_ENRICHED
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = FINSERV_WH
    COMMENT = 'Compliance documents with parsed metadata fields'
AS
SELECT
    d.DOC_ID,
    d.DOC_TYPE,
    d.DOC_CONTENT,
    d.CREATED_AT,
    d.METADATA:regulatory_body::VARCHAR     AS REGULATORY_BODY,
    d.METADATA:status::VARCHAR              AS DOC_STATUS,
    d.METADATA:version::VARCHAR             AS DOC_VERSION,
    d.METADATA:effective_date::VARCHAR      AS EFFECTIVE_DATE,
    d.METADATA:review_cycle::VARCHAR        AS REVIEW_CYCLE,
    d.METADATA:classification::VARCHAR      AS CLASSIFICATION,
    LENGTH(d.DOC_CONTENT)                   AS CONTENT_LENGTH
FROM BASE.COMPLIANCE_DOCUMENTS d;


-- ============================================================
-- 7. VERIFICATION
-- ============================================================

SELECT 'DT_CUSTOMER_PROFILE'   AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CURATED.DT_CUSTOMER_PROFILE
UNION ALL
SELECT 'DT_TRANSACTION_ENRICHED', COUNT(*) FROM CURATED.DT_TRANSACTION_ENRICHED
UNION ALL
SELECT 'DT_SUPPORT_ENRICHED',     COUNT(*) FROM CURATED.DT_SUPPORT_ENRICHED
UNION ALL
SELECT 'DT_MARKET_LATEST',         COUNT(*) FROM CURATED.DT_MARKET_LATEST
UNION ALL
SELECT 'DT_RISK_FACTORS_PARSED',  COUNT(*) FROM CURATED.DT_RISK_FACTORS_PARSED
UNION ALL
SELECT 'DT_COMPLIANCE_ENRICHED',  COUNT(*) FROM CURATED.DT_COMPLIANCE_ENRICHED
ORDER BY TABLE_NAME;
