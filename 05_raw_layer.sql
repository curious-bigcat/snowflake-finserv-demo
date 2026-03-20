/*=============================================================================
  FINSERV DEMO — Step 5: Raw Layer (Streams + Staging)
  Creates streams on BASE tables for CDC and staging tables in the RAW schema.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. STREAMS ON BASE TABLES
-- ============================================================

-- Customer stream — captures new signups and profile updates
CREATE OR REPLACE STREAM RAW.CUSTOMERS_STREAM
    ON TABLE BASE.CUSTOMERS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on base customers table';

-- Accounts stream — captures new accounts and status changes
CREATE OR REPLACE STREAM RAW.ACCOUNTS_STREAM
    ON TABLE BASE.ACCOUNTS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on base accounts table';

-- Transactions stream — append-only (transactions are immutable)
CREATE OR REPLACE STREAM RAW.TRANSACTIONS_STREAM
    ON TABLE BASE.TRANSACTIONS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'Append-only stream on base transactions table';

-- Risk assessments stream — captures new and updated assessments
CREATE OR REPLACE STREAM RAW.RISK_ASSESSMENTS_STREAM
    ON TABLE BASE.RISK_ASSESSMENTS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on base risk assessments';

-- Market data stream — append-only (market data is immutable)
CREATE OR REPLACE STREAM RAW.MARKET_DATA_STREAM
    ON TABLE BASE.MARKET_DATA
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'Append-only stream on base market data';

-- Support tickets stream
CREATE OR REPLACE STREAM RAW.SUPPORT_TICKETS_STREAM
    ON TABLE BASE.SUPPORT_TICKETS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on base support tickets';

-- Compliance documents stream
CREATE OR REPLACE STREAM RAW.COMPLIANCE_DOCS_STREAM
    ON TABLE BASE.COMPLIANCE_DOCUMENTS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on base compliance documents';


-- ============================================================
-- 2. RAW STAGING TABLES
--    Mirror BASE structure with audit columns.
--    Fed by streams via the task DAG (see 08_tasks_and_dag.sql).
-- ============================================================

-- Customers staging (for stream-based processing)
CREATE OR REPLACE TABLE RAW.CUSTOMERS_RAW (
    CUSTOMER_ID       INT,
    FIRST_NAME        VARCHAR(50),
    LAST_NAME         VARCHAR(50),
    EMAIL             VARCHAR(100),
    PHONE             VARCHAR(20),
    DATE_OF_BIRTH     DATE,
    CITY              VARCHAR(50),
    STATE             VARCHAR(50),
    COUNTRY           VARCHAR(50),
    ANNUAL_INCOME     NUMBER(12,2),
    EMPLOYMENT_STATUS VARCHAR(20),
    CREDIT_SCORE      INT,
    SIGNUP_DATE       TIMESTAMP_NTZ,
    LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STREAM_ACTION     VARCHAR(10)
);

-- Transactions staging (for stream-based processing)
CREATE OR REPLACE TABLE RAW.TRANSACTIONS_RAW (
    TXN_ID         INT,
    ACCOUNT_ID     INT,
    TXN_DATE       TIMESTAMP_NTZ,
    TXN_TYPE       VARCHAR(20),
    AMOUNT         NUMBER(12,2),
    MERCHANT_NAME  VARCHAR(100),
    CATEGORY       VARCHAR(50),
    CHANNEL        VARCHAR(20),
    IS_FLAGGED     BOOLEAN,
    LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STREAM_ACTION  VARCHAR(10)
);

-- Support tickets staging
CREATE OR REPLACE TABLE RAW.SUPPORT_TICKETS_RAW (
    TICKET_ID          INT,
    CUSTOMER_ID        INT,
    CREATED_AT         TIMESTAMP_NTZ,
    SUBJECT            VARCHAR(200),
    PRIORITY           VARCHAR(10),
    BODY               TEXT,
    RESOLUTION_STATUS  VARCHAR(20),
    ASSIGNED_TO        VARCHAR(50),
    LOADED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STREAM_ACTION      VARCHAR(10)
);


-- ============================================================
-- 3. VERIFICATION
-- ============================================================

-- List all streams
SHOW STREAMS IN SCHEMA RAW;

-- Check stream status
SELECT SYSTEM$STREAM_HAS_DATA('RAW.CUSTOMERS_STREAM')        AS CUSTOMERS_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.ACCOUNTS_STREAM')          AS ACCOUNTS_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.TRANSACTIONS_STREAM')      AS TRANSACTIONS_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.RISK_ASSESSMENTS_STREAM')  AS RISK_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.MARKET_DATA_STREAM')       AS MARKET_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.SUPPORT_TICKETS_STREAM')   AS TICKETS_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.COMPLIANCE_DOCS_STREAM')   AS COMPLIANCE_HAS_DATA;
