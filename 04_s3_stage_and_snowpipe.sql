/*=============================================================================
  FINSERV DEMO — Step 04: S3 Stage, Snowpipe & COPY INTO
  Sets up external S3 access, file formats, stages, Snowpipe (auto-ingest),
  and manual COPY INTO alternatives.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;
USE SCHEMA RAW;

-- ============================================================
-- 1. STORAGE INTEGRATION (S3)
-- ============================================================
-- NOTE: Replace <your-aws-role-arn> and <your-bucket> with real values.
--       After CREATE, run DESC INTEGRATION to get STORAGE_AWS_IAM_USER_ARN
--       and STORAGE_AWS_EXTERNAL_ID for the IAM trust policy.

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_FINSERV_INTEGRATION
    TYPE                  = EXTERNAL_STAGE
    STORAGE_PROVIDER      = 'S3'
    STORAGE_AWS_ROLE_ARN  = 'arn:aws:iam::123456789012:role/snowflake-finserv-role'
    ENABLED               = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://your-finserv-bucket/finserv-demo/');

-- Get the Snowflake-side ARN + External ID for IAM trust policy:
DESC INTEGRATION S3_FINSERV_INTEGRATION;
-- Look for: STORAGE_AWS_IAM_USER_ARN, STORAGE_AWS_EXTERNAL_ID


-- ============================================================
-- 2. FILE FORMAT
-- ============================================================

CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
    TYPE                 = CSV
    FIELD_DELIMITER      = ','
    SKIP_HEADER          = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF              = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL  = TRUE
    TRIM_SPACE           = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT              = 'Standard CSV format for S3 ingestion';


-- ============================================================
-- 3. EXTERNAL STAGE
-- ============================================================

CREATE STAGE IF NOT EXISTS S3_FINSERV_STAGE
    STORAGE_INTEGRATION = S3_FINSERV_INTEGRATION
    URL                 = 's3://your-finserv-bucket/finserv-demo/'
    FILE_FORMAT         = CSV_FORMAT
    COMMENT             = 'External S3 stage for finserv CSV files';

-- Verify stage contents (after uploading CSV files)
-- LIST @S3_FINSERV_STAGE;


-- ============================================================
-- 4. LANDING TABLES (S3 targets, separate from BASE tables)
-- ============================================================

CREATE TABLE IF NOT EXISTS CUSTOMERS_S3 (
    FIRST_NAME         VARCHAR(50),
    LAST_NAME          VARCHAR(50),
    EMAIL              VARCHAR(100),
    PHONE              VARCHAR(30),
    DATE_OF_BIRTH      DATE,
    CITY               VARCHAR(50),
    STATE              VARCHAR(50),
    COUNTRY            VARCHAR(50),
    ANNUAL_INCOME      NUMBER(12,2),
    EMPLOYMENT_STATUS  VARCHAR(20),
    CREDIT_SCORE       NUMBER(4,0),
    SIGNUP_DATE        TIMESTAMP_NTZ,
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE       VARCHAR(500) DEFAULT METADATA$FILENAME
);

CREATE TABLE IF NOT EXISTS TRANSACTIONS_S3 (
    ACCOUNT_ID      NUMBER,
    TXN_DATE        TIMESTAMP_NTZ,
    TXN_TYPE        VARCHAR(15),
    AMOUNT          NUMBER(12,2),
    MERCHANT_NAME   VARCHAR(100),
    CATEGORY        VARCHAR(30),
    CHANNEL         VARCHAR(15),
    IS_FLAGGED      BOOLEAN,
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE    VARCHAR(500) DEFAULT METADATA$FILENAME
);

CREATE TABLE IF NOT EXISTS RISK_ASSESSMENTS_S3 (
    CUSTOMER_ID     NUMBER,
    ASSESSED_AT     TIMESTAMP_NTZ,
    RISK_DATA_RAW   VARCHAR(16777216),
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE    VARCHAR(500) DEFAULT METADATA$FILENAME
);


-- ============================================================
-- 5. SNOWPIPE (Auto-Ingest from S3 → SQS)
-- ============================================================
-- NOTE: After creating pipes, run SHOW PIPES to get the
-- notification_channel ARN. Configure it in your S3 bucket's
-- event notifications (s3:ObjectCreated:*).

CREATE PIPE IF NOT EXISTS PIPE_CUSTOMERS_S3
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest customer CSVs from S3'
AS
    COPY INTO CUSTOMERS_S3 (FIRST_NAME, LAST_NAME, EMAIL, PHONE, DATE_OF_BIRTH,
                            CITY, STATE, COUNTRY, ANNUAL_INCOME, EMPLOYMENT_STATUS,
                            CREDIT_SCORE, SIGNUP_DATE)
    FROM @S3_FINSERV_STAGE/customers
    FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
    ON_ERROR = 'CONTINUE';

CREATE PIPE IF NOT EXISTS PIPE_TRANSACTIONS_S3
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest transaction CSVs from S3'
AS
    COPY INTO TRANSACTIONS_S3 (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
                                MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED)
    FROM @S3_FINSERV_STAGE/transactions
    FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
    ON_ERROR = 'CONTINUE';

CREATE PIPE IF NOT EXISTS PIPE_RISK_ASSESSMENTS_S3
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest risk assessment CSVs from S3'
AS
    COPY INTO RISK_ASSESSMENTS_S3 (CUSTOMER_ID, ASSESSED_AT, RISK_DATA_RAW)
    FROM @S3_FINSERV_STAGE/risk_assessments
    FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
    ON_ERROR = 'CONTINUE';

-- Get SQS notification channel ARNs:
SHOW PIPES IN SCHEMA RAW;


-- ============================================================
-- 6. MANUAL COPY INTO (Alternative to Snowpipe)
-- ============================================================
-- Use these for one-time or on-demand loads.

-- Customers
COPY INTO CUSTOMERS_S3 (FIRST_NAME, LAST_NAME, EMAIL, PHONE, DATE_OF_BIRTH,
                        CITY, STATE, COUNTRY, ANNUAL_INCOME, EMPLOYMENT_STATUS,
                        CREDIT_SCORE, SIGNUP_DATE)
FROM @S3_FINSERV_STAGE/customers
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = FALSE;

-- Transactions
COPY INTO TRANSACTIONS_S3 (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
                            MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED)
FROM @S3_FINSERV_STAGE/transactions
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = FALSE;

-- Risk Assessments (VARIANT column loaded as raw text, parse later)
COPY INTO RISK_ASSESSMENTS_S3 (CUSTOMER_ID, ASSESSED_AT, RISK_DATA_RAW)
FROM @S3_FINSERV_STAGE/risk_assessments
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
ON_ERROR = 'CONTINUE'
FORCE = FALSE;


-- ============================================================
-- 7. POST-LOAD: Parse VARIANT from CSV Text
-- ============================================================
-- After COPY INTO, convert RISK_DATA_RAW (string) → VARIANT

CREATE OR REPLACE VIEW RISK_ASSESSMENTS_S3_PARSED AS
SELECT
    CUSTOMER_ID,
    ASSESSED_AT,
    TRY_PARSE_JSON(RISK_DATA_RAW) AS RISK_DATA,
    _LOADED_AT,
    _SOURCE_FILE
FROM RISK_ASSESSMENTS_S3;


-- ============================================================
-- 8. VERIFY
-- ============================================================

SELECT 'CUSTOMERS_S3'        AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CUSTOMERS_S3
UNION ALL SELECT 'TRANSACTIONS_S3',    COUNT(*) FROM TRANSACTIONS_S3
UNION ALL SELECT 'RISK_ASSESSMENTS_S3', COUNT(*) FROM RISK_ASSESSMENTS_S3
ORDER BY TABLE_NAME;

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('PIPE_CUSTOMERS_S3')      AS PIPE_CUSTOMERS;
SELECT SYSTEM$PIPE_STATUS('PIPE_TRANSACTIONS_S3')    AS PIPE_TRANSACTIONS;
SELECT SYSTEM$PIPE_STATUS('PIPE_RISK_ASSESSMENTS_S3') AS PIPE_RISK;
