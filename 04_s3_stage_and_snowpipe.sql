/*=============================================================================
  FINSERV DEMO — Step 04: S3 Stage, Snowpipe & BASE Integration
  Configures S3 external stage with Snowpipe auto-ingest for 3 tables:
    - TRANSACTIONS     (from CSV → S3 → Snowpipe → BASE)
    - SUPPORT_TICKETS  (from CSV → S3 → Snowpipe → BASE)
    - RISK_ASSESSMENTS (from CSV → S3 → Snowpipe → BASE)

  Data flow:
    Python (file 03) → S3 bucket → Snowpipe auto-ingest → landing tables (*_S3)
    → MERGE/INSERT into BASE tables → streams → curated → consumption

  Prerequisites:
    - S3 bucket with event notifications (SQS) configured for Snowpipe
    - IAM role with trust policy for Snowflake external ID
    - File 02 must be run first (BASE tables + CUSTOMERS/ACCOUNTS data)
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;
USE SCHEMA RAW;

-- ============================================================
-- 1. S3 STORAGE INTEGRATION
-- ============================================================
-- Update the ROLE_ARN and STORAGE_ALLOWED_LOCATIONS with your S3 details.
-- After creation, run: DESC STORAGE INTEGRATION S3_FINSERV_INTEGRATION;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into your
-- IAM role's trust policy.

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_FINSERV_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::123456789012:role/snowflake-finserv-role'
    ENABLED                   = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://your-finserv-bucket/finserv-demo/');

-- Retrieve the values needed for your IAM trust policy:
-- DESC STORAGE INTEGRATION S3_FINSERV_INTEGRATION;


-- ============================================================
-- 2. FILE FORMAT
-- ============================================================

CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
    TYPE                = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER         = 1
    NULL_IF             = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


-- ============================================================
-- 3. EXTERNAL STAGE (S3)
-- ============================================================

CREATE STAGE IF NOT EXISTS S3_FINSERV_STAGE
    STORAGE_INTEGRATION = S3_FINSERV_INTEGRATION
    URL                 = 's3://your-finserv-bucket/finserv-demo/'
    FILE_FORMAT         = CSV_FORMAT
    COMMENT             = 'External S3 stage for Snowpipe auto-ingest';


-- ============================================================
-- 4. LANDING TABLES
-- ============================================================
-- These mirror the CSV schemas with audit columns appended.
-- Snowpipe loads data here; MERGE then pushes into BASE.

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
    _SOURCE_FILE    VARCHAR(500)  DEFAULT METADATA$FILENAME
);

CREATE TABLE IF NOT EXISTS SUPPORT_TICKETS_S3 (
    CUSTOMER_ID        NUMBER,
    CREATED_AT         TIMESTAMP_NTZ,
    SUBJECT            VARCHAR(200),
    PRIORITY           VARCHAR(10),
    BODY               TEXT,
    RESOLUTION_STATUS  VARCHAR(20),
    ASSIGNED_TO        VARCHAR(50),
    _LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE       VARCHAR(500)  DEFAULT METADATA$FILENAME
);

CREATE TABLE IF NOT EXISTS RISK_ASSESSMENTS_S3 (
    CUSTOMER_ID     NUMBER,
    ASSESSED_AT     TIMESTAMP_NTZ,
    RISK_DATA_RAW   VARCHAR(16777216),
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE    VARCHAR(500)  DEFAULT METADATA$FILENAME
);

-- Parsed view for risk assessments (converts raw JSON string → VARIANT)
CREATE OR REPLACE VIEW RISK_ASSESSMENTS_S3_PARSED AS
SELECT
    CUSTOMER_ID,
    ASSESSED_AT,
    TRY_PARSE_JSON(RISK_DATA_RAW) AS RISK_DATA,
    _LOADED_AT,
    _SOURCE_FILE
FROM RISK_ASSESSMENTS_S3;


-- ============================================================
-- 5. SNOWPIPE — AUTO-INGEST FROM S3
-- ============================================================
-- Each pipe watches a subfolder in the S3 stage.
-- AUTO_INGEST=TRUE means Snowflake processes files automatically
-- when S3 sends SQS event notifications.
--
-- After creating pipes:
--   SHOW PIPES IN SCHEMA RAW;
-- Copy the notification_channel ARN and configure it as the
-- SQS destination for your S3 bucket event notifications.

CREATE PIPE IF NOT EXISTS PIPE_TRANSACTIONS_S3
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest transaction CSVs from S3'
AS
    COPY INTO TRANSACTIONS_S3 (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
                                MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED)
    FROM @S3_FINSERV_STAGE/transactions/
    FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
    ON_ERROR = 'CONTINUE';

CREATE PIPE IF NOT EXISTS PIPE_SUPPORT_TICKETS_S3
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest support ticket CSVs from S3'
AS
    COPY INTO SUPPORT_TICKETS_S3 (CUSTOMER_ID, CREATED_AT, SUBJECT, PRIORITY,
                                   BODY, RESOLUTION_STATUS, ASSIGNED_TO)
    FROM @S3_FINSERV_STAGE/support_tickets/
    FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
    ON_ERROR = 'CONTINUE';

CREATE PIPE IF NOT EXISTS PIPE_RISK_ASSESSMENTS_S3
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest risk assessment CSVs from S3'
AS
    COPY INTO RISK_ASSESSMENTS_S3 (CUSTOMER_ID, ASSESSED_AT, RISK_DATA_RAW)
    FROM @S3_FINSERV_STAGE/risk_assessments/
    FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
    ON_ERROR = 'CONTINUE';

-- Get SQS notification channel ARNs to configure S3 event notifications:
SHOW PIPES IN SCHEMA RAW;


-- ============================================================
-- 6. MERGE INTO BASE — Feed Snowpipe data into the medallion pipeline
-- ============================================================
-- Once Snowpipe loads data into landing tables, run these MERGEs
-- to push data into BASE. The downstream pipeline (streams → curated
-- → consumption) reads from BASE and auto-refreshes.
--
-- In production, schedule these as a Snowflake Task on a 5-minute cadence
-- or trigger them after confirming Snowpipe loads via SYSTEM$PIPE_STATUS.

-- 6a. Transactions: INSERT (append-only, no natural key to deduplicate)
INSERT INTO BASE.TRANSACTIONS (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
                                MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED)
SELECT ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
       MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED
FROM RAW.TRANSACTIONS_S3;

-- 6b. Support Tickets: INSERT (append-only, tickets have no natural key in CSV)
INSERT INTO BASE.SUPPORT_TICKETS (CUSTOMER_ID, CREATED_AT, SUBJECT, PRIORITY,
                                   BODY, RESOLUTION_STATUS, ASSIGNED_TO)
SELECT CUSTOMER_ID, CREATED_AT, SUBJECT, PRIORITY,
       BODY, RESOLUTION_STATUS, ASSIGNED_TO
FROM RAW.SUPPORT_TICKETS_S3;

-- 6c. Risk Assessments: MERGE on CUSTOMER_ID + ASSESSED_AT (composite key)
MERGE INTO BASE.RISK_ASSESSMENTS tgt
USING (
    SELECT
        CUSTOMER_ID,
        ASSESSED_AT,
        TRY_PARSE_JSON(RISK_DATA_RAW) AS RISK_DATA
    FROM RAW.RISK_ASSESSMENTS_S3
) src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.ASSESSED_AT = src.ASSESSED_AT
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, ASSESSED_AT, RISK_DATA
) VALUES (
    src.CUSTOMER_ID, src.ASSESSED_AT, src.RISK_DATA
);


-- ============================================================
-- 7. LOCAL TESTING FALLBACK (Internal Stage + COPY INTO)
-- ============================================================
-- If you don't have an S3 bucket, use the internal stage approach
-- below instead of sections 1, 3, and 5 above.
/*
-- Create internal stage:
CREATE STAGE IF NOT EXISTS CSV_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT     = 'Internal stage for local CSV ingestion';

-- Upload CSVs from local (run in SnowSQL or Snowflake CLI):
-- PUT file://./csv_output/transactions.csv      @RAW.CSV_STAGE/transactions      AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://./csv_output/support_tickets.csv   @RAW.CSV_STAGE/support_tickets   AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://./csv_output/risk_assessments.csv  @RAW.CSV_STAGE/risk_assessments  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Load into landing tables:
COPY INTO TRANSACTIONS_S3 (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
                            MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED)
FROM @CSV_STAGE/transactions
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
FORCE = TRUE ON_ERROR = 'CONTINUE';

COPY INTO SUPPORT_TICKETS_S3 (CUSTOMER_ID, CREATED_AT, SUBJECT, PRIORITY,
                               BODY, RESOLUTION_STATUS, ASSIGNED_TO)
FROM @CSV_STAGE/support_tickets
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
FORCE = TRUE ON_ERROR = 'CONTINUE';

COPY INTO RISK_ASSESSMENTS_S3 (CUSTOMER_ID, ASSESSED_AT, RISK_DATA_RAW)
FROM @CSV_STAGE/risk_assessments
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
FORCE = TRUE ON_ERROR = 'CONTINUE';

-- Then run section 6 (MERGE INTO BASE) above.
*/


-- ============================================================
-- 8. VERIFY
-- ============================================================

-- Snowpipe status
SELECT SYSTEM$PIPE_STATUS('RAW.PIPE_TRANSACTIONS_S3')      AS TRANSACTIONS_PIPE;
SELECT SYSTEM$PIPE_STATUS('RAW.PIPE_SUPPORT_TICKETS_S3')    AS SUPPORT_TICKETS_PIPE;
SELECT SYSTEM$PIPE_STATUS('RAW.PIPE_RISK_ASSESSMENTS_S3')   AS RISK_ASSESSMENTS_PIPE;

-- Landing table counts (from Snowpipe)
SELECT 'TRANSACTIONS_S3'      AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM TRANSACTIONS_S3
UNION ALL SELECT 'SUPPORT_TICKETS_S3',   COUNT(*) FROM SUPPORT_TICKETS_S3
UNION ALL SELECT 'RISK_ASSESSMENTS_S3',  COUNT(*) FROM RISK_ASSESSMENTS_S3
ORDER BY TABLE_NAME;

-- BASE table counts (after MERGE — should include Snowpipe-sourced rows)
SELECT 'BASE.TRANSACTIONS'      AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BASE.TRANSACTIONS
UNION ALL SELECT 'BASE.SUPPORT_TICKETS',   COUNT(*) FROM BASE.SUPPORT_TICKETS
UNION ALL SELECT 'BASE.RISK_ASSESSMENTS',  COUNT(*) FROM BASE.RISK_ASSESSMENTS
ORDER BY TABLE_NAME;
