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

    {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ObjectLevelActions",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion"
            ],
            "Resource": "arn:aws:s3:::bsuresh-s3-finserv-integration/*"
        },
        {
            "Sid": "BucketLevelActions",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::bsuresh-s3-finserv-integration"
        }
    ]
}
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
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::484577546576:role/s3-finserv-role'
    ENABLED                   = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://bsuresh-s3-finserv-integration/');

-- Retrieve the values needed for your IAM trust policy:
DESC STORAGE INTEGRATION S3_FINSERV_INTEGRATION;
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
    URL                 = 's3://bsuresh-s3-finserv-integration/'
    FILE_FORMAT         = CSV_FORMAT
    COMMENT             = 'External S3 stage for Snowpipe auto-ingest';

ls @S3_FINSERV_STAGE;
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
    _SOURCE_FILE    VARCHAR(500)  DEFAULT NULL
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
    _SOURCE_FILE       VARCHAR(500)  DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS RISK_ASSESSMENTS_S3 (
    CUSTOMER_ID     NUMBER,
    ASSESSED_AT     TIMESTAMP_NTZ,
    RISK_DATA_RAW   VARCHAR(16777216),
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _SOURCE_FILE    VARCHAR(500)  DEFAULT NULL
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
-- 8. SNOWPIPE STREAMING DEMO — Synthetic Data via Python SP
-- ============================================================
-- Demonstrates Snowpipe Streaming concepts using a Python stored
-- procedure that generates synthetic financial transactions and
-- inserts them in micro-batches, simulating real-time streaming
-- ingestion into the medallion pipeline.
--
-- Snowpipe Streaming enables low-latency data loading by writing
-- rows directly into Snowflake tables without staging files. Here
-- we emulate that pattern with a Python SP that produces realistic
-- financial transaction data in configurable micro-batches.
--
-- Usage:
--   CALL BASE.SP_STREAM_SYNTHETIC_TRANSACTIONS(5, 100, 1);
--   → 5 batches × 100 rows = 500 rows, 1-second delay between batches
-- ============================================================

USE SCHEMA BASE;

-- 8a. Target table for streamed data
CREATE TABLE IF NOT EXISTS STREAMING_TRANSACTIONS (
    TXN_ID          NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    ACCOUNT_ID      NUMBER,
    TXN_DATE        TIMESTAMP_NTZ,
    TXN_TYPE        VARCHAR(15),
    AMOUNT          NUMBER(12,2),
    MERCHANT_NAME   VARCHAR(100),
    CATEGORY        VARCHAR(30),
    CHANNEL         VARCHAR(15),
    IS_FLAGGED      BOOLEAN DEFAULT FALSE,
    _BATCH_ID       NUMBER,
    _STREAMED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STREAMING_TXN PRIMARY KEY (TXN_ID)
);

-- 8b. Stream on the streaming table (feeds into medallion pipeline)
CREATE STREAM IF NOT EXISTS RAW.STREAM_STREAMING_TRANSACTIONS
    ON TABLE BASE.STREAMING_TRANSACTIONS
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on Snowpipe Streaming target table';

-- 8c. Python SP — generates synthetic transactions in micro-batches
CREATE OR REPLACE PROCEDURE SP_STREAM_SYNTHETIC_TRANSACTIONS(
    NUM_BATCHES     INT,
    ROWS_PER_BATCH  INT,
    DELAY_SECONDS   INT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import time
import random
from datetime import datetime, timedelta

def run(session, num_batches: int, rows_per_batch: int, delay_seconds: int) -> str:
    """
    Generates synthetic financial transactions in micro-batches,
    emulating the Snowpipe Streaming pattern of low-latency row ingestion.

    Args:
        num_batches:    Number of micro-batches to produce
        rows_per_batch: Rows per micro-batch
        delay_seconds:  Pause between batches (simulates streaming cadence)
    """

    TXN_TYPES = ['PURCHASE', 'DEPOSIT', 'WITHDRAWAL', 'TRANSFER',
                 'PAYMENT', 'REFUND', 'FEE', 'INTEREST']
    TXN_WEIGHTS = [35, 15, 15, 10, 10, 5, 5, 5]

    MERCHANTS = [
        'Amazon', 'Walmart', 'Target', 'Costco', 'Whole Foods',
        'Starbucks', 'McDonalds', 'Uber', 'Lyft', 'Netflix',
        'Spotify', 'Apple Store', 'Google Play', 'Shell Gas',
        'BP Fuel', 'Chevron', 'Home Depot', 'Lowes', 'Best Buy',
        'Nike', 'Adidas', 'Zara', 'H&M', 'Trader Joes',
        'CVS Pharmacy', 'Walgreens', 'Delta Airlines', 'United Airlines',
        'Hilton Hotels', 'Marriott', 'Airbnb', 'DoorDash', 'Grubhub'
    ]

    CATEGORIES = ['RETAIL', 'GROCERIES', 'DINING', 'TRANSPORTATION',
                  'ENTERTAINMENT', 'UTILITIES', 'HEALTHCARE', 'TRAVEL',
                  'SUBSCRIPTION', 'FUEL', 'HOME_IMPROVEMENT', 'CLOTHING',
                  'TRANSFER', 'FEE']

    CHANNELS = ['ONLINE', 'IN_STORE', 'MOBILE', 'ATM', 'WIRE', 'ACH']
    CHANNEL_WEIGHTS = [30, 25, 25, 10, 5, 5]

    CATEGORY_MERCHANTS = {
        'RETAIL':           ['Amazon', 'Walmart', 'Target', 'Costco', 'Best Buy', 'Nike', 'Adidas'],
        'GROCERIES':        ['Whole Foods', 'Trader Joes', 'Costco', 'Walmart', 'Target'],
        'DINING':           ['Starbucks', 'McDonalds', 'DoorDash', 'Grubhub'],
        'TRANSPORTATION':   ['Uber', 'Lyft', 'Delta Airlines', 'United Airlines'],
        'ENTERTAINMENT':    ['Netflix', 'Spotify', 'Apple Store', 'Google Play'],
        'FUEL':             ['Shell Gas', 'BP Fuel', 'Chevron'],
        'HEALTHCARE':       ['CVS Pharmacy', 'Walgreens'],
        'TRAVEL':           ['Delta Airlines', 'United Airlines', 'Hilton Hotels', 'Marriott', 'Airbnb'],
        'HOME_IMPROVEMENT': ['Home Depot', 'Lowes'],
        'CLOTHING':         ['Nike', 'Adidas', 'Zara', 'H&M'],
        'SUBSCRIPTION':     ['Netflix', 'Spotify', 'Apple Store'],
    }

    AMOUNT_RANGES = {
        'PURCHASE':   (1.50, 2500.00),
        'DEPOSIT':    (100.00, 15000.00),
        'WITHDRAWAL': (20.00, 5000.00),
        'TRANSFER':   (50.00, 25000.00),
        'PAYMENT':    (25.00, 5000.00),
        'REFUND':     (5.00, 500.00),
        'FEE':        (1.00, 75.00),
        'INTEREST':   (0.50, 200.00),
    }

    total_rows = 0
    total_flagged = 0

    for batch_num in range(1, num_batches + 1):
        rows = []
        for _ in range(rows_per_batch):
            account_id = random.randint(1, 3000)
            txn_type = random.choices(TXN_TYPES, weights=TXN_WEIGHTS)[0]
            category = random.choice(CATEGORIES)

            # Pick merchant correlated with category when possible
            merchant = random.choice(
                CATEGORY_MERCHANTS.get(category, MERCHANTS)
            )

            # Amount varies by transaction type
            lo, hi = AMOUNT_RANGES.get(txn_type, (10.00, 1000.00))
            amount = round(random.uniform(lo, hi), 2)

            channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]

            # ~3% flagged rate; 15% for amounts over 5000
            is_flagged = random.random() < (0.03 if amount < 5000 else 0.15)
            if is_flagged:
                total_flagged += 1

            # Timestamp within the last 5 minutes (simulates real-time)
            txn_date = datetime.now() - timedelta(
                seconds=random.randint(0, 300)
            )

            rows.append([
                account_id,
                txn_date.strftime('%Y-%m-%d %H:%M:%S.%f'),
                txn_type,
                amount,
                merchant,
                category,
                channel,
                is_flagged,
                batch_num,
            ])

        # Insert micro-batch via SQL (bypasses column ordering issues)
        placeholders = ', '.join(['(?, ?, ?, ?, ?, ?, ?, ?, ?)'] * len(rows))
        flat_values = [v for row in rows for v in row]

        session.sql(
            f"""INSERT INTO FINSERV_DB.BASE.STREAMING_TRANSACTIONS
                (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
                 MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED, _BATCH_ID)
            SELECT
                COLUMN1::NUMBER,
                COLUMN2::TIMESTAMP_NTZ,
                COLUMN3::VARCHAR,
                COLUMN4::NUMBER(12,2),
                COLUMN5::VARCHAR,
                COLUMN6::VARCHAR,
                COLUMN7::VARCHAR,
                COLUMN8::BOOLEAN,
                COLUMN9::NUMBER
            FROM VALUES {placeholders}""",
            flat_values
        ).collect()

        total_rows += rows_per_batch

        # Delay between batches to simulate streaming cadence
        if batch_num < num_batches and delay_seconds > 0:
            time.sleep(delay_seconds)

    return (
        f"Streaming complete: {num_batches} batches, "
        f"{total_rows} total rows inserted, "
        f"{total_flagged} flagged transactions"
    )
$$;

-- 8d. Run the streaming demo (5 batches × 200 rows = 1,000 rows, 1s delay)
-- CALL BASE.SP_STREAM_SYNTHETIC_TRANSACTIONS(5, 200, 1);

-- 8e. Verify streamed data
-- SELECT _BATCH_ID, COUNT(*) AS ROW_COUNT, SUM(AMOUNT) AS BATCH_TOTAL,
--        SUM(CASE WHEN IS_FLAGGED THEN 1 ELSE 0 END) AS FLAGGED_COUNT,
--        MIN(_STREAMED_AT) AS FIRST_ARRIVAL, MAX(_STREAMED_AT) AS LAST_ARRIVAL
-- FROM BASE.STREAMING_TRANSACTIONS
-- GROUP BY _BATCH_ID
-- ORDER BY _BATCH_ID;

-- Check stream has data for downstream consumption
-- SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_STREAMING_TRANSACTIONS') AS HAS_DATA;


-- ============================================================
-- 9. VERIFY
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
