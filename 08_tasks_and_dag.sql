/*=============================================================================
  FINSERV DEMO — Step 8: Tasks & DAG
  Orchestrates stream-based processing from BASE → RAW → CONSUMPTION.

  DAG Structure:
  ┌───────────────────────────┐
  │  TASK_ROOT_SCHEDULER      │  (Root – every 5 min)
  └──────────┬────────────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
  ┌──────────────┐ ┌───────────────────┐
  │TASK_PROCESS  │ │TASK_PROCESS       │
  │_TRANSACTIONS │ │_TICKETS           │
  │(stream→raw)  │ │(stream→raw)       │
  └──────┬───────┘ └────────┬──────────┘
         │                  │
         └────────┬─────────┘
                  ▼
  ┌──────────────────────────┐
  │ TASK_REFRESH_METRICS     │
  │ (refresh consumption)    │
  └──────────────────────────┘

  NOTE: All tasks in RAW schema (AFTER predecessors must share schema).
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- Ensure target tables exist for stream processing
-- ============================================================

CREATE TABLE IF NOT EXISTS RAW.TRANSACTIONS_RAW (
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

CREATE TABLE IF NOT EXISTS RAW.SUPPORT_TICKETS_RAW (
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
-- 1. ROOT TASK: Scheduler
-- ============================================================

CREATE OR REPLACE TASK RAW.TASK_ROOT_SCHEDULER
    WAREHOUSE = FINSERV_WH
    SCHEDULE  = '5 MINUTE'
    COMMENT   = 'Root task: triggers child stream processors every 5 minutes'
AS
    SELECT 1;


-- ============================================================
-- 2. CHILD: Process Transactions Stream → RAW
-- ============================================================

CREATE OR REPLACE TASK RAW.TASK_PROCESS_TRANSACTIONS
    WAREHOUSE = FINSERV_WH
    AFTER RAW.TASK_ROOT_SCHEDULER
    WHEN SYSTEM$STREAM_HAS_DATA('FINSERV_DB.RAW.TRANSACTIONS_STREAM')
AS
    INSERT INTO RAW.TRANSACTIONS_RAW (
        TXN_ID, ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
        MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED, STREAM_ACTION
    )
    SELECT
        TXN_ID, ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT,
        MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED,
        METADATA$ACTION
    FROM RAW.TRANSACTIONS_STREAM;


-- ============================================================
-- 3. CHILD: Process Support Tickets Stream → RAW
-- ============================================================

CREATE OR REPLACE TASK RAW.TASK_PROCESS_TICKETS
    WAREHOUSE = FINSERV_WH
    AFTER RAW.TASK_ROOT_SCHEDULER
    WHEN SYSTEM$STREAM_HAS_DATA('FINSERV_DB.RAW.SUPPORT_TICKETS_STREAM')
AS
    MERGE INTO RAW.SUPPORT_TICKETS_RAW tgt
    USING (
        SELECT
            TICKET_ID, CUSTOMER_ID, CREATED_AT, SUBJECT, PRIORITY,
            BODY, RESOLUTION_STATUS, ASSIGNED_TO,
            METADATA$ACTION AS STREAM_ACTION
        FROM RAW.SUPPORT_TICKETS_STREAM
    ) src
    ON tgt.TICKET_ID = src.TICKET_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.RESOLUTION_STATUS = src.RESOLUTION_STATUS,
        tgt.ASSIGNED_TO       = src.ASSIGNED_TO,
        tgt.LOADED_AT         = CURRENT_TIMESTAMP(),
        tgt.STREAM_ACTION     = src.STREAM_ACTION
    WHEN NOT MATCHED THEN INSERT (
        TICKET_ID, CUSTOMER_ID, CREATED_AT, SUBJECT, PRIORITY,
        BODY, RESOLUTION_STATUS, ASSIGNED_TO, STREAM_ACTION
    ) VALUES (
        src.TICKET_ID, src.CUSTOMER_ID, src.CREATED_AT, src.SUBJECT, src.PRIORITY,
        src.BODY, src.RESOLUTION_STATUS, src.ASSIGNED_TO, src.STREAM_ACTION
    );


-- ============================================================
-- 4. GRANDCHILD: Refresh Consumption Metrics
-- ============================================================

CREATE OR REPLACE TASK RAW.TASK_REFRESH_METRICS
    WAREHOUSE = FINSERV_WH
    AFTER RAW.TASK_PROCESS_TRANSACTIONS, RAW.TASK_PROCESS_TICKETS
AS
    MERGE INTO CONSUMPTION.PIPELINE_METRICS tgt
    USING (
        SELECT
            CURRENT_DATE()                                       AS METRIC_DATE,
            (SELECT COUNT(*) FROM BASE.CUSTOMERS)                AS TOTAL_CUSTOMERS,
            (SELECT COUNT(*) FROM BASE.ACCOUNTS)                 AS TOTAL_ACCOUNTS,
            (SELECT COUNT(*) FROM BASE.TRANSACTIONS)             AS TOTAL_TRANSACTIONS,
            (SELECT SUM(AMOUNT) FROM BASE.TRANSACTIONS)          AS TOTAL_VOLUME,
            (SELECT SUM(BALANCE) FROM BASE.ACCOUNTS WHERE STATUS = 'ACTIVE') AS AUM,
            (SELECT COUNT(*) FROM BASE.SUPPORT_TICKETS)          AS TOTAL_TICKETS
    ) src
    ON tgt.METRIC_DATE = src.METRIC_DATE
    WHEN MATCHED THEN UPDATE SET
        tgt.TOTAL_CUSTOMERS    = src.TOTAL_CUSTOMERS,
        tgt.TOTAL_ACCOUNTS     = src.TOTAL_ACCOUNTS,
        tgt.TOTAL_TRANSACTIONS = src.TOTAL_TRANSACTIONS,
        tgt.TOTAL_VOLUME       = src.TOTAL_VOLUME,
        tgt.AUM                = src.AUM,
        tgt.TOTAL_TICKETS      = src.TOTAL_TICKETS,
        tgt.REFRESHED_AT       = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        METRIC_DATE, TOTAL_CUSTOMERS, TOTAL_ACCOUNTS,
        TOTAL_TRANSACTIONS, TOTAL_VOLUME, AUM, TOTAL_TICKETS
    ) VALUES (
        src.METRIC_DATE, src.TOTAL_CUSTOMERS, src.TOTAL_ACCOUNTS,
        src.TOTAL_TRANSACTIONS, src.TOTAL_VOLUME, src.AUM, src.TOTAL_TICKETS
    );


-- ============================================================
-- 5. ENABLE THE DAG (resume bottom-up)
-- ============================================================

ALTER TASK RAW.TASK_REFRESH_METRICS       RESUME;
ALTER TASK RAW.TASK_PROCESS_TICKETS       RESUME;
ALTER TASK RAW.TASK_PROCESS_TRANSACTIONS  RESUME;
ALTER TASK RAW.TASK_ROOT_SCHEDULER        RESUME;


-- ============================================================
-- 6. MANUAL TRIGGER (for demo)
-- ============================================================

EXECUTE TASK RAW.TASK_ROOT_SCHEDULER;


-- ============================================================
-- 7. VERIFY DAG
-- ============================================================

SHOW TASKS IN SCHEMA RAW;

SELECT
    NAME,
    SCHEMA_NAME,
    STATE,
    SCHEDULE,
    PREDECESSORS,
    CONDITION
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'FINSERV_DB.RAW.TASK_ROOT_SCHEDULER',
    RECURSIVE => TRUE
));

-- Recent task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
))
ORDER BY SCHEDULED_TIME DESC;
