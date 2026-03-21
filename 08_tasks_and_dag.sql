/*=============================================================================
  FINSERV DEMO — Step 8: Tasks & DAG
  Event-driven task DAG for work that Dynamic Tables cannot handle:
    - Flagged transaction alerting (stream → filter → alert table)
    - High-priority ticket escalation (stream → filter → escalation table)
    - Pipeline metrics snapshot (aggregate → daily metric row)

  DAG Structure:
  ┌───────────────────────────┐
  │  TASK_ROOT_SCHEDULER      │  (Root – every 5 min)
  └──────────┬────────────────┘
             │
     ┌───────┴────────┐
     ▼                ▼
  ┌────────────────┐ ┌─────────────────────┐
  │TASK_DETECT     │ │TASK_ESCALATE        │
  │_FLAGGED_TXN    │ │_TICKETS             │
  │(stream→alerts) │ │(stream→escalations) │
  └───────┬────────┘ └──────────┬──────────┘
          │                     │
          └─────────┬───────────┘
                    ▼
  ┌──────────────────────────┐
  │ TASK_REFRESH_METRICS     │
  │ (BASE → PIPELINE_METRICS)│
  └──────────────────────────┘

  NOTE: Dynamic Tables handle the curated/consumption pipeline automatically.
        This DAG handles event-driven side effects only.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. ROOT TASK: Scheduler
-- ============================================================

CREATE OR REPLACE TASK RAW.TASK_ROOT_SCHEDULER
    WAREHOUSE = FINSERV_WH
    SCHEDULE  = '5 MINUTE'
    COMMENT   = 'Root task: triggers event-driven child tasks every 5 minutes'
AS
    SELECT 1;


-- ============================================================
-- 2. CHILD: Detect Flagged Transactions → Alerts
--    Reads new transactions from stream, filters for IS_FLAGGED,
--    and writes to RAW.TRANSACTION_ALERTS.
-- ============================================================

CREATE OR REPLACE TASK RAW.TASK_DETECT_FLAGGED_TXN
    WAREHOUSE = FINSERV_WH
    AFTER RAW.TASK_ROOT_SCHEDULER
    WHEN SYSTEM$STREAM_HAS_DATA('FINSERV_DB.RAW.TRANSACTIONS_STREAM')
AS
    INSERT INTO RAW.TRANSACTION_ALERTS (
        TXN_ID, ACCOUNT_ID, TXN_DATE, AMOUNT,
        MERCHANT_NAME, CATEGORY, CHANNEL, ALERT_REASON
    )
    SELECT
        TXN_ID,
        ACCOUNT_ID,
        TXN_DATE,
        AMOUNT,
        MERCHANT_NAME,
        CATEGORY,
        CHANNEL,
        CASE
            WHEN AMOUNT > 5000 THEN 'HIGH_VALUE_FLAGGED'
            WHEN MERCHANT_NAME ILIKE '%offshore%' THEN 'SUSPICIOUS_MERCHANT'
            ELSE 'FLAGGED_TRANSACTION'
        END AS ALERT_REASON
    FROM RAW.TRANSACTIONS_STREAM
    WHERE IS_FLAGGED = TRUE;


-- ============================================================
-- 3. CHILD: Escalate High-Priority Tickets
--    Reads new/updated tickets from stream, filters for
--    HIGH/URGENT priority that are still OPEN, writes to
--    RAW.TICKET_ESCALATIONS.
-- ============================================================

CREATE OR REPLACE TASK RAW.TASK_ESCALATE_TICKETS
    WAREHOUSE = FINSERV_WH
    AFTER RAW.TASK_ROOT_SCHEDULER
    WHEN SYSTEM$STREAM_HAS_DATA('FINSERV_DB.RAW.SUPPORT_TICKETS_STREAM')
AS
    INSERT INTO RAW.TICKET_ESCALATIONS (
        TICKET_ID, CUSTOMER_ID, SUBJECT, PRIORITY,
        RESOLUTION_STATUS, ASSIGNED_TO, ESCALATION_REASON
    )
    SELECT
        TICKET_ID,
        CUSTOMER_ID,
        SUBJECT,
        PRIORITY,
        RESOLUTION_STATUS,
        ASSIGNED_TO,
        CASE
            WHEN PRIORITY = 'URGENT' THEN 'URGENT_PRIORITY'
            WHEN PRIORITY = 'HIGH' THEN 'HIGH_PRIORITY'
            ELSE 'ESCALATION_REVIEW'
        END AS ESCALATION_REASON
    FROM RAW.SUPPORT_TICKETS_STREAM
    WHERE PRIORITY IN ('HIGH', 'URGENT')
      AND METADATA$ACTION = 'INSERT';


-- ============================================================
-- 4. GRANDCHILD: Refresh Pipeline Metrics
--    Aggregates current-state counts from BASE into a daily
--    metrics snapshot in CONSUMPTION.PIPELINE_METRICS.
-- ============================================================

CREATE OR REPLACE TASK RAW.TASK_REFRESH_METRICS
    WAREHOUSE = FINSERV_WH
    AFTER RAW.TASK_DETECT_FLAGGED_TXN, RAW.TASK_ESCALATE_TICKETS
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

ALTER TASK RAW.TASK_REFRESH_METRICS    RESUME;
ALTER TASK RAW.TASK_ESCALATE_TICKETS   RESUME;
ALTER TASK RAW.TASK_DETECT_FLAGGED_TXN RESUME;
ALTER TASK RAW.TASK_ROOT_SCHEDULER     RESUME;


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

-- Verify alert/escalation tables have data after initial run
SELECT 'TRANSACTION_ALERTS'  AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.TRANSACTION_ALERTS
UNION ALL
SELECT 'TICKET_ESCALATIONS', COUNT(*) FROM RAW.TICKET_ESCALATIONS
ORDER BY TABLE_NAME;
