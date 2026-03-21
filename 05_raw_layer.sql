/*=============================================================================
  FINSERV DEMO — Step 5: Raw Layer (Streams + Event Tables)
  Creates streams on the two most active BASE tables for event-driven
  processing via the task DAG (file 08).

  Architecture note:
    - Dynamic Tables (files 06-07) auto-refresh from BASE directly.
    - Streams + Tasks handle event-driven work that DTs cannot do:
      flagged-transaction alerting and high-priority ticket escalation.
    - No redundant staging copies of BASE data.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. STREAMS ON BASE TABLES
--    Only on tables where event-driven processing adds value.
-- ============================================================

-- Transactions stream — detect flagged/suspicious transactions in real time
CREATE OR REPLACE STREAM RAW.TRANSACTIONS_STREAM
    ON TABLE BASE.TRANSACTIONS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'Append-only stream for flagged transaction alerting';

-- Support tickets stream — detect high-priority tickets for escalation
CREATE OR REPLACE STREAM RAW.SUPPORT_TICKETS_STREAM
    ON TABLE BASE.SUPPORT_TICKETS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream for high-priority ticket escalation';


-- ============================================================
-- 2. EVENT TABLES
--    Non-redundant tables that capture derived events,
--    not copies of source data.
-- ============================================================

-- Flagged transaction alerts — populated by TASK_DETECT_FLAGGED_TXN
CREATE OR REPLACE TABLE RAW.TRANSACTION_ALERTS (
    ALERT_ID          NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    TXN_ID            INT NOT NULL,
    ACCOUNT_ID        INT NOT NULL,
    TXN_DATE          TIMESTAMP_NTZ,
    AMOUNT            NUMBER(12,2),
    MERCHANT_NAME     VARCHAR(100),
    CATEGORY          VARCHAR(50),
    CHANNEL           VARCHAR(20),
    ALERT_REASON      VARCHAR(50),
    DETECTED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    REVIEWED          BOOLEAN DEFAULT FALSE,
    CONSTRAINT PK_ALERTS PRIMARY KEY (ALERT_ID)
);

-- High-priority ticket escalations — populated by TASK_ESCALATE_TICKETS
CREATE OR REPLACE TABLE RAW.TICKET_ESCALATIONS (
    ESCALATION_ID     NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    TICKET_ID         INT NOT NULL,
    CUSTOMER_ID       INT NOT NULL,
    SUBJECT           VARCHAR(200),
    PRIORITY          VARCHAR(10),
    RESOLUTION_STATUS VARCHAR(20),
    ASSIGNED_TO       VARCHAR(50),
    ESCALATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ESCALATION_REASON VARCHAR(100),
    CONSTRAINT PK_ESCALATIONS PRIMARY KEY (ESCALATION_ID)
);


-- ============================================================
-- 3. VERIFICATION
-- ============================================================

SHOW STREAMS IN SCHEMA RAW;

SELECT SYSTEM$STREAM_HAS_DATA('RAW.TRANSACTIONS_STREAM')     AS TRANSACTIONS_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.SUPPORT_TICKETS_STREAM')  AS TICKETS_HAS_DATA;

SELECT 'RAW.TRANSACTION_ALERTS'   AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.TRANSACTION_ALERTS
UNION ALL
SELECT 'RAW.TICKET_ESCALATIONS',  COUNT(*) FROM RAW.TICKET_ESCALATIONS
ORDER BY TABLE_NAME;
