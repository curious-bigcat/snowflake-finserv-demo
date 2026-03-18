/*=============================================================================
  HOL SNOWPARK DEMO — Step 6: Tasks & DAG
  Creates a task DAG that processes streams and orchestrates data flow.

  NOTE: All tasks must be in the same schema for AFTER dependencies.

  DAG Structure:
  ┌─────────────────────────┐
  │  TASK_ROOT_SCHEDULER    │  (Root – runs every 5 min)
  └────────┬────────────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
  ┌──────────┐ ┌──────────────┐
  │TASK_PROC │ │TASK_PROC     │
  │_ORDERS   │ │_EVENTS       │
  │(stream→  │ │(stream→      │
  │ curated) │ │ curated)     │
  └────┬─────┘ └──────┬───────┘
       │               │
       └───────┬───────┘
               ▼
  ┌────────────────────────┐
  │ TASK_REFRESH_METRICS   │
  │ (curated → consumption │
  │  summary table)        │
  └────────────────────────┘
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

------------------------------------------------------
-- Root Task: Scheduler (runs every 5 minutes)
------------------------------------------------------
CREATE OR REPLACE TASK RAW.TASK_ROOT_SCHEDULER
    WAREHOUSE = HOL_WH
    SCHEDULE  = '5 MINUTE'
    COMMENT   = 'Root task: checks streams and triggers child tasks'
AS
    SELECT 1;  -- Placeholder; children run when root succeeds

------------------------------------------------------
-- Child Task 1: Process Orders Stream → Curated
-- NOTE: WHEN clause must be last before AS
------------------------------------------------------
CREATE OR REPLACE TASK RAW.TASK_PROCESS_ORDERS
    WAREHOUSE = HOL_WH
    AFTER RAW.TASK_ROOT_SCHEDULER
    WHEN SYSTEM$STREAM_HAS_DATA('HOL_DB.RAW.ORDERS_STREAM')
AS
    MERGE INTO CURATED.ORDERS_FROM_STREAM tgt
    USING (
        SELECT
            s.ORDER_ID,
            s.CUSTOMER_ID,
            c.FIRST_NAME || ' ' || c.LAST_NAME      AS CUSTOMER_NAME,
            s.ORDER_DATE,
            s.STATUS,
            s.TOTAL_AMOUNT,
            ARRAY_SIZE(s.ORDER_DETAILS:line_items)    AS ITEM_COUNT,
            s.ORDER_DETAILS:shipping.method::VARCHAR  AS SHIPPING_METHOD,
            s.ORDER_DETAILS:payment.method::VARCHAR   AS PAYMENT_METHOD,
            s.METADATA$ACTION                         AS STREAM_ACTION
        FROM RAW.ORDERS_STREAM s
        JOIN RAW.CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID
    ) src
    ON tgt.ORDER_ID = src.ORDER_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.STATUS          = src.STATUS,
        tgt.TOTAL_AMOUNT    = src.TOTAL_AMOUNT,
        tgt.ITEM_COUNT      = src.ITEM_COUNT,
        tgt.SHIPPING_METHOD = src.SHIPPING_METHOD,
        tgt.PAYMENT_METHOD  = src.PAYMENT_METHOD,
        tgt.PROCESSED_AT    = CURRENT_TIMESTAMP(),
        tgt.STREAM_ACTION   = src.STREAM_ACTION
    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID, CUSTOMER_ID, CUSTOMER_NAME, ORDER_DATE,
        STATUS, TOTAL_AMOUNT, ITEM_COUNT,
        SHIPPING_METHOD, PAYMENT_METHOD, STREAM_ACTION
    ) VALUES (
        src.ORDER_ID, src.CUSTOMER_ID, src.CUSTOMER_NAME, src.ORDER_DATE,
        src.STATUS, src.TOTAL_AMOUNT, src.ITEM_COUNT,
        src.SHIPPING_METHOD, src.PAYMENT_METHOD, src.STREAM_ACTION
    );

------------------------------------------------------
-- Child Task 2: Process Events Stream → Curated
------------------------------------------------------
CREATE OR REPLACE TASK RAW.TASK_PROCESS_EVENTS
    WAREHOUSE = HOL_WH
    AFTER RAW.TASK_ROOT_SCHEDULER
    WHEN SYSTEM$STREAM_HAS_DATA('HOL_DB.RAW.EVENTS_STREAM')
AS
    INSERT INTO CURATED.EVENTS_FROM_STREAM (
        EVENT_ID, EVENT_TIME, CUSTOMER_ID, CUSTOMER_NAME,
        EVENT_TYPE, PAGE_URL, DEVICE_TYPE, BROWSER, SESSION_ID
    )
    SELECT
        s.EVENT_ID,
        s.EVENT_TIME,
        s.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME,
        s.EVENT_TYPE,
        s.EVENT_DATA:page::VARCHAR,
        s.EVENT_DATA:device.type::VARCHAR,
        s.EVENT_DATA:device.browser::VARCHAR,
        s.EVENT_DATA:session_id::VARCHAR
    FROM RAW.EVENTS_STREAM s
    LEFT JOIN RAW.CUSTOMERS c ON s.CUSTOMER_ID = c.CUSTOMER_ID;

------------------------------------------------------
-- Grandchild Task: Refresh Consumption Metrics
-- Runs after both stream processors complete
------------------------------------------------------
CREATE OR REPLACE TABLE CONSUMPTION.PIPELINE_METRICS (
    METRIC_DATE        DATE,
    TOTAL_ORDERS       INT,
    TOTAL_REVENUE      NUMBER(12,2),
    TOTAL_CUSTOMERS    INT,
    AVG_ORDER_VALUE    NUMBER(12,2),
    TOTAL_EVENTS       INT,
    UNIQUE_SESSIONS    INT,
    REFRESHED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TASK RAW.TASK_REFRESH_METRICS
    WAREHOUSE = HOL_WH
    AFTER RAW.TASK_PROCESS_ORDERS, RAW.TASK_PROCESS_EVENTS
AS
    MERGE INTO CONSUMPTION.PIPELINE_METRICS tgt
    USING (
        SELECT
            CURRENT_DATE()                                AS METRIC_DATE,
            (SELECT COUNT(*)       FROM RAW.ORDERS)       AS TOTAL_ORDERS,
            (SELECT SUM(TOTAL_AMOUNT) FROM RAW.ORDERS)   AS TOTAL_REVENUE,
            (SELECT COUNT(*)       FROM RAW.CUSTOMERS)    AS TOTAL_CUSTOMERS,
            (SELECT AVG(TOTAL_AMOUNT) FROM RAW.ORDERS)   AS AVG_ORDER_VALUE,
            (SELECT COUNT(*)       FROM RAW.WEBSITE_EVENTS) AS TOTAL_EVENTS,
            (SELECT COUNT(DISTINCT EVENT_DATA:session_id::VARCHAR)
             FROM RAW.WEBSITE_EVENTS)                     AS UNIQUE_SESSIONS
    ) src
    ON tgt.METRIC_DATE = src.METRIC_DATE
    WHEN MATCHED THEN UPDATE SET
        tgt.TOTAL_ORDERS    = src.TOTAL_ORDERS,
        tgt.TOTAL_REVENUE   = src.TOTAL_REVENUE,
        tgt.TOTAL_CUSTOMERS = src.TOTAL_CUSTOMERS,
        tgt.AVG_ORDER_VALUE = src.AVG_ORDER_VALUE,
        tgt.TOTAL_EVENTS    = src.TOTAL_EVENTS,
        tgt.UNIQUE_SESSIONS = src.UNIQUE_SESSIONS,
        tgt.REFRESHED_AT    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        METRIC_DATE, TOTAL_ORDERS, TOTAL_REVENUE, TOTAL_CUSTOMERS,
        AVG_ORDER_VALUE, TOTAL_EVENTS, UNIQUE_SESSIONS
    ) VALUES (
        src.METRIC_DATE, src.TOTAL_ORDERS, src.TOTAL_REVENUE, src.TOTAL_CUSTOMERS,
        src.AVG_ORDER_VALUE, src.TOTAL_EVENTS, src.UNIQUE_SESSIONS
    );

------------------------------------------------------
-- Enable the DAG (resume tasks bottom-up)
------------------------------------------------------
ALTER TASK RAW.TASK_REFRESH_METRICS RESUME;
ALTER TASK RAW.TASK_PROCESS_EVENTS  RESUME;
ALTER TASK RAW.TASK_PROCESS_ORDERS  RESUME;
ALTER TASK RAW.TASK_ROOT_SCHEDULER  RESUME;

------------------------------------------------------
-- Manually trigger one run for the demo
------------------------------------------------------
EXECUTE TASK RAW.TASK_ROOT_SCHEDULER;

------------------------------------------------------
-- Verify DAG
------------------------------------------------------
SHOW TASKS IN SCHEMA RAW;

-- View task dependency graph
SELECT
    NAME,
    SCHEMA_NAME,
    STATE,
    SCHEDULE,
    PREDECESSORS,
    CONDITION
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'HOL_DB.RAW.TASK_ROOT_SCHEDULER',
    RECURSIVE => TRUE
));

-- Check recent task runs
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
))
ORDER BY SCHEDULED_TIME DESC;
