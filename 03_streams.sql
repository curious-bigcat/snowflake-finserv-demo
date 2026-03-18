/*=============================================================================
  HOL SNOWPARK DEMO — Step 3: Streams (Change Data Capture)
  Creates streams on raw tables to capture inserts, updates, and deletes.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

------------------------------------------------------
-- Streams on RAW tables
------------------------------------------------------

-- Stream on CUSTOMERS — captures new signups and profile updates
CREATE OR REPLACE STREAM RAW.CUSTOMERS_STREAM
    ON TABLE RAW.CUSTOMERS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on raw customers table';

-- Stream on ORDERS — captures new and updated orders
CREATE OR REPLACE STREAM RAW.ORDERS_STREAM
    ON TABLE RAW.ORDERS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'CDC stream on raw orders table';

-- Stream on WEBSITE_EVENTS — append-only (events are immutable)
CREATE OR REPLACE STREAM RAW.EVENTS_STREAM
    ON TABLE RAW.WEBSITE_EVENTS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
    COMMENT = 'Append-only stream on raw website events';

-- Verify streams
SHOW STREAMS IN SCHEMA RAW;

-- Check if streams have data
SELECT SYSTEM$STREAM_HAS_DATA('RAW.CUSTOMERS_STREAM') AS CUSTOMERS_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.ORDERS_STREAM')    AS ORDERS_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.EVENTS_STREAM')    AS EVENTS_HAS_DATA;
