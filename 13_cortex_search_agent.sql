/*=============================================================================
  FINSERV DEMO — Step 13: Cortex Search Services & Cortex Agent
  1. Upload semantic model YAML to a stage
  2. Create Cortex Search services on support tickets & compliance docs
  3. Create a Cortex Agent combining Analyst + Search tools
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. STAGE FOR SEMANTIC MODEL
-- ============================================================

CREATE STAGE IF NOT EXISTS CONSUMPTION.CORTEX_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for Cortex Analyst semantic model YAML';
-- Upload the semantic model (run from SnowSQL or Snowsight):
-- PUT file:///path/to/12_cortex_analyst_semantic_model.yaml @CONSUMPTION.CORTEX_STAGE
--   AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

-- Verify upload
-- LIST @CONSUMPTION.CORTEX_STAGE;


-- ============================================================
-- 2. CORTEX SEARCH SERVICE — Support Tickets
-- ============================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE CONSUMPTION.SEARCH_SUPPORT_TICKETS
    ON BODY
    ATTRIBUTES SUBJECT, PRIORITY, RESOLUTION_STATUS, ASSIGNED_TO
    WAREHOUSE = FINSERV_WH
    TARGET_LAG = '1 hour'
    COMMENT = 'Full-text search over customer support tickets'
AS (
    SELECT
        t.TICKET_ID::TEXT   AS TICKET_ID,
        t.SUBJECT,
        t.BODY,
        t.PRIORITY,
        t.RESOLUTION_STATUS,
        t.ASSIGNED_TO,
        t.CREATED_AT,
        c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME
    FROM BASE.SUPPORT_TICKETS t
    JOIN BASE.CUSTOMERS c ON t.CUSTOMER_ID = c.CUSTOMER_ID
);


-- ============================================================
-- 3. CORTEX SEARCH SERVICE — Compliance Documents
-- ============================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE CONSUMPTION.SEARCH_COMPLIANCE_DOCS
    ON DOC_CONTENT
    ATTRIBUTES DOC_TYPE, REGULATORY_BODY, STATUS
    WAREHOUSE = FINSERV_WH
    TARGET_LAG = '1 hour'
    COMMENT = 'Full-text search over compliance and regulatory documents'
AS (
    SELECT
        DOC_ID::TEXT        AS DOC_ID,
        DOC_TYPE,
        DOC_CONTENT,
        METADATA:regulatory_body::TEXT  AS REGULATORY_BODY,
        METADATA:status::TEXT           AS STATUS,
        METADATA:effective_date::DATE   AS EFFECTIVE_DATE,
        CREATED_AT
    FROM BASE.COMPLIANCE_DOCUMENTS
);


-- ============================================================
-- 4. VERIFY SEARCH SERVICES
-- ============================================================

SHOW CORTEX SEARCH SERVICES IN SCHEMA CONSUMPTION;

-- Test search (support tickets)
-- SELECT SNOWFLAKE.CORTEX.SEARCH(
--     'FINSERV_DB.CONSUMPTION.SEARCH_SUPPORT_TICKETS',
--     'unauthorized transaction',
--     { 'limit': 5 }
-- );

-- Test search (compliance docs)
-- SELECT SNOWFLAKE.CORTEX.SEARCH(
--     'FINSERV_DB.CONSUMPTION.SEARCH_COMPLIANCE_DOCS',
--     'KYC requirements',
--     { 'limit': 5 }
-- );
