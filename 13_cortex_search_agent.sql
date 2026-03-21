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


-- ============================================================
-- 5. CORTEX AGENT — FINSERV_AGENT (Create via Snowsight UI)
-- ============================================================

/*
  Create a Cortex Agent in Snowsight: Snowsight → AI & ML → Cortex Agent → + Create

  ┌──────────────────────────────────────────────────────────────────────┐
  │  AGENT CONFIGURATION                                                │
  ├──────────────────────────────────────────────────────────────────────┤
  │  Name:       FINSERV_AGENT                                          │
  │  Database:   FINSERV_DB                                             │
  │  Schema:     CONSUMPTION                                            │
  │  Warehouse:  FINSERV_WH                                             │
  └──────────────────────────────────────────────────────────────────────┘

  ── DESCRIPTION ──────────────────────────────────────────────────────────

  Financial Services AI Agent for the FINSERV demo platform. This agent
  answers business questions by routing to the appropriate tool:

  • Cortex Analyst — Generates SQL from natural language to query structured
    data across 4 consumption tables: customer 360 profiles, daily financial
    metrics, channel performance, and risk dashboards. Powered by a semantic
    model that maps business terms to physical columns.

  • Cortex Search (Support Tickets) — Performs hybrid keyword + vector search
    over 1,000+ customer support tickets including subject, body, priority,
    resolution status, and assigned agent. Use for finding complaint patterns,
    specific issues, or ticket details.

  • Cortex Search (Compliance Docs) — Searches 200+ compliance and regulatory
    documents covering KYC, AML, SOX, GDPR, and PCI-DSS requirements.
    Returns document type, content, regulatory body, and status.

  The agent automatically determines which tool to use based on the question:
  structured data questions go to Analyst, unstructured text searches go to
  the appropriate Search service.

  ── TOOLS TO ADD ─────────────────────────────────────────────────────────

  Tool 1: Cortex Analyst
    Type:            Cortex Analyst
    Semantic Model:  @FINSERV_DB.CONSUMPTION.CORTEX_STAGE/12_cortex_analyst_semantic_model.yaml
    Description:     Query structured financial data — customer profiles, transactions,
                     channel performance, risk dashboards, and daily metrics.
                     Covers tables: DT_CUSTOMER_360, DT_DAILY_FINANCIAL_METRICS,
                     DT_CHANNEL_PERFORMANCE, DT_RISK_DASHBOARD.

  Tool 2: Cortex Search — Support Tickets
    Type:            Cortex Search
    Service:         FINSERV_DB.CONSUMPTION.SEARCH_SUPPORT_TICKETS
    Description:     Search customer support tickets by keyword or natural language.
                     Finds complaints, service requests, escalations, and resolution
                     details. Searchable fields: BODY (primary), SUBJECT, PRIORITY,
                     RESOLUTION_STATUS, ASSIGNED_TO, CUSTOMER_NAME.

  Tool 3: Cortex Search — Compliance Documents
    Type:            Cortex Search
    Service:         FINSERV_DB.CONSUMPTION.SEARCH_COMPLIANCE_DOCS
    Description:     Search compliance and regulatory documents for policies,
                     procedures, and requirements. Covers KYC, AML, SOX, GDPR,
                     PCI-DSS frameworks. Searchable fields: DOC_CONTENT (primary),
                     DOC_TYPE, REGULATORY_BODY, STATUS.

  ── EXAMPLE QUESTIONS (use in the "Sample Questions" field) ──────────────

  Structured data (→ Cortex Analyst):
    1.  How many customers are in each segment?
    2.  What is the total transaction volume by channel?
    3.  Show me the daily flagged transaction rate for the last 30 days.
    4.  Who are the top 10 customers by total balance?
    5.  How many customers are in the critical risk tier?
    6.  What is the average credit score by customer segment?
    7.  Which channel has the highest average transaction amount?
    8.  Show monthly revenue trends — total volume, debits, and credits.
    9.  What is the total balance across all customer segments?
    10. How many transactions were flagged today?
    11. Compare ONLINE vs POS channels by total volume and unique customers.
    12. What is the average risk score for HIGH risk tier customers?
    13. Show the top 5 dates with the highest transaction volume.
    14. How many customers have a credit score below 500?
    15. What is the total spent (debits) by PREMIUM segment customers?

  Unstructured search (→ Cortex Search):
    16. Find support tickets about unauthorized transactions.
    17. Show me all escalated tickets with HIGH priority.
    18. Search for tickets assigned to Agent_42.
    19. Find complaints about failed wire transfers.
    20. What are the most common support ticket subjects?
    21. Show me KYC compliance requirements.
    22. Find AML (anti-money laundering) policies.
    23. Search for GDPR data retention guidelines.
    24. What are the PCI-DSS requirements for card data?
    25. Find SOX audit documentation and procedures.

  Cross-tool questions (agent decides routing):
    26. I have a customer with a high risk score — find their support tickets.
    27. Which compliance docs apply to our highest-volume transaction channel?
    28. Are there any support tickets from customers in the CRITICAL risk tier?
    29. What regulations govern the data in our customer 360 profiles?
    30. Summarize flagged transaction trends and related support complaints.

  ── AFTER CREATION — VERIFY ──────────────────────────────────────────────

  Run these after the agent is created:
*/

-- Verify the agent exists
SHOW CORTEX AGENTS IN SCHEMA CONSUMPTION;

-- Describe the agent and its tools
-- DESCRIBE CORTEX AGENT CONSUMPTION.FINSERV_AGENT;

-- Grant usage to roles that need MCP access
GRANT USAGE ON CORTEX AGENT CONSUMPTION.FINSERV_AGENT TO ROLE ACCOUNTADMIN;

-- The MCP server in file 14 references this agent:
--   type: "CORTEX_AGENT_RUN"
--   identifier: "FINSERV_DB.CONSUMPTION.FINSERV_AGENT"
