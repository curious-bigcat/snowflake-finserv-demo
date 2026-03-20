/*=============================================================================
  FINSERV DEMO — Step 14: Snowflake-Managed MCP Server
  Creates a managed MCP server exposing Cortex Search, SQL execution,
  and custom UDF/SP tools via the Model Context Protocol standard.

  Prerequisites:
    - Cortex Search services created (file 13)
    - UDFs and SPs created (file 10)

  Replaces the previous custom Python MCP server (14_mcp_server.py) with
  a native Snowflake-managed MCP server — no external infrastructure needed.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. CREATE THE MANAGED MCP SERVER
-- ============================================================

CREATE OR REPLACE MCP SERVER CONSUMPTION.FINSERV_MCP_SERVER
FROM SPECIFICATION
$$
tools:

  - name: "search_support_tickets"
    type: "CORTEX_SEARCH_SERVICE_QUERY"
    identifier: "FINSERV_DB.CONSUMPTION.SEARCH_SUPPORT_TICKETS"
    title: "Support Ticket Search"
    description: >
      Search customer support tickets, complaints, and service requests.
      Use for finding specific issues, complaint patterns, or ticket details.
      Returns ticket ID, subject, body, priority, resolution status, and customer name.

  - name: "search_compliance_docs"
    type: "CORTEX_SEARCH_SERVICE_QUERY"
    identifier: "FINSERV_DB.CONSUMPTION.SEARCH_COMPLIANCE_DOCS"
    title: "Compliance Document Search"
    description: >
      Search compliance and regulatory documents for policies, regulations,
      KYC/AML requirements, and compliance guidelines. Returns doc ID, type,
      content, regulatory body, and status.

  - name: "sql_executor"
    type: "SYSTEM_EXECUTE_SQL"
    title: "SQL Execution Tool"
    description: >
      Execute SQL queries against the FINSERV_DB database. Use for ad-hoc
      analytics, aggregations, and data exploration across all schemas
      (BASE, RAW, CURATED, CONSUMPTION).

  - name: "anomaly_score"
    type: "GENERIC"
    identifier: "FINSERV_DB.CONSUMPTION.ANOMALY_SCORE"
    title: "Transaction Anomaly Scorer"
    description: >
      Calculate a z-score anomaly indicator for a transaction amount given
      the average and standard deviation. Returns a float — higher values
      indicate more anomalous transactions.
    config:
      type: "function"
      warehouse: "FINSERV_WH"
      input_schema:
        type: "object"
        properties:
          amount:
            description: "The transaction amount to evaluate"
            type: "number"
          avg_amount:
            description: "The average transaction amount for the customer"
            type: "number"
          stddev_amount:
            description: "The standard deviation of transaction amounts"
            type: "number"

  - name: "risk_tier"
    type: "GENERIC"
    identifier: "FINSERV_DB.CONSUMPTION.RISK_TIER"
    title: "Customer Risk Tier Calculator"
    description: >
      Compute a composite risk tier (CRITICAL, HIGH, MEDIUM, LOW, UNKNOWN)
      from a customer's risk score, debt-to-income ratio, and flagged
      transaction count.
    config:
      type: "function"
      warehouse: "FINSERV_WH"
      input_schema:
        type: "object"
        properties:
          risk_score:
            description: "Customer risk score (0-100)"
            type: "number"
          debt_ratio:
            description: "Debt-to-income ratio (0.0-1.0)"
            type: "number"
          flagged_count:
            description: "Number of flagged transactions"
            type: "number"

  - name: "rfm_segmentation"
    type: "GENERIC"
    identifier: "FINSERV_DB.CONSUMPTION.SP_RFM_SEGMENTATION"
    title: "RFM Customer Segmentation"
    description: >
      Run Recency-Frequency-Monetary segmentation on all customers.
      Writes results to CONSUMPTION.CUSTOMER_RFM and returns a success
      message with the count of segmented customers.
    config:
      type: "procedure"
      warehouse: "FINSERV_WH"
      input_schema:
        type: "object"
        properties: {}
$$;


-- ============================================================
-- 2. VERIFY THE MCP SERVER
-- ============================================================

SHOW MCP SERVERS IN SCHEMA CONSUMPTION;

DESCRIBE MCP SERVER CONSUMPTION.FINSERV_MCP_SERVER;


-- ============================================================
-- 3. GRANT ACCESS
-- ============================================================

GRANT USAGE ON MCP SERVER CONSUMPTION.FINSERV_MCP_SERVER TO ROLE ACCOUNTADMIN;


-- ============================================================
-- 4. CONNECTION DETAILS
-- ============================================================

/*
  To connect an MCP client (Claude Desktop, Cursor, Cortex Code, etc.):

  1. Get the MCP server endpoint URL from DESCRIBE output above
  2. Configure your MCP client with:
     - Transport: Streamable HTTP
     - URL: https://<account>.snowflakecomputing.com/api/v2/cortex/mcp/FINSERV_DB/CONSUMPTION/FINSERV_MCP_SERVER/sse
     - Auth: OAuth (Snowflake built-in) or Programmatic Access Token (PAT)

  3. Available tools the client will discover:
     - search_support_tickets  — Cortex Search on support tickets
     - search_compliance_docs  — Cortex Search on compliance documents
     - sql_executor            — Execute SQL against FINSERV_DB
     - anomaly_score           — Z-score anomaly detection UDF
     - risk_tier               — Composite risk tier UDF
     - rfm_segmentation        — RFM segmentation stored procedure
*/
