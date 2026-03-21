/*=============================================================================
  FINSERV DEMO — Step 14: Snowflake-Managed MCP Server
  Creates a managed MCP server backed by a Cortex Agent that combines
  Cortex Analyst (semantic model) and Cortex Search tools.

  Prerequisites:
    - Cortex Search services created (file 13)
    - Semantic model uploaded to stage (file 12)
    - Cortex Agent "FINSERV_AGENT" created via Snowsight UI with:
        * Cortex Analyst tool  → semantic model on @CONSUMPTION.CORTEX_STAGE
        * Cortex Search tools  → SEARCH_SUPPORT_TICKETS, SEARCH_COMPLIANCE_DOCS

  The MCP server exposes the Cortex Agent as a single tool. The agent
  internally routes queries to the appropriate analyst/search backend.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. CREATE THE MANAGED MCP SERVER (Cortex Agent backend)
-- ============================================================

CREATE OR REPLACE MCP SERVER CONSUMPTION.FINSERV_MCP_SERVER
FROM SPECIFICATION
$$
tools:

  - name: "finserv_agent"
    type: "CORTEX_AGENT_RUN"
    identifier: "FINSERV_DB.CONSUMPTION.FINSERV_AGENT"
    title: "FinServ AI Agent"
    description: >
      AI agent for the Financial Services demo. Answers business questions
      about customers, accounts, transactions, risk, and channel performance
      using Cortex Analyst (structured data via semantic model). Also searches
      support tickets and compliance documents using Cortex Search.
      Ask natural language questions — the agent routes to the right tool.
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

  3. The client discovers a single tool:
     - finserv_agent — Routes to Cortex Analyst (structured queries) and
                       Cortex Search (support tickets, compliance docs)

  4. Example prompts the agent handles:
     - "How many customers are in each segment?"        → Cortex Analyst
     - "What is total transaction volume by channel?"    → Cortex Analyst
     - "Find tickets about failed wire transfers"        → Cortex Search
     - "Show me KYC compliance requirements"             → Cortex Search
*/
