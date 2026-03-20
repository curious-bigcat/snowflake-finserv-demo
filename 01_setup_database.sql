/*=============================================================================
  FINSERV DEMO — Step 01: Database & Warehouse Setup
  Creates the warehouse, database, and 4 medallion-architecture schemas.
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- 1. WAREHOUSE
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS FINSERV_WH
    WAREHOUSE_SIZE  = 'X-SMALL'
    AUTO_SUSPEND    = 120
    AUTO_RESUME     = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT         = 'Financial Services demo warehouse';

USE WAREHOUSE FINSERV_WH;

-- ============================================================
-- 2. DATABASE
-- ============================================================

CREATE DATABASE IF NOT EXISTS FINSERV_DB
    COMMENT = 'Financial Services demo — medallion architecture';

USE DATABASE FINSERV_DB;

-- ============================================================
-- 3. SCHEMAS (Medallion Layers)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS BASE
    COMMENT = 'Landing layer – raw source tables with synthetic data';

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Ingestion layer – streams, staging tables, Snowpipe targets';

CREATE SCHEMA IF NOT EXISTS CURATED
    COMMENT = 'Transformation layer – dynamic tables, materialized views';

CREATE SCHEMA IF NOT EXISTS CONSUMPTION
    COMMENT = 'Presentation layer – 360 views, feature tables, KPI aggregates';

-- ============================================================
-- 4. VERIFY
-- ============================================================

SHOW SCHEMAS IN DATABASE FINSERV_DB;

SELECT CURRENT_WAREHOUSE() AS WH,
       CURRENT_DATABASE()  AS DB,
       CURRENT_ROLE()      AS ROLE;
