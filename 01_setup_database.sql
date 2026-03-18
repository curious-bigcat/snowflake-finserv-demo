/*=============================================================================
  HOL SNOWPARK DEMO — Step 1: Database, Schemas & Warehouse Setup
  Run this first to create the foundational infrastructure.
=============================================================================*/

USE ROLE ACCOUNTADMIN;

-- Create warehouse for the demo
CREATE OR REPLACE WAREHOUSE HOL_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for HOL Snowpark demo';

USE WAREHOUSE HOL_WH;

-- Create the demo database
CREATE OR REPLACE DATABASE HOL_DB
    COMMENT = 'Hands-on-Lab database for Snowpark demo';

-- Create the three-layer schemas
CREATE OR REPLACE SCHEMA HOL_DB.RAW
    COMMENT = 'Landing zone for raw ingested data';

CREATE OR REPLACE SCHEMA HOL_DB.CURATED
    COMMENT = 'Cleansed, enriched, and joined data';

CREATE OR REPLACE SCHEMA HOL_DB.CONSUMPTION
    COMMENT = 'Business-ready aggregates and analytics';

-- Verify
SHOW SCHEMAS IN DATABASE HOL_DB;
