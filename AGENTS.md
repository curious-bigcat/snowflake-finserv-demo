# AGENTS.md — Cortex Code Automation Guide

## Project Overview

Deploy and validate a full-stack Snowflake financial services demo (FINSERV) end-to-end.
The project implements a **medallion architecture** (BASE → RAW → CURATED → CONSUMPTION) with
20 files covering data engineering, Cortex AI, MCP, Streamlit, ML, and performance optimization.

## Connection

- **Connection**: `default`
- **Role**: ACCOUNTADMIN
- **Always specify**: `connection=default` on every `snowflake_sql_execute` call

## Execution Order

Execute files sequentially. Deploy each SQL object **individually** (not full files at once)
to enable targeted error handling.

### Phase 1: Foundation

**File 01 — `01_setup_database.sql`**
- Creates: `FINSERV_WH` (X-SMALL), `FINSERV_DB`, schemas BASE/RAW/CURATED/CONSUMPTION
- Verify: `SHOW SCHEMAS IN DATABASE FINSERV_DB` returns 4 custom schemas + PUBLIC/INFORMATION_SCHEMA

**File 02 — `02_base_tables_and_data.sql`**
- Creates 7 tables with GENERATOR() synthetic data
- Expected row counts: CUSTOMERS=2000, ACCOUNTS=3000, TRANSACTIONS=10000, RISK_ASSESSMENTS=2000, MARKET_DATA=5000, SUPPORT_TICKETS=1000, COMPLIANCE_DOCUMENTS=200
- Verify: `SELECT table_name, row_count FROM FINSERV_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='BASE'`
- **Important**: Note actual VARIANT schemas for later files:
  - RISK_ASSESSMENTS.RISK_DATA: `{risk_score, credit_history (string), debt_to_income, risk_factors (array of {factor, score}), assessment_type, model_version}`
  - MARKET_DATA.MARKET_DATA: `{open, high, low, close, volume, indicators: {rsi, macd, moving_avg_50}}`
  - COMPLIANCE_DOCUMENTS.METADATA: `{regulatory_body, status, version, effective_date, review_cycle, classification}`

### Phase 2: Ingestion

**File 04 — `04_s3_stage_and_snowpipe.sql`** (partial deploy)
- Deploy ONLY: `CSV_FORMAT` file format, 3 landing tables (`CUSTOMERS_S3`, `TRANSACTIONS_S3`, `RISK_ASSESSMENTS_S3`)
- **Skip**: Storage integration, external stage, and Snowpipe creation (no real S3 bucket)
- File 03 (`03_csv_generator_and_s3_upload.py`) is local Python only — do not execute in Snowflake

### Phase 3: Medallion Pipeline

**File 05 — `05_raw_layer.sql`**
- Creates 7 streams on BASE tables (with SHOW_INITIAL_ROWS = TRUE) + 3 staging tables
- Verify: All streams return `True` from `SYSTEM$STREAM_HAS_DATA()`

**File 06 — `06_curated_layer.sql`** ⚠️ REQUIRES FIXES
- Apply these column name corrections before executing:
  - `ASSESSMENT_DATE` → `ASSESSED_AT`
  - `RISK_DATA:risk_level` → `RISK_DATA:credit_history`
  - `RISK_DATA:factors` → `RISK_DATA:risk_factors`
  - `RISK_DATA:credit_history.missed_payments` → REMOVE (credit_history is a string, not an object)
  - `RISK_DATA:credit_history.avg_balance` → REMOVE
  - `RISK_DATA:indicators.sma_20` → `indicators:moving_avg_50`
- For `MV_MARKET_LATEST`: Simplify by removing correlated subquery; just parse all VARIANT data
- For `DT_RISK_FACTORS_PARSED`: Parse factors via `f.VALUE:factor::VARCHAR` and `f.VALUE:score::INT`
- Verify row counts: DT_CUSTOMER_PROFILE=2000, DT_TRANSACTION_ENRICHED=10000, DT_SUPPORT_ENRICHED=1000, MV_MARKET_LATEST=5000, DT_RISK_FACTORS_PARSED=6000

**File 07 — `07_consumption_layer.sql`** ⚠️ REQUIRES FIXES
- Apply these column reference corrections (cascading from curated layer fixes):
  - `LATEST_RISK_LEVEL` → `LATEST_CREDIT_HISTORY`
  - `AVG_TRANSACTION_AMOUNT` → `AVG_TRANSACTION`
  - `FLAGGED_RATE_PCT` → `FLAGGED_RATE`
  - `COUNT(*) AS ROWS` → `COUNT(*) AS ROW_COUNT` (ROWS is reserved)
  - Add `TOTAL_TRANSACTION_VOLUME` and `ANNUAL_INCOME` columns where referenced
  - `UNIQUE_CUSTOMERS` alias in DT_CHANNEL_PERFORMANCE → `UNIQUE_ACCOUNTS`
  - DT_MONTHLY_REVENUE aggregation column names must match DT_DAILY_FINANCIAL_METRICS output
- Verify row counts: DT_CUSTOMER_360=2000, DT_DAILY_FINANCIAL_METRICS=~181, DT_RISK_DASHBOARD=~1273, DT_CHANNEL_PERFORMANCE=~905, DT_CHURN_FEATURES=2000, DT_MONTHLY_REVENUE=~7

**File 08 — `08_tasks_and_dag.sql`**
- Creates 4-task DAG: ROOT_SCHEDULER (5 MIN) → PROCESS_TRANSACTIONS + PROCESS_TICKETS → REFRESH_METRICS
- Resume tasks bottom-up, then `EXECUTE TASK RAW.TASK_ROOT_SCHEDULER`
- Verify: All 4 tasks in `started` state via `SHOW TASKS IN SCHEMA RAW`

### Phase 4: Advanced SQL & Snowpark

**File 09 — `09_snowpark_sql_sheet.sql`** ⚠️ REQUIRES FIXES
- 10 advanced SQL queries (read-only, no persistent objects)
- Fix Query 3 (LATERAL FLATTEN):
  - `r.ASSESSMENT_DATE` → `r.ASSESSED_AT`
  - `r.RISK_DATA:risk_level` → `r.RISK_DATA:credit_history`
  - `r.RISK_DATA:factors` → `r.RISK_DATA:risk_factors`
  - Remove `credit_history.missed_payments` and `credit_history.credit_age_months` (credit_history is a string)
  - Fix JOIN syntax: place LATERAL FLATTEN after JOIN, not comma-joined before it
  - Filter on `credit_history = 'POOR'` instead of `risk_level IN ('HIGH', 'CRITICAL')`

**File 10 — `10_snowpark_python_sheet.sql`** ⚠️ REQUIRES FIXES
- Creates 2 SPs, 2 UDFs, 1 UDTF, 1 SP(TABLE)
- Fix `PARSE_RISK_FACTORS` UDTF:
  - `risk_data.get("factors")` → `risk_data.get("risk_factors")`
  - `risk_data.get("risk_level")` → `risk_data.get("credit_history")`
  - Output column `RISK_LEVEL` → `CREDIT_HISTORY`
- Fix `SP_RFM_SEGMENTATION`: Use explicit `.select()` on the result DataFrame before `save_as_table()` to avoid column ordering issues from joins
- Test each object after creation: CALL SPs, SELECT UDFs with sample values, JOIN UDTF with table
- File 11 (`11_snowpark_python_notebook.ipynb`) is local Jupyter only — do not execute in Snowflake

### Phase 5: Cortex AI & MCP

**File 12 — `12_cortex_analyst_semantic_model.yaml`**
- Upload to stage: `PUT file:///path/to/12_cortex_analyst_semantic_model.yaml @CONSUMPTION.CORTEX_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE`
- Or use: `snow stage copy <local-path> @FINSERV_DB.CONSUMPTION.CORTEX_STAGE --overwrite --connection default`

**File 13 — `13_cortex_search_agent.sql`**
- Creates: `CORTEX_STAGE`, `SEARCH_SUPPORT_TICKETS`, `SEARCH_COMPLIANCE_DOCS`
- **Skip Cortex Agent creation** if `CREATE CORTEX AGENT` DDL not available (region-dependent)
- Verify: `SHOW CORTEX SEARCH SERVICES IN SCHEMA CONSUMPTION` returns 2 services, both ACTIVE

**File 14 — `14_managed_mcp_server.sql`** (Snowflake-managed MCP)
- Creates: `FINSERV_MCP_SERVER` with 6 tools (2 Search, 1 SQL, 2 UDF, 1 SP)
- Verify: `SHOW MCP SERVERS IN SCHEMA CONSUMPTION` and `DESCRIBE MCP SERVER CONSUMPTION.FINSERV_MCP_SERVER`
- Note: `14_mcp_server.py` is the legacy custom Python MCP — use the SQL version instead

### Phase 6: Applications & ML (Local Only)

- File 15 (`15_streamlit_dashboard.py`): Run locally with `streamlit run 15_streamlit_dashboard.py`
- File 16 (`16_ml_churn_classification.ipynb`): Local Jupyter notebook
- File 17 (`17_ml_revenue_regression.ipynb`): Local Jupyter notebook
- These files are NOT deployable as SQL to Snowflake

### Phase 7: Validation

**File 18 — `18_monitoring_and_validation.sql`**
- Run all validation queries: row counts, stream status, DT refresh, task DAG, data quality
- **Skip**: Snowpipe status queries (sections 6-7) if pipes were not created
- Expected: All data quality checks return ISSUE_COUNT = 0

**File 19 — `19_incremental_test_data.sql`**
- Batch 1: Insert 3 customers (Aisha, Marco, Yuna), 4 accounts, 5 transactions (1 flagged), 2 tickets
- Batch 2: Insert 2 customers (Lucas, Emma), 3 transactions
- After each batch: `EXECUTE TASK RAW.TASK_ROOT_SCHEDULER` and wait 1-5 min for DT refresh
- Verify: New customers appear in DT_CUSTOMER_360, flagged transaction visible in DT_TRANSACTION_ENRICHED

### Phase 8: Performance

**File 20 — `20_performance_exploration.sql`**
- 10 sections covering EXPLAIN, warehouse sizing, clustering, search optimization, caching, spilling, query acceleration, resource monitors
- Creates: `TRANSACTIONS_CLUSTERED`, `FINSERV_WH_SMALL` (suspend after use), `FINSERV_MONITOR`, `PERFORMANCE_SUMMARY`
- Execute sections sequentially (some depend on prior sections' query history)

## Deployment Rules

1. **Execute objects individually**, not full files — enables targeted error handling
2. **Always use `connection=default`** on every snowflake_sql_execute call
3. **Apply column name fixes** in files 06, 07, 09, 10 before executing (see details above)
4. **Never use `ROWS` as an alias** — it's a reserved word; use `ROW_COUNT`
5. **All Dynamic Tables will use FULL refresh mode** unless the query is simple (this is expected, not an error)
6. **Test each SP/UDF/UDTF** immediately after creation with sample data
7. **Verify row counts** after each layer deployment matches expected values
8. **Skip S3/Snowpipe** sections if no real S3 bucket is configured
9. **Skip Cortex Agent** DDL if not available in the account region

## Validation Checklist

After full deployment, verify:

- [ ] 7 BASE tables with correct row counts
- [ ] 7 streams exist, most showing HAS_DATA = True
- [ ] 5 CURATED objects (4 DTs + 1 MV) all in ACTIVE scheduling state
- [ ] 6 CONSUMPTION DTs all in ACTIVE scheduling state
- [ ] 4-task DAG all in `started` state
- [ ] 6 Snowpark Python objects callable (2 SPs, 2 UDFs, 1 UDTF, 1 SP-TABLE)
- [ ] 2 Cortex Search services in ACTIVE state
- [ ] 1 MCP server with 6 tools
- [ ] SP_PIPELINE_SUMMARY returns 21 rows with all ROW_COUNT > 0
- [ ] All 5 data quality checks return ISSUE_COUNT = 0
- [ ] Incremental test: new customers visible in DT_CUSTOMER_360

## Lint / Type Check Commands

```bash
# SQL validation: use snowflake_sql_execute with only_compile=true
# Python linting:
ruff check 03_csv_generator_and_s3_upload.py 14_mcp_server.py 15_streamlit_dashboard.py
# Streamlit:
streamlit run 15_streamlit_dashboard.py --server.headless true
```
