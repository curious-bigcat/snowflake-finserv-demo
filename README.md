# Snowflake Financial Services Demo (FINSERV)

Full-stack Snowflake demo project implementing a **medallion architecture** data pipeline for a fictional financial services company. Covers 20+ Snowflake features across data engineering, AI/ML, and application development.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FINSERV_DB                                        │
│                                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌────────────────┐    │
│  │   BASE   │───▶│   RAW    │───▶│   CURATED    │───▶│  CONSUMPTION   │    │
│  │          │    │          │    │              │    │                │    │
│  │ 7 tables │    │ 7 streams│    │ 4 dynamic    │    │ 6 dynamic      │    │
│  │ GENERATOR│    │ 3 staging│    │   tables     │    │   tables       │    │
│  │ data     │    │ Snowpipe │    │ 1 mat. view  │    │ 2 SPs, 2 UDFs │    │
│  │          │    │ S3 land. │    │              │    │ 1 UDTF         │    │
│  └──────────┘    └──────────┘    └──────────────┘    └────────────────┘    │
│       │                │                                      │            │
│       │           Task DAG                              MCP Server         │
│       │          (4 tasks)                          Cortex Search (2)      │
│       ▼                                             Streamlit Dashboard    │
│  CSV Generator                                      ML Notebooks (2)      │
│  S3 Upload                                          Semantic Model        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Snowflake Account** | Enterprise edition or higher (for Dynamic Tables, Cortex AI) |
| **Role** | ACCOUNTADMIN (or equivalent with CREATE WAREHOUSE, DATABASE privileges) |
| **Python** | 3.11+ (for local notebooks, CSV generator, Streamlit) |
| **Packages** | `snowflake-connector-python`, `snowflake-snowpark-python`, `streamlit`, `pandas`, `altair` |
| **Optional** | AWS S3 bucket (for Snowpipe ingestion — demo works without it) |

---

## Quick Start

```bash
# 1. Clone the repo
git clone <repo-url> && cd snowpark

# 2. Deploy core pipeline (files 01-08) in order
#    Execute each .sql file in Snowsight or via SnowSQL

# 3. Run advanced SQL patterns
#    Execute 09_snowpark_sql_sheet.sql

# 4. Deploy Snowpark Python objects
#    Execute 10_snowpark_python_sheet.sql

# 5. Deploy Cortex AI services
#    Execute 13_cortex_search_agent.sql and 14_managed_mcp_server.sql

# 6. Run the Streamlit dashboard
streamlit run 15_streamlit_dashboard.py

# 7. Validate the pipeline
#    Execute 18_monitoring_and_validation.sql

# 8. Test incremental data flow
#    Execute 19_incremental_test_data.sql

# 9. Explore performance concepts
#    Execute 20_performance_exploration.sql
```

---

## File-by-File Execution Guide

### Phase 1: Foundation (Files 01-02)

| # | File | What It Does | Objects Created |
|---|------|-------------|-----------------|
| 01 | `01_setup_database.sql` | Creates warehouse, database, and 4 medallion schemas | `FINSERV_WH` (X-SMALL), `FINSERV_DB`, schemas: `BASE`, `RAW`, `CURATED`, `CONSUMPTION` |
| 02 | `02_base_tables_and_data.sql` | Creates 7 base tables and populates them with `GENERATOR()` synthetic data | `CUSTOMERS` (2K rows), `ACCOUNTS` (3K), `TRANSACTIONS` (10K), `RISK_ASSESSMENTS` (2K, VARIANT), `MARKET_DATA` (5K, VARIANT), `SUPPORT_TICKETS` (1K, TEXT), `COMPLIANCE_DOCUMENTS` (200, TEXT+VARIANT) |

### Phase 2: Ingestion (Files 03-04)

| # | File | What It Does | Objects Created |
|---|------|-------------|-----------------|
| 03 | `03_csv_generator_and_s3_upload.py` | **Local Python** — generates CSV files and optionally uploads to S3 | CSV files (local), S3 objects (optional) |
| 04 | `04_s3_stage_and_snowpipe.sql` | Creates file format, S3 landing tables, external stage, and Snowpipe (S3 parts require real bucket) | `CSV_FORMAT`, 3 landing tables (`CUSTOMERS_S3`, `TRANSACTIONS_S3`, `RISK_ASSESSMENTS_S3`), stage, pipes |

### Phase 3: Medallion Pipeline (Files 05-08)

| # | File | What It Does | Objects Created |
|---|------|-------------|-----------------|
| 05 | `05_raw_layer.sql` | Creates 2 CDC streams + 2 event tables for alerting/escalation | `TRANSACTIONS_STREAM`, `SUPPORT_TICKETS_STREAM`, `TRANSACTION_ALERTS`, `TICKET_ESCALATIONS` |
| 06 | `06_curated_layer.sql` | Builds curated dynamic tables | `DT_CUSTOMER_PROFILE`, `DT_TRANSACTION_ENRICHED`, `DT_SUPPORT_ENRICHED`, `DT_MARKET_LATEST`, `DT_RISK_FACTORS_PARSED`, `DT_COMPLIANCE_ENRICHED` |
| 07 | `07_consumption_layer.sql` | Builds consumption-layer analytics tables | `DT_CUSTOMER_360`, `DT_DAILY_FINANCIAL_METRICS`, `DT_RISK_DASHBOARD`, `DT_CHANNEL_PERFORMANCE`, `DT_CHURN_FEATURES`, `DT_MONTHLY_REVENUE`, `DT_MARKET_OVERVIEW`, `DT_COMPLIANCE_SUMMARY`, `DT_RISK_FACTOR_SUMMARY` |
| 08 | `08_tasks_and_dag.sql` | Creates a 4-task DAG for event-driven alerting | `TASK_ROOT_SCHEDULER` → `TASK_DETECT_FLAGGED_TXN` + `TASK_ESCALATE_TICKETS` → `TASK_REFRESH_METRICS` |

### Phase 4: Advanced SQL & Snowpark (Files 09-11)

| # | File | What It Does | Objects Created |
|---|------|-------------|-----------------|
| 09 | `09_snowpark_sql_sheet.sql` | 10 advanced SQL patterns: window functions, LATERAL FLATTEN, PIVOT/UNPIVOT, CTEs, ROLLUP, percentiles, fraud detection | Read-only queries — no persistent objects |
| 10 | `10_snowpark_python_sheet.sql` | 6 Python-in-SQL objects: 2 stored procedures, 2 UDFs, 1 UDTF | `SP_RFM_SEGMENTATION`, `SP_PROCESS_TRANSACTIONS`, `ANOMALY_SCORE` UDF, `RISK_TIER` UDF, `PARSE_RISK_FACTORS` UDTF, `SP_PIPELINE_SUMMARY` |
| 11 | `11_snowpark_python_notebook.ipynb` | **Local Jupyter notebook** — Snowpark DataFrame API exploration | No Snowflake objects (runs locally) |

### Phase 5: Cortex AI & MCP (Files 12-14)

| # | File | What It Does | Objects Created |
|---|------|-------------|-----------------|
| 12 | `12_cortex_analyst_semantic_model.yaml` | Semantic model YAML for Cortex Analyst (upload to stage) | Stage file at `@CONSUMPTION.CORTEX_STAGE/` |
| 13 | `13_cortex_search_agent.sql` | Creates Cortex Search services on support tickets and compliance docs | `SEARCH_SUPPORT_TICKETS`, `SEARCH_COMPLIANCE_DOCS`, `CORTEX_STAGE` |
| 14 | `14_managed_mcp_server.sql` | Creates a **Snowflake-managed MCP server** exposing Search, SQL, and custom UDF/SP tools | `FINSERV_MCP_SERVER` with 6 tools |

> **Note:** `14_mcp_server.py` is the legacy custom Python MCP server. Use `14_managed_mcp_server.sql` instead — it requires no external infrastructure.

### Phase 6: Applications & ML (Files 15-17)

| # | File | What It Does | Objects Created |
|---|------|-------------|-----------------|
| 15 | `15_streamlit_dashboard.py` | **Local Streamlit app** — multi-tab KPI dashboard (Executive, Customers, Transactions, Risk, Channel) | Runs locally with `streamlit run` |
| 16 | `16_ml_churn_classification.ipynb` | **Local notebook** — customer churn prediction (XGBoost/Random Forest) using `DT_CHURN_FEATURES` | Trained model (local) |
| 17 | `17_ml_revenue_regression.ipynb` | **Local notebook** — revenue forecasting regression using `DT_MONTHLY_REVENUE` | Trained model (local) |

### Phase 7: Validation & Testing (Files 18-19)

| # | File | What It Does | Objects Created |
|---|------|-------------|-----------------|
| 18 | `18_monitoring_and_validation.sql` | Pipeline health checks: row counts, stream status, DT refresh history, task DAG, data quality spot checks | Read-only validation queries |
| 19 | `19_incremental_test_data.sql` | Two-batch incremental test: inserts 5 new customers, 4 accounts, 8 transactions, 2 tickets; triggers task DAG; verifies propagation | New rows in base tables, verifies DT refresh |

### Phase 8: Performance (File 20)

| # | File | What It Does | Objects Created |
|---|------|-------------|-----------------|
| 20 | `20_performance_exploration.sql` | 10 performance concepts: EXPLAIN plans, warehouse sizing, clustering, search optimization, caching, spill analysis, query acceleration, resource monitors | `TRANSACTIONS_CLUSTERED`, `FINSERV_WH_SMALL`, `FINSERV_MONITOR`, `PERFORMANCE_SUMMARY` |

---

## Snowflake Features Learning Path

A sequential curriculum for learning Snowflake features using this project. Follow the order below — each topic builds on the previous.

### Level 1: Core Platform

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 1 | **Warehouses** | Virtual Warehouses | 01 | Sizing (X-SMALL→4XL), AUTO_SUSPEND, AUTO_RESUME, INITIALLY_SUSPENDED |
| 2 | **Databases & Schemas** | Logical Organization | 01 | Namespacing, medallion architecture (BASE→RAW→CURATED→CONSUMPTION) |
| 3 | **Table DDL** | CREATE TABLE | 02 | Data types: NUMBER, VARCHAR, TIMESTAMP_NTZ, BOOLEAN, VARIANT, TEXT |
| 4 | **Synthetic Data** | GENERATOR() | 02 | UNIFORM(), RANDOM(), SEQ4(), ARRAY_CONSTRUCT(), OBJECT_CONSTRUCT() |
| 5 | **Semi-Structured Data** | VARIANT | 02 | JSON in columns, dot notation, bracket notation, type casting |

### Level 2: Data Loading

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 6 | **File Formats** | CREATE FILE FORMAT | 04 | CSV parsing: FIELD_DELIMITER, SKIP_HEADER, NULL_IF, TRIM_SPACE |
| 7 | **Stages** | External Stages (S3) | 04 | STORAGE_INTEGRATION, URL, encryption, DIRECTORY |
| 8 | **COPY INTO** | Bulk Loading | 04 | FROM stage, FILE_FORMAT, ON_ERROR, MATCH_BY_COLUMN_NAME |
| 9 | **Snowpipe** | Continuous Loading | 04 | AUTO_INGEST, SQS notifications, SYSTEM$PIPE_STATUS() |

### Level 3: Change Data Capture

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 10 | **Streams** | CDC Tracking | 05 | SHOW_INITIAL_ROWS, METADATA$ACTION, METADATA$ISUPDATE, SYSTEM$STREAM_HAS_DATA() |
| 11 | **Stream Types** | Standard vs Append-only | 05 | Standard (full CDC), Append-only (inserts only) |

### Level 4: Transformations

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 12 | **Dynamic Tables** | Declarative Pipelines | 06-07 | TARGET_LAG (1 min, 5 min, DOWNSTREAM), REFRESH_MODE (FULL vs INCREMENTAL), INITIALIZE |
| 13 | **Materialized Views** | Auto-Maintained Views | 06 | MV on VARIANT data, automatic refresh, query rewrite |
| 14 | **LATERAL FLATTEN** | JSON Array Expansion | 06, 09 | FLATTEN(INPUT =>, OUTER => TRUE), VALUE, INDEX |
| 15 | **QUALIFY** | Window Filter | 06 | ROW_NUMBER() OVER (...) with QUALIFY for deduplication |

### Level 5: Orchestration

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 16 | **Tasks** | Scheduled Execution | 08 | SCHEDULE (CRON/interval), WAREHOUSE, AFTER (predecessors) |
| 17 | **Task DAGs** | Dependency Graphs | 08 | Root→children→grandchild, WHEN conditions, EXECUTE TASK |
| 18 | **Stream + Task** | Event-Driven Processing | 08 | `WHEN SYSTEM$STREAM_HAS_DATA()`, MERGE INTO with stream |

### Level 6: Advanced SQL

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 19 | **Window Functions** | Analytics | 09 | SUM/COUNT/ROW_NUMBER OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN) |
| 20 | **PIVOT / UNPIVOT** | Reshaping Data | 09 | PIVOT (AGG FOR col IN (...)), UNPIVOT (VALUE FOR METRIC IN (...)) |
| 21 | **GROUP BY ROLLUP** | Subtotals | 09 | ROLLUP(), CUBE(), GROUPING SETS |
| 22 | **PERCENTILE_CONT** | Statistical Functions | 09 | WITHIN GROUP (ORDER BY ...), MEDIAN(), STDDEV() |

### Level 7: Snowpark Python

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 23 | **Stored Procedures** | Python SPs | 10 | LANGUAGE PYTHON, PACKAGES, HANDLER, session.table(), write.save_as_table() |
| 24 | **Scalar UDFs** | Python UDFs | 10 | RETURNS FLOAT/VARCHAR, single-row transform, pure Python |
| 25 | **Table UDFs (UDTFs)** | Python UDTFs | 10 | RETURNS TABLE(...), class with process() method, yield rows |
| 26 | **DataFrame API** | Snowpark DataFrames | 11 | col(), filter(), group_by(), agg(), join(), with_column() |

### Level 8: Cortex AI

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 27 | **Semantic Models** | Cortex Analyst | 12 | YAML schema: tables, dimensions, measures, time_dimensions, filters |
| 28 | **Cortex Search** | Vector Search | 13 | ON column, ATTRIBUTES, TARGET_LAG, embedding model |
| 29 | **Cortex Agent** | Multi-Tool Agent | 13 | Tool routing: analyst_text_to_sql, cortex_search |

### Level 9: Integration & Apps

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 30 | **MCP Server** | Managed MCP | 14 | CREATE MCP SERVER, tool types: CORTEX_SEARCH, SYSTEM_EXECUTE_SQL, GENERIC |
| 31 | **Streamlit** | Data Apps | 15 | st.connection("snowflake"), tabs, Altair charts, metrics |
| 32 | **ML Pipelines** | Model Training | 16-17 | Feature engineering from DTs, classification, regression |

### Level 10: Operations & Performance

| # | Topic | Feature | File | Key Concepts |
|---|-------|---------|------|-------------|
| 33 | **Pipeline Monitoring** | Observability | 18 | INFORMATION_SCHEMA views, SYSTEM$ functions, data quality checks |
| 34 | **Incremental Testing** | CDC Validation | 19 | Insert→stream→task→DT refresh→verify counts |
| 35 | **EXPLAIN Plans** | Query Profiling | 20 | EXPLAIN USING TABULAR/JSON, execution plan analysis |
| 36 | **Clustering** | Storage Optimization | 20 | CLUSTER BY, SYSTEM$CLUSTERING_INFORMATION, automatic clustering |
| 37 | **Search Optimization** | Point Lookup Speed | 20 | ADD SEARCH OPTIMIZATION ON EQUALITY/SUBSTRING |
| 38 | **Result Caching** | Query Cache | 20 | USE_CACHED_RESULT, PERCENTAGE_SCANNED_FROM_CACHE |
| 39 | **Resource Monitors** | Cost Guardrails | 20 | CREDIT_QUOTA, TRIGGERS, NOTIFY/SUSPEND/SUSPEND_IMMEDIATE |
| 40 | **Query Acceleration** | Elastic Compute | 20 | SYSTEM$ESTIMATE_QUERY_ACCELERATION, QUERY_ACCELERATION_ELIGIBLE |

---

## Object Inventory

### Tables (BASE)

| Table | Rows | Key Columns |
|-------|------|-------------|
| CUSTOMERS | 2,000 | CUSTOMER_ID, FIRST_NAME, LAST_NAME, CITY, COUNTRY, ANNUAL_INCOME, CREDIT_SCORE |
| ACCOUNTS | 3,000 | ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, BALANCE, CREDIT_LIMIT, STATUS |
| TRANSACTIONS | 10,000 | TXN_ID, ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT, CATEGORY, CHANNEL, IS_FLAGGED |
| RISK_ASSESSMENTS | 2,000 | ASSESSMENT_ID, CUSTOMER_ID, ASSESSED_AT, RISK_DATA (VARIANT) |
| MARKET_DATA | 5,000 | DATA_ID, TICKER, TRADE_DATE, MARKET_DATA (VARIANT) |
| SUPPORT_TICKETS | 1,000 | TICKET_ID, CUSTOMER_ID, SUBJECT, BODY (TEXT), PRIORITY, RESOLUTION_STATUS |
| COMPLIANCE_DOCUMENTS | 200 | DOC_ID, DOC_TYPE, DOC_CONTENT (TEXT), METADATA (VARIANT) |

### Dynamic Tables

| Schema | Table | TARGET_LAG | Refresh Mode | Rows |
|--------|-------|-----------|-------------|------|
| CURATED | DT_CUSTOMER_PROFILE | 1 minute | FULL | 2,000 |
| CURATED | DT_TRANSACTION_ENRICHED | 1 minute | FULL | 10,000 |
| CURATED | DT_SUPPORT_ENRICHED | 1 minute | FULL | 1,000 |
| CURATED | DT_MARKET_LATEST | 1 minute | FULL | ~50 |
| CURATED | DT_RISK_FACTORS_PARSED | 1 minute | INCREMENTAL | 6,000 |
| CURATED | DT_COMPLIANCE_ENRICHED | 1 minute | FULL | 200 |
| CONSUMPTION | DT_CUSTOMER_360 | DOWNSTREAM | FULL | 2,000 |
| CONSUMPTION | DT_DAILY_FINANCIAL_METRICS | DOWNSTREAM | FULL | 181 |
| CONSUMPTION | DT_RISK_DASHBOARD | DOWNSTREAM | FULL | 1,273 |
| CONSUMPTION | DT_CHANNEL_PERFORMANCE | DOWNSTREAM | FULL | 905 |
| CONSUMPTION | DT_CHURN_FEATURES | 5 minutes | FULL | 2,000 |
| CONSUMPTION | DT_MONTHLY_REVENUE | 5 minutes | FULL | 7 |
| CONSUMPTION | DT_MARKET_OVERVIEW | DOWNSTREAM | FULL | ~50 |
| CONSUMPTION | DT_COMPLIANCE_SUMMARY | DOWNSTREAM | FULL | ~10 |
| CONSUMPTION | DT_RISK_FACTOR_SUMMARY | DOWNSTREAM | FULL | ~20 |

### Streams (RAW)

TRANSACTIONS_STREAM, SUPPORT_TICKETS_STREAM

### Event Tables (RAW)

| Table | Purpose |
|-------|---------|
| TRANSACTION_ALERTS | Flagged transactions detected by stream-driven task |
| TICKET_ESCALATIONS | High/urgent tickets escalated by stream-driven task |

### Tasks (RAW)

| Task | Schedule | Predecessors |
|------|----------|-------------|
| TASK_ROOT_SCHEDULER | 5 MINUTE | — |
| TASK_DETECT_FLAGGED_TXN | — | TASK_ROOT_SCHEDULER |
| TASK_ESCALATE_TICKETS | — | TASK_ROOT_SCHEDULER |
| TASK_REFRESH_METRICS | — | TASK_DETECT_FLAGGED_TXN, TASK_ESCALATE_TICKETS |

### Cortex AI Services

| Object | Type | Schema |
|--------|------|--------|
| SEARCH_SUPPORT_TICKETS | Cortex Search Service | CONSUMPTION |
| SEARCH_COMPLIANCE_DOCS | Cortex Search Service | CONSUMPTION |
| FINSERV_MCP_SERVER | Managed MCP Server | CONSUMPTION |
| CORTEX_STAGE | Stage (semantic model) | CONSUMPTION |

### Snowpark Python Objects (CONSUMPTION)

| Object | Type | Description |
|--------|------|-------------|
| SP_RFM_SEGMENTATION | Stored Procedure | RFM customer segmentation |
| SP_PROCESS_TRANSACTIONS | Stored Procedure | Transaction channel summary |
| SP_PIPELINE_SUMMARY | Stored Procedure (TABLE) | Pipeline health report |
| ANOMALY_SCORE | UDF | Z-score anomaly detection |
| RISK_TIER | UDF | Composite risk tier |
| PARSE_RISK_FACTORS | UDTF | Parse VARIANT risk data |

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `ASSESSMENT_DATE` not found | Column is named `ASSESSED_AT` in DDL | Use `ASSESSED_AT` |
| `RISK_DATA:risk_level` returns NULL | Field is `credit_history` (string) | Use `RISK_DATA:credit_history` |
| `RISK_DATA:factors` not found | Field is `risk_factors` (array) | Use `RISK_DATA:risk_factors` |
| `COUNT(*) AS ROWS` fails | `ROWS` is a reserved word | Use `ROW_COUNT` |
| DT shows FULL refresh mode | Complex queries (subqueries, CURRENT_TIMESTAMP, upstream FULL) | Expected behavior; no fix needed |
| Cortex Agent DDL fails | `CREATE CORTEX AGENT` not available in all regions | Skip agent creation; Search services work independently |
| Snowpipe creation fails | No real S3 bucket configured | Deploy file format + landing tables only |
| Insufficient privileges | Wrong role or connection | Use ACCOUNTADMIN role on default connection |

---

## License

Internal demo project — not for production use.
