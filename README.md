# HOL Snowpark Demo -- E-Commerce Analytics Pipeline

End-to-end demo showcasing Snowflake's data engineering capabilities: raw ingestion, CDC streams, dynamic tables, materialized views, task DAGs, Snowpark Python, and a Streamlit dashboard.

---

## Architecture

```
                              HOL_DB Database
 +-----------------+   +--------------------+   +---------------------------+
 |   RAW Schema    |   |  CURATED Schema    |   |   CONSUMPTION Schema      |
 |                 |   |                    |   |                           |
 |  CUSTOMERS      |   |  MV_CUSTOMER_      |   |  DT_DAILY_SALES          |
 |  PRODUCTS       |   |    DIRECTORY       |   |    (Dynamic Table)       |
 |  ORDERS         |   |    (Mat. View)     |   |                           |
 |    (VARIANT)    |   |                    |   |  MV_PRODUCT_CATALOG      |
 |  WEBSITE_EVENTS |   |  DT_CUSTOMER_      |   |    (Materialized View)   |
 |    (VARIANT)    |   |    SUMMARY         |   |                           |
 |                 |   |    (Dynamic Table)  |   |  DT_PRODUCT_PERFORMANCE  |
 |  -- Streams --  |   |                    |   |    (Dynamic Table)       |
 |  CUSTOMERS_     |   |  DT_ORDER_ENRICHED |   |                           |
 |    STREAM       |   |    (Dynamic Table)  |   |  DT_CUSTOMER_360         |
 |  ORDERS_STREAM  |   |                    |   |    (Dynamic Table)       |
 |  EVENTS_STREAM  |   |  DT_EVENT_PARSED   |   |                           |
 |                 |   |    (Dynamic Table)  |   |  DT_CATEGORY_TRENDS      |
 |  -- Tasks --    |   |                    |   |    (Dynamic Table)       |
 |  (see DAG below)|   |  ORDERS_FROM_      |   |                           |
 |                 |   |    STREAM           |   |  PIPELINE_METRICS        |
 |                 |   |  EVENTS_FROM_       |   |    (Task-fed)            |
 |                 |   |    STREAM           |   |                           |
 +-----------------+   +--------------------+   +---------------------------+

Data Flow:  RAW (landing) --> CURATED (enriched/joined) --> CONSUMPTION (business-ready)
```

### Task DAG (all tasks in RAW schema)

```
  TASK_ROOT_SCHEDULER (every 5 min)
    +-- TASK_PROCESS_ORDERS  (when ORDERS_STREAM has data)
    +-- TASK_PROCESS_EVENTS  (when EVENTS_STREAM has data)
    +---- TASK_REFRESH_METRICS (after both above complete)
```

### Key Snowflake constraints demonstrated

- **Materialized Views** are single-table only (no joins). Multi-table aggregations use Dynamic Tables.
- **Task AFTER predecessors** must be in the same schema -- all tasks are in RAW.
- **PARSE_JSON()** cannot be used inside a VALUES clause -- use `SELECT ... UNION ALL` instead.
- **Dynamic Tables** use `TARGET_LAG` for declarative refresh (1 min, 5 min, or DOWNSTREAM).

---

## Prerequisites

- Snowflake account with `ACCOUNTADMIN` role (or a role with CREATE DATABASE, CREATE WAREHOUSE privileges)
- Snowsight access for running SQL worksheets and uploading notebooks
- For the Streamlit dashboard: Python 3.8+ with `streamlit` and `snowflake-connector-python` installed locally, OR deploy as a Streamlit-in-Snowflake app

---

## Files & Execution Order

### Core Pipeline (run in order, 01 through 06)

| # | File | Run In | What it Does |
|---|------|--------|--------------|
| 01 | `01_setup_database.sql` | SQL Worksheet | Creates `HOL_WH` (X-Small warehouse), `HOL_DB` database, and three schemas: `RAW`, `CURATED`, `CONSUMPTION` |
| 02 | `02_raw_data_load.sql` | SQL Worksheet | Creates 4 raw tables with inline sample data (12 customers, 12 products, 15 orders with VARIANT JSON, 31 website events with VARIANT JSON) |
| 03 | `03_streams.sql` | SQL Worksheet | Creates 3 CDC streams on customers, orders, and events tables for change tracking |
| 04 | `04_curated_layer.sql` | SQL Worksheet | Creates 1 materialized view, 3 dynamic tables (1-min lag), and 2 stream-target tables in the CURATED schema |
| 05 | `05_consumption_layer.sql` | SQL Worksheet | Creates 1 materialized view and 4 dynamic tables in the CONSUMPTION schema including the complex `DT_CUSTOMER_360` |
| 06 | `06_tasks_and_dag.sql` | SQL Worksheet | Creates a 4-task DAG (5-minute schedule) for stream processing and metrics refresh |

### Interactive Demos (run in any order after 01-06)

| # | File | Run In | What it Does |
|---|------|--------|--------------|
| 07 | `07_snowpark_sql_sheet.sql` | SQL Worksheet | 10 advanced SQL patterns: LATERAL FLATTEN, window functions, PIVOT, UNPIVOT, funnel analysis |
| 08 | `08_snowpark_python_notebook.ipynb` | Snowflake Notebook | 17-cell notebook covering Snowpark DataFrames, VARIANT processing, window functions, Matplotlib |
| 09 | `09_snowpark_python_sheet.sql` | SQL Worksheet | Creates 3 stored procedures, 1 UDTF, and 1 UDF using Python in Snowflake |
| 10 | `10_monitoring_and_demo.sql` | SQL Worksheet | Pipeline validation, dynamic table refresh history, task execution history, data quality checks |

### Dashboard & Testing

| # | File | Run In | What it Does |
|---|------|--------|--------------|
| 11 | `11_streamlit_dashboard.py` | Streamlit (local or SiS) | 5-tab analytics dashboard with KPIs, charts, customer segmentation, and pipeline health |
| 12 | `12_incremental_test_data.sql` | SQL Worksheet | Inserts incremental data to test the end-to-end pipeline and verify dashboard updates |

---

## Detailed SQL Notebook Guide

### 01_setup_database.sql -- Foundation Infrastructure

**Creates:** `HOL_WH` (X-Small warehouse with auto-suspend/resume), `HOL_DB` database, three schemas (`RAW`, `CURATED`, `CONSUMPTION`).

**Snowflake features demonstrated:**
- `CREATE WAREHOUSE` with `AUTO_SUSPEND`, `AUTO_RESUME`, `INITIALLY_SUSPENDED`
- `CREATE DATABASE` and `CREATE SCHEMA` with comments

**What to observe:** After running, verify the database and schemas appear in the Snowsight object browser. The warehouse starts suspended and will auto-resume on first query.

---

### 02_raw_data_load.sql -- Raw Tables & Sample Data

**Creates 4 tables with realistic e-commerce data:**

| Table | Rows | Type | Key Columns |
|-------|------|------|-------------|
| `RAW.CUSTOMERS` | 12 | Structured | AUTOINCREMENT PK, name, email, city, country (Asia-Pacific focus) |
| `RAW.PRODUCTS` | 12 | Structured | 4 categories: Electronics, Furniture, Accessories, Office |
| `RAW.ORDERS` | 15 | Semi-structured | `ORDER_DETAILS` VARIANT with nested JSON: line_items[], shipping{}, payment{} |
| `RAW.WEBSITE_EVENTS` | 31 | Semi-structured | `EVENT_DATA` VARIANT with page views, cart actions, checkout, search events |

**Snowflake features demonstrated:**
- `VARIANT` data type for semi-structured JSON
- `PARSE_JSON()` with the `SELECT ... UNION ALL` pattern (not VALUES)
- `AUTOINCREMENT` primary keys
- `TIMESTAMP_NTZ` for timezone-naive timestamps

**Key pattern -- PARSE_JSON in INSERT:**
```sql
-- WRONG: PARSE_JSON cannot be used in VALUES clause
INSERT INTO ORDERS (...) VALUES (1, ..., PARSE_JSON('{...}'));  -- ERROR!

-- CORRECT: Use SELECT ... UNION ALL
INSERT INTO ORDERS (...)
SELECT 1, ..., PARSE_JSON('{...}')
UNION ALL
SELECT 2, ..., PARSE_JSON('{...}');
```

**What to observe:** Query `RAW.ORDERS` and expand the `ORDER_DETAILS` variant column to see nested JSON structure with line items, shipping details, and payment info.

---

### 03_streams.sql -- Change Data Capture (CDC)

**Creates 3 streams for tracking changes:**

| Stream | Type | SHOW_INITIAL_ROWS | Tracks |
|--------|------|-------------------|--------|
| `RAW.CUSTOMERS_STREAM` | Standard (insert/update/delete) | TRUE | New signups and profile updates |
| `RAW.ORDERS_STREAM` | Standard (insert/update/delete) | TRUE | New and updated orders |
| `RAW.EVENTS_STREAM` | APPEND_ONLY | TRUE | New website events only (events are immutable) |

**Snowflake features demonstrated:**
- `CREATE STREAM` with `APPEND_ONLY` and `SHOW_INITIAL_ROWS` options
- `SYSTEM$STREAM_HAS_DATA()` function for conditional task execution

**What to observe:** After creation, all streams will have `HAS_DATA = TRUE` because of `SHOW_INITIAL_ROWS = TRUE`. The streams will reset after they are consumed by a DML operation or task.

---

### 04_curated_layer.sql -- Curated Transformations

**Creates 6 objects that transform RAW data into enriched/joined views:**

| Object | Type | Refresh | What it does |
|--------|------|---------|-------------|
| `CURATED.MV_CUSTOMER_DIRECTORY` | Materialized View | Automatic | Single-table customer lookup (no joins -- MV constraint) |
| `CURATED.DT_CUSTOMER_SUMMARY` | Dynamic Table | 1 minute | Joins CUSTOMERS + ORDERS for per-customer aggregates (total spend, order count, tenure) |
| `CURATED.DT_ORDER_ENRICHED` | Dynamic Table | 1 minute | Flattens `ORDER_DETAILS` JSON into individual line items using LATERAL FLATTEN |
| `CURATED.DT_EVENT_PARSED` | Dynamic Table | 1 minute | Extracts structured fields from `EVENT_DATA` JSON (page, device, cart info) |
| `CURATED.ORDERS_FROM_STREAM` | Regular Table | Via Task | Populated by `TASK_PROCESS_ORDERS` from the orders stream |
| `CURATED.EVENTS_FROM_STREAM` | Regular Table | Via Task | Populated by `TASK_PROCESS_EVENTS` from the events stream |

**Snowflake features demonstrated:**
- `CREATE MATERIALIZED VIEW` (single-table only)
- `CREATE DYNAMIC TABLE` with `TARGET_LAG` (all 3 curated DTs use 1 minute)
- `LATERAL FLATTEN` for exploding JSON arrays
- Variant field access (`col:path::TYPE`)

**Key constraint -- MV single-table only:**
```sql
-- WRONG: MVs cannot contain joins
CREATE MATERIALIZED VIEW MV_SUMMARY AS
SELECT c.*, COUNT(o.ORDER_ID) FROM CUSTOMERS c JOIN ORDERS o ...;  -- ERROR!

-- CORRECT: Use Dynamic Table for joins
CREATE DYNAMIC TABLE DT_SUMMARY TARGET_LAG = '1 MINUTE' ... AS
SELECT c.*, COUNT(o.ORDER_ID) FROM CUSTOMERS c JOIN ORDERS o ...;
```

**What to observe:** Query `DT_ORDER_ENRICHED` to see how nested JSON arrays become individual rows via LATERAL FLATTEN. Each order with 2-3 line items produces 2-3 rows with extracted product_id, quantity, unit_price, and discount.

---

### 05_consumption_layer.sql -- Consumption Analytics

**Creates 5 business-ready objects:**

| Object | Type | Refresh | What it does |
|--------|------|---------|-------------|
| `CONSUMPTION.DT_DAILY_SALES` | Dynamic Table | DOWNSTREAM | Daily aggregates: revenue, orders, items sold, payment method splits |
| `CONSUMPTION.MV_PRODUCT_CATALOG` | Materialized View | Automatic | Fast product lookup (single-table from PRODUCTS) |
| `CONSUMPTION.DT_PRODUCT_PERFORMANCE` | Dynamic Table | DOWNSTREAM | Revenue, units, buyers per product (joins PRODUCTS + DT_ORDER_ENRICHED) |
| `CONSUMPTION.DT_CUSTOMER_360` | Dynamic Table | 5 minutes | Complete customer profile: order history + website behavior + segmentation |
| `CONSUMPTION.DT_CATEGORY_TRENDS` | Dynamic Table | DOWNSTREAM | Monthly category/sub-category revenue, units, and customer breakdowns |

**`DT_CUSTOMER_360` segmentation logic:**
- **HIGH_VALUE**: lifetime value >= $500 AND total orders >= 2
- **MEDIUM_VALUE**: lifetime value >= $200
- **LOW_VALUE**: at least 1 order but below thresholds
- **PROSPECT**: no orders yet (signed up but hasn't purchased)

**Snowflake features demonstrated:**
- Dynamic table chains (DOWNSTREAM lag cascades from curated layer)
- `IFF()`, `COALESCE()`, `DATEDIFF()`, `COUNT_IF()`
- `LISTAGG()` for aggregating status values
- CTE-based approach for complex multi-source joins

**What to observe:** Query `DT_CUSTOMER_360` to see a complete customer profile combining order history, website behavior (page views, cart actions, checkouts), and auto-calculated segmentation.

---

### 06_tasks_and_dag.sql -- Task DAG Automation

**Creates a 4-task DAG (all in RAW schema):**

| Task | Trigger | What it does |
|------|---------|-------------|
| `TASK_ROOT_SCHEDULER` | CRON every 5 minutes | Root task -- fires child tasks on schedule |
| `TASK_PROCESS_ORDERS` | AFTER root, WHEN orders stream has data | MERGE INTO curated orders table from stream (upsert) |
| `TASK_PROCESS_EVENTS` | AFTER root, WHEN events stream has data | INSERT INTO curated events table from stream |
| `TASK_REFRESH_METRICS` | AFTER both process tasks | MERGE INTO pipeline_metrics with latest KPI counts |

Also creates `CONSUMPTION.PIPELINE_METRICS` table and starts all tasks with `ALTER TASK ... RESUME`.

**Snowflake features demonstrated:**
- `CREATE TASK` with `SCHEDULE`, `AFTER` dependencies, and `WHEN` conditions
- `SYSTEM$STREAM_HAS_DATA()` for conditional execution
- `MERGE INTO` for upsert patterns
- `EXECUTE TASK` for manual triggering

**Key constraint -- Tasks in same schema:**
```sql
-- WRONG: AFTER cannot reference tasks in different schemas
CREATE TASK CURATED.TASK_PROCESS AFTER RAW.TASK_ROOT ...;  -- ERROR!

-- CORRECT: All tasks in the same schema
CREATE TASK RAW.TASK_PROCESS AFTER RAW.TASK_ROOT ...;
```

**What to observe:** Go to Activity > Task History in Snowsight to see the DAG execution timeline. The root task fires every 5 minutes, child tasks only execute when their stream has data.

---

### 07_snowpark_sql_sheet.sql -- Advanced SQL Patterns

**Demonstrates 10 SQL techniques (run sections individually in Snowsight):**

1. **LATERAL FLATTEN** -- Exploding JSON arrays from ORDER_DETAILS into rows
2. **Window functions: Running totals** -- `SUM() OVER (ORDER BY ...)` for cumulative revenue
3. **Window functions: LAG** -- Previous order comparison per customer
4. **Window functions: RANK** -- Product rankings within categories
5. **PIVOT** -- Payment method cross-tabulation (rows to columns)
6. **UNPIVOT** -- Reversing pivoted data (columns to rows)
7. **Funnel analysis** -- Page view -> Add to cart -> Checkout conversion rates
8. **Time intelligence** -- Month-over-month revenue comparisons
9. **Cohort analysis** -- Customer behavior by signup month
10. **Complex aggregation** -- Multi-level GROUP BY with GROUPING SETS

**What to observe:** Run each section one at a time and examine the output. The funnel analysis section is particularly interesting -- it shows conversion drop-off rates at each stage.

---

### 08_snowpark_python_notebook.ipynb -- Snowpark Python Demo

**How to use:** Upload to Snowflake via Notebooks > + > Import .ipynb

**17 cells (9 code, 8 markdown) covering:**

1. Snowpark Session and `session.table()` API
2. DataFrame operations: `filter()`, `group_by()`, `join()`, `with_column()`
3. VARIANT column processing with `col['path']` accessor syntax
4. Window functions in Snowpark: `Window.partition_by().order_by()`
5. Writing enriched DataFrames back to tables with `df.write.save_as_table()`
6. Querying dynamic tables from Python
7. Matplotlib visualizations of pipeline data

**What to observe:** Each cell builds on the previous one. The notebook walks through the entire pipeline using Python instead of SQL, demonstrating equivalent Snowpark operations.

---

### 09_snowpark_python_sheet.sql -- Python Stored Procedures, UDTFs, UDFs

**Creates 5 Python-in-Snowflake objects:**

| Object | Type | What it does |
|--------|------|-------------|
| `SP_PROCESS_ORDERS` | Stored Procedure | Reads stream data and inserts into curated tables |
| `SP_RFM_SEGMENTATION` | Stored Procedure | Recency-Frequency-Monetary customer scoring -> writes `CUSTOMER_RFM` table |
| `PARSE_EVENT_DETAILS` | UDTF | Explodes event JSON into multiple typed rows (table function) |
| `CATEGORIZE_ORDER` | UDF | Classifies order value as SMALL/MEDIUM/LARGE/PREMIUM |
| `SP_PIPELINE_SUMMARY` | Stored Procedure | Reports row counts across all pipeline objects |

**Snowflake features demonstrated:**
- `CREATE PROCEDURE ... LANGUAGE PYTHON` with `PACKAGES = ('snowflake-snowpark-python')`
- `CREATE FUNCTION ... LANGUAGE PYTHON` for scalar UDFs
- UDTF with `process()` method for multi-row output
- Python handler functions with Snowpark session

**How to test after creating:**
```sql
CALL SP_PIPELINE_SUMMARY();
SELECT CATEGORIZE_ORDER(500.00);
SELECT * FROM TABLE(PARSE_EVENT_DETAILS(PARSE_JSON('{"page":"/home"}')));
CALL SP_RFM_SEGMENTATION();
SELECT * FROM CONSUMPTION.CUSTOMER_RFM;
```

---

### 10_monitoring_and_demo.sql -- Monitoring & Validation

**Provides ready-to-run queries for:**

1. **Pipeline validation** -- Row counts across all 3 layers (RAW, CURATED, CONSUMPTION)
2. **Dynamic Table refresh history** -- `INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY()`
3. **Task execution history** -- `INFORMATION_SCHEMA.TASK_HISTORY()` with status/duration
4. **Stream status checks** -- Which streams have pending data
5. **Data quality spot-checks** -- Verify data integrity across joins
6. **Cleanup commands** -- DROP DATABASE/WAREHOUSE (commented out for safety)

**What to observe:** Run the validation section first to confirm expected row counts:

| Layer | Table | Expected Rows (initial) |
|-------|-------|------------------------|
| RAW | CUSTOMERS | 12 |
| RAW | PRODUCTS | 12 |
| RAW | ORDERS | 15 |
| RAW | WEBSITE_EVENTS | 31 |
| CURATED | DT_CUSTOMER_SUMMARY | 12 |
| CURATED | DT_ORDER_ENRICHED | ~37 |
| CURATED | DT_EVENT_PARSED | 31 |
| CONSUMPTION | DT_DAILY_SALES | ~15 |
| CONSUMPTION | DT_PRODUCT_PERFORMANCE | 12 |
| CONSUMPTION | DT_CUSTOMER_360 | 12 |
| CONSUMPTION | DT_CATEGORY_TRENDS | ~24 |

---

### 12_incremental_test_data.sql -- End-to-End Pipeline Test

**Purpose:** Insert new data into RAW tables to test incremental pipeline processing and verify that changes propagate through streams, dynamic tables, and appear in the dashboard.

**What gets inserted:**

| Section | Data | Details |
|---------|------|---------|
| Section B | 3 new customers | Maya (Miami, USA), Raj (Bangalore, India), Sophie (Berlin, Germany) |
| Section C | 2 new products | Monitor Arm ($89.99, Accessories), Wireless Charger ($35.00, Electronics) |
| Section D | 5 new orders | Mix of new (Maya, Raj, Sophie) and existing (Alice, Bob) customers, with VARIANT JSON |
| Section E | 16 new events | Browsing sessions including a cart-abandon-then-return scenario (Sophie) |

**Verification sections:**

| Section | Purpose |
|---------|---------|
| A | Capture BEFORE row counts across all layers |
| F | Verify streams have `HAS_DATA = TRUE` after inserts |
| G | Manually trigger the task DAG with `EXECUTE TASK` |
| H | Capture AFTER row counts (run 2-5 minutes after G) |
| I | Expected changes summary reference |
| J | Spot-check specific new data in consumption tables |

**Expected changes after pipeline runs:**

| Table | Before | After | Change |
|-------|--------|-------|--------|
| RAW.CUSTOMERS | 12 | 15 | +3 new customers |
| RAW.PRODUCTS | 12 | 14 | +2 new products |
| RAW.ORDERS | 15 | 20 | +5 new orders |
| RAW.WEBSITE_EVENTS | 31 | 47 | +16 new events |
| CURATED.DT_CUSTOMER_SUMMARY | 12 | 15 | +3 new customer summaries |
| CURATED.DT_ORDER_ENRICHED | ~37 | ~50 | +line items from 5 orders |
| CONSUMPTION.DT_DAILY_SALES | ~15 | ~19 | +4 new September dates |
| CONSUMPTION.DT_PRODUCT_PERFORMANCE | 12 | 14 | +2 new products |
| CONSUMPTION.DT_CUSTOMER_360 | 12 | 15 | +3 new customers |

---

## Deployment Guide

### Step 1: Initial Setup (run once, in order)

Open a **SQL Worksheet** in Snowsight and run each file sequentially:

```
01_setup_database.sql    --> Creates warehouse, database, schemas
02_raw_data_load.sql     --> Creates tables and loads sample data
03_streams.sql           --> Creates CDC streams
04_curated_layer.sql     --> Creates curated MVs and dynamic tables
05_consumption_layer.sql --> Creates consumption dynamic tables
06_tasks_and_dag.sql     --> Creates and starts the task DAG
```

After step 06, **wait 1-2 minutes** for dynamic tables to complete their initial refresh.

### Step 2: Verify Pipeline

Run the validation section of `10_monitoring_and_demo.sql` to confirm all objects have expected row counts (see table above).

### Step 3: Interactive Demos (any order)

- **SQL patterns:** Open `07_snowpark_sql_sheet.sql` in Snowsight, run sections individually
- **Python notebook:** Upload `08_snowpark_python_notebook.ipynb` as a Snowflake Notebook (Notebooks > + > Import .ipynb), run cells top to bottom
- **Python in SQL:** Open `09_snowpark_python_sheet.sql` in Snowsight, creates and calls SPs/UDTFs/UDFs

### Step 4: Test Incremental Pipeline

Run `12_incremental_test_data.sql` in a SQL Worksheet:

1. Run **Sections A-E** to capture baseline and insert new data
2. Run **Section F** to verify streams have data
3. Run **Section G** to trigger the DAG manually
4. **Wait 2-5 minutes** for dynamic tables to refresh
5. Run **Sections H-J** to verify consumption tables updated

### Step 5: Launch Dashboard

See [Streamlit Deployment](#streamlit-deployment) below.

---

## Streamlit Dashboard

### What the dashboard shows

The dashboard (`11_streamlit_dashboard.py`) has **5 tabs** that visualize the CONSUMPTION layer:

| Tab | Content |
|-----|---------|
| **Overview** | KPI cards (revenue, orders, customers, avg order value, events, sessions), daily revenue bar chart, orders/customers line chart, payment method split, order status donut, cumulative revenue area chart |
| **Products** | Product KPIs, category filter (segmented control), revenue-by-product bar chart, units vs buyers scatter, list price vs sold price comparison, detailed data table |
| **Customers** | Customer KPIs (total, avg LTV, high-value count, avg conversion), segment donut chart, top customers by LTV, revenue by country, engagement data table with progress bars |
| **Category Trends** | Category KPIs, monthly revenue stacked area chart, monthly units sold bar chart, category revenue share donut, sub-category breakdown table |
| **Pipeline Health** | Layer row count KPIs, pipeline row count bar chart (color-coded by layer), stream CDC status, task DAG status, dynamic table refresh history, pipeline metrics snapshot |

### Streamlit Deployment

#### Option A: Run Locally

1. Install dependencies:
   ```bash
   pip install streamlit snowflake-connector-python snowflake-snowpark-python altair pandas
   ```

2. Create `.streamlit/secrets.toml` in the project directory:
   ```toml
   [connections.snowflake]
   account = "YOUR_ORG-YOUR_ACCOUNT"
   user = "your_username"
   authenticator = "externalbrowser"
   warehouse = "HOL_WH"
   database = "HOL_DB"
   schema = "CONSUMPTION"
   ```

3. Run:
   ```bash
   streamlit run 11_streamlit_dashboard.py
   ```

4. The dashboard opens at `http://localhost:8501`. Click "Refresh Data" to reload from Snowflake.

#### Option B: Deploy as Streamlit-in-Snowflake (SiS)

1. In Snowsight, go to **Streamlit** > **+ Streamlit App**
2. Set the database to `HOL_DB`, schema to `CONSUMPTION`, warehouse to `HOL_WH`
3. Replace the default code with the contents of `11_streamlit_dashboard.py`
4. The app uses `get_active_session()` automatically -- no secrets needed
5. Add `altair` to the packages list if prompted

---

## Testing Incremental Data with the Dashboard

This is the complete end-to-end workflow to verify the pipeline works:

1. **Open the dashboard** (locally or in Snowsight)
2. **Note the current values** on the Overview tab (revenue, orders, customers)
3. **Open a SQL Worksheet** alongside the dashboard
4. **Run `12_incremental_test_data.sql` Sections A through G** to insert new data and trigger the DAG
5. **Wait 2-5 minutes** for dynamic tables to refresh
6. **Click "Refresh Data"** in the dashboard header
7. **Observe the changes across all tabs:**
   - **Overview**: Revenue increases, order/customer counts go up, new September bars in daily sales, cumulative revenue line extends
   - **Products**: "Monitor Arm" and "Wireless Charger" appear in product charts
   - **Customers**: Maya, Raj, and Sophie appear in customer insights; new countries (USA, Germany) in geographic chart
   - **Category Trends**: September entries appear in stacked area chart
   - **Pipeline Health**: Row counts increase across all layers, refresh history shows new entries

---

## Features Demonstrated

| Feature | Where Used |
|---------|-----------|
| **Database / Schema / Warehouse** | `01_setup_database.sql` |
| **Structured Tables** | `02_raw_data_load.sql` -- CUSTOMERS, PRODUCTS |
| **Semi-Structured (VARIANT)** | `02_raw_data_load.sql` -- ORDERS, WEBSITE_EVENTS |
| **Streams (CDC)** | `03_streams.sql` -- append-only and standard streams |
| **Dynamic Tables** | `04`, `05` -- 7 dynamic tables with TARGET_LAG chains |
| **Materialized Views** | `04` -- MV_CUSTOMER_DIRECTORY, `05` -- MV_PRODUCT_CATALOG |
| **Tasks & DAG** | `06` -- root + child + grandchild tasks (all in RAW) |
| **LATERAL FLATTEN** | `04`, `07` -- explode JSON arrays |
| **Window Functions** | `07` -- running totals, LAG, RANK |
| **PIVOT / UNPIVOT** | `07` -- payment method cross-tab |
| **Snowpark Python DataFrames** | `08` -- end-to-end Python pipeline |
| **Python Stored Procedures** | `09` -- SP_PROCESS_ORDERS, SP_RFM_SEGMENTATION |
| **Python UDTF** | `09` -- PARSE_EVENT_DETAILS |
| **Python UDF** | `09` -- CATEGORIZE_ORDER |
| **Streamlit Dashboard** | `11` -- 5-tab interactive analytics with 15+ charts |
| **Incremental Pipeline Test** | `12` -- end-to-end with before/after validation |

---

## Troubleshooting

### "Materialized view cannot contain joins"
Snowflake MVs are single-table only. Use Dynamic Tables for multi-table aggregations.

### "Task predecessor must be in the same schema"
All tasks with AFTER dependencies must share a schema. This demo puts all tasks in RAW.

### "PARSE_JSON cannot be used in VALUES"
Use the `SELECT ... UNION ALL` pattern instead of `VALUES (... PARSE_JSON(...))`.

### Dynamic tables not refreshing
- Check refresh history: `SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())` for errors
- Ensure the warehouse is running: `ALTER WAREHOUSE HOL_WH RESUME`
- Verify TARGET_LAG is set: `SHOW DYNAMIC TABLES IN DATABASE HOL_DB`

### Streams show HAS_DATA = FALSE after insert
- Streams are consumed when read by a DML. If a task already processed the data, the stream resets.
- Insert more data and check again.

### Streamlit "No module named snowflake"
- Install: `pip install snowflake-connector-python snowflake-snowpark-python`
- On Python 3.12+, explicitly add `snowflake-connector-python>=3.3.0`

### Task not firing
- Verify tasks are resumed: `SHOW TASKS IN SCHEMA RAW`
- Check the WHEN condition: streams must have data for child tasks to execute
- Trigger manually: `EXECUTE TASK RAW.TASK_ROOT_SCHEDULER`

### Dashboard shows stale data after incremental insert
- Dynamic Tables need 1-5 minutes to refresh depending on their `TARGET_LAG`
- Click "Refresh Data" button in the dashboard header after waiting
- Check the Pipeline Health tab for Dynamic Table refresh history

### Incremental test data not appearing in dashboard
- Confirm streams picked up the data (Section F should show `TRUE`)
- Confirm the DAG ran (Section G or check Activity > Task History)
- Wait for DT_CUSTOMER_360 specifically (has 5-minute TARGET_LAG)
- Run Section H to verify CONSUMPTION row counts changed

---

## Cleanup

Uncomment and run the cleanup section at the bottom of `10_monitoring_and_demo.sql`:

```sql
-- Suspend tasks (leaf tasks first, root last)
ALTER TASK RAW.TASK_REFRESH_METRICS SUSPEND;
ALTER TASK RAW.TASK_PROCESS_EVENTS  SUSPEND;
ALTER TASK RAW.TASK_PROCESS_ORDERS  SUSPEND;
ALTER TASK RAW.TASK_ROOT_SCHEDULER  SUSPEND;

-- Drop everything
DROP DATABASE IF EXISTS HOL_DB;
DROP WAREHOUSE IF EXISTS HOL_WH;
```
