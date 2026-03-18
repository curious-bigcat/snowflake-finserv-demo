/*=============================================================================
  HOL SNOWPARK DEMO — Step 9: Snowpark Python in SQL Worksheets
  Python Stored Procedures and UDTFs called from SQL.
  Run this in a Snowsight SQL Worksheet.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

-- ============================================================
-- 1. Python Stored Procedure: Process Orders Stream
--    Reads from RAW, flattens VARIANT, writes to CURATED
-- ============================================================
CREATE OR REPLACE PROCEDURE CURATED.SP_PROCESS_ORDERS_PYTHON()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_orders'
AS
$$
def process_orders(session):
    from snowflake.snowpark.functions import col, parse_json, flatten, lit, current_timestamp

    # Read raw orders
    orders_df = session.table("HOL_DB.RAW.ORDERS")

    # Read customers for enrichment
    customers_df = session.table("HOL_DB.RAW.CUSTOMERS").select(
        col("CUSTOMER_ID"),
        (col("FIRST_NAME") + lit(" ") + col("LAST_NAME")).alias("CUSTOMER_NAME"),
        col("CITY").alias("CUSTOMER_CITY"),
        col("COUNTRY").alias("CUSTOMER_COUNTRY")
    )

    # Join orders with customers
    enriched = orders_df.join(customers_df, "CUSTOMER_ID")

    # Extract semi-structured fields using Snowpark
    result = enriched.select(
        col("ORDER_ID"),
        col("CUSTOMER_ID"),
        col("CUSTOMER_NAME"),
        col("CUSTOMER_CITY"),
        col("CUSTOMER_COUNTRY"),
        col("ORDER_DATE"),
        col("STATUS"),
        col("TOTAL_AMOUNT"),
        col("ORDER_DETAILS")["shipping"]["method"].cast("VARCHAR").alias("SHIPPING_METHOD"),
        col("ORDER_DETAILS")["shipping"]["cost"].cast("FLOAT").alias("SHIPPING_COST"),
        col("ORDER_DETAILS")["payment"]["method"].cast("VARCHAR").alias("PAYMENT_METHOD")
    )

    # Write to curated layer
    result.write.mode("overwrite").save_as_table("HOL_DB.CURATED.ORDERS_ENRICHED_PY")

    row_count = session.table("HOL_DB.CURATED.ORDERS_ENRICHED_PY").count()
    return f"SUCCESS: Processed {row_count} enriched orders to CURATED.ORDERS_ENRICHED_PY"
$$;

-- Execute the stored procedure
CALL CURATED.SP_PROCESS_ORDERS_PYTHON();

-- Verify
SELECT * FROM CURATED.ORDERS_ENRICHED_PY ORDER BY ORDER_ID;

-- ============================================================
-- 2. Python Stored Procedure: Customer RFM Segmentation
--    Recency, Frequency, Monetary analysis using Snowpark
-- ============================================================
CREATE OR REPLACE PROCEDURE CONSUMPTION.SP_RFM_SEGMENTATION()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'rfm_segmentation'
AS
$$
def rfm_segmentation(session):
    from snowflake.snowpark.functions import (
        col, count, sum as sum_, max as max_, min as min_,
        datediff, current_timestamp, lit, when, avg as avg_,
        ntile
    )
    from snowflake.snowpark import Window

    # Build RFM metrics per customer
    orders = session.table("HOL_DB.RAW.ORDERS")
    customers = session.table("HOL_DB.RAW.CUSTOMERS")

    rfm = orders.group_by("CUSTOMER_ID").agg(
        datediff("day", max_(col("ORDER_DATE")), current_timestamp()).alias("RECENCY_DAYS"),
        count(col("ORDER_ID")).alias("FREQUENCY"),
        sum_(col("TOTAL_AMOUNT")).alias("MONETARY")
    )

    # Score with ntiles (1=best for recency, 5=best for frequency & monetary)
    rfm_scored = rfm.with_column(
        "R_SCORE",
        ntile(lit(5)).over(Window.order_by(col("RECENCY_DAYS").desc()))
    ).with_column(
        "F_SCORE",
        ntile(lit(5)).over(Window.order_by(col("FREQUENCY").asc()))
    ).with_column(
        "M_SCORE",
        ntile(lit(5)).over(Window.order_by(col("MONETARY").asc()))
    )

    # Compute overall RFM score
    rfm_final = rfm_scored.with_column(
        "RFM_SCORE",
        col("R_SCORE") + col("F_SCORE") + col("M_SCORE")
    )

    # Segment customers
    segmented = rfm_final.with_column(
        "SEGMENT",
        when(col("RFM_SCORE") >= 12, lit("Champions"))
        .when(col("RFM_SCORE") >= 9, lit("Loyal Customers"))
        .when(col("RFM_SCORE") >= 6, lit("Potential Loyalists"))
        .when(col("RFM_SCORE") >= 3, lit("At Risk"))
        .otherwise(lit("Needs Attention"))
    )

    # Join with customer details and save
    result = segmented.join(
        customers.select(
            col("CUSTOMER_ID"),
            (col("FIRST_NAME") + lit(" ") + col("LAST_NAME")).alias("CUSTOMER_NAME"),
            col("EMAIL"),
            col("CITY"),
            col("COUNTRY")
        ),
        "CUSTOMER_ID"
    )

    result.write.mode("overwrite").save_as_table("HOL_DB.CONSUMPTION.CUSTOMER_RFM")

    row_count = session.table("HOL_DB.CONSUMPTION.CUSTOMER_RFM").count()
    return f"SUCCESS: RFM segmentation complete for {row_count} customers"
$$;

-- Execute RFM segmentation
CALL CONSUMPTION.SP_RFM_SEGMENTATION();

-- View RFM results
SELECT
    CUSTOMER_NAME,
    CITY,
    COUNTRY,
    RECENCY_DAYS,
    FREQUENCY,
    MONETARY,
    R_SCORE,
    F_SCORE,
    M_SCORE,
    RFM_SCORE,
    SEGMENT
FROM CONSUMPTION.CUSTOMER_RFM
ORDER BY RFM_SCORE DESC;

-- ============================================================
-- 3. Python UDTF: Parse and Enrich Website Events
--    Returns structured rows from semi-structured event data
-- ============================================================
CREATE OR REPLACE FUNCTION RAW.PARSE_EVENT_DETAILS(EVENT_DATA VARIANT)
RETURNS TABLE (
    DEVICE_TYPE    VARCHAR,
    BROWSER        VARCHAR,
    OS             VARCHAR,
    SESSION_ID     VARCHAR,
    PAGE_URL       VARCHAR,
    DURATION_SEC   INT,
    PRODUCT_NAME   VARCHAR,
    CART_TOTAL     FLOAT,
    SEARCH_QUERY   VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'EventParser'
AS
$$
class EventParser:
    def process(self, event_data):
        if event_data is None:
            return

        device = event_data.get("device", {}) or {}
        yield (
            device.get("type", "unknown"),
            device.get("browser", "unknown"),
            device.get("os", "unknown"),
            event_data.get("session_id"),
            event_data.get("page"),
            event_data.get("duration_sec"),
            event_data.get("product_name"),
            event_data.get("cart_total"),
            event_data.get("query")
        )
$$;

-- Use the UDTF to parse events
SELECT
    e.EVENT_ID,
    e.EVENT_TIME,
    e.CUSTOMER_ID,
    e.EVENT_TYPE,
    parsed.*
FROM RAW.WEBSITE_EVENTS e,
TABLE(RAW.PARSE_EVENT_DETAILS(e.EVENT_DATA)) parsed
ORDER BY e.EVENT_TIME;

-- ============================================================
-- 4. Python UDF: Categorize order size
-- ============================================================
CREATE OR REPLACE FUNCTION CURATED.CATEGORIZE_ORDER(amount FLOAT)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'categorize'
AS
$$
def categorize(amount):
    if amount is None:
        return 'UNKNOWN'
    if amount >= 800:
        return 'PREMIUM'
    elif amount >= 400:
        return 'LARGE'
    elif amount >= 150:
        return 'MEDIUM'
    else:
        return 'SMALL'
$$;

-- Use the UDF in a query
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    TOTAL_AMOUNT,
    CURATED.CATEGORIZE_ORDER(TOTAL_AMOUNT) AS ORDER_CATEGORY
FROM RAW.ORDERS
ORDER BY TOTAL_AMOUNT DESC;

-- ============================================================
-- 5. Python SP: Pipeline Summary Report
--    Queries all layers and returns a summary
-- ============================================================
CREATE OR REPLACE PROCEDURE CONSUMPTION.SP_PIPELINE_SUMMARY()
RETURNS TABLE (LAYER VARCHAR, OBJECT_NAME VARCHAR, OBJECT_TYPE VARCHAR, ROW_COUNT INT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'pipeline_summary'
AS
$$
def pipeline_summary(session):
    objects = [
        ("RAW",         "CUSTOMERS",              "TABLE"),
        ("RAW",         "PRODUCTS",               "TABLE"),
        ("RAW",         "ORDERS",                 "TABLE"),
        ("RAW",         "WEBSITE_EVENTS",         "TABLE"),
        ("CURATED",     "MV_CUSTOMER_DIRECTORY",  "MATERIALIZED VIEW"),
        ("CURATED",     "DT_CUSTOMER_SUMMARY",    "DYNAMIC TABLE"),
        ("CURATED",     "DT_ORDER_ENRICHED",      "DYNAMIC TABLE"),
        ("CURATED",     "DT_EVENT_PARSED",        "DYNAMIC TABLE"),
        ("CURATED",     "ORDERS_ENRICHED_PY",     "TABLE (Python SP)"),
        ("CONSUMPTION", "DT_DAILY_SALES",         "DYNAMIC TABLE"),
        ("CONSUMPTION", "MV_PRODUCT_CATALOG",     "MATERIALIZED VIEW"),
        ("CONSUMPTION", "DT_PRODUCT_PERFORMANCE", "DYNAMIC TABLE"),
        ("CONSUMPTION", "DT_CUSTOMER_360",        "DYNAMIC TABLE"),
        ("CONSUMPTION", "DT_CATEGORY_TRENDS",     "DYNAMIC TABLE"),
        ("CONSUMPTION", "CUSTOMER_RFM",           "TABLE (Python SP)"),
    ]

    results = []
    for schema, table, obj_type in objects:
        try:
            cnt = session.table(f"HOL_DB.{schema}.{table}").count()
        except Exception:
            cnt = -1
        results.append((schema, table, obj_type, cnt))

    return session.create_dataframe(
        results,
        schema=["LAYER", "OBJECT_NAME", "OBJECT_TYPE", "ROW_COUNT"]
    )
$$;

-- Run pipeline summary
CALL CONSUMPTION.SP_PIPELINE_SUMMARY();
