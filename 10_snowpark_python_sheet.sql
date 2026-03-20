/*=============================================================================
  FINSERV DEMO — Step 10: Snowpark Python in SQL Sheets
  Python Stored Procedures, UDFs, and UDTFs called from SQL.
  Run this in a Snowsight SQL Worksheet.
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;

-- ============================================================
-- 1. PYTHON SP: Customer RFM Segmentation
--    Recency, Frequency, Monetary scoring using Snowpark.
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
        col, count, sum as sum_, max as max_,
        datediff, current_timestamp, lit, when, ntile
    )
    from snowflake.snowpark import Window

    # Build RFM metrics from transactions via accounts
    transactions = session.table("FINSERV_DB.CURATED.DT_TRANSACTION_ENRICHED")
    customers = session.table("FINSERV_DB.BASE.CUSTOMERS")

    rfm = transactions.filter(col("TXN_TYPE") == "DEBIT").group_by("CUSTOMER_ID").agg(
        datediff("day", max_(col("TXN_DATE")), current_timestamp()).alias("RECENCY_DAYS"),
        count(col("TXN_ID")).alias("FREQUENCY"),
        sum_(col("AMOUNT")).alias("MONETARY")
    )

    # Ntile scoring (1=worst, 5=best)
    rfm_scored = rfm.with_column(
        "R_SCORE", ntile(lit(5)).over(Window.order_by(col("RECENCY_DAYS").desc()))
    ).with_column(
        "F_SCORE", ntile(lit(5)).over(Window.order_by(col("FREQUENCY").asc()))
    ).with_column(
        "M_SCORE", ntile(lit(5)).over(Window.order_by(col("MONETARY").asc()))
    ).with_column(
        "RFM_SCORE", col("R_SCORE") + col("F_SCORE") + col("M_SCORE")
    )

    # Segment customers
    segmented = rfm_scored.with_column(
        "SEGMENT",
        when(col("RFM_SCORE") >= 12, lit("Champions"))
        .when(col("RFM_SCORE") >= 9, lit("Loyal Customers"))
        .when(col("RFM_SCORE") >= 6, lit("Potential Loyalists"))
        .when(col("RFM_SCORE") >= 3, lit("At Risk"))
        .otherwise(lit("Needs Attention"))
    )

    # Join with customer details
    result = segmented.join(
        customers.select(
            col("CUSTOMER_ID"),
            (col("FIRST_NAME") + lit(" ") + col("LAST_NAME")).alias("CUSTOMER_NAME"),
            col("EMAIL"), col("CITY"), col("COUNTRY")
        ),
        "CUSTOMER_ID"
    )

    result.write.mode("overwrite").save_as_table("FINSERV_DB.CONSUMPTION.CUSTOMER_RFM")
    row_count = session.table("FINSERV_DB.CONSUMPTION.CUSTOMER_RFM").count()
    return f"SUCCESS: RFM segmentation complete for {row_count} customers"
$$;

-- Execute and verify
CALL CONSUMPTION.SP_RFM_SEGMENTATION();

SELECT CUSTOMER_NAME, CITY, COUNTRY, RECENCY_DAYS, FREQUENCY, MONETARY,
       R_SCORE, F_SCORE, M_SCORE, RFM_SCORE, SEGMENT
FROM CONSUMPTION.CUSTOMER_RFM
ORDER BY RFM_SCORE DESC
LIMIT 20;


-- ============================================================
-- 2. PYTHON SP: Process Enriched Transactions from Stream
--    Reads transactions, enriches, writes to curated.
-- ============================================================

CREATE OR REPLACE PROCEDURE CURATED.SP_PROCESS_TRANSACTIONS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_transactions'
AS
$$
def process_transactions(session):
    from snowflake.snowpark.functions import col, lit, current_timestamp

    txns = session.table("FINSERV_DB.BASE.TRANSACTIONS")
    accounts = session.table("FINSERV_DB.BASE.ACCOUNTS").select(
        col("ACCOUNT_ID"),
        col("CUSTOMER_ID"),
        col("ACCOUNT_TYPE"),
        col("BALANCE").alias("ACCOUNT_BALANCE"),
        col("STATUS").alias("ACCOUNT_STATUS")
    )
    customers = session.table("FINSERV_DB.BASE.CUSTOMERS").select(
        col("CUSTOMER_ID"),
        (col("FIRST_NAME") + lit(" ") + col("LAST_NAME")).alias("CUSTOMER_NAME"),
        col("CITY").alias("CUSTOMER_CITY"),
        col("COUNTRY").alias("CUSTOMER_COUNTRY"),
        col("CREDIT_SCORE")
    )

    enriched = txns.join(accounts, "ACCOUNT_ID").join(customers, "CUSTOMER_ID")
    enriched.write.mode("overwrite").save_as_table("FINSERV_DB.CURATED.TRANSACTIONS_ENRICHED_PY")

    row_count = session.table("FINSERV_DB.CURATED.TRANSACTIONS_ENRICHED_PY").count()
    return f"SUCCESS: Processed {row_count} enriched transactions"
$$;

CALL CURATED.SP_PROCESS_TRANSACTIONS();
SELECT * FROM CURATED.TRANSACTIONS_ENRICHED_PY LIMIT 10;


-- ============================================================
-- 3. PYTHON UDF: Transaction Anomaly Score
--    Scores a transaction based on amount vs average.
-- ============================================================

CREATE OR REPLACE FUNCTION CURATED.ANOMALY_SCORE(
    amount FLOAT,
    avg_amount FLOAT,
    stddev_amount FLOAT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'score'
AS
$$
def score(amount, avg_amount, stddev_amount):
    if amount is None or avg_amount is None or stddev_amount is None:
        return 0.0
    if stddev_amount == 0:
        return 0.0
    z_score = abs((amount - avg_amount) / stddev_amount)
    # Normalize to 0-100 scale, cap at 100
    return min(round(z_score * 25, 2), 100.0)
$$;

-- Use the anomaly score UDF
WITH stats AS (
    SELECT
        CATEGORY,
        AVG(AMOUNT)    AS AVG_AMOUNT,
        STDDEV(AMOUNT) AS STDDEV_AMOUNT
    FROM BASE.TRANSACTIONS
    GROUP BY CATEGORY
)
SELECT
    t.TXN_ID,
    t.CATEGORY,
    t.AMOUNT,
    s.AVG_AMOUNT,
    CURATED.ANOMALY_SCORE(t.AMOUNT, s.AVG_AMOUNT, s.STDDEV_AMOUNT) AS ANOMALY_SCORE
FROM BASE.TRANSACTIONS t
JOIN stats s ON t.CATEGORY = s.CATEGORY
ORDER BY ANOMALY_SCORE DESC
LIMIT 20;


-- ============================================================
-- 4. PYTHON UDF: Categorize Customer Risk Tier
-- ============================================================

CREATE OR REPLACE FUNCTION CURATED.RISK_TIER(
    risk_score INT,
    credit_score INT,
    debt_to_income FLOAT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'categorize'
AS
$$
def categorize(risk_score, credit_score, debt_to_income):
    if risk_score is None:
        return 'UNASSESSED'
    # Composite risk evaluation
    composite = risk_score * 0.5
    if credit_score is not None:
        composite += (850 - credit_score) / 850 * 30
    if debt_to_income is not None:
        composite += min(debt_to_income * 20, 20)
    if composite >= 70:
        return 'CRITICAL'
    elif composite >= 50:
        return 'HIGH'
    elif composite >= 30:
        return 'MEDIUM'
    elif composite >= 15:
        return 'LOW'
    else:
        return 'MINIMAL'
$$;

-- Apply risk tier UDF
SELECT
    CUSTOMER_ID,
    FIRST_NAME || ' ' || LAST_NAME AS CUSTOMER_NAME,
    CREDIT_SCORE,
    LATEST_RISK_SCORE,
    DEBT_TO_INCOME,
    CURATED.RISK_TIER(LATEST_RISK_SCORE, CREDIT_SCORE, DEBT_TO_INCOME) AS RISK_TIER
FROM CURATED.DT_CUSTOMER_PROFILE
WHERE LATEST_RISK_SCORE IS NOT NULL
ORDER BY LATEST_RISK_SCORE DESC
LIMIT 20;


-- ============================================================
-- 5. PYTHON UDTF: Parse Risk Assessment Factors
--    Returns one row per risk factor from VARIANT data.
-- ============================================================

CREATE OR REPLACE FUNCTION RAW.PARSE_RISK_FACTORS(RISK_DATA VARIANT)
RETURNS TABLE (
    RISK_SCORE       INT,
    RISK_LEVEL       VARCHAR,
    FACTOR           VARCHAR,
    DEBT_TO_INCOME   FLOAT,
    MISSED_PAYMENTS  INT,
    CREDIT_AGE       INT,
    AVG_BALANCE      FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'RiskParser'
AS
$$
class RiskParser:
    def process(self, risk_data):
        if risk_data is None:
            return
        score = risk_data.get("risk_score")
        level = risk_data.get("risk_level", "UNKNOWN")
        dti = risk_data.get("debt_to_income")
        history = risk_data.get("credit_history", {}) or {}
        missed = history.get("missed_payments")
        age = history.get("credit_age_months")
        avg_bal = history.get("avg_balance")

        factors = risk_data.get("factors", []) or []
        if not factors:
            yield (score, level, "NONE", dti, missed, age, avg_bal)
        else:
            for factor in factors:
                yield (score, level, str(factor), dti, missed, age, avg_bal)
$$;

-- Use the UDTF
SELECT
    r.CUSTOMER_ID,
    r.ASSESSMENT_DATE,
    parsed.*
FROM BASE.RISK_ASSESSMENTS r,
TABLE(RAW.PARSE_RISK_FACTORS(r.RISK_DATA)) parsed
WHERE parsed.RISK_LEVEL IN ('HIGH', 'CRITICAL')
ORDER BY parsed.RISK_SCORE DESC
LIMIT 30;


-- ============================================================
-- 6. PYTHON SP: Pipeline Summary Report
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
        ("BASE",        "CUSTOMERS",              "TABLE"),
        ("BASE",        "ACCOUNTS",               "TABLE"),
        ("BASE",        "TRANSACTIONS",           "TABLE"),
        ("BASE",        "RISK_ASSESSMENTS",       "TABLE (VARIANT)"),
        ("BASE",        "MARKET_DATA",            "TABLE (VARIANT)"),
        ("BASE",        "SUPPORT_TICKETS",        "TABLE (TEXT)"),
        ("BASE",        "COMPLIANCE_DOCUMENTS",   "TABLE (TEXT+VARIANT)"),
        ("CURATED",     "DT_CUSTOMER_PROFILE",    "DYNAMIC TABLE"),
        ("CURATED",     "DT_TRANSACTION_ENRICHED","DYNAMIC TABLE"),
        ("CURATED",     "DT_SUPPORT_ENRICHED",    "DYNAMIC TABLE"),
        ("CURATED",     "MV_MARKET_LATEST",       "MATERIALIZED VIEW"),
        ("CURATED",     "DT_RISK_FACTORS_PARSED", "DYNAMIC TABLE"),
        ("CONSUMPTION", "DT_CUSTOMER_360",        "DYNAMIC TABLE"),
        ("CONSUMPTION", "DT_DAILY_FINANCIAL_METRICS", "DYNAMIC TABLE"),
        ("CONSUMPTION", "DT_RISK_DASHBOARD",      "DYNAMIC TABLE"),
        ("CONSUMPTION", "DT_CHANNEL_PERFORMANCE", "DYNAMIC TABLE"),
        ("CONSUMPTION", "DT_CHURN_FEATURES",      "DYNAMIC TABLE"),
        ("CONSUMPTION", "DT_MONTHLY_REVENUE",     "DYNAMIC TABLE"),
        ("CONSUMPTION", "CUSTOMER_RFM",           "TABLE (Python SP)"),
    ]
    results = []
    for schema, table, obj_type in objects:
        try:
            cnt = session.table(f"FINSERV_DB.{schema}.{table}").count()
        except Exception:
            cnt = -1
        results.append((schema, table, obj_type, cnt))
    return session.create_dataframe(
        results,
        schema=["LAYER", "OBJECT_NAME", "OBJECT_TYPE", "ROW_COUNT"]
    )
$$;

CALL CONSUMPTION.SP_PIPELINE_SUMMARY();
