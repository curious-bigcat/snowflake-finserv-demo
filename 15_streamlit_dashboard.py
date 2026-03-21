"""
=============================================================================
  FINSERV DEMO — Step 15: Streamlit Dashboard
  Multi-tab KPI dashboard for the financial services consumption layer.

  Run locally:   streamlit run 15_streamlit_dashboard.py
  Run in Snowsight: Upload as a Streamlit app
=============================================================================
"""

import streamlit as st
import pandas as pd
import altair as alt

# ============================================================
# CONNECTION
# ============================================================
@st.cache_resource
def get_connection():
    """Get Snowflake connection — works in Snowsight and locally."""
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        return st.connection("snowflake")


def run_query(sql):
    """Execute SQL and return a pandas DataFrame."""
    conn = get_connection()
    if hasattr(conn, "sql"):
        return conn.sql(sql).to_pandas()
    else:
        return conn.query(sql)


# ============================================================
# DATA LOADERS (cached 5 min)
# ============================================================
@st.cache_data(ttl=300)
def load_customer_360():
    return run_query("SELECT * FROM FINSERV_DB.CONSUMPTION.DT_CUSTOMER_360")

@st.cache_data(ttl=300)
def load_daily_metrics():
    return run_query("SELECT * FROM FINSERV_DB.CONSUMPTION.DT_DAILY_FINANCIAL_METRICS ORDER BY METRIC_DATE")

@st.cache_data(ttl=300)
def load_risk_dashboard():
    return run_query("SELECT * FROM FINSERV_DB.CONSUMPTION.DT_RISK_DASHBOARD")

@st.cache_data(ttl=300)
def load_channel_performance():
    return run_query("SELECT * FROM FINSERV_DB.CONSUMPTION.DT_CHANNEL_PERFORMANCE ORDER BY METRIC_DATE")

@st.cache_data(ttl=300)
def load_pipeline_metrics():
    return run_query("""
        SELECT 'CUSTOMERS' AS TBL, COUNT(*) AS CNT FROM FINSERV_DB.BASE.CUSTOMERS
        UNION ALL SELECT 'ACCOUNTS', COUNT(*) FROM FINSERV_DB.BASE.ACCOUNTS
        UNION ALL SELECT 'TRANSACTIONS', COUNT(*) FROM FINSERV_DB.BASE.TRANSACTIONS
        UNION ALL SELECT 'RISK_ASSESSMENTS', COUNT(*) FROM FINSERV_DB.BASE.RISK_ASSESSMENTS
        UNION ALL SELECT 'MARKET_DATA', COUNT(*) FROM FINSERV_DB.BASE.MARKET_DATA
        UNION ALL SELECT 'SUPPORT_TICKETS', COUNT(*) FROM FINSERV_DB.BASE.SUPPORT_TICKETS
        UNION ALL SELECT 'COMPLIANCE_DOCS', COUNT(*) FROM FINSERV_DB.BASE.COMPLIANCE_DOCUMENTS
        ORDER BY TBL
    """)

@st.cache_data(ttl=300)
def load_monthly_revenue():
    return run_query("SELECT * FROM FINSERV_DB.CONSUMPTION.DT_MONTHLY_REVENUE ORDER BY REVENUE_MONTH")

@st.cache_data(ttl=300)
def load_churn_summary():
    return run_query("""
        SELECT IS_CHURNED, COUNT(*) AS CUSTOMER_COUNT,
               ROUND(AVG(TOTAL_BALANCE), 2) AS AVG_BALANCE,
               ROUND(AVG(RISK_SCORE), 1) AS AVG_RISK,
               ROUND(AVG(TOTAL_TRANSACTIONS), 0) AS AVG_TXNS
        FROM FINSERV_DB.CONSUMPTION.DT_CHURN_FEATURES
        GROUP BY IS_CHURNED
    """)


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="FinServ Analytics",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Financial Services Analytics Dashboard")

# Sidebar
with st.sidebar:
    st.header("FinServ Demo")
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.caption("Powered by Snowflake")

# Tabs
tab_overview, tab_risk, tab_customers, tab_channels, tab_pipeline = st.tabs([
    "Overview", "Risk", "Customers", "Channels", "Pipeline Health"
])


# ============================================================
# TAB 1: OVERVIEW
# ============================================================
with tab_overview:
    st.header("Financial Overview")

    daily = load_daily_metrics()
    c360 = load_customer_360()
    try:
        monthly = load_monthly_revenue()
    except Exception:
        monthly = pd.DataFrame()

    # KPI cards
    col1, col2, col3, col4 = st.columns(4)
    total_volume = daily["TOTAL_VOLUME"].sum() if not daily.empty else 0
    total_txns = daily["TOTAL_TRANSACTIONS"].sum() if not daily.empty else 0
    total_customers = len(c360) if not c360.empty else 0
    total_aum = c360["TOTAL_BALANCE"].sum() if not c360.empty else 0

    col1.metric("Total Volume", f"${total_volume:,.0f}")
    col2.metric("Total Transactions", f"{total_txns:,.0f}")
    col3.metric("Total Customers", f"{total_customers:,}")
    col4.metric("Assets Under Mgmt", f"${total_aum:,.0f}")

    # Daily volume chart
    if not daily.empty:
        st.subheader("Daily Transaction Volume")
        volume_chart = alt.Chart(daily).mark_area(
            opacity=0.6, color="#1f77b4"
        ).encode(
            x=alt.X("METRIC_DATE:T", title="Date"),
            y=alt.Y("TOTAL_VOLUME:Q", title="Volume ($)"),
            tooltip=["METRIC_DATE:T", "TOTAL_VOLUME:Q", "TOTAL_TRANSACTIONS:Q"]
        ).properties(height=300)
        st.altair_chart(volume_chart, use_container_width=True)

    # Monthly revenue trend
    if not monthly.empty:
        st.subheader("Monthly Revenue Trend")
        monthly_chart = alt.Chart(monthly).mark_bar(
            color="#2ca02c", opacity=0.8
        ).encode(
            x=alt.X("REVENUE_MONTH:T", title="Month"),
            y=alt.Y("MONTHLY_VOLUME:Q", title="Monthly Volume ($)"),
            tooltip=["REVENUE_MONTH:T", "MONTHLY_VOLUME:Q", "MONTHLY_TRANSACTIONS:Q"]
        ).properties(height=250)
        st.altair_chart(monthly_chart, use_container_width=True)

    # Flagged transactions trend
    if not daily.empty:
        st.subheader("Flagged Transaction Rate")
        flag_chart = alt.Chart(daily).mark_line(
            color="#d62728", strokeWidth=2
        ).encode(
            x=alt.X("METRIC_DATE:T", title="Date"),
            y=alt.Y("FLAGGED_RATE:Q", title="Flag Rate (%)"),
            tooltip=["METRIC_DATE:T", "FLAGGED_RATE:Q", "FLAGGED_COUNT:Q"]
        ).properties(height=200)
        st.altair_chart(flag_chart, use_container_width=True)


# ============================================================
# TAB 2: RISK
# ============================================================
with tab_risk:
    st.header("Risk Dashboard")

    risk = load_risk_dashboard()
    if not risk.empty:
        # Risk tier distribution
        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader("Risk Tier Distribution")
            tier_counts = risk["RISK_TIER"].value_counts().reset_index()
            tier_counts.columns = ["RISK_TIER", "COUNT"]
            tier_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "MINIMAL"]
            tier_colors = {"CRITICAL": "#d62728", "HIGH": "#ff7f0e", "MEDIUM": "#ffbb78",
                          "LOW": "#2ca02c", "MINIMAL": "#98df8a"}

            tier_chart = alt.Chart(tier_counts).mark_bar().encode(
                x=alt.X("RISK_TIER:N", sort=tier_order, title="Risk Tier"),
                y=alt.Y("COUNT:Q", title="Customers"),
                color=alt.Color("RISK_TIER:N", scale=alt.Scale(
                    domain=list(tier_colors.keys()), range=list(tier_colors.values())
                ), legend=None),
                tooltip=["RISK_TIER:N", "COUNT:Q"]
            ).properties(height=300)
            st.altair_chart(tier_chart, use_container_width=True)

        with col2:
            st.subheader("Risk Score vs Balance")
            scatter = alt.Chart(risk).mark_circle(size=40, opacity=0.6).encode(
                x=alt.X("LATEST_RISK_SCORE:Q", title="Risk Score"),
                y=alt.Y("TOTAL_BALANCE:Q", title="Total Balance ($)"),
                color=alt.Color("RISK_TIER:N", scale=alt.Scale(
                    domain=list(tier_colors.keys()), range=list(tier_colors.values())
                )),
                tooltip=["CUSTOMER_NAME:N", "LATEST_RISK_SCORE:Q",
                         "TOTAL_BALANCE:Q", "CREDIT_SCORE:Q"]
            ).properties(height=300)
            st.altair_chart(scatter, use_container_width=True)

        # High risk customer table
        st.subheader("High Risk Customers")
        high_risk = risk[risk["RISK_TIER"].isin(["CRITICAL", "HIGH"])].sort_values(
            "LATEST_RISK_SCORE", ascending=False
        ).head(20)
        st.dataframe(
            high_risk[["CUSTOMER_NAME", "CITY", "COUNTRY", "CREDIT_SCORE",
                       "TOTAL_BALANCE", "LATEST_RISK_SCORE", "DEBT_TO_INCOME", "RISK_TIER"]],
            use_container_width=True, hide_index=True
        )


# ============================================================
# TAB 3: CUSTOMERS
# ============================================================
with tab_customers:
    st.header("Customer Insights")

    c360 = load_customer_360()
    churn = load_churn_summary()

    if not c360.empty:
        # Segment distribution
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Customer Segments")
            seg_counts = c360["CUSTOMER_SEGMENT"].value_counts().reset_index()
            seg_counts.columns = ["SEGMENT", "COUNT"]
            seg_chart = alt.Chart(seg_counts).mark_arc(innerRadius=50).encode(
                theta=alt.Theta("COUNT:Q"),
                color=alt.Color("SEGMENT:N", scale=alt.Scale(scheme="tableau10")),
                tooltip=["SEGMENT:N", "COUNT:Q"]
            ).properties(height=300)
            st.altair_chart(seg_chart, use_container_width=True)

        with col2:
            st.subheader("Churn Risk Summary")
            if not churn.empty:
                churn_display = churn.copy()
                churn_display["IS_CHURNED"] = churn_display["IS_CHURNED"].map({0: "Active", 1: "Churned"})
                st.dataframe(churn_display, use_container_width=True, hide_index=True)

                churn_chart = alt.Chart(churn_display).mark_bar().encode(
                    x=alt.X("IS_CHURNED:N", title="Status"),
                    y=alt.Y("CUSTOMER_COUNT:Q", title="Customers"),
                    color=alt.Color("IS_CHURNED:N", scale=alt.Scale(
                        domain=["Active", "Churned"], range=["#2ca02c", "#d62728"]
                    ), legend=None)
                ).properties(height=200)
                st.altair_chart(churn_chart, use_container_width=True)

        # Top customers table
        st.subheader("Top Customers by Balance")
        top_customers = c360.nlargest(20, "TOTAL_BALANCE")
        st.dataframe(
            top_customers[["FIRST_NAME", "LAST_NAME", "CITY", "COUNTRY", "CUSTOMER_SEGMENT",
                          "TOTAL_BALANCE", "TOTAL_TRANSACTIONS", "TOTAL_SPENT",
                          "LATEST_RISK_SCORE", "TOTAL_TICKETS"]],
            use_container_width=True, hide_index=True
        )

        # Country distribution
        st.subheader("Customers by Country")
        country_dist = c360["COUNTRY"].value_counts().reset_index().head(15)
        country_dist.columns = ["COUNTRY", "COUNT"]
        country_chart = alt.Chart(country_dist).mark_bar(color="#1f77b4", opacity=0.8).encode(
            x=alt.X("COUNT:Q", title="Customers"),
            y=alt.Y("COUNTRY:N", sort="-x", title="Country"),
            tooltip=["COUNTRY:N", "COUNT:Q"]
        ).properties(height=350)
        st.altair_chart(country_chart, use_container_width=True)


# ============================================================
# TAB 4: CHANNELS
# ============================================================
with tab_channels:
    st.header("Channel Performance")

    channel = load_channel_performance()
    if not channel.empty:
        # Aggregate by channel
        channel_agg = channel.groupby("CHANNEL").agg({
            "TRANSACTION_COUNT": "sum",
            "TOTAL_VOLUME": "sum",
            "UNIQUE_CUSTOMERS": "sum",
            "FLAGGED_COUNT": "sum"
        }).reset_index()

        # KPI cards per channel
        st.subheader("Channel Summary")
        cols = st.columns(len(channel_agg))
        for i, (_, row) in enumerate(channel_agg.iterrows()):
            with cols[i]:
                st.metric(row["CHANNEL"],
                         f"${row['TOTAL_VOLUME']:,.0f}",
                         f"{row['TRANSACTION_COUNT']:,.0f} txns")

        # Volume by channel over time
        st.subheader("Daily Volume by Channel")
        channel_time = alt.Chart(channel).mark_area(opacity=0.7).encode(
            x=alt.X("METRIC_DATE:T", title="Date"),
            y=alt.Y("TOTAL_VOLUME:Q", title="Volume ($)", stack="zero"),
            color=alt.Color("CHANNEL:N", scale=alt.Scale(scheme="tableau10")),
            tooltip=["METRIC_DATE:T", "CHANNEL:N", "TOTAL_VOLUME:Q", "TRANSACTION_COUNT:Q"]
        ).properties(height=350)
        st.altair_chart(channel_time, use_container_width=True)

        # Channel comparison bar chart
        st.subheader("Total Volume by Channel")
        vol_chart = alt.Chart(channel_agg).mark_bar(opacity=0.8).encode(
            x=alt.X("CHANNEL:N", title="Channel", sort="-y"),
            y=alt.Y("TOTAL_VOLUME:Q", title="Total Volume ($)"),
            color=alt.Color("CHANNEL:N", legend=None, scale=alt.Scale(scheme="tableau10")),
            tooltip=["CHANNEL:N", "TOTAL_VOLUME:Q", "TRANSACTION_COUNT:Q"]
        ).properties(height=300)
        st.altair_chart(vol_chart, use_container_width=True)


# ============================================================
# TAB 5: PIPELINE HEALTH
# ============================================================
with tab_pipeline:
    st.header("Pipeline Health")

    pipeline = load_pipeline_metrics()
    if not pipeline.empty:
        st.subheader("Row Counts by Table")
        row_chart = alt.Chart(pipeline).mark_bar(color="#1f77b4", opacity=0.8).encode(
            x=alt.X("CNT:Q", title="Row Count"),
            y=alt.Y("TBL:N", sort="-x", title="Table"),
            tooltip=["TBL:N", "CNT:Q"]
        ).properties(height=300)
        st.altair_chart(row_chart, use_container_width=True)

        st.dataframe(pipeline, use_container_width=True, hide_index=True)

    # Dynamic table status
    st.subheader("Dynamic Table Refresh Status")
    try:
        dt_status = run_query("SHOW DYNAMIC TABLES IN DATABASE FINSERV_DB")
        if not dt_status.empty:
            display_cols = [c for c in ["name", "schema_name", "target_lag", "refresh_mode",
                                        "scheduling_state"] if c in dt_status.columns]
            if display_cols:
                st.dataframe(dt_status[display_cols], use_container_width=True, hide_index=True)
            else:
                st.dataframe(dt_status, use_container_width=True, hide_index=True)
    except Exception as e:
        st.info(f"Dynamic table status unavailable: {e}")

    # Stream status
    st.subheader("Stream Status")
    try:
        stream_info = run_query("SHOW STREAMS IN DATABASE FINSERV_DB")
        if not stream_info.empty:
            display_cols = [c for c in ["name", "schema_name", "source_type", "table_name",
                                        "stale", "stale_after"] if c in stream_info.columns]
            if display_cols:
                st.dataframe(stream_info[display_cols], use_container_width=True, hide_index=True)
            else:
                st.dataframe(stream_info, use_container_width=True, hide_index=True)
    except Exception as e:
        st.info(f"Stream status unavailable: {e}")

    # Pipe status
    st.subheader("Snowpipe Status")
    try:
        pipe_info = run_query("SHOW PIPES IN SCHEMA FINSERV_DB.RAW")
        if not pipe_info.empty:
            display_cols = [c for c in ["name", "definition", "notification_channel"]
                           if c in pipe_info.columns]
            if display_cols:
                st.dataframe(pipe_info[display_cols].head(10),
                             use_container_width=True, hide_index=True)
            else:
                st.dataframe(pipe_info.head(10), use_container_width=True, hide_index=True)
        else:
            st.info("No pipes found in RAW schema")
    except Exception as e:
        st.info(f"Pipe status unavailable: {e}")
