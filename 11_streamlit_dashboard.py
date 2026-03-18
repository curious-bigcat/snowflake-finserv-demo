"""
HOL Snowpark Demo — Consumption Layer Dashboard

Interactive dashboard visualizing the CONSUMPTION schema objects:
- KPI metrics from PIPELINE_METRICS
- Daily sales trends from DT_DAILY_SALES
- Product performance from DT_PRODUCT_PERFORMANCE
- Customer insights from DT_CUSTOMER_360
- Category trends from DT_CATEGORY_TRENDS
- Pipeline health monitoring

Runs locally with: streamlit run 11_streamlit_dashboard.py
Runs in Snowsight: Upload as Streamlit app (uses get_active_session)
"""

import streamlit as st
import altair as alt
import pandas as pd

st.set_page_config(
    page_title="HOL Demo — E-Commerce Analytics",
    page_icon=":material/analytics:",
    layout="wide",
)


# =============================================================================
# Snowflake Connection
# =============================================================================

def get_connection():
    """Get Snowflake connection. Works both in Snowsight and locally."""
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        return session, "snowpark"
    except Exception:
        conn = st.connection("snowflake")
        return conn, "stconnection"


def run_query(conn, conn_type, sql, ttl=600):
    """Run a query and return a pandas DataFrame."""
    if conn_type == "snowpark":
        return conn.sql(sql).to_pandas()
    else:
        return conn.query(sql, ttl=ttl)


CONN, CONN_TYPE = get_connection()


# =============================================================================
# Data Loading (cached)
# =============================================================================

@st.cache_data(ttl=300, show_spinner=False)
def load_pipeline_metrics():
    return run_query(CONN, CONN_TYPE, """
        SELECT TOTAL_ORDERS, TOTAL_REVENUE, TOTAL_CUSTOMERS,
               AVG_ORDER_VALUE, TOTAL_EVENTS, UNIQUE_SESSIONS, REFRESHED_AT
        FROM HOL_DB.CONSUMPTION.PIPELINE_METRICS
        ORDER BY METRIC_DATE DESC LIMIT 1
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_daily_sales():
    return run_query(CONN, CONN_TYPE, """
        SELECT SALE_DATE, NUM_ORDERS, UNIQUE_CUSTOMERS, TOTAL_ITEMS_SOLD,
               GROSS_REVENUE, TOTAL_DISCOUNTS, AVG_SHIPPING_COST,
               CREDIT_CARD_ORDERS, ALTERNATIVE_PAYMENT_ORDERS
        FROM HOL_DB.CONSUMPTION.DT_DAILY_SALES
        ORDER BY SALE_DATE
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_product_performance():
    return run_query(CONN, CONN_TYPE, """
        SELECT PRODUCT_NAME, CATEGORY, SUB_CATEGORY, LIST_PRICE,
               TIMES_ORDERED, TOTAL_UNITS_SOLD, TOTAL_REVENUE,
               AVG_LINE_VALUE, TOTAL_DISCOUNT_GIVEN, UNIQUE_BUYERS
        FROM HOL_DB.CONSUMPTION.DT_PRODUCT_PERFORMANCE
        ORDER BY TOTAL_REVENUE DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_customer_360():
    return run_query(CONN, CONN_TYPE, """
        SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, COUNTRY,
               TOTAL_ORDERS, LIFETIME_VALUE, AVG_ORDER_VALUE,
               TOTAL_EVENTS, PAGE_VIEWS, ADD_TO_CARTS, CHECKOUTS,
               CUSTOMER_SEGMENT, CART_CONVERSION_RATE
        FROM HOL_DB.CONSUMPTION.DT_CUSTOMER_360
        ORDER BY LIFETIME_VALUE DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_category_trends():
    return run_query(CONN, CONN_TYPE, """
        SELECT MONTH, CATEGORY, SUB_CATEGORY,
               NUM_ORDERS, UNITS_SOLD, REVENUE, UNIQUE_CUSTOMERS
        FROM HOL_DB.CONSUMPTION.DT_CATEGORY_TRENDS
        ORDER BY MONTH, CATEGORY
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_row_counts():
    return run_query(CONN, CONN_TYPE, """
        SELECT 'RAW.CUSTOMERS' AS TBL, COUNT(*) AS CNT FROM HOL_DB.RAW.CUSTOMERS
        UNION ALL SELECT 'RAW.PRODUCTS', COUNT(*) FROM HOL_DB.RAW.PRODUCTS
        UNION ALL SELECT 'RAW.ORDERS', COUNT(*) FROM HOL_DB.RAW.ORDERS
        UNION ALL SELECT 'RAW.WEBSITE_EVENTS', COUNT(*) FROM HOL_DB.RAW.WEBSITE_EVENTS
        UNION ALL SELECT 'CURATED.DT_CUSTOMER_SUMMARY', COUNT(*) FROM HOL_DB.CURATED.DT_CUSTOMER_SUMMARY
        UNION ALL SELECT 'CURATED.DT_ORDER_ENRICHED', COUNT(*) FROM HOL_DB.CURATED.DT_ORDER_ENRICHED
        UNION ALL SELECT 'CURATED.DT_EVENT_PARSED', COUNT(*) FROM HOL_DB.CURATED.DT_EVENT_PARSED
        UNION ALL SELECT 'CONSUMPTION.DT_DAILY_SALES', COUNT(*) FROM HOL_DB.CONSUMPTION.DT_DAILY_SALES
        UNION ALL SELECT 'CONSUMPTION.DT_PRODUCT_PERFORMANCE', COUNT(*) FROM HOL_DB.CONSUMPTION.DT_PRODUCT_PERFORMANCE
        UNION ALL SELECT 'CONSUMPTION.DT_CUSTOMER_360', COUNT(*) FROM HOL_DB.CONSUMPTION.DT_CUSTOMER_360
        UNION ALL SELECT 'CONSUMPTION.DT_CATEGORY_TRENDS', COUNT(*) FROM HOL_DB.CONSUMPTION.DT_CATEGORY_TRENDS
        UNION ALL SELECT 'CONSUMPTION.PIPELINE_METRICS', COUNT(*) FROM HOL_DB.CONSUMPTION.PIPELINE_METRICS
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_order_status_breakdown():
    return run_query(CONN, CONN_TYPE, """
        SELECT STATUS, COUNT(*) AS CNT, SUM(TOTAL_AMOUNT) AS REVENUE
        FROM HOL_DB.RAW.ORDERS
        GROUP BY STATUS
        ORDER BY CNT DESC
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_dynamic_table_refresh_history():
    return run_query(CONN, CONN_TYPE, """
        SELECT NAME, STATE, STATE_MESSAGE,
               REFRESH_START_TIME, REFRESH_END_TIME,
               DATEDIFF('second', REFRESH_START_TIME, REFRESH_END_TIME) AS DURATION_SEC
        FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
            NAME_PREFIX => 'HOL_DB.'
        ))
        ORDER BY REFRESH_START_TIME DESC
        LIMIT 20
    """)


# =============================================================================
# Page Header
# =============================================================================

with st.container(horizontal=True, horizontal_alignment="distribute", vertical_alignment="center"):
    st.markdown("# :material/analytics: E-Commerce Analytics")
    if st.button(":material/refresh: Refresh Data", type="tertiary"):
        st.cache_data.clear()
        st.rerun()

st.caption("HOL Snowpark Demo — Powered by Snowflake Dynamic Tables & Materialized Views")

# =============================================================================
# Tab Layout
# =============================================================================

tab_overview, tab_products, tab_customers, tab_trends, tab_pipeline = st.tabs([
    ":material/dashboard: Overview",
    ":material/inventory_2: Products",
    ":material/group: Customers",
    ":material/trending_up: Category Trends",
    ":material/monitor_heart: Pipeline Health",
])


# =============================================================================
# Tab 1: Overview — KPIs + Daily Sales
# =============================================================================

with tab_overview:
    # KPI Row
    metrics_df = load_pipeline_metrics()
    if not metrics_df.empty:
        row = metrics_df.iloc[0]
        with st.container(horizontal=True):
            st.metric("Total Revenue", f"${row['TOTAL_REVENUE']:,.2f}", border=True)
            st.metric("Total Orders", f"{int(row['TOTAL_ORDERS']):,}", border=True)
            st.metric("Total Customers", f"{int(row['TOTAL_CUSTOMERS']):,}", border=True)
            st.metric("Avg Order Value", f"${row['AVG_ORDER_VALUE']:,.2f}", border=True)
            st.metric("Website Events", f"{int(row['TOTAL_EVENTS']):,}", border=True)
            st.metric("Unique Sessions", f"{int(row['UNIQUE_SESSIONS']):,}", border=True)

    # Daily Sales Charts
    sales_df = load_daily_sales()
    if not sales_df.empty:
        sales_df["SALE_DATE"] = pd.to_datetime(sales_df["SALE_DATE"])

        col1, col2 = st.columns(2)

        with col1:
            with st.container(border=True):
                st.markdown("**Daily Revenue**")
                revenue_chart = (
                    alt.Chart(sales_df)
                    .mark_bar(color="#29B5E8", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                    .encode(
                        x=alt.X("SALE_DATE:T", title="Date"),
                        y=alt.Y("GROSS_REVENUE:Q", title="Revenue ($)"),
                        tooltip=[
                            alt.Tooltip("SALE_DATE:T", title="Date", format="%b %d, %Y"),
                            alt.Tooltip("GROSS_REVENUE:Q", title="Revenue", format="$,.2f"),
                            alt.Tooltip("NUM_ORDERS:Q", title="Orders"),
                        ],
                    )
                    .properties(height=300)
                )
                st.altair_chart(revenue_chart, use_container_width=True)

        with col2:
            with st.container(border=True):
                st.markdown("**Orders & Customers per Day**")
                orders_melted = sales_df.melt(
                    id_vars=["SALE_DATE"],
                    value_vars=["NUM_ORDERS", "UNIQUE_CUSTOMERS"],
                    var_name="Metric",
                    value_name="Count",
                )
                orders_melted["Metric"] = orders_melted["Metric"].map({
                    "NUM_ORDERS": "Orders",
                    "UNIQUE_CUSTOMERS": "Unique Customers",
                })
                orders_chart = (
                    alt.Chart(orders_melted)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("SALE_DATE:T", title="Date"),
                        y=alt.Y("Count:Q", title="Count"),
                        color=alt.Color("Metric:N", legend=alt.Legend(orient="bottom")),
                        tooltip=[
                            alt.Tooltip("SALE_DATE:T", title="Date", format="%b %d, %Y"),
                            alt.Tooltip("Metric:N"),
                            alt.Tooltip("Count:Q"),
                        ],
                    )
                    .properties(height=300)
                )
                st.altair_chart(orders_chart, use_container_width=True)

        # Payment Methods + Order Status
        col_pay, col_status = st.columns(2)

        with col_pay:
            with st.container(border=True):
                st.markdown("**Payment Method Split**")
                pay_df = sales_df[["SALE_DATE", "CREDIT_CARD_ORDERS", "ALTERNATIVE_PAYMENT_ORDERS"]].copy()
                pay_melted = pay_df.melt(
                    id_vars=["SALE_DATE"],
                    value_vars=["CREDIT_CARD_ORDERS", "ALTERNATIVE_PAYMENT_ORDERS"],
                    var_name="Method",
                    value_name="Orders",
                )
                pay_melted["Method"] = pay_melted["Method"].map({
                    "CREDIT_CARD_ORDERS": "Credit Card",
                    "ALTERNATIVE_PAYMENT_ORDERS": "Alternative",
                })
                pay_chart = (
                    alt.Chart(pay_melted)
                    .mark_bar()
                    .encode(
                        x=alt.X("SALE_DATE:T", title="Date"),
                        y=alt.Y("Orders:Q", title="Line Items", stack=True),
                        color=alt.Color("Method:N", legend=alt.Legend(orient="bottom")),
                        tooltip=[
                            alt.Tooltip("SALE_DATE:T", title="Date", format="%b %d, %Y"),
                            alt.Tooltip("Method:N"),
                            alt.Tooltip("Orders:Q"),
                        ],
                    )
                    .properties(height=250)
                )
                st.altair_chart(pay_chart, use_container_width=True)

        with col_status:
            with st.container(border=True):
                st.markdown("**Order Status Breakdown**")
                status_df = load_order_status_breakdown()
                if not status_df.empty:
                    status_chart = (
                        alt.Chart(status_df)
                        .mark_arc(innerRadius=50)
                        .encode(
                            theta=alt.Theta("CNT:Q"),
                            color=alt.Color(
                                "STATUS:N",
                                scale=alt.Scale(
                                    domain=["DELIVERED", "SHIPPED", "PROCESSING"],
                                    range=["#2ECC71", "#29B5E8", "#FFB347"],
                                ),
                                legend=alt.Legend(orient="bottom"),
                            ),
                            tooltip=[
                                alt.Tooltip("STATUS:N", title="Status"),
                                alt.Tooltip("CNT:Q", title="Orders"),
                                alt.Tooltip("REVENUE:Q", title="Revenue", format="$,.2f"),
                            ],
                        )
                        .properties(height=250)
                    )
                    st.altair_chart(status_chart, use_container_width=True)

    # Cumulative revenue over time
    if not sales_df.empty:
        with st.container(border=True):
            st.markdown("**Cumulative Revenue Over Time**")
            cumulative_df = sales_df[["SALE_DATE", "GROSS_REVENUE"]].copy()
            cumulative_df = cumulative_df.sort_values("SALE_DATE")
            cumulative_df["CUMULATIVE_REVENUE"] = cumulative_df["GROSS_REVENUE"].cumsum()
            cum_chart = (
                alt.Chart(cumulative_df)
                .mark_area(
                    color=alt.Gradient(
                        gradient="linear",
                        stops=[
                            alt.GradientStop(color="#29B5E8", offset=0),
                            alt.GradientStop(color="rgba(41,181,232,0.1)", offset=1),
                        ],
                        x1=1, x2=1, y1=1, y2=0,
                    ),
                    line={"color": "#29B5E8"},
                )
                .encode(
                    x=alt.X("SALE_DATE:T", title="Date"),
                    y=alt.Y("CUMULATIVE_REVENUE:Q", title="Cumulative Revenue ($)"),
                    tooltip=[
                        alt.Tooltip("SALE_DATE:T", title="Date", format="%b %d, %Y"),
                        alt.Tooltip("CUMULATIVE_REVENUE:Q", title="Cumulative", format="$,.2f"),
                        alt.Tooltip("GROSS_REVENUE:Q", title="Daily", format="$,.2f"),
                    ],
                )
                .properties(height=250)
            )
            st.altair_chart(cum_chart, use_container_width=True)


# =============================================================================
# Tab 2: Product Performance
# =============================================================================

with tab_products:
    prod_df = load_product_performance()
    if not prod_df.empty:
        # KPIs
        with st.container(horizontal=True):
            st.metric("Total Products", len(prod_df), border=True)
            st.metric("Total Product Revenue", f"${prod_df['TOTAL_REVENUE'].sum():,.2f}", border=True)
            st.metric("Total Units Sold", f"{int(prod_df['TOTAL_UNITS_SOLD'].sum()):,}", border=True)
            st.metric("Avg Revenue/Product", f"${prod_df['TOTAL_REVENUE'].mean():,.2f}", border=True)

        # Category filter
        categories = ["All"] + sorted(prod_df["CATEGORY"].unique().tolist())
        selected_cat = st.segmented_control("Filter by category", categories, default="All")
        if selected_cat and selected_cat != "All":
            filtered_prod = prod_df[prod_df["CATEGORY"] == selected_cat]
        else:
            filtered_prod = prod_df

        col1, col2 = st.columns(2)

        with col1:
            with st.container(border=True):
                st.markdown("**Revenue by Product**")
                prod_chart = (
                    alt.Chart(filtered_prod)
                    .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                    .encode(
                        x=alt.X("TOTAL_REVENUE:Q", title="Total Revenue ($)"),
                        y=alt.Y("PRODUCT_NAME:N", sort="-x", title=None),
                        color=alt.Color("CATEGORY:N", legend=alt.Legend(orient="bottom")),
                        tooltip=[
                            alt.Tooltip("PRODUCT_NAME:N", title="Product"),
                            alt.Tooltip("TOTAL_REVENUE:Q", title="Revenue", format="$,.2f"),
                            alt.Tooltip("TOTAL_UNITS_SOLD:Q", title="Units Sold"),
                            alt.Tooltip("UNIQUE_BUYERS:Q", title="Buyers"),
                        ],
                    )
                    .properties(height=400)
                )
                st.altair_chart(prod_chart, use_container_width=True)

        with col2:
            with st.container(border=True):
                st.markdown("**Units Sold vs Unique Buyers**")
                scatter = (
                    alt.Chart(filtered_prod)
                    .mark_circle(size=100)
                    .encode(
                        x=alt.X("TOTAL_UNITS_SOLD:Q", title="Units Sold"),
                        y=alt.Y("UNIQUE_BUYERS:Q", title="Unique Buyers"),
                        size=alt.Size("TOTAL_REVENUE:Q", legend=None),
                        color=alt.Color("CATEGORY:N", legend=alt.Legend(orient="bottom")),
                        tooltip=[
                            alt.Tooltip("PRODUCT_NAME:N", title="Product"),
                            alt.Tooltip("TOTAL_UNITS_SOLD:Q", title="Units"),
                            alt.Tooltip("UNIQUE_BUYERS:Q", title="Buyers"),
                            alt.Tooltip("TOTAL_REVENUE:Q", title="Revenue", format="$,.2f"),
                        ],
                    )
                    .properties(height=400)
                )
                st.altair_chart(scatter, use_container_width=True)

        # Revenue vs List Price comparison
        with st.container(border=True):
            st.markdown("**Discount impact: list price vs avg line value**")
            price_compare = filtered_prod[["PRODUCT_NAME", "LIST_PRICE", "AVG_LINE_VALUE"]].copy()
            price_melted = price_compare.melt(
                id_vars=["PRODUCT_NAME"],
                value_vars=["LIST_PRICE", "AVG_LINE_VALUE"],
                var_name="Price Type",
                value_name="Price",
            )
            price_melted["Price Type"] = price_melted["Price Type"].map({
                "LIST_PRICE": "List Price",
                "AVG_LINE_VALUE": "Avg Sold Price",
            })
            price_chart = (
                alt.Chart(price_melted)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("PRODUCT_NAME:N", title=None, sort="-y"),
                    y=alt.Y("Price:Q", title="Price ($)"),
                    color=alt.Color("Price Type:N", legend=alt.Legend(orient="bottom")),
                    xOffset="Price Type:N",
                    tooltip=[
                        alt.Tooltip("PRODUCT_NAME:N", title="Product"),
                        alt.Tooltip("Price Type:N"),
                        alt.Tooltip("Price:Q", format="$,.2f"),
                    ],
                )
                .properties(height=300)
            )
            st.altair_chart(price_chart, use_container_width=True)

        # Detailed Table
        with st.container(border=True):
            st.markdown("**Product Details**")
            st.dataframe(
                filtered_prod,
                column_config={
                    "TOTAL_REVENUE": st.column_config.NumberColumn("Total Revenue", format="$%.2f"),
                    "LIST_PRICE": st.column_config.NumberColumn("List Price", format="$%.2f"),
                    "AVG_LINE_VALUE": st.column_config.NumberColumn("Avg Line Value", format="$%.2f"),
                    "TOTAL_DISCOUNT_GIVEN": st.column_config.NumberColumn("Discounts", format="$%.2f"),
                },
                hide_index=True,
                use_container_width=True,
            )


# =============================================================================
# Tab 3: Customer Insights
# =============================================================================

with tab_customers:
    cust_df = load_customer_360()
    if not cust_df.empty:
        # KPIs
        with st.container(horizontal=True):
            st.metric("Total Customers", len(cust_df), border=True)
            st.metric("Avg LTV", f"${cust_df['LIFETIME_VALUE'].mean():,.2f}", border=True)
            high_value = len(cust_df[cust_df["CUSTOMER_SEGMENT"] == "HIGH_VALUE"])
            st.metric("High-value Customers", high_value, border=True)
            avg_conv = cust_df.loc[cust_df["CART_CONVERSION_RATE"] > 0, "CART_CONVERSION_RATE"].mean()
            st.metric("Avg Cart Conversion", f"{avg_conv:.1f}%", border=True)

        # Segment Distribution
        segment_counts = cust_df["CUSTOMER_SEGMENT"].value_counts().reset_index()
        segment_counts.columns = ["Segment", "Count"]

        col1, col2 = st.columns(2)

        with col1:
            with st.container(border=True):
                st.markdown("**Customer Segments**")
                seg_chart = (
                    alt.Chart(segment_counts)
                    .mark_arc(innerRadius=50)
                    .encode(
                        theta=alt.Theta("Count:Q"),
                        color=alt.Color(
                            "Segment:N",
                            scale=alt.Scale(
                                domain=["HIGH_VALUE", "MEDIUM_VALUE", "LOW_VALUE", "PROSPECT"],
                                range=["#29B5E8", "#71D4F5", "#FFB347", "#D3D3D3"],
                            ),
                            legend=alt.Legend(orient="bottom"),
                        ),
                        tooltip=[
                            alt.Tooltip("Segment:N"),
                            alt.Tooltip("Count:Q"),
                        ],
                    )
                    .properties(height=350)
                )
                st.altair_chart(seg_chart, use_container_width=True)

        with col2:
            with st.container(border=True):
                st.markdown("**Top Customers by Lifetime Value**")
                top_chart = (
                    alt.Chart(cust_df.head(10))
                    .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                    .encode(
                        x=alt.X("LIFETIME_VALUE:Q", title="Lifetime Value ($)"),
                        y=alt.Y("FIRST_NAME:N", sort="-x", title=None),
                        color=alt.Color("CUSTOMER_SEGMENT:N", legend=alt.Legend(orient="bottom")),
                        tooltip=[
                            alt.Tooltip("FIRST_NAME:N", title="Customer"),
                            alt.Tooltip("LIFETIME_VALUE:Q", title="LTV", format="$,.2f"),
                            alt.Tooltip("TOTAL_ORDERS:Q", title="Orders"),
                            alt.Tooltip("CUSTOMER_SEGMENT:N", title="Segment"),
                        ],
                    )
                    .properties(height=350)
                )
                st.altair_chart(top_chart, use_container_width=True)

        # Geographic distribution
        with st.container(border=True):
            st.markdown("**Revenue by Country**")
            geo_df = cust_df.groupby("COUNTRY", as_index=False).agg({
                "LIFETIME_VALUE": "sum",
                "CUSTOMER_ID": "count",
                "TOTAL_ORDERS": "sum",
            }).rename(columns={"CUSTOMER_ID": "NUM_CUSTOMERS"})
            geo_df = geo_df.sort_values("LIFETIME_VALUE", ascending=False)
            geo_chart = (
                alt.Chart(geo_df)
                .mark_bar(color="#29B5E8", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("LIFETIME_VALUE:Q", title="Total Lifetime Value ($)"),
                    y=alt.Y("COUNTRY:N", sort="-x", title=None),
                    tooltip=[
                        alt.Tooltip("COUNTRY:N"),
                        alt.Tooltip("LIFETIME_VALUE:Q", title="Revenue", format="$,.2f"),
                        alt.Tooltip("NUM_CUSTOMERS:Q", title="Customers"),
                        alt.Tooltip("TOTAL_ORDERS:Q", title="Orders"),
                    ],
                )
                .properties(height=300)
            )
            st.altair_chart(geo_chart, use_container_width=True)

        # Engagement metrics
        with st.container(border=True):
            st.markdown("**Customer Engagement & Orders**")
            st.dataframe(
                cust_df[["FIRST_NAME", "LAST_NAME", "CITY", "COUNTRY",
                         "TOTAL_ORDERS", "LIFETIME_VALUE", "AVG_ORDER_VALUE",
                         "TOTAL_EVENTS", "PAGE_VIEWS", "ADD_TO_CARTS",
                         "CHECKOUTS", "CUSTOMER_SEGMENT", "CART_CONVERSION_RATE"]],
                column_config={
                    "LIFETIME_VALUE": st.column_config.NumberColumn("LTV", format="$%.2f"),
                    "AVG_ORDER_VALUE": st.column_config.NumberColumn("Avg Order", format="$%.2f"),
                    "CART_CONVERSION_RATE": st.column_config.ProgressColumn(
                        "Cart Conv %", min_value=0, max_value=100
                    ),
                },
                hide_index=True,
                use_container_width=True,
            )


# =============================================================================
# Tab 4: Category Trends
# =============================================================================

with tab_trends:
    cat_df = load_category_trends()
    if not cat_df.empty:
        cat_df["MONTH"] = pd.to_datetime(cat_df["MONTH"])

        # Category KPIs
        with st.container(horizontal=True):
            st.metric("Categories", cat_df["CATEGORY"].nunique(), border=True)
            st.metric("Sub-Categories", cat_df["SUB_CATEGORY"].nunique(), border=True)
            st.metric("Total Category Revenue", f"${cat_df['REVENUE'].sum():,.2f}", border=True)
            st.metric("Months of Data", cat_df["MONTH"].nunique(), border=True)

        # Revenue by category over time
        cat_agg = cat_df.groupby(["MONTH", "CATEGORY"], as_index=False).agg({
            "REVENUE": "sum",
            "UNITS_SOLD": "sum",
            "NUM_ORDERS": "sum",
            "UNIQUE_CUSTOMERS": "sum",
        })

        col1, col2 = st.columns(2)

        with col1:
            with st.container(border=True):
                st.markdown("**Monthly Revenue by Category**")
                rev_trend = (
                    alt.Chart(cat_agg)
                    .mark_area(opacity=0.7, line=True)
                    .encode(
                        x=alt.X("MONTH:T", title="Month"),
                        y=alt.Y("REVENUE:Q", title="Revenue ($)", stack=True),
                        color=alt.Color("CATEGORY:N", legend=alt.Legend(orient="bottom")),
                        tooltip=[
                            alt.Tooltip("MONTH:T", title="Month", format="%b %Y"),
                            alt.Tooltip("CATEGORY:N"),
                            alt.Tooltip("REVENUE:Q", title="Revenue", format="$,.2f"),
                        ],
                    )
                    .properties(height=350)
                )
                st.altair_chart(rev_trend, use_container_width=True)

        with col2:
            with st.container(border=True):
                st.markdown("**Monthly Units Sold by Category**")
                units_trend = (
                    alt.Chart(cat_agg)
                    .mark_bar()
                    .encode(
                        x=alt.X("MONTH:T", title="Month"),
                        y=alt.Y("UNITS_SOLD:Q", title="Units Sold", stack=True),
                        color=alt.Color("CATEGORY:N", legend=alt.Legend(orient="bottom")),
                        tooltip=[
                            alt.Tooltip("MONTH:T", title="Month", format="%b %Y"),
                            alt.Tooltip("CATEGORY:N"),
                            alt.Tooltip("UNITS_SOLD:Q", title="Units"),
                        ],
                    )
                    .properties(height=350)
                )
                st.altair_chart(units_trend, use_container_width=True)

        # Category revenue share (total)
        with st.container(border=True):
            st.markdown("**Category Revenue Share (all time)**")
            cat_total = cat_df.groupby("CATEGORY", as_index=False)["REVENUE"].sum()
            cat_total["PCT"] = (cat_total["REVENUE"] / cat_total["REVENUE"].sum() * 100).round(1)
            share_chart = (
                alt.Chart(cat_total)
                .mark_arc(innerRadius=50)
                .encode(
                    theta=alt.Theta("REVENUE:Q"),
                    color=alt.Color("CATEGORY:N", legend=alt.Legend(orient="bottom")),
                    tooltip=[
                        alt.Tooltip("CATEGORY:N"),
                        alt.Tooltip("REVENUE:Q", title="Revenue", format="$,.2f"),
                        alt.Tooltip("PCT:Q", title="Share %", format=".1f"),
                    ],
                )
                .properties(height=300)
            )
            st.altair_chart(share_chart, use_container_width=True)

        # Sub-category detail table
        with st.container(border=True):
            st.markdown("**Sub-Category Breakdown**")
            sub_agg = cat_df.groupby(["CATEGORY", "SUB_CATEGORY"], as_index=False).agg({
                "REVENUE": "sum",
                "UNITS_SOLD": "sum",
                "NUM_ORDERS": "sum",
                "UNIQUE_CUSTOMERS": "sum",
            }).sort_values("REVENUE", ascending=False)
            st.dataframe(
                sub_agg,
                column_config={
                    "REVENUE": st.column_config.NumberColumn("Total Revenue", format="$%.2f"),
                },
                hide_index=True,
                use_container_width=True,
            )


# =============================================================================
# Tab 5: Pipeline Health
# =============================================================================

with tab_pipeline:
    # Row counts across pipeline
    counts_df = load_row_counts()
    if not counts_df.empty:
        # Layer summary KPIs
        raw_rows = counts_df[counts_df["TBL"].str.startswith("RAW.")]["CNT"].sum()
        curated_rows = counts_df[counts_df["TBL"].str.startswith("CURATED.")]["CNT"].sum()
        consumption_rows = counts_df[counts_df["TBL"].str.startswith("CONSUMPTION.")]["CNT"].sum()
        with st.container(horizontal=True):
            st.metric("RAW layer rows", f"{int(raw_rows):,}", border=True)
            st.metric("CURATED layer rows", f"{int(curated_rows):,}", border=True)
            st.metric("CONSUMPTION layer rows", f"{int(consumption_rows):,}", border=True)
            st.metric("Total pipeline objects", len(counts_df), border=True)

        with st.container(border=True):
            st.markdown("**Pipeline Row Counts (RAW -> CURATED -> CONSUMPTION)**")
            # Add layer column for color coding
            display_df = counts_df.copy()
            display_df["LAYER"] = display_df["TBL"].apply(
                lambda x: x.split(".")[0] if "." in str(x) else "OTHER"
            )
            layer_chart = (
                alt.Chart(display_df)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=alt.X("CNT:Q", title="Row Count"),
                    y=alt.Y("TBL:N", sort="-x", title=None),
                    color=alt.Color(
                        "LAYER:N",
                        scale=alt.Scale(
                            domain=["RAW", "CURATED", "CONSUMPTION"],
                            range=["#FFB347", "#71D4F5", "#29B5E8"],
                        ),
                        legend=alt.Legend(orient="bottom"),
                    ),
                    tooltip=[
                        alt.Tooltip("TBL:N", title="Table"),
                        alt.Tooltip("CNT:Q", title="Rows"),
                        alt.Tooltip("LAYER:N", title="Layer"),
                    ],
                )
                .properties(height=400)
            )
            st.altair_chart(layer_chart, use_container_width=True)

    # Stream status + Task DAG
    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.markdown("**Stream Status (CDC)**")
            stream_df = run_query(CONN, CONN_TYPE, """
                SELECT 'CUSTOMERS_STREAM' AS STREAM_NAME,
                       SYSTEM$STREAM_HAS_DATA('HOL_DB.RAW.CUSTOMERS_STREAM') AS HAS_DATA
                UNION ALL
                SELECT 'ORDERS_STREAM',
                       SYSTEM$STREAM_HAS_DATA('HOL_DB.RAW.ORDERS_STREAM')
                UNION ALL
                SELECT 'EVENTS_STREAM',
                       SYSTEM$STREAM_HAS_DATA('HOL_DB.RAW.EVENTS_STREAM')
            """)
            for _, row in stream_df.iterrows():
                status = "Has new data" if row["HAS_DATA"] == "True" else "Up to date"
                icon = ":material/fiber_new:" if row["HAS_DATA"] == "True" else ":material/check_circle:"
                st.markdown(f"{icon} **{row['STREAM_NAME']}**: {status}")

    with col2:
        with st.container(border=True):
            st.markdown("**Task DAG Status**")
            task_df = run_query(CONN, CONN_TYPE, """
                SELECT NAME, STATE, SCHEDULE, PREDECESSORS
                FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
                    TASK_NAME => 'HOL_DB.RAW.TASK_ROOT_SCHEDULER',
                    RECURSIVE => TRUE
                ))
                ORDER BY NAME
            """)
            if not task_df.empty:
                for _, row in task_df.iterrows():
                    state_icon = ":material/play_circle:" if row["STATE"] == "started" else ":material/pause_circle:"
                    st.markdown(f"{state_icon} **{row['NAME']}** — {row['STATE']}")
            else:
                st.info("No task information available. Tasks may not be started.")

    # Dynamic Table Refresh History
    with st.container(border=True):
        st.markdown("**Recent Dynamic Table Refreshes**")
        dt_hist = load_dynamic_table_refresh_history()
        if not dt_hist.empty:
            st.dataframe(
                dt_hist,
                column_config={
                    "REFRESH_START_TIME": st.column_config.DatetimeColumn("Start", format="MMM DD HH:mm:ss"),
                    "REFRESH_END_TIME": st.column_config.DatetimeColumn("End", format="MMM DD HH:mm:ss"),
                    "DURATION_SEC": st.column_config.NumberColumn("Duration (s)", format="%d"),
                },
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.caption("No refresh history available yet.")

    # Pipeline metrics history
    with st.container(border=True):
        st.markdown("**Latest Pipeline Metrics Snapshot**")
        pm_df = load_pipeline_metrics()
        if not pm_df.empty:
            st.dataframe(pm_df, hide_index=True, use_container_width=True)
