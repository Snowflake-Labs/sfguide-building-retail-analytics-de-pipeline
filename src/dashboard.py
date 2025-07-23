import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

import json
import base64

# Configure the Streamlit page with a wide layout and custom title/icon
st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# Initialize Snowpark connection with caching for better performance
@st.cache_resource
def init_connection():
    try:
        # Try to get active session from Streamlit in Snowflake
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except ImportError:
        from snowpark_session import sp_session
        # Fallback to local session if not running in Streamlit in Snowflake
        return sp_session
try:
    session = init_connection()
except Exception as e:
    print(e)
    from snowpark_session import sp_session
    session = sp_session

session.add_packages("plotly")

# Helper function to execute SQL queries and return cleaned pandas DataFrame
# Uses caching with 1-hour TTL to improve performance
@st.cache_data(ttl=3600)
def run_query(query):
    result = session.sql(query).collect()
    # Convert to pandas and clean column names
    df = pd.DataFrame(result)
    df.columns = [col.strip('"') for col in df.columns]
    return df

# Main dashboard title
st.title("📊 Retail Analytics Dashboard")

# SECTION 1: Key Business Metrics
# Display four key performance indicators (KPIs) in a row
st.header("Key Business Metrics")
col1, col2, col3, col4 = st.columns(4)

# KPI 1: Total Revenue across all categories
total_revenue = run_query("""
    SELECT SUM(total_revenue) as total
    FROM ANALYTICS_TOP_SELLING_CATEGORIES_BY_REVENUE
""")
col1.metric("Total Revenue", f"${total_revenue['TOTAL'][0]:,.2f}")

# KPI 2: Total number of orders processed
total_orders = run_query("""
    SELECT SUM(total_orders) as total
    FROM ANALYTICS_TOP_SELLING_CATEGORIES_BY_REVENUE
""")
col2.metric("Total Orders", f"{total_orders['TOTAL'][0]:,}")

# KPI 3: Average value per order
avg_order = run_query("""
    SELECT AVG(avg_order_value) as avg
    FROM ANALYTICS_TOP_SELLING_CATEGORIES_BY_REVENUE
""")
col3.metric("Average Order Value", f"${avg_order['AVG'][0]:,.2f}")

# KPI 4: Total unique products in inventory
total_products = run_query("""
    SELECT COUNT(DISTINCT product_id) as total
    FROM GOLD_PRODUCT_PERFORMANCE
""")
col4.metric("Total Products", f"{total_products['TOTAL'][0]:,}")

# SECTION 2: Sales Analysis
# Two visualizations showing revenue by category and order status distribution
st.header("Sales Analysis")

col1, col2 = st.columns(2)

# Visualization 1: Bar chart of top 10 categories by revenue
with col1:
    st.subheader("💰 Top Categories by Revenue")
    category_df = run_query("""
        SELECT category, total_revenue
        FROM ANALYTICS_TOP_SELLING_CATEGORIES_BY_REVENUE
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    fig = px.bar(category_df, 
                 x='CATEGORY', 
                 y='TOTAL_REVENUE',
                 title="Top 10 Categories by Revenue")
    st.plotly_chart(fig, use_container_width=True)

# Visualization 2: Pie chart showing distribution of order statuses
with col2:
    st.subheader("🛒 Order Status Distribution")
    status_df = run_query("""
        SELECT "status" as status, SUM("count") as total_count
        FROM ANALYTICS_ORDER_STATUS_COUNTS
        GROUP BY status
    """)
    fig = px.pie(status_df, 
                 values='TOTAL_COUNT', 
                 names='STATUS',
                 title="Order Status Distribution")
    st.plotly_chart(fig, use_container_width=True)

# SECTION 3: Customer Insights
# Analysis of customer spending patterns and geographical distribution
st.header("👥 Customer Insights")

col1, col2 = st.columns(2)

# Visualization 1: Scatter plot of customer spending patterns
with col1:
    st.subheader("💰 Customer Spending Analysis")
    customer_df = run_query("""
        SELECT 
            "total_spend" as total_spend,
            "order_count" as order_count,
            "avg_order_value" as avg_order_value
        FROM ANALYTICS_CUSTOMER_STATS
        ORDER BY total_spend DESC
        LIMIT 100
    """)
    # Convert column names to lowercase for consistency
    customer_df.columns = customer_df.columns.str.lower()
    # Convert columns to numeric types
    customer_df['total_spend'] = pd.to_numeric(customer_df['total_spend'])
    customer_df['order_count'] = pd.to_numeric(customer_df['order_count'])
    customer_df['avg_order_value'] = pd.to_numeric(customer_df['avg_order_value'])
    
    fig = px.scatter(customer_df,
                     x='order_count',
                     y='total_spend',
                     size='avg_order_value',
                     title="Customer Spending Patterns")
    st.plotly_chart(fig, use_container_width=True)

# Visualization 2: Treemap showing revenue distribution by state and category
with col2:
    st.subheader("📍 State-wise Category Analysis")
    state_df = run_query("""
        SELECT 
            state,
            category,
            total_revenue
        FROM ANALYTICS_STATE_CATEGORY_ANALYSIS
        WHERE state IS NOT NULL 
        AND category IS NOT NULL
        AND total_revenue > 0
        ORDER BY total_revenue DESC
        LIMIT 20
    """)
    # Convert column names to lowercase for consistency
    state_df.columns = state_df.columns.str.lower()
    # Convert total_revenue to numeric
    state_df['total_revenue'] = pd.to_numeric(state_df['total_revenue'])
    # Ensure state and category are strings
    state_df['state'] = state_df['state'].astype(str)
    state_df['category'] = state_df['category'].astype(str)
    
    fig = px.treemap(state_df,
                     path=['state', 'category'],
                     values='total_revenue',
                     title="Revenue by State and Category")
    st.plotly_chart(fig, use_container_width=True)

# SECTION 4: Product Performance
# Detailed analysis of product performance and reviews
st.header("Product Performance")

# Table showing top selling products with key metrics
st.subheader("🛍️ Top Selling Products")
product_df = run_query("""
    SELECT 
        product_name,
        category,
        total_orders,
        total_quantity_sold,
        total_revenue
    FROM ANALYTICS_TOP_SELLING_PRODUCTS_BY_REVENUE
    ORDER BY total_revenue DESC
    LIMIT 10
""")
st.dataframe(product_df, use_container_width=True)

# Product reviews analysis with interactive tabs
st.subheader("📝 Product Reviews Analysis")
review_df = run_query("""
    SELECT 
        pa.product_id,
        p.product_name,
        p.specifications,
        pa.avg_rating,
        pa.total_reviews,
        pa.positive_reviews,
        pa.negative_reviews
    FROM GOLD_PRODUCT_REVIEWS_ANALYTICS pa
    LEFT JOIN SILVER_PRODUCTS p ON pa.product_id = p.product_id
    ORDER BY total_reviews DESC
    LIMIT 10
""")
# Convert column names to lowercase for consistency
review_df.columns = review_df.columns.str.lower()

# Create tabs for different views of product data
tab1, tab2 = st.tabs(["Reviews Distribution", "Product Specifications"])

# Tab 1: Bar chart showing positive vs negative reviews
with tab1:
    fig = px.bar(review_df,
                 x='product_name',
                 y=['positive_reviews', 'negative_reviews'],
                 title="Product Reviews Distribution",
                 barmode='group')
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    for _, row in review_df.iterrows():
        with st.expander(f"Specifications for {row['product_name']}"):
            # Convert specifications string to a more readable format
            if pd.notna(row['specifications']):
                specs = row['specifications']
                # If specifications is a JSON string, try to parse it
                try:
                    import json
                    specs_dict = json.loads(specs)
                    for key, value in specs_dict.items():
                        st.write(f"**{key}**: {value}")
                except:
                    # If not JSON, display as is
                    st.json(specs)
            else:
                st.write("No specifications available")

# SECTION 5: Technical Specifications
# Analysis of product technical specifications
st.header("🔋 Battery and 💾 Storage Specifications")

# Query to get battery and storage specifications
specs_df = run_query("""
    SELECT 
        p.product_id,
        p.product_name,
        MAX(CASE WHEN spec_name ILIKE 'battery life' THEN spec_value END) AS battery_life,
        MAX(CASE WHEN spec_name ILIKE 'storage' THEN spec_value END) AS storage
    FROM PUBLIC.SILVER_PRODUCTS p
    LEFT JOIN PUBLIC.BRONZE_PRODUCT_SPECIFICATIONS s
      ON p.product_id = s.product_id
    GROUP BY p.product_id, p.product_name
""")

# Normalize column names
specs_df.columns = specs_df.columns.str.lower()

# Extract numeric values
specs_df['battery_life_hours'] = specs_df['battery_life'] \
    .str.extract(r'(\d+)').astype(float)
specs_df['storage_gb'] = specs_df['storage'] \
    .str.extract(r'(\d+)').astype(float)

# Visualization 1: Battery life comparison across products
batt_df = specs_df.dropna(subset=['battery_life_hours']) \
                  .sort_values('battery_life_hours', ascending=False)
fig_batt = px.bar(
    batt_df,
    x='product_name',
    y='battery_life_hours',
    title="Battery Life by Product",
    labels={'battery_life_hours': 'Battery Life (hours)', 'product_name': 'Product'}
)
st.plotly_chart(fig_batt, use_container_width=True)

# Visualization 2: Storage capacity comparison across products
storage_df = specs_df.dropna(subset=['storage_gb']) \
                     .sort_values('storage_gb', ascending=False)
fig_storage = px.bar(
    storage_df,
    x='product_name',
    y='storage_gb',
    title="Storage Capacity by Product",
    labels={'storage_gb': 'Storage (GB)', 'product_name': 'Product'}
)
st.plotly_chart(fig_storage, use_container_width=True)

# SECTION 6: Customer Preferences
# Analysis of customer preferences and trends
st.header("👥 Top Customer Preferences")

pref_df = run_query("""
  SELECT 
        cast(preference_type as varchar) || ' - ' || cast(preference_value as varchar)  as preference,
        COUNT(*) AS count
    FROM PUBLIC.BRONZE_CUSTOMER_PREFERENCES
    GROUP BY preference
    ORDER BY count DESC
    LIMIT 10;
""")

# Clean column names
pref_df.columns = pref_df.columns.str.lower()

# Visualization of top customer preferences
fig_pref = px.bar(
    pref_df,
    x='preference',
    y='count',
    title="Top 10 Customer Preferences",
    labels={'preference': 'Preference', 'count': 'Count'}
)
st.plotly_chart(fig_pref, use_container_width=True)


# Add HTML Export Section
st.header("📄 Export Top Customer Preferences to HTML")
filename = st.text_input("Output File name", "All_Customer_Pref_Counts.html")
# Button for the user to trigger an action
if st.button("Export"):
    with st.spinner('Exporting charts to stage...'):
        session.call("usp_saveChartsToStage",filename)
    st.text(f'Exported file to @SOURCE_FILES with file name {filename}. Clik on the below hyper link to download the html file')
    pre_signed_file=session.sql(f"""
                                    select get_presigned_url('@SOURCE_FILES', '{filename}')
                                """).collect()[0][0]
    st.markdown(f'<a href="{pre_signed_file}" target="_blank">{filename}</a>', unsafe_allow_html=True)

# Footer with last update timestamp
st.markdown("---")
st.markdown("Dashboard last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
