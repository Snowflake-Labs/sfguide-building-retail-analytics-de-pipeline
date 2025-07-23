"""
Customer Analytics Pipeline using Snowpark Pandas API

This script implements a comprehensive customer analytics pipeline that:
1. Loads data from multiple sources (Oracle and SQL Server) in parallel
2. Performs various analytics on customer, order, inventory, and product data
3. Writes results back to Snowflake tables for further analysis and reporting

Key Features:
- Parallel data loading using ThreadPoolExecutor
- Data transformation using Snowpark Pandas
- Efficient analytics operations
- Parallel table loading to Snowflake
"""

from operator import inv
import modin.pandas as pd
import snowflake.snowpark.modin.plugin
import os
import logging

from snowflake.snowpark import Session
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from snowflake import telemetry
# from data_integration_for_analytics_copy import get_oracle_customer_data,get_oracle_product_data,get_sql_server_inventory_data,get_sql_server_order_data
from data_integration_for_analytics import main

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("customer_analytics")

def load_data(session:Session):
    """
    Loads data from multiple sources concurrently using ThreadPoolExecutor.
    This approach significantly improves performance by parallelizing data loading operations.
    """
    print("Loading data in parallel...")
    
    # Define the list of tables to load with their parameters
    # table_params = [
    #     {'source': 'oracle', 'source_table': 'customers'},
    #     {'source': 'oracle', 'source_table': 'products'},
    #     {'source': 'azuresql', 'source_table': 'orders'},
    #     {'source': 'azuresql', 'source_table': 'inventory'},
    #     {'source': 'mysql', 'source_table': 'order_items'}
    # ]

    table_params = [
        {'source': 'mysql', 'source_table': 'customers'},
        {'source': 'mysql', 'source_table': 'products'},
        {'source': 'mysql', 'source_table': 'orders'},
        {'source': 'mysql', 'source_table': 'inventory'},
        {'source': 'mysql', 'source_table': 'order_items'}
    ]
    
    # Create a function to load a single table
    def load_single_table(params):
        logger.info(f"Loading data from {params['source']} table {params['source_table']}")
        return main(session, params['source'], params['source_table'])
    
    
    # Load all tables in parallel
    with ThreadPoolExecutor(max_workers=len(table_params)) as executor:
        futures = [executor.submit(load_single_table, params) for params in table_params]
        results = [future.result() for future in futures]
    # telemetry.add_event("num_tables", len(table_params))
    logger.info('Loaded data successfully')
    return results

# We are using Snowpark Pandas to perform the analytics.

def customer_analytics(customers, orders):
    """
    Performs customer analytics by calculating:
    - Total spend per customer
    - Number of orders per customer
    - Average order value per customer
    """
    df = orders.merge(customers, on='customer_id', how='left',suffixes=(None, '_customers'))
    total_spend = df.groupby(['customer_id','first_name','last_name'])['total_amount'] \
                    .sum().reset_index(name='total_spend')
    order_count = df.groupby('customer_id')['order_id'] \
                    .count().reset_index(name='order_count')
    avg_order = df.groupby('customer_id')['total_amount'] \
                    .mean().reset_index(name='avg_order_value')
    cust_stats = total_spend.merge(order_count, on='customer_id') \
                    .merge(avg_order, on='customer_id')
    return cust_stats

def order_analytics(orders):
    """
    Analyzes order data to generate:
    - Monthly revenue trends
    - Order status distribution by month
    - Payment method distribution by month
    """
    orders['order_month'] = orders['order_date'].dt.strftime('%Y-%m')
    revenue_month = orders.groupby('order_month')['total_amount'] \
                        .sum().reset_index(name='monthly_revenue')
    # Add a formatted order_month column as YYYY-MM
    status_counts = orders.groupby(['order_month', 'status']).size().reset_index(name='count')
    payment_counts = orders.groupby(['order_month', 'payment_method']).size().reset_index(name='count')
    return revenue_month, status_counts, payment_counts

def inventory_analytics(products, inventory):
    """
    Performs inventory analysis across multiple dimensions:
    - Stock levels per product
    - Stock levels per category
    - Stock distribution across warehouses
    """
    inv_prod = inventory.groupby('product_id')['quantity'] \
                    .sum().reset_index(name='total_stock')
    inv_prod = products[['product_id','product_name','category']].merge(inv_prod, on='product_id', how='left')
    inv_cat = inv_prod.groupby('category')['total_stock'] \
                    .sum().reset_index(name='category_stock')
    inv_wh = inventory.groupby('warehouse_id')['quantity'] \
                    .sum().reset_index(name='warehouse_stock')
    return inv_prod, inv_cat, inv_wh

def product_analytics(products):
    """
    Analyzes product pricing across categories:
    - Average price per category
    - Minimum price per category
    - Maximum price per category
    """
    price_stats = products.groupby('category')['price'] \
                    .agg(['mean','min','max']).reset_index()
    price_stats = price_stats.rename(columns={'mean':'avg_price','min':'min_price','max':'max_price'})
    return price_stats

# submit each DataFrame and its target table name in parallel
def load_into_snowflake_in_parallel(df_table_map: dict, parallell_degree: int):
    """
    Executes parallel loading of DataFrames to Snowflake tables.
    Uses ThreadPoolExecutor for concurrent table loading operations.
    
    Args:
        df_table_map: Dictionary mapping table names to DataFrames
        parallell_degree: Number of parallel threads to use
    """
    def run_query( df: pd.DataFrame, table_name: str):
        print(f"Loading {table_name} to Snowflake")
        df=clean_column_names(df)
        logger.info(f"Loading {df.shape[0]} rows of {table_name} to Snowflake")
        res = df.to_snowflake(name=table_name, if_exists="replace", index=False)
        return res
    results = []
    with ThreadPoolExecutor(parallell_degree) as executor:
        # submit each DataFrame and its target table name in parallel
        futures = [executor.submit(run_query, df, table_name)
                   for table_name, df in df_table_map.items()]
        for future in futures:
            results.append(future.result())
    return results

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures column names are valid Snowflake identifiers by:
    - Removing quotes
    - Stripping whitespace
    """
    df.columns = [str(col).lower().strip("'\"") for col in df.columns]
    # df.columns = df.columns.str.lower()
    return df

def run(session:Session):
    """
    Main execution function that:
    1. Loads data from all sources
    2. Performs all analytics operations
    3. Cleans and prepares data for Snowflake
    4. Loads results to Snowflake tables in parallel
    5. Tracks and reports execution time
    """
    st_time=datetime.now()
    freeze_support()
    # telemetry.set_span_attribute("usp_load_customer_analytics", "begin")
    telemetry.add_event("customer_analytics_loaded_data_begin", {"source_data": "MySQL Tales", "transformation": "customer analytics using Snowpark Pandas APIs"})

    # telemetry.add_event("customer_analytics_loaded_data", {"source_data": "MySQL Tales", "transformation": "customer analytics using Snowpark Pandas APIs"})
    customers, products, orders, inventory,order_items = load_data(session)

    customers=clean_column_names(customers)
    products=clean_column_names(products)
    orders=clean_column_names(orders)
    inventory=clean_column_names(inventory)
    order_items=clean_column_names(order_items)

    cust_stats = customer_analytics(customers, orders)
    # print(cust_stats.head(5))
    
    rev_month, status_counts, payment_counts = order_analytics(orders)
    # print(rev_month.head(5))

    inv_prod, inv_cat, inv_wh = inventory_analytics(products, inventory)
    # print(inv_prod.head(5)  )
  

    price_stats = product_analytics(products)
    # print(price_stats.head(5))

    # map Snowflake table names to their corresponding DataFrames
    df_table_dict = {
        'analytics_customer_stats': cust_stats,
        'analytics_monthly_revenue': rev_month,
        'analytics_order_status_counts': status_counts,
        'analytics_payment_method_counts': payment_counts,
        'analytics_inventory_per_product': inv_prod,
        'analytics_inventory_per_category': inv_cat,
        'analytics_inventory_per_warehouse': inv_wh,
        'analytics_product_price_stats': price_stats
    }
    # load all DataFrames to Snowflake in parallel and verify
    results = load_into_snowflake_in_parallel( df_table_dict, 10)
    logger.info(f"Loaded {len(results)} tables to Snowflake in parallel")

    ed_time=datetime.now()
    logger.info(f"Time taken: {ed_time - st_time}")
    telemetry.add_event("customer_analytics_loaded_data_end", {"source_data": "MySQL Tales", "transformation": "customer analytics using Snowpark Pandas APIs"})

    # print('Analytics completed. CSVs saved in', OUTPUT_DIR)
from multiprocessing import freeze_support
if __name__ == '__main__':
    # freeze_support()
    from snowpark_session import sp_session as session
    run(session) 