import logging
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, lit, sum as sf_sum, count_distinct, avg, rank,when,trim,split,sproc
from snowflake.snowpark.window import Window

from snowflake.cortex import sentiment,complete,extract_answer,summarize,translate,classify_text
from snowflake.snowpark.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderAnalytics:
    def __init__(self, orders_df: DataFrame, order_items_df: DataFrame, products_df: DataFrame, customers_df: DataFrame):
        """
        Initialize the OrderAnalytics class with the required dataframes.
        
        Args:
            orders_df: DataFrame containing order information
            order_items_df: DataFrame containing order items information
            products_df: DataFrame containing product information
            customers_df: DataFrame containing customer information
        """
        self.orders_df = orders_df
        self.order_items_df = order_items_df
        self.products_df = products_df
        self.customers_df = customers_df

    def get_order_details(self) -> DataFrame:
        """
        Get detailed order information including product details and customer state.
        
        Returns:
            DataFrame with order details including product information and customer state
        """
        try:
            # Merge orders with order items
            order_details = self.orders_df.join(
                self.order_items_df,
                self.orders_df.order_id == self.order_items_df.order_id,
                'left',
                rsuffix='_order_items'
            )
            
            # Merge with products
            order_details = order_details.join(
                self.products_df,
                order_details.product_id == self.products_df.product_id,
                'left',
                rsuffix='_products'
            )
            
            # Merge with customers to get state information
            order_details = order_details.join(
                self.customers_df,
                order_details.customer_id == self.customers_df.customer_id,
                'left',
                rsuffix='_customer'
            )

            order_details=order_details.withColumn('street_temp', 
                          extract_answer(col('address'),'what is name of the state of the customer')[0]['answer'] \
                          ).withColumn('state', when(col('street_temp').contains(lit(',')),trim(split(col('street_temp'),lit(','))[1])               
                ).otherwise(col('street_temp')).cast(StringType()))
        
            
            # Calculate line total
            order_details = order_details.with_column(
                'line_total',
                col('quantity') * col('unit_price')
            )
            
            return order_details
            
        except Exception as e:
            logger.error(f"Error in get_order_details: {str(e)}")
            raise
            
    def get_product_performance(self) -> DataFrame:
        """
        Calculate product performance metrics.
        
        Returns:
            DataFrame with product performance metrics
        """
        try:
            # Merge order items with products
            product_performance = self.order_items_df.join(
                self.products_df,
                self.order_items_df.product_id == self.products_df.product_id,
                'left',
                rsuffix='_products'
            )
            
            # Calculate metrics
            performance_metrics = product_performance.group_by(
                'product_id', 'product_name', 'category'
            ).agg(
                count_distinct('order_id').alias('total_orders'),
                sf_sum('quantity').alias('total_quantity_sold'),
                sf_sum(col('unit_price') * col('quantity')).alias('total_revenue')
            )
            
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Error in get_product_performance: {str(e)}")
            raise
            
    def get_category_performance(self) -> DataFrame:
        """
        Calculate category performance metrics.
        
        Returns:
            DataFrame with category performance metrics
        """
        try:
            # Get product performance first
            product_performance = self.get_product_performance()
            
            # Aggregate by category
            category_performance = product_performance.group_by('category').agg(
                sf_sum('total_orders').alias('total_orders'),
                sf_sum('total_quantity_sold').alias('total_quantity_sold'),
                sf_sum('total_revenue').alias('total_revenue')
            )
            
            # Calculate average order value
            category_performance = category_performance.with_column(
                'avg_order_value',
                col('total_revenue') / col('total_orders')
            )
            
            return category_performance
            
        except Exception as e:
            logger.error(f"Error in get_category_performance: {str(e)}")
            raise
            
    def get_customer_order_history(self, customer_id: str) -> DataFrame:
        """
        Get order history for a specific customer.
        
        Args:
            customer_id: The ID of the customer
            
        Returns:
            DataFrame with customer's order history
        """
        try:
            # Get order details
            order_details = self.get_order_details()
            
            # Filter for customer's orders and sort
            customer_history = order_details.filter(
                col('customer_id') == lit(customer_id)
            ).sort('order_date', ascending=False)
            
            return customer_history
            
        except Exception as e:
            logger.error(f"Error in get_customer_order_history: {str(e)}")
            raise

    def get_top_selling_products(self, top_n: int = 10) -> DataFrame:
        """
        Calculate top selling products based on quantity and revenue.
        
        Args:
            top_n: Number of top products to return
            
        Returns:
            DataFrame with top selling products
        """
        try:
            # Get product performance
            product_performance = self.get_product_performance()
            
            # Get top by quantity
            top_by_quantity = product_performance.sort(
                'total_quantity_sold', ascending=False
            ).limit(top_n).with_column('metric', lit('quantity_sold'))
            
            # Get top by revenue
            top_by_revenue = product_performance.sort(
                'total_revenue', ascending=False
            ).limit(top_n).with_column('metric', lit('revenue'))
            
            # Union the results
            top_products = top_by_quantity.union(top_by_revenue)
            
            return top_products
            
        except Exception as e:
            logger.error(f"Error in get_top_selling_products: {str(e)}")
            raise
            
    def get_top_selling_categories(self, top_n: int = 5) -> DataFrame:
        """
        Calculate top selling categories based on quantity and revenue.
        
        Args:
            top_n: Number of top categories to return
            
        Returns:
            DataFrame with top selling categories
        """
        try:
            # Get category performance
            category_performance = self.get_category_performance()
            
            # Get top by quantity
            top_by_quantity = category_performance.sort(
                'total_quantity_sold', ascending=False
            ).limit(top_n).with_column('metric', lit('quantity_sold'))
            
            # Get top by revenue
            top_by_revenue = category_performance.sort(
                'total_revenue', ascending=False
            ).limit(top_n).with_column('metric', lit('revenue'))
            
            # Union the results
            top_categories = top_by_quantity.union(top_by_revenue)
            
            return top_categories
            
        except Exception as e:
            logger.error(f"Error in get_top_selling_categories: {str(e)}")
            raise
            
    def get_state_category_analysis(self) -> DataFrame:
        """
        Calculate state-wise category analysis showing highest purchasing states per category.
        
        Returns:
            DataFrame with state-wise category analysis
        """
        try:
            # Get order details
            order_details = self.get_order_details()
            
            # Group by state and category
            state_category_analysis = order_details.group_by(['state', 'category']).agg(
                count_distinct('order_id').alias('order_count'),
                sf_sum('quantity').alias('total_quantity'),
                sf_sum('line_total').alias('total_revenue')
            )
            
            # Calculate average order value
            state_category_analysis = state_category_analysis.with_column(
                'avg_order_value',
                col('total_revenue') / col('order_count')
            )
            
            # Get top states for each category using window functions
            window_spec = Window.partition_by('category').order_by(col('total_revenue').desc())
            top_states_by_category = state_category_analysis.with_column(
                'rank', rank().over(window_spec)
            ).filter(col('rank') <= 3).drop('rank')
            
            return top_states_by_category
            
        except Exception as e:
            logger.error(f"Error in get_state_category_analysis: {str(e)}")
            raise

def clean_col_names(df: DataFrame) -> DataFrame:
    '''
        removes annoying single & double quotes in the column names
        also makes all characters in the column names lowercase
    '''
    for col_name in df.columns:
        new_name = col_name.lower().replace('\'', '').replace('"', '')
        df = df.rename(col_name, new_name)
    return df
    
def main(session:Session):
    # Example usage
    try:
        # Read the data
        orders_df = session.table("BRONZE_ORDERS")
        order_items_df = session.table("BRONZE_ORDER_ITEMS")
        products_df = session.table("BRONZE_PRODUCTS")
        customers_df = session.table("BRONZE_CUSTOMERS")  # Assuming customer data is in a table
        
        orders_df = clean_col_names(orders_df)
        order_items_df = clean_col_names(order_items_df)
        products_df = clean_col_names(products_df)
        customers_df = clean_col_names(customers_df)

        # Initialize analytics
        analytics = OrderAnalytics(orders_df, order_items_df, products_df, customers_df)
        
        # Get various analytics
        order_details = analytics.get_order_details()
        # print(order_details.show(2))
        
        # Get product performance
        product_performance = analytics.get_product_performance()
        category_performance = analytics.get_category_performance()
        # print(category_performance.show(3))
        # print(product_performance.show(3))
        
        # Get top selling products
        top_products = analytics.get_top_selling_products(top_n=10)
        logger.info("Top selling products by quantity:")
        logger.info(f"Top selling products by quantity: {top_products.filter(col('metric') == 'quantity_sold').count()}")
        top_products.filter(col('metric') == 'quantity_sold').write.mode('overwrite').save_as_table('analytics_top_selling_products_by_quantity')
        logger.info("Top selling products by revenue:")
        logger.info(f"Top selling products by revenue: {top_products.filter(col('metric') == 'revenue').count()}")
        top_products.filter(col('metric') == 'revenue').write.mode('overwrite').save_as_table('analytics_top_selling_products_by_revenue')
        
        # Get top selling categories
        top_categories = analytics.get_top_selling_categories(top_n=5)
        logger.info("Top selling categories by quantity:")
        logger.info(f"Top selling categories by quantity: {top_categories.filter(col('metric') == 'quantity_sold').count()}")
        top_categories.filter(col('metric') == 'quantity_sold').write.mode('overwrite').save_as_table('analytics_top_selling_categories_by_quantity')
        logger.info("Top selling categories by revenue:")
        logger.info(f"Top selling categories by revenue: {top_categories.filter(col('metric') == 'revenue').count()}")
        top_categories.filter(col('metric') == 'revenue').write.mode('overwrite').save_as_table('analytics_top_selling_categories_by_revenue')
        
        # Get state category analysis
        state_category_analysis = analytics.get_state_category_analysis()
        logger.info("Top states by category:")
        logger.info(f"Top states by category: {state_category_analysis.count()}")
        state_category_analysis.write.mode('overwrite').save_as_table('analytics_state_category_analysis')
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    from snowpark_session import sp_session as session
    main(session)

 