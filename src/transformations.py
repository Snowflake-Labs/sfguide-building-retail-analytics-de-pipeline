"""
This module implements a data transformation pipeline for a retail analytics system using Snowflake Snowpark.
The pipeline follows a medallion architecture pattern with three layers:
- Bronze (raw data)
- Silver (cleaned and enriched data)
- Gold (analytics-ready data)

Key Features:
1. Data Cleaning and Standardization
2. Customer Data Enrichment
3. Product Analytics
4. Sentiment Analysis on Reviews
5. Performance Metrics Calculation
"""

# from concurrent.futures import ThreadPoolExecutor
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from snowflake.snowpark.functions import col, count, sum, avg, max, mode, current_timestamp, array_agg, object_construct,when,lit,split,trim,exp,explode,date_trunc
from snowflake.snowpark import Session,DataFrame
from snowflake.snowpark.functions import sproc
from snowflake.snowpark import DataFrame,Session
from snowflake.snowpark.functions import when
from snowflake.cortex import sentiment,complete,extract_answer,summarize,translate,classify_text
from snowflake.snowpark.types import StringType,FloatType,ArrayType,TimestampType

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataTransformations:
    """
    Main class for handling data transformations in the retail pipeline.
    Implements methods for transforming data between bronze, silver, and gold layers.
    Uses Snowpark for distributed data processing and Snowflake Cortex for AI/ML capabilities.
    """
    def __init__(self, session: snowpark.Session,dataframe_type:str):
        self.session = session
        self.dataframe_type = dataframe_type
        # self.iceberg_config = {
        #     "external_volume": "iceberg_external_volume_summit_demo",
        #     "catalog": "SNOWFLAKE",
        #     "base_location": "retail_gold_layer/",
        #     "storage_serialization_policy": "OPTIMIZED",
        # }

    def clean_col_names(self,df: DataFrame) -> DataFrame:
        '''
        Standardizes column names by:
        - Converting to lowercase
        - Removing single and double quotes
        - Ensuring consistent naming conventions
        '''
        df_res = df
        for col in df.columns:
            df_res = df_res.rename(col, col.lower().replace('\'', '').replace('"', ''))
        return df_res

    def bronze_to_silver_transformation(self):
        """
        Transforms raw bronze data into cleaned and enriched silver layer data.
        
        Key transformations:
        1. Customer data enrichment with preferences
        2. Product data enrichment with specifications
        3. Review sentiment analysis using Snowflake Cortex
        4. Address standardization and state extraction
        5. Data type standardization and cleaning
        """
        # Get bronze tables as Snowpark DataFrames
        bronze_customer_df = self.session.table("BRONZE_CUSTOMERS")
        bronze_product_df = self.session.table("BRONZE_PRODUCTS")
        bronze_inventory_df = self.session.table("BRONZE_INVENTORY")
        bronze_order_df = self.session.table("BRONZE_ORDERS")
        bronze_order_items_df = self.session.table("BRONZE_ORDER_ITEMS")

        bronze_customer_df=self.clean_col_names(bronze_customer_df)
        bronze_product_df=self.clean_col_names(bronze_product_df)
        bronze_inventory_df=self.clean_col_names(bronze_inventory_df)
        bronze_order_df=self.clean_col_names(bronze_order_df)
        bronze_order_items_df=self.clean_col_names(bronze_order_items_df)



        bronze_customer_prefs_df = self.session.sql(''' SELECT CUSTOMER_ID,ARRAY_AGG(OBJECT_CONSTRUCT('TYPE',PREFERENCE_TYPE,'VALUE',PREFERENCE_VALUE)) AS PREFERENCES FROM BRONZE_CUSTOMER_PREFERENCES GROUP BY CUSTOMER_ID''').select("customer_id","preferences")

        bronze_product_specs_df = self.session.sql('''select PRODUCT_ID,ARRAY_AGG(OBJECT_CONSTRUCT('NAME',SPEC_NAME,'VALUE',SPEC_VALUE)) AS specifications from BRONZE_PRODUCT_SPECIFICATIONS GROUP BY PRODUCT_ID''').select("PRODUCT_ID","specifications")
        bronze_reviews_df = self.session.table("BRONZE_PRODUCT_REVIEWS")
        
        # Transform customer data using Snowpark APIs
        silver_customer_df = bronze_customer_df.join(
            bronze_customer_prefs_df,
            bronze_customer_df["customer_id"] == bronze_customer_prefs_df["customer_id"],
            "left",
            rsuffix='_right'
        )
        
        # Transform product data using Snowpark APIs
        silver_product_df = bronze_product_df.join(
            bronze_product_specs_df,
            bronze_product_df.col("product_id") == bronze_product_specs_df.col("product_id"),
            "left",
            rsuffix='_right'
        )

        # Transform reviews data
        silver_reviews_df = bronze_reviews_df.withColumn(
            "sentiment_score",
            when(sentiment(col("content")) > 0.6, 'Positive').otherwise(
                when(sentiment(col("content")) < 0.4, 'Negative').otherwise('Neutral')
            )
        )

        # Write to silver tables
        logger.info("Writing to silver table- SILVER_CUSTOMERS")    
        silver_customer_df.write.mode("overwrite").saveAsTable("SILVER_CUSTOMERS")
        logger.info("Writing to silver table- SILVER_PRODUCTS")
        silver_product_df.write.mode("overwrite").saveAsTable("SILVER_PRODUCTS")
        logger.info("Writing to silver table- SILVER_PRODUCT_REVIEWS")
        silver_reviews_df.write.mode("overwrite").saveAsTable("SILVER_PRODUCT_REVIEWS")
        # Write orders to silver order table
        
        silver_customer_df=self.session.table("SILVER_CUSTOMERS")
        silver_order_df = bronze_order_df
        silver_customer_df=silver_customer_df.withColumn('street_temp', 
                          extract_answer(col('address'),'what is name of the state of the customer')[0]['answer'] \
                          ).withColumn('state', when(col('street_temp').contains(lit(',')),trim(split(col('street_temp'),lit(','))[1])               
                ).otherwise(col('street_temp')).cast(StringType()))
        
        silver_order_df = silver_order_df.join(
            silver_customer_df.select(col("customer_id").alias("customer_id"), "state"),
            "customer_id",
            "left",
            rsuffix='_right'
        )
        logger.info("Writing to silver table- SILVER_ORDERS")
        silver_order_df.write.mode("overwrite").saveAsTable("SILVER_ORDERS")

    def silver_to_gold_transformation(self):
        """
        Transforms silver layer data into analytics-ready gold layer data.
        
        Key analytics:
        1. Customer Insights:
           - Total orders and spending
           - Average order value
           - Last order date
           - State-wise analysis
        
        2. Product Review Analytics:
           - Sentiment analysis
           - Aspect-based ratings
           - Verified purchase metrics
           - Review volume analysis
        
        3. Product Performance:
           - Monthly sales metrics
           - Order volume analysis
           - Price analysis
           - Category performance
        """
        # Get silver tables as Snowpark DataFrames
        silver_customer_df = self.session.table("SILVER_CUSTOMERS")
        silver_product_df = self.session.table("SILVER_PRODUCTS")
        silver_order_df = self.session.table("SILVER_ORDERS")
        silver_reviews_df = self.session.table("SILVER_PRODUCT_REVIEWS")
        bronze_inventory_df = self.session.table("BRONZE_INVENTORY")
        bronze_order_items_df = self.session.table("BRONZE_ORDER_ITEMS")


        bronze_inventory_df=self.clean_col_names(bronze_inventory_df)
        bronze_order_items_df=self.clean_col_names(bronze_order_items_df)

        
        # Transform customer insights using Snowpark APIs
        customer_joined_df = silver_customer_df.join(
            silver_order_df,
            silver_customer_df.col("customer_id") == silver_order_df.col("CUSTOMER_ID"),
            "left",
            rsuffix='_rright'
        ).select(
            silver_customer_df.col("customer_id").cast("string").alias("customer_id"),
            silver_order_df.col("state").cast("string").alias("state"),
            silver_order_df.col("order_id"),
            silver_order_df.col("total_amount"),
            silver_order_df.col("order_date")
        )
        
        gold_customer_insights_df = customer_joined_df.group_by(
            "customer_id",
            "state"
        ).agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_order_value"),
            max(col("order_date").cast("TIMESTAMP_NTZ")).alias("last_order_date"),
            current_timestamp().alias("last_updated")
        )

        # Write to gold table
        logger.info("Writing to gold table- GOLD_CUSTOMER_INSIGHTS")
        self.session.sql("truncate table GOLD_CUSTOMER_INSIGHTS").collect()
        gold_customer_insights_df.write.mode("append").saveAsTable("GOLD_CUSTOMER_INSIGHTS",iceberg_config={ "BASE_LOCATION" : "retail_gold_layer/GOLD_CUSTOMER_INSIGHTS/","EXTERNAL_VOLUME" : "iceberg_external_volume_de_demo","storage_serialization_policy": "OPTIMIZED"})
        
        # Transform product reviews analytics
        df = self.session.sql('select * from SILVER_PRODUCT_REVIEWS').with_column("aspects",col("aspects").astype(ArrayType())).select(explode(col("aspects")),"product_id","review_id","customer_id","rating","title","content","review_date","verified_purchase","sentiment_score")
        silver_reviews_df = df.select(col("VALUE").getField("name").astype(StringType()).alias("aspect_name"),col("VALUE").getField("rating").astype(FloatType()).alias("aspect_rating"),"product_id","review_id","customer_id","rating","title","content","review_date","verified_purchase","sentiment_score")

        silver_reviews_df = (
            silver_reviews_df
            .group_by(
                "product_id",
                "review_id",
                "customer_id",
                "rating",
                "title",
                "content",
                "review_date",
                "verified_purchase",
                "sentiment_score"
            )
            .pivot("aspect_name")
            .agg(max(col("aspect_rating")))
        )

        # Cast product_id to string before grouping
        silver_reviews_df_with_cast = silver_reviews_df.select(
            col("product_id").cast("string").alias("product_id"),
            "rating",
            "review_id",
            "verified_purchase",
            "'Battery Life'",
            "'Quality'",
            "'Value for Money'",
            "sentiment_score"
        )
        
        gold_reviews_analytics_df = silver_reviews_df_with_cast.group_by("product_id").agg(
            avg("rating").alias("avg_rating"),
            count("review_id").alias("total_reviews"),
            sum(when(col("verified_purchase") == "true",1).otherwise(0)).alias("verified_reviews"),
            # sum("helpful_votes").alias("helpful_votes"),
            avg("'Battery Life'").alias("avg_battery_rating"),
            avg("'Quality'").alias("avg_quality_rating"),
            avg("'Value for Money'").alias("avg_value_rating"),
            count(when(col("sentiment_score") == "Positive",1)).alias("positive_reviews"),
            count(when(col("sentiment_score") == "Negative",1)).alias("negative_reviews"),
            current_timestamp().alias("last_updated")
        )

        # Write to gold table
        logger.info("Writing to gold table- GOLD_PRODUCT_REVIEWS_ANALYTICS")
        self.session.sql("truncate table GOLD_PRODUCT_REVIEWS_ANALYTICS").collect()
        gold_reviews_analytics_df.write.mode("append").saveAsTable("GOLD_PRODUCT_REVIEWS_ANALYTICS",iceberg_config={ "BASE_LOCATION" : "retail_gold_layer/GOLD_PRODUCT_REVIEWS_ANALYTICS/","EXTERNAL_VOLUME" : "iceberg_external_volume_de_demo","storage_serialization_policy": "OPTIMIZED"})
        

        # Transform product performance using Snowpark API
        # Get order metrics from bronze_order_items with monthly aggregation
        order_metrics = bronze_order_items_df.join(
            silver_order_df,
            bronze_order_items_df.order_id == silver_order_df.order_id,
            'left',
            rsuffix='_rrrrright'
        ).group_by(
            'product_id',
            date_trunc('month', col('order_date')).alias('order_month')
        ).agg(
            count('order_id').alias('total_orders'),
            sum('quantity').alias('total_quantity'),
            sum(col('unit_price') * col('quantity')).alias('total_sales'),
            avg('unit_price').alias('avg_price')
        )

        # Join with silver product data
        product_performance = silver_product_df.join(
            order_metrics,
            silver_product_df.col("product_id") == order_metrics.col("product_id"),
            'left',
            rsuffix='_rrrrright'
        )

        # Select only the columns needed for the gold table
        gold_product_performance = product_performance.select([
            'product_id', 
            'product_name',
            'category',
            'order_month',
            'total_sales', 
            'total_orders', 
            'avg_price',
            'specifications'
        ])

        # Write to gold table
        logger.info("Writing to gold table- GOLD_PRODUCT_PERFORMANCE")
        gold_product_performance.write.mode("overwrite").saveAsTable("GOLD_PRODUCT_PERFORMANCE")



# @sproc(name="BRONZE_TO_SILVER_TRANSFORM", replace=True,
#       packages=["snowflake-snowpark-python","pandas"], is_permanent=True)
def bronze_to_silver_transform(session: Session) -> str:
    """
    Entry point for bronze to silver transformation.
    Can be registered as a Snowflake stored procedure for scheduled execution.
    """
    DataTransformations(session).bronze_to_silver_transformation()
    return "OK"

# @sproc(name="SILVER_TO_GOLD_TRANSFORM", replace=True,
#       packages=["snowflake-snowpark-python","pandas"], is_permanent=True)
def silver_to_gold_transform(session: Session) -> str:
    """
    Entry point for silver to gold transformation.
    Can be registered as a Snowflake stored procedure for scheduled execution.
    """
    DataTransformations(session).silver_to_gold_transformation()
    return "OK"



# from multiprocessing import freeze_support
if __name__ == "__main__":
    """
    Local execution entry point for testing and development.
    Uses a local Snowpark session for running transformations.
    """
    from Temp.test_source import sp_session 
    data_transformer = DataTransformations(sp_session,'snowpark')
    # data_transformer.bronze_to_silver_transformation()
    data_transformer.silver_to_gold_transformation()
