# define the factory method for creating a connection to Oracle

from snowflake.snowpark import Session,DataFrame
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.functions import col, current_timestamp, explode,sql_expr,lit
from snowflake.snowpark.types import ArrayType,StructType,StructField,StringType
import json

# from dotenv import load_dotenv
import os
import logging
from typing import Optional, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# load_dotenv()

class DataIntegration:

    def __init__(self,session:Session):
        self.session = session

    def load_product_reviews_to_bronze(self,json_file_path: str):
            """
            Ingest product reviews from JSON to Snowflake bronze layer
            """
            # Read JSON file using Snowpark
            df = self.session.read.format("json").option('STRIP_OUTER_ARRAY',True).load(json_file_path)
            
            # Select the relevant columns
            reviews_df = df.select(sql_expr("$1:review_id::varchar").alias('review_id'), \
                sql_expr("$1:product_id::varchar").alias('product_id'),
                sql_expr("$1:customer_id::varchar").alias('customer_id'),
                sql_expr("$1:rating::decimal").alias('rating'),
                sql_expr("$1:title::varchar").alias('title'),
                sql_expr("$1:content::varchar").alias('content'),
                sql_expr("$1:date::date").cast("DATE").alias('review_date'),
                sql_expr("$1:verified_purchase::varchar").alias('verified_purchase'),
                sql_expr("$1:helpful_votes::int").alias('helpful_votes'),
                sql_expr("$1:aspects::variant").alias('aspects')

            )
            
            # Write to bronze table
            _ = reviews_df.write.mode("overwrite").saveAsTable("BRONZE_PRODUCT_REVIEWS")

    def load_cust_pref_xml_to_bronze(self, xml_file: str, table_name: str):
            """
            Load XML data into bronze tables using Snowpark APIs
            """
            # Read XML file using Snowpark
            df = self.session.read \
            .format("xml") \
            .option('STRIP_OUTER_ELEMENT', True) \
            .option("rowTag", "customer")  \
            .load('@SOURCE_FILES/customer_preferences.xml') 
            
            # Add source file and timestamp columns
            df = df.withColumn("source_file", lit(xml_file)).withColumn("last_updated", current_timestamp())
            df_pref = df.withColumn("pref", (col("'preference'").astype(ArrayType())))
            # Explode the preference column
            df_explode=df_pref.select(col("'customer_id'"),col('source_file'),col('last_updated'),explode("pref").alias("prefer"))

            # Remove both ' and " from ends of column names
            old_cols = df_explode.columns
            new_cols = [c.strip("'\"") for c in old_cols] 
            df_clean = df_explode
            for old_name, new_name in zip(old_cols, new_cols):
                if old_name != new_name:
                    df_clean = df_clean.withColumnRenamed(old_name, new_name)

            df_final=df_clean.select(col("customer_id").astype(StringType()).alias("customer_id"), col('source_file'), col('last_updated'), \
                            col("PREFER").getField("type").astype(StringType()).alias("preference_type"), \
                            col("PREFER").getField("value").astype(StringType()).alias("preference_value")
                            ) 
            
            # Write to bronze table
            print('writing to table',table_name)
            df_final.write.mode("overwrite").saveAsTable('BRONZE_CUSTOMER_PREFERENCES') 

    def load_product_specs_xml_to_bronze(self, xml_file: str, table_name: str):
            """
            Load XML data into bronze tables using Snowpark APIs
            """
            # Read XML file using Snowpark
            df_prod = self.session.read \
                    .format("xml") \
                    .option('STRIP_OUTER_ELEMENT', True) \
                    .option("rowTag", "product")  \
                    .load(xml_file) 
            
            # Add source file and timestamp columns
            df_prod = df_prod.withColumn("source_file", lit(xml_file)).withColumn("last_updated", current_timestamp())
            # Explode the preference column
            # Remove both ' and " from ends of column names
            old_cols = df_prod.columns
            new_cols = [c.strip("'\"") for c in old_cols] 
            df_clean = df_prod
            for old_name, new_name in zip(old_cols, new_cols):
                if old_name != new_name:
                    df_clean = df_clean.withColumnRenamed(old_name, new_name)

            df_clean = df_clean.withColumn("specfics", (col("specification").astype(ArrayType())))
            df_explode=df_clean.select(col("product_id"),col("source_file"),col("last_updated"),explode("specfics").alias("specs"))


                # Select and rename columns for product specs

            df_final=df_explode.select(col("product_id").astype(StringType()).alias("product_id"), col('source_file'), col('last_updated'), \
                            col("SPECS").getField("name").astype(StringType()).alias("spec_name"), \
                            col("SPECS").getField("value").astype(StringType()).alias("spec_value")
                            ) 
            
            # Write to bronze table
            print('writing to table',table_name)
            df_final.write.mode("overwrite").saveAsTable(table_name) 



# enable multiprocessing support, required for client connections from macOS and Windows

if __name__ == '__main__':
    pass

    # data_ingestion = DataIngestion(session)
    # logging.info("Loading product reviews data")
    # # df = data_ingestion.get_oracle_customer_data('snowpark')
    # data_ingestion.ingest_product_reviews('@SOURCE_FILES/product_reviews.json')
    # logging.info("Product reviews data loaded")
    # print(df.show(2))
    # logging.info("Oracle customer data retrieved")
    # df.write.mode("overwrite").saveAsTable("BRONZE_CUSTOMERS")
    # df = get_oracle_product_data('snowpark')
    # df.write.mode("overwrite").saveAsTable("BRONZE_PRODUCTS")
    # df = get_sql_server_inventory_data('snowpark')
    # df.write.mode("overwrite").saveAsTable("BRONZE_INVENTORY")
    # df = get_sql_server_order_data('snowpark')
    # df.write.mode("overwrite").saveAsTable("BRONZE_ORDERS")
    # print(df.head(5))
    # ingest_product_reviews(sp_session,'@SOURCE_FILES/product_reviews.json')
    # print(df.head(5))
    # load_product_specs_xml_to_bronze(sp_session,'@SOURCE_FILES/product_specifications.xml','BRONZE_PRODUCT_SPECIFICATIONS')
    # load_cust_pref_xml_to_bronze(sp_session,'@SOURCE_FILES/customer_preferences.xml','BRONZE_CUSTOMER_PREFERENCES')