from snowflake.snowpark import Session
# from cryptography.hazmat.backends import default_backend
# from cryptography.hazmat.primitives.asymmetric import rsa
# from cryptography.hazmat.primitives.asymmetric import dsa
# from cryptography.hazmat.primitives import serialization
from snowflake.snowpark.functions import sproc
import json
from dotenv import load_dotenv
import os


load_dotenv()

connection_parameters = {
  "account": os.getenv("SNOWFLAKE_ACCOUNT"),
  "user": os.getenv("SNOWFLAKE_USER"),
  "password": os.getenv("PAT_TOKEN"),
  "role": os.getenv("SNOWFLAKE_ROLE"),  
  "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),  
  "database": os.getenv("SNOWFLAKE_DATABASE"), 
  "schema": os.getenv("SNOWFLAKE_SCHEMA")
}  

sp_session = Session.builder.configs(connection_parameters).create()
