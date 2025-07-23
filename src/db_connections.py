import os
import logging
from typing import Optional, Tuple
from snowflake.snowpark import Session, DataFrame


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db_connections")

# Initialize credentials as None
oracle_username_password_object = None
azure_sql_username_password_object = None
my_sql_username_password_object = None

# Initialize host names as None
my_sql_host_name = None
oracle_host_name = None
azure_sql_host_name = None

# Try to get credentials from Snowflake secrets at module level when executed from a stored procedure
# This is a workaround to get the credentials from Snowflake secrets at module level when executed from a stored procedure
# Ensure the secrets are created in Snowflake before running this code in the stored procedure
# There is logic to fallback to environment variables if the secrets are not found in Snowflake
# This is to ensure the code can be run locally as well as in Snowflake and has code for three sources (MySQL, Azure SQL and Oracle)

try:
    import _snowflake
    # Uncomment the below 2 lines to get the credentials from Snowflake secrets if want to read tables from Oracle, Azure SQL as well assuming you have created the secrets in Snowflake
    
    # oracle_username_password_object = _snowflake.get_username_password('oracle_cred')
    # azure_sql_username_password_object = _snowflake.get_username_password('azuresql_cred')

    my_sql_username_password_object = _snowflake.get_username_password('mysql_cred')
    my_sql_host_name = _snowflake.get_generic_secret_string('mysql_hostname')
    # logger.info(f"MySQL host name: {my_sql_host_name}")
    logger.info("Successfully loaded credentials from Snowflake secrets")
except ImportError:
    logger.info("Running outside Snowflake - will use environment variables")
except Exception as e:
    logger.warning(f"Failed to get credentials from Snowflake secrets: {str(e)}")

def get_db_credentials(source_name:str) -> Tuple[str, str]:
    """
    Get database credentials either from environment variables (local execution)
    or from Snowflake secrets (Snowflake stored procedure execution)
    """
    if source_name == 'oracle':
        if oracle_username_password_object is not None:
            return oracle_username_password_object.username, oracle_username_password_object.password
    elif source_name == 'azuresql':
        if azure_sql_username_password_object is not None:
            return azure_sql_username_password_object.username, azure_sql_username_password_object.password
    elif source_name == 'mysql':
        if my_sql_username_password_object is not None:
            return my_sql_username_password_object.username, my_sql_username_password_object.password
    else:
        raise ValueError(f"Invalid source name: {source_name}")
    
    # Fallback to environment variables
    if source_name == 'azuresql':
        user = 'sqladmin'
        password = os.getenv('AZURE_SQL_PASSWORD')
    elif source_name == 'oracle':
        user = 'admin'
        password = os.getenv('ORACLE_PASSWORD')
    elif source_name == 'mysql':
        user = 'sqladmin'
        password = os.getenv('MYSQL_PASSWORD')
    
    if not user or not password:
        raise ValueError("Database credentials not found in environment variables")
    
    return user, password

def get_host_name(source_name:str) -> str:
    if source_name == 'mysql':
        if my_sql_host_name is not None:
            return my_sql_host_name
        elif os.getenv('MYSQL_HOST') is not None:
            return os.getenv('MYSQL_HOST')
        else:
            raise ValueError("MySQL host name not found in environment variables")
    if source_name == 'azuresql':
        if azure_sql_host_name is not None:
            return azure_sql_host_name
        elif os.getenv('AZURE_SQL_HOST') is not None:
            return os.getenv('AZURE_SQL_HOST')
        else:
            raise ValueError("Azure SQL host name not found in environment variables")
    if source_name == 'oracle':
        if oracle_host_name is not None:
            return oracle_host_name
        elif os.getenv('ORACLE_HOST') is not None:
            return os.getenv('ORACLE_HOST')
        else:
            raise ValueError("Oracle host name not found in environment variables")
    else:
        raise ValueError(f"Invalid source name: {source_name}")

def create_sql_server_connection():
    import pyodbc
    HOST = get_host_name('azuresql')
    PORT = "1433"
    USERNAME, PASSWORD = get_db_credentials('azuresql')
    DATABASE = "sqldemodb"
    connection_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={HOST},{PORT};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
    )
    connection = pyodbc.connect(connection_str)
    return connection

def create_oracle_db_connection():
    import oracledb
    """Create Oracle database connection using credentials from appropriate source"""
    HOST = get_host_name('oracle')
    PORT = "1521"   
    SERVICE_NAME = "orcl"
    
    USERNAME, PASSWORD = get_db_credentials('oracle')
    DSN = f"{HOST}:{PORT}/{SERVICE_NAME}"
    connection = oracledb.connect(
        user=USERNAME,
        password=PASSWORD,
        dsn=DSN
    )
    return connection

def create_mysql_db_connection():
    import pymysql
    """Create MySQL database connection using credentials from appropriate source"""
    HOST = get_host_name('mysql')
    PORT = 3306
    USERNAME, PASSWORD = get_db_credentials('mysql')
    DATABASE = "retail_db"

    connection = pymysql.connect(
        host=HOST,
        port=PORT,
        db=DATABASE,
        user=USERNAME,
        password=PASSWORD,
    )
    return connection