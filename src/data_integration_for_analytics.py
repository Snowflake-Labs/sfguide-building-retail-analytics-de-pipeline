import os
import logging
from typing import Optional, Tuple
from snowflake.snowpark import Session, DataFrame
from db_connections import create_mysql_db_connection, create_oracle_db_connection, create_sql_server_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_integration_for_analytics_copy")

# # Initialize credentials as None
# oracle_username_password_object = None
# azure_sql_username_password_object = None
# my_sql_username_password_object = None

# # Try to get credentials from Snowflake secrets at module level wwhen executed from a stored procedure
# try:
#     import _snowflake
#     # oracle_username_password_object = _snowflake.get_username_password('oracle_cred')
#     # azure_sql_username_password_object = _snowflake.get_username_password('azuresql_cred')
#     my_sql_username_password_object = _snowflake.get_username_password('mysql_cred')
#     logger.info("Successfully loaded credentials from Snowflake secrets")
# except ImportError:
#     logger.info("Running outside Snowflake - will use environment variables")
# except Exception as e:
#     logger.warning(f"Failed to get credentials from Snowflake secrets: {str(e)}")

# def get_db_credentials(source_name:str) -> Tuple[str, str]:
#     """
#     Get database credentials either from environment variables (local execution)
#     or from Snowflake secrets (Snowflake stored procedure execution)
#     """
#     if source_name == 'oracle':
#         if oracle_username_password_object is not None:
#             return oracle_username_password_object.username, oracle_username_password_object.password
#     elif source_name == 'azuresql':
#         if azure_sql_username_password_object is not None:
#             return azure_sql_username_password_object.username, azure_sql_username_password_object.password
#     elif source_name == 'mysql':
#         if my_sql_username_password_object is not None:
#             return my_sql_username_password_object.username, my_sql_username_password_object.password
#     else:
#         raise ValueError(f"Invalid source name: {source_name}")
    
#     # Fallback to environment variables
#     if source_name == 'azuresql':
#         user = 'sqladmin'
#         password = os.getenv('AZURE_SQL_PASSWORD')
#     elif source_name == 'oracle':
#         user = 'admin'
#         password = os.getenv('ORACLE_PASSWORD')
#     elif source_name == 'mysql':
#         user = 'sqladmin'
#         password = os.getenv('MYSQL_PASSWORD')
    
#     if not user or not password:
#         raise ValueError("Database credentials not found in environment variables")
    
#     return user, password

# def create_sql_server_connection():
#     import pyodbc
#     HOST = "sqlhostname"
#     PORT = "1433"
#     USERNAME, PASSWORD = get_db_credentials('azuresql')
#     DATABASE = "sqldemodb"
#     connection_str = (
#         f"DRIVER={{ODBC Driver 18 for SQL Server}};"
#         f"SERVER={HOST},{PORT};"
#         f"DATABASE={DATABASE};"
#         f"UID={USERNAME};"
#         f"PWD={PASSWORD};"
#     )
#     connection = pyodbc.connect(connection_str)
#     return connection

# def create_oracle_db_connection():
#     import oracledb
#     """Create Oracle database connection using credentials from appropriate source"""
#     HOST = "oraclehostname"
#     PORT = "1521"   
#     SERVICE_NAME = "orcl"
    
#     USERNAME, PASSWORD = get_db_credentials('oracle')
#     DSN = f"{HOST}:{PORT}/{SERVICE_NAME}"
#     connection = oracledb.connect(
#         user=USERNAME,
#         password=PASSWORD,
#         dsn=DSN
#     )
#     return connection

# def create_mysql_db_connection():
#     import pymysql
#     """Create MySQL database connection using credentials from appropriate source"""
#     HOST = "mysqlhostname"
#     PORT = 3306
#     USERNAME, PASSWORD = get_db_credentials('mysql')
#     DATABASE = "retail_db"

#     connection = pymysql.connect(
#         host=HOST,
#         port=PORT,
#         db=DATABASE,
#         user=USERNAME,
#         password=PASSWORD,
#     )
#     return connection

def clean_col_names(df: DataFrame) -> DataFrame:
    """
    Removes annoying single & double quotes in the column names
    also makes all characters in the column names lowercase
    """
    df_res = df
    for col in df.columns:
        df_res = df_res.rename(col, col.lower().replace('\'', '').replace('"', ''))
    return df_res

def main(session: Optional[Session] = None, source: str ='mysql' , source_table: str = 'orders'):
    """
    Main function that can be called both as a standalone script and as a Snowflake stored procedure
    
    Args:
        session: Snowpark session (None when running locally)
        source: Source name (oracle or azuresql or mysql)
        source_table: Source table name in Oracle or Azure SQL or MySQL
        dest_table_name: Target table name in Snowflake
    """
    try:
        if source == 'oracle':
            connection = create_oracle_db_connection
        elif source == 'azuresql':
            connection = create_sql_server_connection
        elif source == 'mysql':
            connection = create_mysql_db_connection
        logger.info(f"Starting reading from {source} {source_table} Table.")
        
        # Read data from Oracle
        if source == 'oracle':
            df = session.read.dbapi(
                connection,
                table=source_table
            )
        elif source == 'azuresql':
            df = session.read.dbapi(
                connection,
                table=source_table
            )
        elif source == 'mysql':
            df = session.read.dbapi(
                connection,
                table=source_table
            )
        
        # Clean column names
        df = clean_col_names(df)
        
        # Write to Snowflake table
        # df.to_snowpark_pandas()
        
        logger.info(f"Loaded data from {source_table} table from {source} and returning the dataframe")
        return df.to_snowpark_pandas()
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

if __name__ == "__main__":
    # pass
    # When running as a standalone script
    from snowpark_session import sp_session as session
    print(main(session,"mysql" ,source_table="orders") )