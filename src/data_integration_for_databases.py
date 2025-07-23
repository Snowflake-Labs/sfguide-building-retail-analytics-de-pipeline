import os
import logging
from typing import Optional, Tuple
from snowflake.snowpark import Session, DataFrame
from db_connections import create_mysql_db_connection, create_oracle_db_connection, create_sql_server_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_integration_for_databases")


def clean_col_names(df: DataFrame) -> DataFrame:
    """
    Removes annoying single & double quotes in the column names
    also makes all characters in the column names lowercase
    """
    df_res = df
    for col in df.columns:
        df_res = df_res.rename(col, col.lower().replace('\'', '').replace('"', ''))
    return df_res

def main(session: Optional[Session] = None, source: str ='oracle' , source_table: str = 'products' , dest_table_name: str = 'BRONZE_PRODUCTS'):
    """
    Main function that can be called both as a standalone script and as a Snowflake stored procedure
    
    Args:
        session: Snowpark session (None when running locally)
        source: Source name (oracle or azuresql or mysql)
        source_table: Source table name in Oracle, Azure SQL or MySQL
        dest_table_name: Target table name in Snowflake
    """
    try:
        connection = None
        if source == 'oracle':
            connection = create_oracle_db_connection
        elif source == 'azuresql':
            connection = create_sql_server_connection
        elif source == 'mysql':
            connection = create_mysql_db_connection
        else:
            raise ValueError(f"Invalid source: {source}")
        
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
        df.write.mode("overwrite").saveAsTable(dest_table_name)
        
        logger.info(f"Loaded data into Snowflake {dest_table_name} table from {source} {source_table}")
        return "Data loaded successfully"
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

if __name__ == "__main__":
    # pass
    # When running as a standalone script
    from snowpark_session import sp_session as session
    main(session,"mysql" ,source_table="orders", dest_table_name="test1_BRONZE_ORDERS")
    session.sql("select * from test1_BRONZE_ORDERS").show()