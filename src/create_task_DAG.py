from snowflake.snowpark import Session

import logging
import json
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_task_dag(session,root_task_name):
    # Create the root task (Task0)
    session.sql(f"""
    CREATE OR REPLACE TASK {root_task_name}
    WAREHOUSE = RETAIL_WH
    SCHEDULE = 'USING CRON 0 0 * * * America/Los_Angeles'
    AS
    SELECT CURRENT_DATE();
    """).collect()

    # Create Task1
    session.sql(f"""
    CREATE OR REPLACE TASK productreview_task
    WAREHOUSE = RETAIL_WH
    AFTER {root_task_name}
    AS
    CALL usp_loadProductReviews_json();
    """).collect()

        # Create Task2
    session.sql(f"""
    CREATE OR REPLACE TASK customer_preferences_task
    WAREHOUSE = RETAIL_WH
    AFTER {root_task_name}
    AS
    CALL usp_loadCustomerPreferences_xml();
    """).collect()

    # Create Task3
    session.sql(f"""
    CREATE OR REPLACE TASK product_specification_task
    WAREHOUSE = RETAIL_WH
    AFTER {root_task_name}
    AS
    CALL usp_loadProductSpecification_xml();
    """).collect()

    # Create Task4
    session.sql(f"""
    CREATE OR REPLACE TASK mysql_customer_task
    WAREHOUSE = RETAIL_WH
    AFTER {root_task_name}
    AS
    CALL usp_loadMySQLData('mysql','customers','BRONZE_CUSTOMERS');
    """).collect()

    # Create Task5
    session.sql(f"""
    CREATE OR REPLACE TASK mysql_product_task
    WAREHOUSE = RETAIL_WH
    AFTER {root_task_name}
    AS
    CALL usp_loadMySQLData('mysql','products','BRONZE_PRODUCTS');
    """).collect()

    # Create Task6
    session.sql(f"""
    CREATE OR REPLACE TASK mysql_inventory_task
    WAREHOUSE = RETAIL_WH
    AFTER {root_task_name}
    AS
    CALL usp_loadMySQLData('mysql','inventory','BRONZE_INVENTORY');
    """).collect()

    # Create Task7
    session.sql(f"""
    CREATE OR REPLACE TASK mysql_order_task
    WAREHOUSE = RETAIL_WH
    AFTER {root_task_name}
    AS
    CALL usp_loadMySQLData('mysql','orders','BRONZE_ORDERS');
    """).collect()

    # Create Task4
    session.sql(f"""
    CREATE OR REPLACE TASK mysql_order_items_task
    WAREHOUSE = RETAIL_WH
    AFTER {root_task_name}
    AS
    CALL usp_loadMySQLData('mysql','order_items','BRONZE_ORDER_ITEMS');
    """).collect()

    #Create Task8
    session.sql(f"""
    CREATE OR REPLACE TASK bronze_silver_task
    WAREHOUSE = RETAIL_WH
    AFTER productreview_task,mysql_customer_task,mysql_product_task,mysql_inventory_task,mysql_order_task,mysql_order_items_task
    AS
    CALL usp_loadBronzeToSilver();
    """).collect()

        #Create Task9
    session.sql(f"""
    CREATE OR REPLACE TASK data_validation_task
    WAREHOUSE = RETAIL_WH
    AFTER bronze_silver_task
    AS
    CALL usp_generateValidationResults();
    """).collect()

    #Create Task10
    session.sql(f"""
    CREATE OR REPLACE TASK silver_gold_task
    WAREHOUSE = RETAIL_WH
    AFTER bronze_silver_task
    AS
    CALL usp_loadSilverToGold();
    """).collect()

        #Create Task11
    session.sql(f"""
    CREATE OR REPLACE TASK order_analytics_task
    WAREHOUSE = RETAIL_WH
    AFTER silver_gold_task
    AS
    CALL usp_loadOrderAnalytics();
    """).collect()

    #Create Task12
    session.sql(f"""
    CREATE OR REPLACE TASK customer_analytics_task
    WAREHOUSE = RETAIL_WH
    AFTER silver_gold_task
    AS
    CALL usp_load_customer_analytics();
    """).collect()


    

def drop_tasks(session,root_task_name):
    logger.info("Suspending root_task")
    session.sql(f'alter task {root_task_name} suspend').collect()

    res= session.sql(f''' select name
        from table(information_schema.task_dependents(task_name => '{root_task_name}', recursive => true))''').collect()
    for r in res:
        logger.info(f"Dropping task: {r.NAME}")
        session.sql(f'drop task {r.NAME}').collect()
    logger.info("All tasks deleted successfully")
   # session.sql('drop task GET_GRAPH_STATS').collect()
    return 'Done'

def resume_tasks(session,root_task_name):
    logger.info("Resuming root_task")
    session.sql(f'''SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('{root_task_name}')''').collect()

    logger.info("All tasks resumed successfully")
    
if __name__ == "__main__":
    import sys
    from snowpark_session import sp_session as session
 
    try:
        drop_tasks(session,'retail_root_task')
        create_task_dag(session,'retail_root_task')
        resume_tasks(session,'retail_root_task')

        logger.info("Task DAG created successfully!")
    except Exception as e:
        logger.error(f"Error in task DAG operation: {str(e)}")
    finally:
        session.close()
