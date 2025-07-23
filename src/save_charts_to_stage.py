from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, cast, concat, lit
import os


def main(session:Session, html_file_name:str):
    # Create Snowpark session    
    try:
        # Read the data using Snowpark DataFrame
        df = session.sql("""
            SELECT 
                cast(preference_type as varchar) || ' - ' || cast(preference_value as varchar) as preference,
                COUNT(*) AS count
            FROM PUBLIC.BRONZE_CUSTOMER_PREFERENCES
            GROUP BY preference
            ORDER BY count DESC
            LIMIT 10
        """)
        

        df.write.copy_into_location("@SOURCE_FILES/customer_preference_counts.csv", file_format_type="csv", format_type_options={"field_delimiter": ",","field_optionally_enclosed_by":'"','null_if':"",'empty_field_as_null':True,'compression':"none",'encoding':"UTF-8"},header=True,single=True,overwrite=True)
        print("Successfully saved data to stage 'SOURCE_FILES' as 'customer_preference_counts.csv'")

        _ = session.sql(f'''
                    copy files into  @SOURCE_FILES from 
                    ( select udf_save_customer_pref_chart_html(build_scoped_file_url(@SOURCE_FILES, 'customer_preference_counts.csv')), 
                    '{html_file_name}');
                    ''').collect()
        print(f"Successfully saved data to stage 'SOURCE_FILES' as '{html_file_name}'")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    from snowpark_session import sp_session as session
    main(session,'customer_pref_count_1.html')
