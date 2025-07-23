CREATE OR REPLACE   PROCEDURE usp_generateValidationResults()
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.9
  ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
  PACKAGES = ('snowflake-snowpark-python')
  ARTIFACT_REPOSITORY_PACKAGES = ('great-expectations==0.15.14','pandas==2.1.4')
  HANDLER = 'generateValidationResults'
  AS
$$
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, S3StoreBackendDefaults
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from snowflake.snowpark.types import IntegerType, StringType, StructField,VariantType,StructType,BooleanType
# from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context import DataContext
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.checkpoint import Checkpoint   
from snowflake.snowpark import Session
                     
def generateValidationResults(session: Session) -> str:
    
    from pathlib import Path
    import os ,sys ,json ,tarfile
    
    data_context_config = DataContextConfig(
    datasources={
        "dataframe_datasource": DatasourceConfig(
            class_name="PandasDatasource",
            batch_kwargs_generators={
                "subdir_reader": {
                    "class_name": "SubdirReaderBatchKwargsGenerator",
                    "base_directory": "/tmp/great_expectation/",
                }
            },
        )
    },
    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/tmp/great_expectation"),
    )
    
    # Creating the GE context here
    context = BaseDataContext(project_config=data_context_config)
    
    # Providing the datasource details which here is the pandas DF. We define the actual DF in the batch request which is defined after creating the DS
    
    datasource_config = {
    "name": "pandas_dataframe_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "batch_identifiers": ["default_identifier_name"],
        },
    },
            }
    con='done'

    # Adding the DS to the context
    context.add_datasource(**datasource_config)
    
    # Converting the Snowpark DF into Pandas DF.
    df=session.sql("select top 2000 * from SILVER_PRODUCT_REVIEWS").to_pandas()
    
    #Creating the batch request whivh will be used 
    batch_request = RuntimeBatchRequest(
                                datasource_name="pandas_dataframe_datasource",
                                data_connector_name="default_runtime_data_connector_name",
                                data_asset_name="PandasData",  # This can be anything that identifies this data_asset for you
                                runtime_parameters={"batch_data": df},  # df is your dataframe, you have created above.
                                batch_identifiers={"default_identifier_name": "default_identifier"},
                                )
    
    # Creating the expecation suite
    context.create_expectation_suite(
    expectation_suite_name="pandas_expectation_suite", overwrite_existing=True)
    
    # Creating the validator which takes the batch request and expectation suite name
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="pandas_expectation_suite"
    )
    
    #Creating the required expectation. You can also create custom expectations as well. You can add additional inbuilt expectations as per the requirement
    validator.expect_column_values_to_be_in_set("VERIFIED_PURCHASE",["FALSE"])
    validator.expect_column_min_to_be_between("HELPFUL_VOTES",12,20)
    
    #Saving the expectation 
    validator.save_expectation_suite(discard_failed_expectations=False)
    
    # Creating the checkpoint without writing to the file system and running by passing the run time parameters

    my_checkpoint_name = "pandas_checkpoint"
    checkpoint_config = {
                "name": my_checkpoint_name,
                "config_version": 1.0,
                "class_name": "SimpleCheckpoint",
                "run_name_template": "%Y%m%d-%H%M%S-my-pandas_run-name-template",
            }
            
    context.add_checkpoint(**checkpoint_config)

    # run expectation_suite against Pandas dataframe
    res = context.run_checkpoint(
            checkpoint_name = my_checkpoint_name,
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": "pandas_expectation_suite",
                }
            ],
        )
    
        
    # Defining the schema, creating the Snowpark DF and and writing the validation results to a table.
    schema = StructType([StructField("RunStatus", BooleanType()),StructField("RunId", VariantType()), StructField("RunValidation", VariantType())])

    df=session.create_dataframe([[res.success, json.loads(str(res.run_id)),json.loads(str(res.list_validation_results()))]], schema)

    df.write.mode('append').saveAsTable('GreatExpeactionValidationsResults')
    return 'SUCCESS'
$$;