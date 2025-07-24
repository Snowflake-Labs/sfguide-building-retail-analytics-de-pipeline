# Building a Retail Analytics Data Engineering Pipeliene in Snowflake

## Overview

This solution demonstrates building a modern data engineering pipeline for retail analytics using Snowflake. It integrates diverse data sources—including structured files, OLTP databases, and semi-structured formats—into a unified Snowflake environment. The pipeline automates data ingestion, transformation, and validation, enabling robust analytics and reporting. By leveraging Snowflake’s scalability and advanced features, the solution empowers retailers to gain actionable insights, optimize operations, and make data-driven decisions efficiently.

This solution showcases building comprehensive retail analytics data pipeline, leveraging the following key Snowflake features and integrations

- Snowpark DB APIs for loading data from MySQL Databases. It also has the code to ingest data from Oracle and Azure SQL.
- Snowpark API for XML and Json loading and processing
- Cortex APIs to extract sentiments and state names from address
- Snowpark Pandas API for Customer Analytics
- Artifact Repository for using OSS libararies within Snowflake Stored Procedures/UDFs
- External File Write to write html files to snowflake stage
- Using Snowflake Trail for obsevability
- Tasks and DAG for pipeline orchestration
- Snowflake Managed Iceberg Tables for storing gold layer data
- Streamlit for Analytical Dashboard


## Step-by-Step Guide

For prerequisites, environment setup, step-by-step guide and instructions, please refer to the [QuickStart Guide](https://quickstarts.snowflake.com/guide/building-retail-analytics-de-pipeline/index.html?index=..%2F..index#0).

