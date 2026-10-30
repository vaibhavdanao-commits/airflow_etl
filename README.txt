Retail Data Engineering & AI Analytics Platform

A complete retail data engineering project that demonstrates data
ingestion, ETL processing, data quality validation, dimensional
modeling, Apache Iceberg storage, orchestration, pipeline monitoring,
and AI-powered retail analytics.

The project is designed to run locally on Windows and can be extended
for AWS-based data engineering workflows.

Table of Contents

1.  Project Overview
2.  Key Features
3.  Technology Stack
4.  Project Architecture
5.  Prerequisites
6.  Installation and Setup
7.  Configuration
8.  Understanding the Data
9.  Running the Data Pipeline
10. Data Quality and Validation
11. Dimensional Warehouse and Apache Iceberg
12. SCD Type 2 Processing
13. Business Data Marts
14. PySpark Optimization
15. Data Lineage and Pipeline Monitoring
16. AI-Powered Retail Analytics
17. Airflow Orchestration
18. AWS Integration
19. Running Tests
20. Troubleshooting
21. Security and Best Practices
22. Future Enhancements

------------------------------------------------------------------------

1. Project Overview

This project implements a retail data engineering platform that
processes customer, product, store, and sales data through a structured
ETL workflow.

The platform supports raw data ingestion, transformation, data quality
checks, curated datasets, dimensional warehouse storage, analytical data
marts, and business reporting.

An AI analytics layer enables users to ask business questions in natural
language and generate SQL queries against the dimensional warehouse.

Business objectives

-   Build a reusable and maintainable ETL pipeline.
-   Validate data before loading it into analytical layers.
-   Maintain data quality and processing metadata.
-   Implement a dimensional model for retail analytics.
-   Store analytical data using Apache Iceberg.
-   Support historical dimension tracking through SCD Type 2.
-   Create curated data marts for sales, customers, and products.
-   Monitor pipeline executions and processing failures.
-   Provide interactive analytics through Streamlit.
-   Explore natural-language-to-SQL capabilities using locally hosted
    open-source language models.
-   Support future deployment to AWS.

2. Key Features

  -----------------------------------------------------------------------
  Component                           Description
  ----------------------------------- -----------------------------------
  Data ingestion                      Reads retail source files and
                                      tracks ingestion metadata

  ETL processing                      Extracts, validates, transforms,
                                      and loads data

  Data quality                        Checks required fields, nulls,
                                      duplicates, schemas, and accepted
                                      values

  Data organization                   Separates raw, processed, and
                                      curated datasets

  Dimensional modeling                Implements a retail star schema

  Apache Iceberg                      Stores analytical tables in an open
                                      table format

  SCD Type 2                          Tracks historical changes in
                                      supported dimensions

  Data marts                          Supports sales, customer, and
                                      product analytics

  PySpark optimization                Demonstrates efficient joins,
                                      partitioning, and execution
                                      strategies

  Pipeline monitoring                 Tracks status, duration, counts,
                                      and failures

  Airflow                             Supports scheduled and
                                      dependency-based orchestration

  AI analytics                        Supports natural-language questions
                                      and SQL generation

  Streamlit                           Provides interactive dashboards

  AWS integration                     Supports exploration of AWS Glue,
                                      S3, Athena, and related services
  -----------------------------------------------------------------------

3. Technology Stack

  -----------------------------------------------------------------------
  Technology                          Purpose
  ----------------------------------- -----------------------------------
  Python                              ETL logic, utilities, orchestration
                                      integration

  SQL                                 Data analysis and transformations

  PySpark                             Distributed data processing and
                                      optimization

  Apache Airflow                      Workflow scheduling and
                                      orchestration

  Apache Iceberg                      Analytical table storage

  PyIceberg                           Accessing Iceberg tables from
                                      Python

  DuckDB                              Local analytical SQL execution,
                                      where configured

  Pandas / PyArrow                    Data processing and
                                      interoperability

  Streamlit                           Interactive dashboards

  Ollama                              Local language-model execution,
                                      when configured

  ChromaDB                            Vector storage for
                                      retrieval-augmented generation,
                                      when configured

  SQLGlot                             SQL parsing and query validation,
                                      when configured

  AWS Glue                            Managed ETL and catalog integration

  Amazon S3                           Cloud object storage

  Amazon Athena                       SQL analytics on supported data
                                      sources

  Amazon Redshift                     Cloud data warehouse integration

  Git / GitHub                        Version control and source
                                      management
  -----------------------------------------------------------------------

Some components are optional and depend on the installed modules and
configuration.

4. Project Architecture

The following diagram illustrates the intended end-to-end architecture.

    flowchart TD
        A[Retail Source Files] --> B[Data Ingestion]
        B --> C[Raw Layer]
        C --> D[Schema and Data Quality Checks]
        D --> E[Transformation]
        E --> F[Processed Layer]
        F --> G[Curated Layer]
        G --> H[Dimensional Modeling]
        H --> I[Apache Iceberg Warehouse]
        I --> J[Business Data Marts]
        J --> K[SQL Analytics]
        I --> L[AI Query Engine]
        L --> M[Local LLM and RAG]
        K --> N[Streamlit Dashboards]
        M --> N
        B --> O[Ingestion Metadata]
        D --> P[Data Quality Reports]
        O --> Q[Monitoring Dashboard]
        P --> Q
        R[Airflow Scheduler] --> B
        R --> D
        R --> E

The diagram represents the overall design. The exact execution path
depends on which pipeline and dashboard modules are enabled in the local
configuration.


5. Prerequisites

Install the following software before running the project.

-   Windows 10/11 or another supported operating system.
-   Python version compatible with the installed project dependencies.
-   Git, if cloning the repository.
-   A terminal such as PowerShell or Command Prompt.
-   Access to the project’s retail source data.
-   Apache Airflow, if running Airflow orchestration.
-   Ollama and a compatible local model, if running AI-powered queries.
-   AWS credentials and permissions, only if using AWS services.

Check the installed versions:

    python --version
    python -m pip --version
    git --version

If Python is not recognized, install Python and ensure it is added to
the system PATH.

6. Installation and Setup

Step 1: Get the source code

If the project is already available locally, navigate to its root
directory:

    cd D:\airflow_etl

Alternatively, clone the repository:

    git clone https://github.com/vaibhaydanao-commits/airflow_etl.git
    cd airflow_etl

If the GitHub repository URL differs, use the repository’s actual URL.

Step 2: Create a virtual environment

    python -m venv .venv

Activate it:

    .\.venv\Scripts\Activate.ps1

If PowerShell blocks activation, use Command Prompt:

    .venv\Scripts\activate.bat

To deactivate the environment:

    deactivate

Step 3: Install dependencies

Upgrade pip:

    python -m pip install --upgrade pip

Install the project requirements:

    python -m pip install -r requirements.txt



Use the actual requirements file belonging to the dashboard you want to
run.

Step 4: Configure environment variables

Create a .env file in the directory expected by the application.

Example configuration:

    # Local project configuration
    PROJECT_ROOT=D:/airflow_etl

    # Apache Iceberg configuration
    ICEBERG_CATALOG_NAME=dimensional_model
    ICEBERG_CATALOG_DB=D:/airflow_etl/Warehouse/dimensional_model_catalog.db
    ICEBERG_WAREHOUSE=D:/airflow_etl/Warehouse/dimensional_model
    ICEBERG_NAMESPACE=star_schema

    # AI configuration, if enabled
    OLLAMA_BASE_URL=http://localhost:11434/v1
    OLLAMA_MODEL=llama3.2

    # Application configuration
    MAX_RESULT_ROWS=500

This is a sample configuration. Retain the variable names and values
expected by your existing code, and add any pipeline-specific settings
that are required.

Do not commit passwords, API keys, access tokens, or other secrets to
GitHub.

Step 5: Verify the project structure

The following is a logical overview of the project. Some folders may
have different names or may not yet exist in your checkout.

    airflow_etl/
    ├── dashboard/
    ├── raw/
    ├── processed/
    ├── quarantine/
    ├── curated/
    ├── Warehouse/
    │   ├── dimensional_model_catalog.db
    │   └── dimensional_model/
    ├── src/
    │   ├── glue_job/
    │   ├── lambda_function/
    │   ├── test/
    │   ├── warehouse/
    │   ├── config.py
    │   ├── create_bucket_and_Data_Lake_On_S3.py
    │   ├── data_to_s3.py
    │   └── extract_source_to_data.py
    ├── dags/
    │   ├── airflow_etl_dag.py
    │   ├── file_watcher.py
    │   ├── local_etl_dag.py
    ├── monitoring_dashboard/
    ├── ai_iceberg_dashboard/
    ├── logs/
    ├── reports/
    ├── run_pipeline.py
    ├── requirements.txt
    ├── .venv
    ├── .gitignore
    └── README.txt

Keep the README aligned with the actual folder names and entry points in
the repository.

7. Configuration

Local paths

The current local warehouse configuration uses the following paths:

  -------------------------------------------------------------------------------------------
  Setting                             Example
  ----------------------------------- -------------------------------------------------------
  Project root                        D:\airflow_etl

  Catalog database                    D:\airflow_etl\Warehouse\dimensional_model_catalog.db

  Warehouse directory                 D:\airflow_etl\Warehouse\dimensional_model

  Namespace                           star_schema

  Catalog name                        dimensional_model
  -------------------------------------------------------------------------------------------

Use absolute paths when required by your configuration, and ensure the
directories are accessible to the application.

Configuration checklist

Before running the pipeline:

-   Confirm the raw source files are available.
-   Confirm the configured directories exist or can be created.
-   Confirm the Iceberg catalog and warehouse paths are correct.
-   Confirm required environment variables are loaded.
-   Confirm the active virtual environment contains the required
    packages.
-   Confirm that any external services, such as Ollama or AWS, are
    accessible when needed.

Do not recreate or overwrite an existing warehouse simply to resolve a
connection error.

8. Understanding the Data

The project uses retail data such as orders, customers, products,
stores, and dates.

Orders

Typical fields include:

    order_id
    customer_id
    product_id
    order_date
    quantity
    unit_price
    payment_method
    order_status
    store_id

Customers

Typical fields include:

    customer_id
    customer_name
    email
    city
    state
    signup_date
    customer_segment

Products

Typical fields include:

    product_id
    product_name
    category
    price
    supplier

Data flow

1.  Read source files from the raw layer.
2.  Validate schema and mandatory fields.
3.  Apply data quality checks.
4.  Transform and standardize the source data.
5.  Write validated outputs to the appropriate downstream layer.
6.  Load or refresh the required warehouse tables.
7.  Run analytical queries against the curated or warehouse layer.

Field names can vary between source files and curated tables. Confirm
the schemas before executing transformations.

9. Running the Data Pipeline

Run the pipeline entry point

From the project root:

    cd D:\airflow_etl
    .\.venv\Scripts\Activate.ps1
    python run_pipeline.py

Use this command if run_pipeline.py is the configured entry point in
your checkout.

If your pipeline uses a different script, run that script instead. The
command above should not be assumed to execute every optional module
automatically.

Recommended execution sequence

For a full local run, the intended sequence is:

1.  Verify input files.
2.  Execute ingestion.
3.  Run schema validation.
4.  Execute data quality checks.
5.  Transform the data.
6.  Write processed and curated outputs.
7.  Update the dimensional warehouse where configured.
8.  Generate ingestion and processing metadata.
9.  Refresh analytical outputs or data marts.
10. Review logs, reports, and dashboard results.

Verify the output

After execution, check:

-   Whether the expected output files or tables exist.
-   Whether record counts match expectations.
-   Whether invalid records have been identified.
-   Whether the processing status is correct.
-   Whether errors are captured in logs.
-   Whether the resulting warehouse tables are readable.

A successful script exit alone does not guarantee that every downstream
dataset is correct.

10. Data Quality and Validation

The data quality layer helps prevent invalid records from entering
downstream analytical datasets.

Depending on the validation module, checks may include:

-   Required-column validation.
-   Schema and data-type validation.
-   Null checks on mandatory fields.
-   Duplicate detection using business keys.
-   Accepted-value validation.
-   Record-count reconciliation.
-   Invalid-record identification.
-   Processing failure reporting.

Example validation fields for orders include order_id, customer_id,
product_id, quantity, and order_status.

Example accepted order statuses:

    CANCELLED
    SHIPPED
    DELIVERED

Use the values and capitalization defined by the actual source schema
and validation rules.

Expected outputs

Data quality execution should make it possible to determine:

-   Total records processed.
-   Valid records.
-   Rejected records.
-   Validation failures.
-   Pipeline status.
-   Processing duration.

For reliable reconciliation, confirm that the reported valid and
rejected counts follow the same counting rules and that duplicates or
discarded records are handled consistently.

11. Dimensional Warehouse and Apache Iceberg

The project uses a retail dimensional model to support analytical
queries.

Star schema

The model includes a central sales fact table and supporting dimensions.

  Table          Purpose
  -------------- -----------------------------------
  fact_sales     Sales measures and dimension keys
  dim_customer   Customer attributes
  dim_product    Product attributes
  dim_store      Store attributes
  dim_date       Calendar and date attributes

Measures

Common measures include:

    SUM(fact_sales.sales_amount) AS total_revenue,
    SUM(fact_sales.quantity) AS total_quantity

Example analytical query

The following is an illustrative query. Adapt table names and date
columns to the SQL engine and registered schemas.

    SELECT
        d.year,
        d.month,
        SUM(f.sales_amount) AS total_revenue,
        SUM(f.quantity) AS total_quantity
    FROM fact_sales AS f
    JOIN dim_date AS d
        ON f.date_key = d.date_key
    GROUP BY
        d.year,
        d.month
    ORDER BY
        d.year,
        d.month;

Important considerations

-   Join fact and dimension tables using their defined keys.
-   Use dimension tables for product, customer, store, and date
    attributes.
-   Confirm the SQL engine has access to the Iceberg catalog.
-   Validate row counts and metric totals after transformations.
-   Avoid modifying warehouse data from read-only analytics dashboards.
-   Preserve existing catalog and warehouse paths.

The query above illustrates the model; its exact date column names
depend on the actual dim_date schema.

12. SCD Type 2 Processing

Slowly Changing Dimension Type 2 (SCD Type 2) maintains historical
versions of dimension records when tracked attributes change.

A typical SCD Type 2 dimension uses fields such as:

    business_key
    effective_from
    effective_to
    version
    is_current

The actual business key and attributes depend on the dimension.

Expected processing behavior

1.  Identify new records.
2.  Detect changes in tracked attributes.
3.  Expire the previous current record.
4.  Insert a new version for the changed record.
5.  Preserve the historical record.
6.  Maintain one current version per business key.

A far-future effective date such as 9999-12-31 may represent the current
version, depending on the implementation.

Validation

Check that:

-   Historical versions are preserved.
-   Current records have the expected effective dates.
-   Version numbers are consistent.
-   Only one record is current for each business key.
-   Unchanged records are not unnecessarily duplicated.

The exact SCD execution command should be documented alongside the
implemented SCD script.

13. Business Data Marts

The project aims to provide at least three curated analytical datasets.

Sales Summary

Typical output columns:

    date
    region
    category
    total_orders
    total_quantity
    total_revenue

Use cases:

-   Daily and monthly sales trends.
-   Regional performance.
-   Category-level revenue.
-   Sales volume analysis.

Customer Analytics

Typical output columns:

    customer_id
    total_orders
    total_spend
    average_order_value
    last_order_date
    customer_segment

Use cases:

-   Customer segmentation.
-   Repeat-purchase analysis.
-   Customer spending patterns.
-   Average order value analysis.

Product Analytics

Typical output columns:

    product_id
    product_name
    category
    total_orders
    total_quantity
    total_revenue

Use cases:

-   Best-selling products.
-   Category performance.
-   Product revenue comparisons.
-   Product demand analysis.

These schemas describe the target data marts. Confirm that the
corresponding Iceberg tables and build scripts have been implemented
before treating them as available outputs.

14. PySpark Optimization

The optimization exercise focuses on joining large retail datasets:

  Dataset            Example volume
  ----------- ---------------------
  Orders        100 million records
  Customers       5 million records
  Products          100,000 records

The purpose is to demonstrate scalable transformation strategies.

Optimization techniques

Broadcast joins

Broadcast a genuinely small dimension table when its estimated in-memory
size is safe for the executors.

    from pyspark.sql.functions import broadcast

    result = orders.join(
        broadcast(products),
        on="product_id",
        how="left"
    )

Do not assume that a table with a small row count is always safe to
broadcast; check its serialized size and executor memory.

Repartitioning

Use repartitioning when it improves distribution for expensive joins or
downstream processing. Avoid unnecessary repartition operations.

Shuffle reduction

Filter early, select only required columns, and use appropriate join
strategies to reduce data movement.

Caching

Cache data only when it is reused and the memory cost is justified.
Unpersist it when no longer required.

Partition pruning and predicate pushdown

Apply filters that can be pushed to the source and restrict reads to
relevant partitions where supported.

Avoid unnecessary collect()

Keep large datasets distributed. Use collect() only when the resulting
data is known to be small.

File sizing

Avoid excessive small files and oversized files. Choose output
partitioning and target file sizes according to the storage system and
workload.

Validation

Compare execution plans, shuffle volume, runtime, and output correctness
before and after optimization. The appropriate strategy depends on the
cluster, data distribution, and Spark configuration.

15. Data Lineage and Pipeline Monitoring

The monitoring layer records how source files move through the pipeline
and whether processing completes successfully.

Ingestion metadata

Typical metadata fields:

    file_name
    source
    ingestion_time
    record_count
    processing_status
    processing_time
    pipeline_version
    error_count

Monitoring metrics

-   Job execution status.
-   Start and end time.
-   Execution duration.
-   Input and output record counts.
-   Valid and rejected records.
-   Data quality failures.
-   Pipeline failures and error details.

Example report

    File Name       : orders_20260910.csv
    Records         : 125430
    Valid           : 124982
    Rejected        : 448
    Status          : SUCCESS
    Processed At    : 2026-09-10 03:45

The values above are illustrative.

A completed ingestion record should be associated with the correct
pipeline execution and source file. Failed runs should capture
diagnostic details where possible.

Launch the monitoring dashboard

If the monitoring application is implemented as dashboard/app.py, run:

    cd D:\airflow_etl\dashboard
    streamlit run app.py

Otherwise, use the actual monitoring dashboard entry point.

Open the local URL shown by Streamlit, usually:

    http://localhost:8501

Confirm the configured metadata source is populated before expecting
live monitoring results.

16. AI-Powered Retail Analytics

The AI analytics layer is intended to let business users ask questions
in natural language, generate read-only SQL, inspect query results, and
view relevant visualizations.

Intended workflow

1.  The user enters a retail business question.
2.  The query engine identifies relevant tables and columns.
3.  Context is retrieved from the configured schema or RAG components.
4.  A local language model generates SQL.
5.  The application validates the SQL.
6.  The SQL executes against the configured analytical engine.
7.  Results are displayed in a table, KPI, or relevant chart.
8.  The insight generator summarizes the results where enabled.

Example questions

    What is the total revenue by month?
    Which product categories generate the highest revenue?
    Which customer segments spend the most?
    Compare sales across stores.
    What is the monthly order volume?
    Which products have the highest sales quantity?

Components

Depending on the configured implementation, the AI application may use:

-   Streamlit for the user interface.
-   PyIceberg for reading warehouse tables.
-   DuckDB for local SQL execution.
-   Ollama for local language-model inference.
-   ChromaDB for vector retrieval.
-   SQLGlot for SQL parsing and validation.
-   A query engine for execution and result formatting.
-   A visualization module for selecting relevant charts.

Start Ollama

Install Ollama separately if it is not already installed, then pull the
model configured in .env.

For example:

    ollama pull llama3.2
    ollama list

Start the service if it is not already running:

    ollama serve

If Ollama is already running as a Windows service or background
application, do not start a second instance.

Check the configured endpoint and model name against the installed
model. Model availability and performance depend on system memory and
hardware.

Launch the AI dashboard

If your AI dashboard entry point is ai_iceberg_dashboard/dashboard.py,
use:

    cd D:\airflow_etl\ai_iceberg_dashboard
    streamlit run dashboard.py

If the implementation is located elsewhere, replace the directory and
filename with the actual entry point.

Open the local URL displayed by Streamlit.

Read-only and security requirements

The AI query engine should:

-   Permit only approved read-only SQL statements.
-   Reject write operations such as INSERT, UPDATE, DELETE, and DROP.
-   Restrict access to approved schemas and tables.
-   Limit query results.
-   Apply execution timeouts.
-   Validate generated SQL before execution.
-   Avoid exposing credentials in prompts, logs, or error messages.

SQL validation is a safety layer, not a replacement for database
permissions. Enforce read-only access at the database or query-engine
level as well.

The dashboard should read the existing Iceberg warehouse rather than
create, append, or overwrite warehouse tables as part of ordinary
analytics requests.

17. Airflow Orchestration

Where Airflow is configured, it can schedule and coordinate ingestion,
validation, transformation, and downstream processing.

Typical workflow

    Ingestion
        |
        v
    Schema Validation
        |
        v
    Data Quality Checks
        |
        v
    Transformation
        |
        v
    Curated Data
        |
        v
    Warehouse / Data Marts
        |
        v
    Metadata and Monitoring

Important Airflow concepts

-   DAGs define workflow dependencies.
-   Tasks execute pipeline steps.
-   Retries handle transient failures.
-   Logs help diagnose task errors.
-   Scheduling automates execution.
-   Idempotent tasks reduce duplicate processing during reruns.

Running Airflow

Airflow is not automatically available simply because the repository
contains a DAG file. It must be installed and configured for a supported
environment.

For an existing Airflow installation, use its configured scheduler,
webserver or API service, metadata database, and DAG directory.

To execute the project locally without Airflow, use the standalone
Python entry points that are actually implemented.

Do not assume that Windows-native Airflow installation is supported by
every Airflow version; use a compatible environment such as WSL2 or
Linux when required.

18. AWS Integration

The project includes or explores AWS-oriented data engineering
capabilities.

  AWS service             Intended use
  ----------------------- ------------------------------------------------
  Amazon S3               Object storage for raw and curated data
  AWS Glue                Managed ETL processing and catalog integration
  AWS Glue Data Catalog   Table and schema metadata
  Amazon Athena           SQL analytics
  Amazon Redshift         Data warehouse analytics
  AWS Lambda              Event-driven automation
  Amazon CloudWatch       Logs and monitoring
  Amazon RDS              Relational data sources
  AWS DMS                 Database migration and replication

These services require appropriate AWS configuration and permissions.
Their presence in the architecture does not mean that every workflow is
already deployed to AWS.

AWS checklist

Before running a cloud workflow:

1.  Configure the AWS CLI or the approved credential mechanism.
2.  Select the correct AWS account and region.
3.  Verify the IAM permissions required by the pipeline.
4.  Confirm S3 bucket and Glue catalog settings.
5.  Verify access to source and destination resources.
6.  Review CloudWatch logs or the relevant service logs.
7.  Validate output record counts and data quality.

Never hardcode AWS access keys in Python scripts or commit them to
source control.

19. Running Tests

Activate the virtual environment and navigate to the project root:

    cd D:\airflow_etl
    .\.venv\Scripts\Activate.ps1

Run the project’s unittest suite:

    python -m unittest discover -s tests -v

This command assumes that the project tests use Python’s unittest
discovery conventions.

Recommended testing coverage

  Test area           What to validate
  ------------------- ----------------------------------------------
  Ingestion           Correct file discovery and record counts
  Schema validation   Required columns and compatible data types
  Data quality        Nulls, duplicates, invalid values
  Transformations     Correct business logic and output schemas
  SCD Type 2          Historical versions and current-record rules
  Warehouse           Table access, keys, and metric consistency
  Data marts          Aggregations and expected output columns
  Monitoring          Metadata completeness and failure reporting
  AI SQL              Read-only enforcement and correct joins
  Visualizations      Appropriate rendering for result types

Tests that depend on AWS, Ollama, or an existing Iceberg catalog may
require additional configuration or fixtures.

20. Troubleshooting

Python or pip is not recognized

Verify the Python installation and PATH settings. Run:

    python --version
    python -m pip --version

Requirements installation fails

Upgrade pip, activate the correct virtual environment, and review the
dependency error. Some packages may require specific Python versions or
operating-system dependencies.

Iceberg catalog cannot be opened

Check:

-   The configured catalog path.
-   The warehouse directory.
-   PyIceberg dependencies and FileIO configuration.
-   Permissions for the catalog and warehouse.
-   The namespace and table identifiers.

Avoid creating a new catalog until you have confirmed the location of
the existing catalog and its metadata.

Namespace or tables are missing

Verify the catalog configuration, namespace, and registered table names.
Confirm that the dashboard and ETL pipeline use the same catalog
settings.

Streamlit dashboard does not start

Check the entry-point filename and directory, activate the correct
environment, and install the dashboard’s requirements.

Ollama is slow or fails to load a model

Check the model name, service status, available RAM, and the model’s
hardware requirements. Try a smaller compatible model if necessary.
Review the Ollama logs and the application’s timeout settings.

AI-generated SQL is incorrect

Verify the registered table schemas, join keys, metric definitions, and
query validation logic. Use schema context and representative test
questions to improve the prompt and SQL-generation workflow.

AWS returns AccessDenied

Verify the active AWS identity, region, IAM policies, and
resource-specific permissions. An installed AWS CLI or valid login does
not guarantee permission to create Glue jobs, scripts, catalog entries,
or S3 objects.

Duplicate records appear after reruns

Review task idempotency, business keys, merge logic, partition overwrite
behavior, and ingestion watermarks. A successful retry should not create
unintended duplicates.

21. Security and Best Practices

-   Keep secrets in environment variables or an approved secret manager.
-   Exclude .env, virtual environments, caches, generated data, and
    sensitive files from Git.
-   Use least-privilege IAM roles.
-   Keep analytical SQL read-only wherever possible.
-   Validate inputs and generated SQL.
-   Add structured logging and useful error messages.
-   Make pipeline tasks safe to retry.
-   Track data quality and record-count reconciliation.
-   Preserve historical data when implementing SCD Type 2.
-   Avoid loading entire datasets into driver memory.
-   Test transformations before running them on large datasets.
-   Document environment-specific configuration.
-   Keep dependencies reproducible and review package updates.

22. Future Enhancements

Potential improvements include:

-   Fully automated Airflow scheduling.
-   Production deployment of the ETL workflow to AWS.
-   Automated ingestion metadata capture for every run.
-   Enhanced data quality reporting and alerting.
-   Optimized Spark transformations for larger datasets.
-   Incremental processing and reliable watermarks.
-   Expanded Iceberg-based analytical data marts.
-   Improved local LLM performance and SQL accuracy.
-   Retrieval-augmented generation for business definitions and schema
    context.
-   Automated pipeline failure analysis.
-   Forecasting and anomaly detection.
-   CI/CD, automated tests, and deployment validation.

------------------------------------------------------------------------

Quick Start Checklist

Use this checklist when setting up the project for the first time.

-   ☐ Obtain the project source code.
-   ☐ Install a compatible Python version.
-   ☐ Create and activate .venv.
-   ☐ Install requirements.txt.
-   ☐ Configure .env.
-   ☐ Verify the raw input data.
-   ☐ Verify the Iceberg catalog and warehouse paths.
-   ☐ Run the implemented ETL entry point.
-   ☐ Review logs and data quality reports.
-   ☐ Run the unit tests.
-   ☐ Launch the relevant Streamlit dashboard.
-   ☐ Configure Ollama if using local AI analytics.
-   ☐ Configure Airflow or AWS separately if required.

Support and Contribution

When reporting an issue, include the relevant module, execution command,
error message, environment details, and expected behavior. Remove
credentials and sensitive business data before sharing logs.

Contributions should follow the existing project structure, include
appropriate tests, and document any new configuration or execution
requirements.

------------------------------------------------------------------------

Project: Retail Data Engineering & AI Analytics Platform
Primary language: Python
Analytics interface: Streamlit
Local warehouse: Apache Iceberg
Workflow orchestration: Python / Apache Airflow, depending on
configuration
Cloud integration: AWS services, where configured
