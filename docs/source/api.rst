API Reference
=============

* `Amazon S3`_
* `AWS Glue Catalog`_
* `Amazon Athena`_
* `Amazon Redshift`_
* `PostgreSQL`_
* `MySQL`_
* `Microsoft SQL Server`_
* `DynamoDB`_
* `Amazon Timestream`_
* `Amazon EMR`_
* `Amazon CloudWatch Logs`_
* `Amazon QuickSight`_
* `AWS STS`_
* `AWS Secrets Manager`_
* `Global Configurations`_

Amazon S3
---------

.. currentmodule:: awswrangler.s3

.. autosummary::
    :toctree: stubs

    copy_objects
    delete_objects
    describe_objects
    does_object_exist
    download
    get_bucket_region
    list_directories
    list_objects
    merge_datasets
    merge_upsert_table
    read_csv
    read_excel
    read_fwf
    read_json
    read_parquet
    read_parquet_metadata
    read_parquet_table
    size_objects
    store_parquet_metadata
    to_csv
    to_excel
    to_json
    to_parquet
    upload
    wait_objects_exist
    wait_objects_not_exist

AWS Glue Catalog
----------------

.. currentmodule:: awswrangler.catalog

.. autosummary::
    :toctree: stubs

    add_column
    add_csv_partitions
    add_parquet_partitions
    create_csv_table
    create_database
    create_parquet_table
    databases
    delete_column
    delete_database
    delete_partitions
    delete_all_partitions
    delete_table_if_exists
    does_table_exist
    drop_duplicated_columns
    extract_athena_types
    get_columns_comments
    get_csv_partitions
    get_databases
    get_parquet_partitions
    get_partitions
    get_table_description
    get_table_location
    get_table_number_of_versions
    get_table_parameters
    get_table_types
    get_table_versions
    get_tables
    overwrite_table_parameters
    sanitize_column_name
    sanitize_dataframe_columns_names
    sanitize_table_name
    search_tables
    table
    tables
    upsert_table_parameters

Amazon Athena
-------------

.. currentmodule:: awswrangler.athena

.. autosummary::
    :toctree: stubs

    create_athena_bucket
    get_query_columns_types
    get_query_execution
    get_work_group
    read_sql_query
    read_sql_table
    repair_table
    start_query_execution
    stop_query_execution
    wait_query

Amazon Redshift
---------------

.. currentmodule:: awswrangler.redshift

.. autosummary::
    :toctree: stubs

    connect
    connect_temp
    copy
    copy_from_files
    read_sql_query
    read_sql_table
    to_sql
    unload
    unload_to_files

PostgreSQL
----------

.. currentmodule:: awswrangler.postgresql

.. autosummary::
    :toctree: stubs

    connect
    read_sql_query
    read_sql_table
    to_sql

MySQL
-----

.. currentmodule:: awswrangler.mysql

.. autosummary::
    :toctree: stubs

    connect
    read_sql_query
    read_sql_table
    to_sql

Microsoft SQL Server
____________________

.. currentmodule:: awswrangler.sqlserver

.. autosummary::
    :toctree: stubs

    connect
    read_sql_query
    read_sql_table
    to_sql

DynamoDB
--------

.. currentmodule:: awswrangler.dynamodb

.. autosummary::
    :toctree: stubs

    delete_items
    get_table
    put_csv
    put_df
    put_items
    put_json

Amazon Timestream
-----------------

.. currentmodule:: awswrangler.timestream

.. autosummary::
    :toctree: stubs

    create_database
    create_table
    delete_database
    delete_table
    query
    write

Amazon EMR
----------

.. currentmodule:: awswrangler.emr

.. autosummary::
    :toctree: stubs

    build_spark_step
    build_step
    create_cluster
    get_cluster_state
    get_step_state
    submit_ecr_credentials_refresh
    submit_spark_step
    submit_step
    submit_steps
    terminate_cluster

Amazon CloudWatch Logs
----------------------

.. currentmodule:: awswrangler.cloudwatch

.. autosummary::
    :toctree: stubs

    read_logs
    run_query
    start_query
    wait_query

Amazon QuickSight
-----------------

.. currentmodule:: awswrangler.quicksight

.. autosummary::
    :toctree: stubs

    cancel_ingestion
    create_athena_data_source
    create_athena_dataset
    create_ingestion
    delete_all_dashboards
    delete_all_data_sources
    delete_all_datasets
    delete_all_templates
    delete_dashboard
    delete_data_source
    delete_dataset
    delete_template
    describe_dashboard
    describe_data_source
    describe_data_source_permissions
    describe_dataset
    describe_ingestion
    get_dashboard_id
    get_dashboard_ids
    get_data_source_arn
    get_data_source_arns
    get_data_source_id
    get_data_source_ids
    get_dataset_id
    get_dataset_ids
    get_template_id
    get_template_ids
    list_dashboards
    list_data_sources
    list_datasets
    list_groups
    list_group_memberships
    list_iam_policy_assignments
    list_iam_policy_assignments_for_user
    list_ingestions
    list_templates
    list_users
    list_user_groups

AWS STS
-------

.. currentmodule:: awswrangler.sts

.. autosummary::
    :toctree: stubs

    get_account_id
    get_current_identity_arn
    get_current_identity_name

AWS Secrets Manager
-------------------

.. currentmodule:: awswrangler.secretsmanager

.. autosummary::
    :toctree: stubs

    get_secret
    get_secret_json

Amazon Chime
-------------------

.. currentmodule:: awswrangler.chime

.. autosummary::
    :toctree: stubs

    post_message

Global Configurations
---------------------

.. currentmodule:: awswrangler.config

.. autosummary::
    :toctree: stubs

    reset
    to_pandas
