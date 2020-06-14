API Reference
=============

Amazon S3
---------

.. currentmodule:: awswrangler.s3

.. autosummary::
    :toctree: stubs

    copy_objects
    delete_objects
    describe_objects
    does_object_exist
    get_bucket_region
    list_directories
    list_objects
    merge_datasets
    read_csv
    read_fwf
    read_json
    read_parquet
    read_parquet_metadata
    read_parquet_table
    size_objects
    store_parquet_metadata
    to_csv
    to_json
    to_parquet
    wait_objects_exist
    wait_objects_not_exist

AWS Glue Catalog
----------------

.. currentmodule:: awswrangler.catalog

.. autosummary::
    :toctree: stubs

    add_csv_partitions
    add_parquet_partitions
    create_csv_table
    create_database
    create_parquet_table
    databases
    delete_database
    delete_table_if_exists
    does_table_exist
    drop_duplicated_columns
    extract_athena_types
    get_columns_comments
    get_csv_partitions
    get_databases
    get_engine
    get_parquet_partitions
    get_table_description
    get_table_location
    get_table_parameters
    get_table_types
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
    get_work_group
    read_sql_query
    read_sql_table
    repair_table
    start_query_execution
    stop_query_execution
    wait_query

Databases (Redshift, PostgreSQL, MySQL)
---------------------------------------

.. currentmodule:: awswrangler.db

.. autosummary::
    :toctree: stubs

    copy_files_to_redshift
    copy_to_redshift
    get_engine
    get_redshift_temp_engine
    read_sql_query
    read_sql_table
    to_sql
    unload_redshift
    unload_redshift_to_files
    write_redshift_copy_manifest

EMR
---

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

CloudWatch Logs
---------------

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
