.. note:: Due the new major version 1.0.0 with breaking changes, please make sure that all your old projects has dependencies frozen on the desired version (e.g. `pip install awswrangler==0.3.2`). You can always check the legacy docs `here <https://aws-data-wrangler.readthedocs.io/en/legacy/>`_.

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
    create_parquet_table
    databases
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
