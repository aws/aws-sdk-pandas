.. note:: We just released a new major version `1.0` with breaking changes. Please make sure that all your old projects has the dependencies frozen on the desired version (e.g. `pip install awswrangler==0.3.2`).

API Reference
=============

Amazon S3
---------

.. currentmodule:: awswrangler.s3

.. autosummary::
    :toctree: stubs

    delete_objects
    describe_objects
    does_object_exist
    get_bucket_region
    list_objects
    read_csv
    read_fwf
    read_json
    read_parquet
    read_parquet_table
    read_parquet_metadata
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

    add_parquet_partitions
    create_parquet_table
    databases
    delete_table_if_exists
    does_table_exist
    get_databases
    get_parquet_partitions
    get_table_location
    get_table_types
    get_tables
    search_tables
    table
    tables
    sanitize_column_name
    sanitize_dataframe_columns_names
    sanitize_table_name
    drop_duplicated_columns
    get_engine

Amazon Athena
-------------

.. currentmodule:: awswrangler.athena

.. autosummary::
    :toctree: stubs

    read_sql_query
    read_sql_table
    repair_table
    start_query_execution
    stop_query_execution
    wait_query
    create_athena_bucket
    get_query_columns_types
    get_work_group

Databases (Redshift, PostgreSQL, MySQL)
---------------------------------------

.. currentmodule:: awswrangler.db

.. autosummary::
    :toctree: stubs

    to_sql
    read_sql_query
    read_sql_table
    get_engine
    get_redshift_temp_engine
    copy_to_redshift
    copy_files_to_redshift
    unload_redshift
    unload_redshift_to_files
    write_redshift_copy_manifest

EMR
---

.. currentmodule:: awswrangler.emr

.. autosummary::
    :toctree: stubs

    create_cluster
    get_cluster_state
    terminate_cluster
    submit_step
    submit_steps
    build_step
    get_step_state

CloudWatch Logs
---------------

.. currentmodule:: awswrangler.cloudwatch

.. autosummary::
    :toctree: stubs

    read_logs
    run_query
    start_query
    wait_query
