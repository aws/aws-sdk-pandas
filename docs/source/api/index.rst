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
    read_parquet
    read_parquet_metadata
    size_objects
    store_parquet_metadata
    to_csv
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

Amazon Athena
-------------

.. currentmodule:: awswrangler.athena

.. autosummary::
    :toctree: stubs

    create_athena_bucket
    get_query_columns_types
    normalize_column_name
    normalize_table_name
    read_sql_query
    repair_table
    start_query_execution
    wait_query
