API Reference
=============

Amazon S3
---------

.. currentmodule:: awswrangler.s3

.. autosummary::
    :toctree: stubs

    get_bucket_region
    does_object_exist
    list_objects
    describe_objects
    size_objects
    delete_objects
    read_csv
    read_parquet
    read_parquet_metadata
    store_parquet_metadata
    to_csv
    to_parquet

AWS Glue Catalog
----------------

.. currentmodule:: awswrangler.catalog

.. autosummary::
    :toctree: stubs

    delete_table_if_exists
    does_table_exist
    create_parquet_table
    add_parquet_partitions
    get_parquet_partitions
    get_table_types
    get_databases
    databases
    get_tables
    tables
    search_tables
    get_table_location
    table

Amazon Athena
-------------

.. currentmodule:: awswrangler.athena

.. autosummary::
    :toctree: stubs

    read_sql_query
    repair_table
    wait_query
    start_query_execution
    normalize_column_name
    normalize_table_name
    create_athena_bucket
    get_query_columns_types
