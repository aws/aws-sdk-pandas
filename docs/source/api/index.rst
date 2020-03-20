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

Amazon Athena
-------------

.. currentmodule:: awswrangler.athena

.. autosummary::
     :toctree: stubs

     normalize_column_name
     normalize_table_name
