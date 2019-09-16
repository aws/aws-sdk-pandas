.. AWS Data Wrangler documentation master file, created by
   sphinx-quickstart on Sun Aug 18 12:05:01 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

AWS Data Wrangler
=============================================

*Utility belt to handle data on AWS.*

Use Cases
---------

Pandas
``````
* Pandas -> Parquet (S3) (Parallel)
* Pandas -> CSV (S3) (Parallel)
* Pandas -> Glue Catalog
* Pandas -> Athena (Parallel)
* Pandas -> Redshift (Parallel)
* CSV (S3) -> Pandas (One shot or Batching)
* Athena -> Pandas (One shot or Batching)
* CloudWatch Logs Insights -> Pandas (NEW)
* Encrypt Pandas Dataframes on S3 with KMS keys (NEW)

PySpark
```````
* PySpark -> Redshift (Parallel) (NEW)

General
```````
* List S3 objects (Parallel)
* Delete S3 objects (Parallel)
* Delete listed S3 objects (Parallel)
* Delete NOT listed S3 objects (Parallel)
* Copy listed S3 objects (Parallel)
* Get the size of S3 objects (Parallel)
* Get CloudWatch Logs Insights query results (NEW)


Table Of Contents
-----------------

.. toctree::
   :maxdepth: 4

   installation
   examples
   divingdeep
   contributing
   api/modules
   license
