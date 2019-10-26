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
* CloudWatch Logs Insights -> Pandas
* Encrypt Pandas Dataframes on S3 with KMS keys

PySpark
```````
* PySpark -> Redshift (Parallel)
* Register Glue table from Dataframe stored on S3
* Flatten nested DataFrames (NEW)

General
```````
* List S3 objects (Parallel)
* Delete S3 objects (Parallel)
* Delete listed S3 objects (Parallel)
* Delete NOT listed S3 objects (Parallel)
* Copy listed S3 objects (Parallel)
* Get the size of S3 objects (Parallel)
* Get CloudWatch Logs Insights query results
* Load partitions on Athena/Glue table (repair table)
* Create EMR cluster (For humans) (NEW)
* Terminate EMR cluster (NEW)
* Get EMR cluster state (NEW)
* Submit EMR step(s) (For humans) (NEW)
* Get EMR step state (NEW)
* Athena query to receive the result as python primitives (Iterable[Dict[str, Any]) (NEW)


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
