.. AWS Data Wrangler documentation master file, created by
   sphinx-quickstart on Sun Aug 18 12:05:01 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. figure:: _static/logo.png
    :align: center
    :alt: alternate text
    :figclass: align-center

*Utility belt to handle data on AWS.*

Use Cases
---------

Pandas
``````
* Pandas -> Parquet (S3) (Parallel)
* Pandas -> CSV (S3) (Parallel)
* Pandas -> Glue Catalog Table
* Pandas -> Athena (Parallel)
* Pandas -> Redshift (Append/Overwrite/Upsert) (Parallel)
* Pandas -> Aurora (MySQL/PostgreSQL) (Append/Overwrite) (Via S3) (NEW)
* Parquet (S3) -> Pandas (Parallel)
* CSV (S3) -> Pandas (One shot or Batching)
* Glue Catalog Table -> Pandas (Parallel)
* Athena -> Pandas (One shot, Batching or Parallel)
* Redshift -> Pandas (Parallel)
* CloudWatch Logs Insights -> Pandas
* Aurora -> Pandas (MySQL) (Via S3) (NEW)
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
* Create EMR cluster (For humans)
* Terminate EMR cluster
* Get EMR cluster state
* Submit EMR step(s) (For humans)
* Get EMR step state
* Get EMR step state
* Athena query to receive the result as python primitives (*Iterable[Dict[str, Any]*)
* Load and Unzip SageMaker jobs outputs
* Load and Unzip SageMaker models
* Redshift -> Parquet (S3)
* Aurora -> CSV (S3) (MySQL) (NEW :star:)


Table Of Contents
-----------------

.. toctree::
   :maxdepth: 4

   installation
   examples
   divingdeep
   stepbystep
   contributing
   api/modules
   license
