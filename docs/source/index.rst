.. AWS Data Wrangler documentation master file, created by
   sphinx-quickstart on Sun Aug 18 12:05:01 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. figure:: _static/logo.png
    :align: center
    :alt: alternate text
    :figclass: align-center

*DataFrames on AWS*

`Read the Tutorials <https://github.com/awslabs/aws-data-wrangler/tree/master/tutorials>`_:
    - `Catalog & Metadata <https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/catalog_and_metadata.ipynb>`_
    - `Athena Nested <https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/athena_nested.ipynb>`_
    - `S3 Write Modes <https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/s3_write_modes.ipynb>`_

|

Use Cases
---------

Pandas
``````

+------------------------+------------------+------------------------+
| FROM                   | TO               | Features               |
+========================+==================+========================+
| Pandas DataFrame       | Amazon S3        | Parquet, CSV,          |
|                        |                  | Partitions,            |
|                        |                  | Parallelism,           |
|                        |                  | Overwrite/Ap           |
|                        |                  | pend/Partitions-Upsert |
|                        |                  | modes,KMS Encryption,  |
|                        |                  | Glue Metadata (Athena, |
|                        |                  | Spectrum, Spark, Hive, |
|                        |                  | Presto)                |
+------------------------+------------------+------------------------+
| Amazon S3              | Pandas DataFrame | Parquet (Pushdown      |
|                        |                  | filters), CSV,         |
|                        |                  | Partitions,            |
|                        |                  | Parallelism,KMS        |
|                        |                  | Encryption, Multiple   |
|                        |                  | files                  |
+------------------------+------------------+------------------------+
| Amazon Athena          | Pandas DataFrame | Workgroups, S3 output  |
|                        |                  | path, Encryption, and  |
|                        |                  | two different          |
|                        |                  | engines:-              |
|                        |                  | ctas_approach=False    |
|                        |                  | **->** Batching and    |
|                        |                  | restrict memory        |
|                        |                  | environments-          |
|                        |                  | ctas_approach=True     |
|                        |                  | **->** Blazing fast,   |
|                        |                  | parallelism and        |
|                        |                  | enhanced data types    |
+------------------------+------------------+------------------------+
| Pandas DataFrame       | Amazon Redshift  | Blazing fast using     |
|                        |                  | parallel parquet on S3 |
|                        |                  | behind the             |
|                        |                  | scenesA                |
|                        |                  | ppend/Overwrite/Upsert |
|                        |                  | modes                  |
+------------------------+------------------+------------------------+
| Amazon Redshift        | Pandas DataFrame | Blazing fast using     |
|                        |                  | parallel parquet on S3 |
|                        |                  | behind the scenes      |
+------------------------+------------------+------------------------+
| Pandas DataFrame       | Amazon Aurora    | Supported engines:     |
|                        |                  | MySQL,                 |
|                        |                  | PostgreSQLBlazing fast |
|                        |                  | using parallel CSV on  |
|                        |                  | S3 behind the          |
|                        |                  | scenesAppend/Overwrite |
|                        |                  | modes                  |
+------------------------+------------------+------------------------+
| Amazon Aurora          | Pandas DataFrame | Supported engines:     |
|                        |                  | MySQLBlazing fast      |
|                        |                  | using parallel CSV on  |
|                        |                  | S3 behind the scenes   |
+------------------------+------------------+------------------------+
| CloudWatch Logs        | Pandas DataFrame | Query results          |
| Insights               |                  |                        |
+------------------------+------------------+------------------------+
| Glue Catalog           | Pandas DataFrame | List and get Tables    |
|                        |                  | details. Good fit with |
|                        |                  | Jupyter Notebooks.     |
+------------------------+------------------+------------------------+

PySpark
```````
+----------------------+----------------------+----------------------+
| FROM                 | TO                   | Features             |
+======================+======================+======================+
| PySpark DataFrame    | Amazon Redshift      | Blazing fast using   |
|                      |                      | parallel parquet on  |
|                      |                      | S3 behind the        |
|                      |                      | scenesApp            |
|                      |                      | end/Overwrite/Upsert |
|                      |                      | modes                |
+----------------------+----------------------+----------------------+
| PySpark DataFrame    | Glue Catalog         | Register Parquet or  |
|                      |                      | CSV DataFrame on     |
|                      |                      | Glue Catalog         |
+----------------------+----------------------+----------------------+
| Nested               | Flat                 | Flatten structs and  |
| PySparkDataFrame     | PySparkDataFrames    | break up arrays in   |
|                      |                      | child tables         |
+----------------------+----------------------+----------------------+

General
```````
+----------------------------------+----------------------------------+
| Feature                          | Details                          |
+==================================+==================================+
| List S3 objects                  | e.g.                             |
|                                  | wr.s3.list_objects("s3://...")   |
+----------------------------------+----------------------------------+
| Delete S3 objects                | Parallel                         |
+----------------------------------+----------------------------------+
| Delete listed S3 objects         | Parallel                         |
+----------------------------------+----------------------------------+
| Delete NOT listed S3 objects     | Parallel                         |
+----------------------------------+----------------------------------+
| Copy listed S3 objects           | Parallel                         |
+----------------------------------+----------------------------------+
| Get the size of S3 objects       | Parallel                         |
+----------------------------------+----------------------------------+
| Get CloudWatch Logs Insights     |                                  |
| query results                    |                                  |
+----------------------------------+----------------------------------+
| Load partitions on Athena/Glue   | Through "MSCK REPAIR TABLE"      |
| table                            |                                  |
+----------------------------------+----------------------------------+
| Create EMR cluster               | "For humans"                     |
+----------------------------------+----------------------------------+
| Terminate EMR cluster            | "For humans"                     |
+----------------------------------+----------------------------------+
| Get EMR cluster state            | "For humans"                     |
+----------------------------------+----------------------------------+
| Submit EMR step(s)               | "For humans"                     |
+----------------------------------+----------------------------------+
| Get EMR step state               | "For humans"                     |
+----------------------------------+----------------------------------+
| Query Athena to receive python   | Returns *Iterable[Dict[str,      |
| primitives                       | Any]*                            |
+----------------------------------+----------------------------------+
| Load and Unzip SageMaker jobs    |                                  |
| outputs                          |                                  |
+----------------------------------+----------------------------------+
| Dump Amazon Redshift as Parquet  |                                  |
| files on S3                      |                                  |
+----------------------------------+----------------------------------+
| Dump Amazon Aurora as CSV files  | Only for MySQL engine            |
| on S3                            |                                  |
+----------------------------------+----------------------------------+

|

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
