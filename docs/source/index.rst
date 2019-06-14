.. AWS Data Wrangler documentation master file, created by
   sphinx-quickstart on Sat Mar  9 10:02:49 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

AWS Data Wrangler
=============================================

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/ambv/black

*The missing link between AWS services and the most popular Python data libraries.*

.. warning::
    This project is in BETA version. And was not tested in battle yet.

AWS Data Wrangler aims to fill a gap between AWS Analytics Services (Glue, Athena, EMR, Redshift) and the most popular Python libraries for **lightweight** workloads.

Typical ETL
-----------

.. code-block:: python

   import pandas
   import awswrangler

   df = pandas.read_...  # Read from anywhere

   # Typical Pandas, Numpy or Pyarrow transformation HERE!

   session = awswrangler.Session()
   session.pandas.to_parquet(  # Storing the data and metadata to Data Lake
       dataframe=dataframe,
       database="database",
       path="s3://...",
       partition_cols=["col_name"],
   )


Table Of Contents
-----------------

.. toctree::
   :maxdepth: 3

   rationale
   benchmarks
   installation
   usage/index
   dependencies
   limitations
   api/modules
   contributing
   license