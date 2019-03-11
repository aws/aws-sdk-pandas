.. _doc_usage_reading:

Reading
============

Reading from Data Lake to Pandas Dataframe:

.. code-block:: python

    df = awswrangler.athena.read("database", "select * from table")

Reading from "infinite" S3 source to Pandas Dataframe through generators. That can set a maximum chunk size in bytes to fit in any memory size:

.. code-block:: python

    for df in awswrangler.s3.read(path="s3://...", max_size=500):
        print(df)
