.. _doc_usage_reading:

Reading
============

Reading from Data Lake to Pandas Dataframe:

.. code-block:: python

    df = awswrangler.athena.read("database", "select * from table")

S3 object to Pandas Dataframe:

.. code-block:: python

    for df in awswrangler.s3.read(path="s3://..."):
        print(df)
