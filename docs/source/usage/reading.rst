.. _doc_usage_reading:

Reading
============

Reading from Data Lake to Pandas Dataframe:

.. code-block:: python

    session = awswrangler.Session()
    dataframe = session.pandas.read_sql_athena(
        sql="select * from table",
        database="database"
    )

S3 object to Pandas Dataframe:

.. code-block:: python

    session = awswrangler.Session()
    dataframe = session.pandas.read_csv(path="s3://...")
