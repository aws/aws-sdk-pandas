.. _doc_usage_writing:

Writing
============

Writing Pandas Dataframe to Data Lake:

.. code-block:: python

    session = awswrangler.Session()
    dataframe = session.pandas.read_sql_athena(
        sql="select * from table",
        database="database"
    )
