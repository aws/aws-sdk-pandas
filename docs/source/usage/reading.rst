.. _doc_usage_reading:

Reading
============

Reading from Data Lake to Pandas Dataframe:

.. code-block:: python

    df = awswrangler.athena.read("database", "select * from table")