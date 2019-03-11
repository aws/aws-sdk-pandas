.. _doc_usage_writing:

Writing
============

Writing Pandas Dataframe to Data Lake:

.. code-block:: python

    awswrangler.s3.write(
            df=df,
            database="database",
            path="s3://...",
            file_format="parquet",
            preserve_index=True,
            mode="overwrite",
            partition_cols=["col"],
        )
