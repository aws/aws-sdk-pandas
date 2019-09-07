.. _doc_examples:

Examples
========

Pandas
------

Writing Pandas Dataframe to S3 + Glue Catalog
`````````````````````````````````````````````

.. code-block:: python

    session = awswrangler.Session()
    session.pandas.to_parquet(
        dataframe=dataframe,
        database="database",
        path="s3://...",
        partition_cols=["col_name"],
    )


**P.S.** If a Glue Database name is passed, all the metadata will be created in the Glue Catalog. If not, only the s3 data write will be done.

Writing Pandas Dataframe to S3 as Parquet encrypting with a KMS key
```````````````````````````````````````````````````````````````````

.. code-block:: python

    extra_args = {
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "YOUR_KMY_KEY_ARN"
    }
    session = awswrangler.Session(s3_additional_kwargs=extra_args)
    session.pandas.to_parquet(
        path="s3://..."
    )


Reading from AWS Athena to Pandas
`````````````````````````````````

.. code-block:: python

    session = awswrangler.Session()
    dataframe = session.pandas.read_sql_athena(
        sql="select * from table",
        database="database"
    )


Reading from AWS Athena to Pandas in chunks (For memory restrictions)
`````````````````````````````````````````````````````````````````````

.. code-block:: python

    session = awswrangler.Session()
    dataframe_iter = session.pandas.read_sql_athena(
        sql="select * from table",
        database="database",
        max_result_size=512_000_000  # 512 MB
    )
    for dataframe in dataframe_iter:
        print(dataframe)  # Do whatever you want


Reading from S3 (CSV) to Pandas
```````````````````````````````

.. code-block:: python

    session = awswrangler.Session()
    dataframe = session.pandas.read_csv(path="s3://...")


Reading from S3 (CSV) to Pandas in chunks (For memory restrictions)
```````````````````````````````````````````````````````````````````

.. code-block:: python

    session = awswrangler.Session()
    dataframe_iter = session.pandas.read_csv(
        path="s3://...",
        max_result_size=512_000_000  # 512 MB
    )
    for dataframe in dataframe_iter:
        print(dataframe)  # Do whatever you want

Reading from CloudWatch Logs Insights to Pandas
```````````````````````````````````````````````

.. code-block:: python

    session = awswrangler.Session()
    dataframe = session.pandas.read_log_query(
        log_group_names=[LOG_GROUP_NAME],
        query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    )



Typical Pandas ETL
``````````````````

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


PySpark
-------

Loading Pyspark Dataframe to Redshift
`````````````````````````````````````

.. code-block:: python

    session = awswrangler.Session(spark_session=spark)
    session.spark.to_redshift(
        dataframe=df,
        path="s3://...",
        connection=conn,
        schema="public",
        table="table",
        iam_role="IAM_ROLE_ARN",
        mode="append",
    )

General
-------

Deleting a bunch of S3 objects
``````````````````````````````

.. code-block:: python

    session = awswrangler.Session()
    session.s3.delete_objects(path="s3://...")

Get CloudWatch Logs Insights query results
``````````````````````````````````````````

.. code-block:: python

    session = awswrangler.Session()
    results = session.cloudwatchlogs.query(
        log_group_names=[LOG_GROUP_NAME],
        query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    )
