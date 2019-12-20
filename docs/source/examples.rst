.. _doc_examples:

Examples
========

Pandas
------

Writing Pandas Dataframe to S3 + Glue Catalog
`````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    wr.pandas.to_parquet(
        dataframe=df,
        database="database",
        path="s3://...",
        partition_cols=["col_name"],
    )


**P.S.** If a Glue Database name is passed, all the metadata will be created in the Glue Catalog. If not, only the s3 data write will be done.

Writing Pandas Dataframe to S3 as Parquet encrypting with a KMS key
```````````````````````````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    extra_args = {
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "YOUR_KMY_KEY_ARN"
    }
    sess = wr.Session(s3_additional_kwargs=extra_args)
    sess.pandas.to_parquet(
        path="s3://..."
    )


Reading from AWS Athena to Pandas
`````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    df = wr.pandas.read_sql_athena(
        sql="select * from table",
        database="database"
    )


Reading from AWS Athena to Pandas in chunks (For memory restrictions)
`````````````````````````````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    df_iter = wr.pandas.read_sql_athena(
        sql="select * from table",
        database="database",
        max_result_size=512_000_000  # 512 MB
    )

    for df in df_iter:
        print(df)  # Do whatever you want


Reading from AWS Athena to Pandas with the blazing fast CTAS approach
`````````````````````````````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    sess = wr.Session(athena_ctas_approach=True)
    df = sess.pandas.read_sql_athena(
        sql="select * from table",
        database="database"
    )


Reading from S3 (CSV) to Pandas
```````````````````````````````

.. code-block:: python

    import awswrangler as wr

    df = wr.pandas.read_csv(path="s3://...")


Reading from S3 (CSV) to Pandas in chunks (For memory restrictions)
```````````````````````````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    df_iter = wr.pandas.read_csv(
        path="s3://...",
        max_result_size=512_000_000  # 512 MB
    )

    for df in df_iter:
        print(df)  # Do whatever you want

Reading from CloudWatch Logs Insights to Pandas
```````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    df = wr.pandas.read_log_query(
        log_group_names=[LOG_GROUP_NAME],
        query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    )


Typical Pandas ETL
``````````````````

.. code-block:: python

    import pandas
    import awswrangler as wr

    df = pandas.read_...  # Read from anywhere

    # Typical Pandas, Numpy or Pyarrow transformation HERE!

    wr.pandas.to_parquet(  # Storing the data and metadata to Data Lake
        dataframe=df,
        database="database",
        path="s3://...",
        partition_cols=["col_name"],
    )


Loading Pandas Dataframe to Redshift
````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    wr.pandas.to_redshift(
        dataframe=df,
        path="s3://temp_path",
        schema="...",
        table="...",
        connection=con,
        iam_role="YOUR_ROLE_ARN",
        mode="overwrite",
        preserve_index=False,
    )


Extract Redshift query to Pandas DataFrame
``````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    df = wr.pandas.read_sql_redshift(
        sql="SELECT ...",
        iam_role="YOUR_ROLE_ARN",
        connection=con,
        temp_s3_path="s3://temp_path")


PySpark
-------

Loading Pyspark Dataframe to Redshift
`````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    wr.spark.to_redshift(
        dataframe=df,
        path="s3://...",
        connection=conn,
        schema="public",
        table="table",
        iam_role="IAM_ROLE_ARN",
        mode="append",
    )

Register Glue table from Dataframe stored on S3
```````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    dataframe.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy(["year", "month"]) \
            .save(compression="gzip", path="s3://...")
    sess = wr.Session(spark_session=spark)
    sess.spark.create_glue_table(
        dataframe=dataframe,
        file_format="parquet",
        partition_by=["year", "month"],
        path="s3://...",
        compression="gzip",
        database="my_database")

Flatten nested PySpark DataFrame
```````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    sess = awswrangler.Session(spark_session=spark)
    dfs = sess.spark.flatten(dataframe=df_nested)
    for name, df_flat in dfs.items():
        print(name)
        df_flat.show()

General
-------

Deleting a bunch of S3 objects
``````````````````````````````

.. code-block:: python

    import awswrangler as wr

    wr.s3.delete_objects(path="s3://...")

Get CloudWatch Logs Insights query results
``````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    results = wr.cloudwatchlogs.query(
        log_group_names=[LOG_GROUP_NAME],
        query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    )

Load partitions on Athena/Glue table (repair table)
```````````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    wr.athena.repair_table(database="db_name", table="tbl_name")

Create EMR cluster
```````````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    cluster_id = wr.emr.create_cluster(
        cluster_name="wrangler_cluster",
        logging_s3_path=f"s3://BUCKET_NAME/emr-logs/",
        emr_release="emr-5.28.0",
        subnet_id="SUBNET_ID",
        emr_ec2_role="EMR_EC2_DefaultRole",
        emr_role="EMR_DefaultRole",
        instance_type_master="m5.xlarge",
        instance_type_core="m5.xlarge",
        instance_type_task="m5.xlarge",
        instance_ebs_size_master=50,
        instance_ebs_size_core=50,
        instance_ebs_size_task=50,
        instance_num_on_demand_master=1,
        instance_num_on_demand_core=1,
        instance_num_on_demand_task=1,
        instance_num_spot_master=0,
        instance_num_spot_core=1,
        instance_num_spot_task=1,
        spot_bid_percentage_of_on_demand_master=100,
        spot_bid_percentage_of_on_demand_core=100,
        spot_bid_percentage_of_on_demand_task=100,
        spot_provisioning_timeout_master=5,
        spot_provisioning_timeout_core=5,
        spot_provisioning_timeout_task=5,
        spot_timeout_to_on_demand_master=True,
        spot_timeout_to_on_demand_core=True,
        spot_timeout_to_on_demand_task=True,
        python3=True,
        spark_glue_catalog=True,
        hive_glue_catalog=True,
        presto_glue_catalog=True,
        bootstraps_paths=None,
        debugging=True,
        applications=["Hadoop", "Spark", "Ganglia", "Hive"],
        visible_to_all_users=True,
        key_pair_name=None,
        spark_jars_path=[f"s3://...jar"],
        maximize_resource_allocation=True,
        keep_cluster_alive_when_no_steps=True,
        termination_protected=False,
        spark_pyarrow=True,
        tags={
            "foo": "boo"
        }
    )
    print(cluster_id)

Athena query to receive the result as python primitives (Iterable[Dict[str, Any])
`````````````````````````````````````````````````````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    for row in wr.athena.query(query="...", database="..."):
        print(row)

Load and unzip SageMaker job output
```````````````````````````````````

.. code-block:: python

    import awswrangler as wr

    outputs = wr.sagemaker.get_job_outputs("s3://...")

