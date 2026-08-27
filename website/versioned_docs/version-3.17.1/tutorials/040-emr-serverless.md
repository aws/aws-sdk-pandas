---
id: 040-emr-serverless
title: "EMR Serverless"
sidebar_position: 40
sidebar_label: "40 - EMR Serverless"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/040%20-%20EMR%20Serverless.ipynb
---

# EMR Serverless

> This page is generated from [`tutorials/040 - EMR Serverless.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/040%20-%20EMR%20Serverless.ipynb). Open it in Jupyter to run it yourself.
Amazon EMR Serverless is a new deployment option for Amazon EMR. EMR Serverless provides a serverless runtime environment that simplifies the operation of analytics applications that use the latest open source frameworks, such as Apache Spark and Apache Hive. With EMR Serverless, you don’t have to configure, optimize, secure, or operate clusters to run applications with these frameworks.
More in [User Guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html).

## Spark

### Create a Spark application

```python
import awswrangler as wr

spark_application_id: str = wr.emr_serverless.create_application(
    name="my-spark-application",
    application_type="Spark",
    release_label="emr-6.10.0",
)
```

```text
/var/folders/_n/7dm3ff5d5fb01gjt6ms150km0000gs/T/ipykernel_11468/3968622978.py:3: SDKPandasExperimentalWarning: `create_application`: This API is experimental and may change in future AWS SDK for Pandas releases. 
  spark_application_id: str = wr.emr_serverless.create_application(
```

### Run a Spark job

```python
iam_role_arn = "arn:aws:iam::...:role/..."

wr.emr_serverless.run_job(
    application_id=spark_application_id,
    execution_role_arn=iam_role_arn,
    job_driver_args={
        "entryPoint": "/usr/lib/spark/examples/jars/spark-examples.jar",
        "entryPointArguments": ["1"],
        "sparkSubmitParameters": "--class org.apache.spark.examples.SparkPi --conf spark.executor.cores=4 --conf spark.executor.memory=20g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=1",
    },
    job_type="Spark",
)
```

## Hive

### Create a Hive application

```python
hive_application_id: str = wr.emr_serverless.create_application(
    name="my-hive-application",
    application_type="Hive",
    release_label="emr-6.10.0",
)
```

```text
/var/folders/_n/7dm3ff5d5fb01gjt6ms150km0000gs/T/ipykernel_11468/3826130602.py:1: SDKPandasExperimentalWarning: `create_application`: This API is experimental and may change in future AWS SDK for Pandas releases. 
  hive_application_id: str = wr.emr_serverless.create_application(
```

### Run a Hive job

```python
path = "s3://my-bucket/path"

wr.emr_serverless.run_job(
    application_id=hive_application_id,
    execution_role_arn="arn:aws:iam::...:role/...",
    job_driver_args={
        "query": f"{path}/hive-query.ql",
        "parameters": f"--hiveconf hive.exec.scratchdir={path}/scratch --hiveconf hive.metastore.warehouse.dir={path}/warehouse",
    },
    job_type="Hive",
)
```
