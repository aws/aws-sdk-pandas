---
id: 041-apache-spark-on-amazon-athena
title: "Apache Spark on Amazon Athena"
sidebar_position: 41
sidebar_label: "41 - Apache Spark on Amazon Athena"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/041%20-%20Apache%20Spark%20on%20Amazon%20Athena.ipynb
---

# Apache Spark on Amazon Athena

> This page is generated from [`tutorials/041 - Apache Spark on Amazon Athena.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/041%20-%20Apache%20Spark%20on%20Amazon%20Athena.ipynb). Open it in Jupyter to run it yourself.
Amazon Athena makes it easy to interactively run data analytics and exploration using Apache Spark without the need to plan for, configure, or manage resources. Running Apache Spark applications on Athena means submitting Spark code for processing and receiving the results directly without the need for additional configuration.

More in [User Guide](https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark.html).

### Run a Spark calculation

For this tutorial, you will need Spark-enabled Athena Workgroup. For the steps to create one, visit [Getting started with Apache Spark on Amazon Athena.
](https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-getting-started.html#notebooks-spark-getting-started-creating-a-spark-enabled-workgroup)

```python
import awswrangler as wr

workgroup: str = "my-spark-workgroup"

result = wr.athena.run_spark_calculation(
    code="print(spark)",
    workgroup=workgroup,
)
```

### Create and re-use a session

It is possible to create a session and re-use it launching multiple calculations with the same resources. To create a session, use:

```python
session_id: str = wr.athena.create_spark_session(
    workgroup=workgroup,
)
```

Now, to use the session, pass `session_id`:

```python
result = wr.athena.run_spark_calculation(
    code="print(spark)",
    workgroup=workgroup,
    session_id=session_id,
)
```
