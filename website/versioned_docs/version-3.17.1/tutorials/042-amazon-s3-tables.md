---
id: 042-amazon-s3-tables
title: "Amazon S3 Tables"
sidebar_position: 42
sidebar_label: "42 - Amazon S3 Tables"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/042%20-%20Amazon%20S3%20Tables.ipynb
---

# Amazon S3 Tables

> This page is generated from [`tutorials/042 - Amazon S3 Tables.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/042%20-%20Amazon%20S3%20Tables.ipynb). Open it in Jupyter to run it yourself.
[Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) provide analytics-optimized tabular storage using Apache Iceberg format. S3 Tables introduce **table buckets**, **namespaces**, and **tables**.

AWS SDK for pandas supports S3 Tables through the `wr.s3` module. Read and write operations require the `pyiceberg` optional dependency:

```
pip install awswrangler[pyiceberg]
```

```python
! pip install awswrangler[pyiceberg]
```

```python
import getpass

import pandas as pd

import awswrangler as wr
```

```python
bucket_name = getpass.getpass("Enter a table bucket name:")
```

## Creating resources

### Create a Table Bucket

```python
bucket_arn = wr.s3.create_table_bucket(name=bucket_name)
print(f"Table bucket ARN: {bucket_arn}")
```

### Create a Namespace

```python
namespace = "tutorial"

wr.s3.create_namespace(
    table_bucket_arn=bucket_arn,
    namespace=namespace,
)
```

## Write

### Writing a DataFrame

`to_iceberg` automatically creates the table if it does not exist.

```python
df = pd.DataFrame(
    {
        "order_id": [1, 2, 3],
        "amount": [10.50, 20.00, 15.75],
        "region": ["us", "eu", "us"],
    }
)

wr.s3.to_iceberg(
    df=df,
    table_bucket_arn=bucket_arn,
    namespace=namespace,
    table_name="orders",
)
```

### Appending data

```python
df_new = pd.DataFrame(
    {
        "order_id": [4, 5],
        "amount": [30.00, 12.25],
        "region": ["eu", "us"],
    }
)

wr.s3.to_iceberg(
    df=df_new,
    table_bucket_arn=bucket_arn,
    namespace=namespace,
    table_name="orders",
    mode="append",
)
```

### Overwriting data

```python
df_replace = pd.DataFrame(
    {
        "order_id": [100, 200],
        "amount": [99.99, 49.99],
        "region": ["ap", "ap"],
    }
)

wr.s3.to_iceberg(
    df=df_replace,
    table_bucket_arn=bucket_arn,
    namespace=namespace,
    table_name="orders",
    mode="overwrite",
)
```

## Read

### Read entire table

```python
df = wr.s3.from_iceberg(
    table_bucket_arn=bucket_arn,
    namespace=namespace,
    table_name="orders",
)
df
```

### Column selection and row filtering

```python
df = wr.s3.from_iceberg(
    table_bucket_arn=bucket_arn,
    namespace=namespace,
    table_name="orders",
    columns=["order_id", "amount"],
    row_filter="amount > 50.0",
)
df
```

### Limiting rows

```python
df = wr.s3.from_iceberg(
    table_bucket_arn=bucket_arn,
    namespace=namespace,
    table_name="orders",
    limit=1,
)
df
```

## Using the AWS Glue Iceberg REST endpoint

By default, read and write operations use the S3 Tables REST endpoint. To use the [AWS Glue Iceberg REST endpoint](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue-endpoint.html) instead, set `wr.config.s3tables_catalog_endpoint_url`. This enables integration with services that work through the Glue Data Catalog (e.g. Amazon Athena, Amazon Redshift).

### Prerequisites

Before using the Glue endpoint, your table bucket must be [integrated with the AWS Glue Data Catalog](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html). This requires:

1. **An IAM role for Lake Formation** with `s3tables:*` permissions and a trust policy allowing `lakeformation.amazonaws.com` to assume it.
2. **A Lake Formation resource registration** for `arn:aws:s3tables:<region>:<account>:bucket/*` with `WithFederation=True` and `HybridAccessEnabled=True`.
3. **A Glue federated catalog** named `s3tablescatalog` linked to S3 Tables via the `aws:s3tables` connection.
4. **Lake Formation permissions** granting the caller access to the catalog, databases, and tables.

For step-by-step instructions, see [Integrating S3 Tables with AWS analytics services](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html).

```python
# Point read/write at the Glue Iceberg REST endpoint
wr.config.s3tables_catalog_endpoint_url = "https://glue.<region>.amazonaws.com/iceberg"

df = wr.s3.from_iceberg(
    table_bucket_arn=bucket_arn,
    namespace=namespace,
    table_name="orders",
)

# Reset to default (S3 Tables endpoint)
wr.config.s3tables_catalog_endpoint_url = None
```

## Deleting resources

```python
wr.s3.delete_table(
    table_bucket_arn=bucket_arn,
    namespace=namespace,
    table_name="orders",
)
wr.s3.delete_namespace(
    table_bucket_arn=bucket_arn,
    namespace=namespace,
)
wr.s3.delete_table_bucket(
    table_bucket_arn=bucket_arn,
)
```
