# AWS Data Wrangler

> Utility belt to handle data on AWS.

[![Documentation Status](https://readthedocs.org/projects/aws-data-wrangler/badge/?version=latest)](https://aws-data-wrangler.readthedocs.io/en/latest/?badge=latest)

**[Read the documentation](https://aws-data-wrangler.readthedocs.io)**

---

*Contents:* **[Use Cases](#Use-Cases)** | **[Installation](#Installation)** | **[Examples](#Examples)** | **[Diving Deep](#Diving-Deep)**

---

## Use Cases

### Pandas
* Pandas -> Parquet (S3) (Parallel :rocket:)
* Pandas -> CSV (S3) (Parallel :rocket:)
* Pandas -> Glue Catalog
* Pandas -> Athena (Parallel :rocket:)
* Pandas -> Redshift (Parallel :rocket:)
* CSV (S3) -> Pandas (One shot or Batching)
* Athena -> Pandas (One shot or Batching)
* CloudWatch Logs Insights -> Pandas (NEW :star:)
* Encrypt Pandas Dataframes on S3 with KMS keys (NEW :star:)

### PySpark
* PySpark -> Redshift (Parallel :rocket:) (NEW :star:)

### General
* List S3 objects (Parallel :rocket:)
* Delete S3 objects (Parallel :rocket:)
* Delete listed S3 objects (Parallel :rocket:)
* Delete NOT listed S3 objects (Parallel :rocket:)
* Copy listed S3 objects (Parallel :rocket:)
* Get the size of S3 objects (Parallel :rocket:)
* Get CloudWatch Logs Insights query results (NEW :star:)

## Installation

`pip install awswrangler`

Runs only with Python 3.6 and beyond.

Runs anywhere (AWS Lambda, AWS Glue, EMR, EC2, on-premises, local, etc).

*P.S.* Lambda Layer bundle and Glue egg are available to [download](https://github.com/awslabs/aws-data-wrangler/releases). It's just upload to your account and run! :rocket:

## Examples

### Pandas

#### Writing Pandas Dataframe to S3 + Glue Catalog

```py3
session = awswrangler.Session()
session.pandas.to_parquet(
    dataframe=dataframe,
    database="database",
    path="s3://...",
    partition_cols=["col_name"],
)
```

If a Glue Database name is passed, all the metadata will be created in the Glue Catalog. If not, only the s3 data write will be done.

#### Writing Pandas Dataframe to S3 as Parquet encrypting with a KMS key

```py3
extra_args = {
    "ServerSideEncryption": "aws:kms",
    "SSEKMSKeyId": "YOUR_KMY_KEY_ARN"
}
session = awswrangler.Session(s3_additional_kwargs=extra_args)
session.pandas.to_parquet(
    path="s3://..."
)
```

#### Reading from AWS Athena to Pandas

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_sql_athena(
    sql="select * from table",
    database="database"
)
```

#### Reading from AWS Athena to Pandas in chunks (For memory restrictions)

```py3
session = awswrangler.Session()
dataframe_iter = session.pandas.read_sql_athena(
    sql="select * from table",
    database="database",
    max_result_size=512_000_000  # 512 MB
)
for dataframe in dataframe_iter:
    print(dataframe)  # Do whatever you want
```

#### Reading from S3 (CSV) to Pandas

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_csv(path="s3://...")
```

#### Reading from S3 (CSV) to Pandas in chunks (For memory restrictions)

```py3
session = awswrangler.Session()
dataframe_iter = session.pandas.read_csv(
    path="s3://...",
    max_result_size=512_000_000  # 512 MB
)
for dataframe in dataframe_iter:
    print(dataframe)  # Do whatever you want
```

#### Reading from CloudWatch Logs Insights to Pandas

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_log_query(
    log_group_names=[LOG_GROUP_NAME],
    query="fields @timestamp, @message | sort @timestamp desc | limit 5",
)
```

#### Typical Pandas ETL

```py3
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
```

### PySpark

#### Loading PySpark Dataframe to Redshift

```py3
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
```

### General

#### Deleting a bunch of S3 objects (parallel :rocket:)

```py3
session = awswrangler.Session()
session.s3.delete_objects(path="s3://...")
```

#### Get CloudWatch Logs Insights query results

```py3
session = awswrangler.Session()
results = session.cloudwatchlogs.query(
    log_group_names=[LOG_GROUP_NAME],
    query="fields @timestamp, @message | sort @timestamp desc | limit 5",
)
```

## Diving Deep

### Pandas to Redshift Flow

![Pandas to Redshift Flow](docs/source/_static/pandas-to-redshift-flow.jpg?raw=true "Pandas to Redshift Flow")

### Spark to Redshift Flow

![Spark to Redshift Flow](docs/source/_static/spark-to-redshift-flow.jpg?raw=true "Spark to Redshift Flow")
