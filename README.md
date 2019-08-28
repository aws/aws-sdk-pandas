# AWS Data Wrangler (beta)

> Utility belt to handle data on AWS.

---

*Contents:* **[Use Cases](#Use-Cases)** | **[Installation](#Installation)** | **[Examples](#Examples)** | **[Diving Deep](#Diving-Deep)**

---

## Use Cases

* Pandas -> Parquet (S3)
* Pandas -> CSV (S3)
* Pandas -> Glue Catalog
* Pandas -> Athena
* Pandas -> Redshift
* CSV (S3) -> Pandas (One shot or Batching)
* Athena -> Pandas (One shot or Batching)
* PySpark -> Redshift
* Delete S3 objects (parallel :rocket:)
* Encrypt S3 data with KMS keys

## Installation

`pip install awswrangler`

Runs only with Python 3.6 and beyond.

Runs anywhere (AWS Lambda, AWS Glue, EMR, EC2, on-premises, local, etc).

*P.S.* Lambda Layer bundle and Glue egg are available to [download](https://github.com/awslabs/aws-data-wrangler/releases). It's just upload to your account and run! :rocket:

## Examples

### Writing Pandas Dataframe to S3 + Glue Catalog

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

### Writing Pandas Dataframe to S3 as Parquet encrypting with a KMS key

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

### Reading from AWS Athena to Pandas

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_sql_athena(
    sql="select * from table",
    database="database"
)
```

### Reading from AWS Athena to Pandas in chunks (For memory restrictions)

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

### Reading from S3 (CSV) to Pandas

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_csv(path="s3://...")
```

### Reading from S3 (CSV) to Pandas in chunks (For memory restrictions)

```py3
session = awswrangler.Session()
dataframe_iter = session.pandas.read_csv(
    path="s3://...",
    max_result_size=512_000_000  # 512 MB
)
for dataframe in dataframe_iter:
    print(dataframe)  # Do whatever you want
```

### Typical Pandas ETL

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

### Loading Pyspark Dataframe to Redshift

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

### Deleting a bunch of S3 objects

```py3
session = awswrangler.Session()
session.s3.delete_objects(path="s3://...")
```

## Diving Deep

### Pandas to Redshift Flow

![Pandas to Redshift Flow](docs/source/_static/pandas-to-redshift-flow.jpg?raw=true "Pandas to Redshift Flow")

### Spark to Redshift Flow

![Spark to Redshift Flow](docs/source/_static/spark-to-redshift-flow.jpg?raw=true "Spark to Redshift Flow")
