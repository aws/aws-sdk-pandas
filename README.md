# AWS Data Wrangler (beta)

> Utility belt to handle data on AWS.

---

*Contents:* **[Use Cases](#Use-Cases)** | **[Installation](#Installation)** | **[Examples](#Examples)** | **[Diving Deep](#Diving Deep)**

---

## Use Cases

* Pandas -> Parquet (S3)
* Pandas -> CSV (S3)
* Pandas -> Glue Catalog
* Pandas -> Athena
* Pandas -> Redshift
* CSV (S3) -> Pandas
* Athena -> Pandas
* PySpark -> Redshift

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

### Reading from AWS Athena to Pandas

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_sql_athena(
    sql="select * from table",
    database="database"
)
```

### Reading from S3 (CSV) to Pandas

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_csv(path="s3://...")
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

##Diving Deep

### Pandas to Redshift Flow

![Pandas to Redshift Flow](docs/pandas_to_redshift/pandas-to-redshift-flow.jpg?raw=true "Pandas to Redshift Flow")
