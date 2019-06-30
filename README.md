# AWS Data Wrangler (BETA)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

> Utilities for Pandas and Apache Spark on AWS

AWS Data Wrangler aims to fill a gap between AWS Analytics Services (Glue, Athena, EMR, Redshift, S3) and the most popular Python data libraries ([Pandas](https://pandas.pydata.org/), [Apache Spark](https://spark.apache.org/)).

---

*Contents:* **[Use Cases](#Use-Cases)** | **[Installation](#Installation)** | **[Usage](#Usage)** | **[License](#License)**

---

## Use Cases

* Pandas Dataframe -> Parquet (S3)
* Pandas Dataframe -> CSV (S3)
* Pandas Dataframe -> Glue Catalog
* Pandas Dataframe -> Redshift
* Pandas Dataframe -> Athena
* CSV (S3) -> Pandas Dataframe
* Athena -> Pandas Dataframe
* Spark Dataframe -> Redshift

## Installation

`pip install awswrangler`

AWS Data Wrangler runs only Python 3.6 and beyond.
And runs on AWS Lambda, AWS Glue, EC2, on-premises, local, etc.

## Usage

### Writing Pandas Dataframe to Data Lake

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

### Reading from Data Lake to Pandas Dataframe

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_sql_athena(
    sql="select * from table",
    database="database"
)
```

### Reading from S3 file to Pandas Dataframe

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

### Loading Spark Dataframe to Redshift

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

## License

This library is licensed under the Apache 2.0 License.
