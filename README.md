# AWS Data Wrangler

> Utility belt to handle data on AWS.

[![Documentation Status](https://readthedocs.org/projects/aws-data-wrangler/badge/?version=latest)](https://aws-data-wrangler.readthedocs.io/en/latest/?badge=latest)

**[Read the documentation](https://aws-data-wrangler.readthedocs.io)**

---

*Contents:* **[Use Cases](#Use-Cases)** | **[Installation](#Installation)** | **[Examples](#Examples)** | **[Diving Deep](#Diving-Deep)** | **[Contributing](#Contributing)**

---

## Use Cases

### Pandas
* Pandas -> Parquet (S3) (Parallel)
* Pandas -> CSV (S3) (Parallel)
* Pandas -> Glue Catalog
* Pandas -> Athena (Parallel)
* Pandas -> Redshift (Parallel)
* CSV (S3) -> Pandas (One shot or Batching)
* Athena -> Pandas (One shot or Batching)
* CloudWatch Logs Insights -> Pandas
* Encrypt Pandas Dataframes on S3 with KMS keys

### PySpark
* PySpark -> Redshift (Parallel)
* Register Glue table from Dataframe stored on S3 (NEW :star:)

### General
* List S3 objects (Parallel)
* Delete S3 objects (Parallel)
* Delete listed S3 objects (Parallel)
* Delete NOT listed S3 objects (Parallel)
* Copy listed S3 objects (Parallel)
* Get the size of S3 objects (Parallel)
* Get CloudWatch Logs Insights query results
* Load partitions on Athena/Glue table (repair table) (NEW :star:)

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

#### Register Glue table from Dataframe stored on S3

```py3
dataframe.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy(["year", "month"]) \
        .save(compression="gzip", path="s3://...")
session = awswrangler.Session(spark_session=spark)
session.spark.create_glue_table(dataframe=dataframe,
                                file_format="parquet",
                                partition_by=["year", "month"],
                                path="s3://...",
                                compression="gzip",
                                database="my_database")
```

### General

#### Deleting a bunch of S3 objects (parallel)

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

#### Load partitions on Athena/Glue table (repair table)

```py3
session = awswrangler.Session()
session.athena.repair_table(database="db_name", table="tbl_name")
```

## Diving Deep

### Pandas to Redshift Flow

![Pandas to Redshift Flow](docs/source/_static/pandas-to-redshift-flow.jpg?raw=true "Pandas to Redshift Flow")

### Spark to Redshift Flow

![Spark to Redshift Flow](docs/source/_static/spark-to-redshift-flow.jpg?raw=true "Spark to Redshift Flow")

## Contributing

* AWS Data Wrangler practically only makes integrations. So we prefer to dedicate our energy / time writing integration tests instead of unit tests. We really like an end-to-end approach for all features.

* All integration tests are between a local Docker container and a remote/real AWS service.

* We have a Docker recipe to set up the local end (testing/Dockerfile).

* We have a Cloudformation to set up the AWS end (testing/template.yaml).

### Step-by-step

**DISCLAIMER**: Make sure to know what you are doing. This steps will charge some services on your AWS account. And requires a minimum security skills to keep your environment safe.

* Pick up a Linux or MacOS.

* Install Python 3.6+

* Install Docker and configure at least 4 cores and 8 GB of memory

* Fork the AWS Data Wrangler repository and clone that into your development environment

* Go to the project's directory create a Python's virtual environment for the project (**python -m venv venv && source venv/bin/activate**)

* Run **./install-dev.sh**

* Go to the *testing* directory

* Configure the parameters.json file with your AWS environment infos (Make sure that your Redshift will not be open for the World! Configure your security group to only give access for your IP.)

* Deploy the Cloudformation stack **./deploy-cloudformation.sh**

* Open the docker image **./open-image.sh**

* Inside the image you finally can run **./run-tests.sh**
