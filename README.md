# AWS Data Wrangler (BETA)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)


> Utilities for Pandas and Apache Spark on AWS


AWS Data Wrangler aims to fill a gap between AWS Analytics Services (Glue, Athena, EMR, Redshift) and the most popular Python data libraries (Pandas, Apache Spark).


---

*Contents:* **[Use Cases](#Use-Cases)** | **[Installation](#Installation)** | **[Usage](#Usage)** | **[Rationale](#Rationale)** | **[Dependencies](#Dependencies)** | **[Known Limitations](#Known-Limitations)** | **[Contributing](#Contributing)** | **[License](#License)**

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

**P.S.** The Lambda Layer bundle and the Glue egg are available to [download](https://github.com/awslabs/aws-data-wrangler/releases). It's just upload to your account and run! :rocket:

## Usage

### Writing Pandas Dataframe to Data Lake:

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

### Reading from Data Lake to Pandas Dataframe:

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_sql_athena(
    sql="select * from table",
    database="database"
)
```

### Reading from S3 file to Pandas Dataframe:

```py3
session = awswrangler.Session()
dataframe = session.pandas.read_csv(path="s3://...")
```

### Typical Pandas ETL:

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

### Loading Spark Dataframe to Redshift:

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

## Rationale

The rationale behind AWS Data Wrangler is to use the right tool for each job.
This project was developed to support two kinds of challenges: Small data (Pandas) and Big Data (Apache Spark).
That is never so clear choose the right tool to wrangle your data, that depends of a lot of different factors, but a good rule of thumb that we discovered during the tests is that if your workload is something around 5 GB in plan text or less, so you should go with Pandas, otherwise go with Apache Spark.

For example, in **[AWS Glue](https://aws.amazon.com/glue/)** you can choose between two different types of Job, distributed with Apache Spark or single node with Python Shell.

Bellow we can see an illustration exemplifying how you can go faster and cheaper even with the simples solution. 

![Rationale Image](docs/rationale.png?raw=true "Rationale")

## Dependencies

AWS Data Wrangler project relies on others great initiatives:
* **[Boto3](https://github.com/boto/boto3)**
* **[Pandas](https://github.com/pandas-dev/pandas)**
* **[Apache Arrow](https://github.com/apache/arrow)**
* **[Dask s3fs](https://github.com/dask/s3fs)**
* **[Pg8000](https://github.com/tlocke/pg8000)**
* **[Apache Spark](https://github.com/apache/spark)**
* **[Tenacity](https://github.com/jd/tenacity)**

## Known Limitations

* By now only writes in Parquet and CSV file formats
* By now there are not compression support
* By now there are not nested type support

## Contributing

For almost all features we need rely on AWS Services that didn't have mock tools in the community yet (AWS Glue, AWS Athena). So we are focusing on integration tests instead unit tests.

## License

This library is licensed under the Apache 2.0 License. 
