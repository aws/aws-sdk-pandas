# AWS Data Wrangler (BETA)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

> The missing link between AWS services and the most popular Python data libraries.

# CAUTION: This project is in BETA version. And was not tested in battle yet.

**[Read the docs!](https://aws-data-wrangler.readthedocs.io)**

**[Check how AWS Wrangler can process small data more than 10x cheaper and 10x faster than Spark!](https://aws-data-wrangler.readthedocs.io/en/latest/benchmarks.html)**

AWS Data Wrangler aims to fill a gap between AWS Analytics Services (Glue, Athena, EMR, Redshift) and the most popular Python libraries for ***lightweight*** workloads.

The rationale behind AWS Data Wrangler is to use the right tool for each job. And this project was developed with the lightweight jobs in mind. That is never so clear and depends of a lot of different factors, but a good rule of thumb that we discovered during the tests is that if your workload is something around 5 GB in plan text or less, so you should go with AWS Data Wrangler instead of the big data tools.

Usually there are two different types of use cases when dealing with data, heavy workloads which are dealt better using distributed tools and services like EMR/Spark and lightweight workloads that can be treated most efficient using simpler tools, and this is when aws data wrangler comes into action.

For example, in **[AWS Glue](https://aws.amazon.com/glue/)** you can choose between two different types of Job, distributed with Apache Spark or single node with Python Shell. In this case data wrangler would use the single node with Python Shell job option (Or even AWS Lambda), resulting in [less cost and more speed](https://aws-data-wrangler.readthedocs.io/en/latest/benchmarks.html).

![Rationale Image](docs/source/_static/rationale.png?raw=true "Rationale")

---

*Contents:* **[Installation](#Installation)** | **[Usage](#Usage)** | **[Known Limitations](#Known-Limitations)** | **[Contributing](#Contributing)** | **[Dependencies](#Dependencies)** | **[License](#License)**

---

## Installation

`pip install awswrangler`

AWS Data Wrangler runs only Python 3.6 and beyond.
And runs on AWS Lambda, AWS Glue, EC2, on-premises and local.

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

### Typical ETL:

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

## Dependencies

AWS Data Wrangler project relies on others great initiatives:
* **[Boto3](https://github.com/boto/boto3)**
* **[Pandas](https://github.com/pandas-dev/pandas)**
* **[Apache Arrow](https://github.com/apache/arrow)**
* **[Dask s3fs](https://github.com/dask/s3fs)**
* **[PyGreSQL](http://www.pygresql.org/)**

## Known Limitations

* By now only writes in Parquet and CSV file formats
* By now there are not compression support
* By now there are not nested type support

## Contributing

For almost all features we need rely on AWS Services that didn't have mock tools in the community yet (AWS Glue, AWS Athena). So we are focusing on integration tests instead unit tests.

So, you will need provide a S3 bucket and a Glue/Athena database through environment variables.

`export AWSWRANGLER_TEST_BUCKET=...`

`export AWSWRANGLER_TEST_DATABASE=...`

CAUTION: This may this may incur costs in your AWS Account

`make init`

Make your changes...

`make format`

`make lint`

`make test`

## License

This library is licensed under the Apache 2.0 License. 
