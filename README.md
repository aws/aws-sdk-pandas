# AWS Data Wrangler (BETA)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

> The missing link between AWS services and the most popular Python data libraries.

> The right tool for each job.

# CAUTION: This project is in BETA version. And was not tested in battle yet.

AWS Data Wrangler aims to fill a gap between AWS Analytics Services (Glue, Athena, EMR, Redshift) and the most popular Python libraries for ***lightweight*** workloads.

The rationale behind AWS Data Wrangler is to use ***the right tool for each job***. That is never so clear and depends of a lot of different factors, but a good rule of thumb that we discoverd during the tests is that if your workload is something around 5 GB in plan text or less, so you should go with AWS Data Wrangler instead of the consagrated big data tools.

**[AWS Glue](https://aws.amazon.com/glue/)** is perfect to help illustrate the rationale. There are two different types of Job, distributed with **[Apache Spark](https://spark.apache.org/)** or single node with Python Shell.

![Rationale Image](docs/_static/rationale.png?raw=true "Rationale")

---

*Contents:* **[Installation](#Installation)** | **[Usage](#Usage)** | **[Known Limitations](#Known-Limitations)** | **[Contributing](#Contributing)** | **[Dependencies](#Dependencies)** | **[License](#License)**

---

## Installation

`pip install awswrangler`

AWS Data Wrangler runs on Python 2 and 3.

## Usage

### Writing Pandas Dataframe to Data Lake:

```py3
awswrangler.s3.write(
        df=df,
        database="database",
        path="s3://...",
        file_format="parquet",
        preserve_index=True,
        mode="overwrite",
        partition_cols=["col"],
    )
```

If a Glue Database name is passed, all the metadata will be created in the Glue Catalog. If not, only the s3 data write will be done.

### Reading from Data Lake to Pandas Dataframe:

```py3
df = awswrangler.athena.read("database", "select * from table")
```

### Typical ETL:

```py3
import pandas
import awswrangler

df = pandas.read_csv("s3//your_bucket/your_object.csv")  # Read from anywhere

# Typical Pandas, Numpy or Pyarrow transformation HERE!

awswrangler.s3.write(  # Storing the data and metadata to Data Lake
        df=df,
        database="database",
        path="s3://...",
        file_format="parquet",
        preserve_index=True,
        mode="overwrite",
        partition_cols=["col"],
    )
```

## Dependencies

AWS Data Wrangler project relies on others great initiatives:
* **[Boto3](https://github.com/boto/boto3)**
* **[Pandas](https://github.com/pandas-dev/pandas)**
* **[Apache Arrow](https://github.com/apache/arrow)**
* **[Dask s3fs](https://github.com/dask/s3fs)**

## Known Limitations

* By now only writes in Parquet and CSV file formats
* By now only reads through AWS Athena
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
