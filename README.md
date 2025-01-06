# AWS SDK for pandas (awswrangler)

*Pandas on AWS*

Easy integration with Athena, Glue, Redshift, Timestream, OpenSearch, Neptune, QuickSight, Chime, CloudWatchLogs, DynamoDB, EMR, SecretManager, PostgreSQL, MySQL, SQLServer and S3 (Parquet, CSV, JSON and EXCEL).

![AWS SDK for pandas](https://github.com/aws/aws-sdk-pandas/blob/main/docs/source/_static/logo2.png?raw=true "AWS SDK for pandas")
![tracker](https://d3tiqpr4kkkomd.cloudfront.net/img/pixel.png?asset=GVOYN2BOOQ573LTVIHEW)

> An [AWS Professional Service](https://aws.amazon.com/professional-services/) open source initiative | aws-proserve-opensource@amazon.com

[![PyPi](https://img.shields.io/pypi/v/awswrangler)](https://pypi.org/project/awswrangler/)
[![Conda](https://img.shields.io/conda/vn/conda-forge/awswrangler)](https://anaconda.org/conda-forge/awswrangler)
[![Python Version](https://img.shields.io/pypi/pyversions/awswrangler.svg)](https://pypi.org/project/awswrangler/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
![Static Checking](https://github.com/aws/aws-sdk-pandas/workflows/Static%20Checking/badge.svg?branch=main)
[![Documentation Status](https://readthedocs.org/projects/aws-sdk-pandas/badge/?version=latest)](https://aws-sdk-pandas.readthedocs.io/?badge=latest)

| Source | Downloads | Installation Command |
|--------|-----------|----------------------|
| **[PyPi](https://pypi.org/project/awswrangler/)**  | [![PyPI Downloads](https://img.shields.io/pypi/dm/awswrangler)](https://pypi.org/project/awswrangler/) | `pip install awswrangler` |
| **[Conda](https://anaconda.org/conda-forge/awswrangler)** | [![Conda Downloads](https://img.shields.io/conda/dn/conda-forge/awswrangler.svg)](https://anaconda.org/conda-forge/awswrangler) | `conda install -c conda-forge awswrangler` |

> ⚠️ **Starting version 3.0, optional modules must be installed explicitly:**<br>
➡️`pip install 'awswrangler[redshift]'`

## Table of contents

- [Quick Start](#quick-start)
- [At Scale](#at-scale)
- [Read The Docs](#read-the-docs)
- [Getting Help](#getting-help)
- [Logging](#logging)

## Quick Start

Installation command: `pip install awswrangler`

> ⚠️ **Starting version 3.0, optional modules must be installed explicitly:**<br>
➡️`pip install 'awswrangler[redshift]'`

```py3
import awswrangler as wr
import pandas as pd
from datetime import datetime

df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"]})

# Storing data on Data Lake
wr.s3.to_parquet(
    df=df,
    path="s3://bucket/dataset/",
    dataset=True,
    database="my_db",
    table="my_table"
)

# Retrieving the data directly from Amazon S3
df = wr.s3.read_parquet("s3://bucket/dataset/", dataset=True)

# Retrieving the data from Amazon Athena
df = wr.athena.read_sql_query("SELECT * FROM my_table", database="my_db")

# Get a Redshift connection from Glue Catalog and retrieving data from Redshift Spectrum
con = wr.redshift.connect("my-glue-connection")
df = wr.redshift.read_sql_query("SELECT * FROM external_schema.my_table", con=con)
con.close()

# Amazon Timestream Write
df = pd.DataFrame({
    "time": [datetime.now(), datetime.now()],   
    "my_dimension": ["foo", "boo"],
    "measure": [1.0, 1.1],
})
rejected_records = wr.timestream.write(df,
    database="sampleDB",
    table="sampleTable",
    time_col="time",
    measure_col="measure",
    dimensions_cols=["my_dimension"],
)

# Amazon Timestream Query
wr.timestream.query("""
SELECT time, measure_value::double, my_dimension
FROM "sampleDB"."sampleTable" ORDER BY time DESC LIMIT 3
""")

```

## At scale
AWS SDK for pandas can also run your workflows at scale by leveraging [Modin](https://modin.readthedocs.io/en/stable/) and [Ray](https://www.ray.io/). Both projects aim to speed up data workloads by distributing processing over a cluster of workers.

Read our [docs](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/scale.html) or head to our latest [tutorials](https://github.com/aws/aws-sdk-pandas/tree/main/tutorials) to learn more.

## [Read The Docs](https://aws-sdk-pandas.readthedocs.io/)

- [**What is AWS SDK for pandas?**](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/about.html)
- [**Install**](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html)
  - [PyPi (pip)](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#pypi-pip)
  - [Conda](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#conda)
  - [AWS Lambda Layer](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#aws-lambda-layer)
  - [AWS Glue Python Shell Jobs](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#aws-glue-python-shell-jobs)
  - [AWS Glue PySpark Jobs](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#aws-glue-pyspark-jobs)
  - [Amazon SageMaker Notebook](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#amazon-sagemaker-notebook)
  - [Amazon SageMaker Notebook Lifecycle](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#amazon-sagemaker-notebook-lifecycle)
  - [EMR](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#emr)
  - [From source](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/install.html#from-source)
- [**At scale**](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/scale.html)
  - [Getting Started](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/scale.html#getting-started)
  - [Supported APIs](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/scale.html#supported-apis)
  - [Resources](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/scale.html#resources)
- [**Tutorials**](https://github.com/aws/aws-sdk-pandas/tree/main/tutorials)
  - [001 - Introduction](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/001%20-%20Introduction.ipynb)
  - [002 - Sessions](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/002%20-%20Sessions.ipynb)
  - [003 - Amazon S3](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/003%20-%20Amazon%20S3.ipynb)
  - [004 - Parquet Datasets](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/004%20-%20Parquet%20Datasets.ipynb)
  - [005 - Glue Catalog](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/005%20-%20Glue%20Catalog.ipynb)
  - [006 - Amazon Athena](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/006%20-%20Amazon%20Athena.ipynb)
  - [007 - Databases (Redshift, MySQL, PostgreSQL, SQL Server and Oracle)](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/007%20-%20Redshift%2C%20MySQL%2C%20PostgreSQL%2C%20SQL%20Server%2C%20Oracle.ipynb)
  - [008 - Redshift - Copy & Unload.ipynb](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/008%20-%20Redshift%20-%20Copy%20%26%20Unload.ipynb)
  - [009 - Redshift - Append, Overwrite and Upsert](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/009%20-%20Redshift%20-%20Append%2C%20Overwrite%2C%20Upsert.ipynb)
  - [010 - Parquet Crawler](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/010%20-%20Parquet%20Crawler.ipynb)
  - [011 - CSV Datasets](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/011%20-%20CSV%20Datasets.ipynb)
  - [012 - CSV Crawler](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/012%20-%20CSV%20Crawler.ipynb)
  - [013 - Merging Datasets on S3](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/013%20-%20Merging%20Datasets%20on%20S3.ipynb)
  - [014 - Schema Evolution](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/014%20-%20Schema%20Evolution.ipynb)
  - [015 - EMR](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/015%20-%20EMR.ipynb)
  - [016 - EMR & Docker](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/016%20-%20EMR%20%26%20Docker.ipynb)
  - [017 - Partition Projection](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/017%20-%20Partition%20Projection.ipynb)
  - [018 - QuickSight](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/018%20-%20QuickSight.ipynb)
  - [019 - Athena Cache](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/019%20-%20Athena%20Cache.ipynb)
  - [020 - Spark Table Interoperability](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/020%20-%20Spark%20Table%20Interoperability.ipynb)
  - [021 - Global Configurations](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb)
  - [022 - Writing Partitions Concurrently](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/022%20-%20Writing%20Partitions%20Concurrently.ipynb)
  - [023 - Flexible Partitions Filter](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/023%20-%20Flexible%20Partitions%20Filter.ipynb)
  - [024 - Athena Query Metadata](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/024%20-%20Athena%20Query%20Metadata.ipynb)
  - [025 - Redshift - Loading Parquet files with Spectrum](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/025%20-%20Redshift%20-%20Loading%20Parquet%20files%20with%20Spectrum.ipynb)
  - [026 - Amazon Timestream](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/026%20-%20Amazon%20Timestream.ipynb)
  - [027 - Amazon Timestream 2](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/027%20-%20Amazon%20Timestream%202.ipynb)
  - [028 - Amazon DynamoDB](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/028%20-%20DynamoDB.ipynb)
  - [029 - S3 Select](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/029%20-%20S3%20Select.ipynb)
  - [030 - Data Api](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/030%20-%20Data%20Api.ipynb)
  - [031 - OpenSearch](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/031%20-%20OpenSearch.ipynb)
  - [033 - Amazon Neptune](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/033%20-%20Amazon%20Neptune.ipynb)
  - [034 - Distributing Calls Using Ray](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/034%20-%20Distributing%20Calls%20using%20Ray.ipynb)
  - [035 - Distributing Calls on Ray Remote Cluster](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/035%20-%20Distributing%20Calls%20on%20Ray%20Remote%20Cluster.ipynb)
  - [037 - Glue Data Quality](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/037%20-%20Glue%20Data%20Quality.ipynb)
  - [038 - OpenSearch Serverless](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/038%20-%20OpenSearch%20Serverless.ipynb)
  - [039 - Athena Iceberg](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/039%20-%20Athena%20Iceberg.ipynb)
  - [040 - EMR Serverless](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/040%20-%20EMR%20Serverless.ipynb)
  - [041 - Apache Spark on Amazon Athena](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/041%20-%20Apache%20Spark%20on%20Amazon%20Athena.ipynb)
- [**API Reference**](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html)
  - [Amazon S3](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-s3)
  - [AWS Glue Catalog](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#aws-glue-catalog)
  - [Amazon Athena](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-athena)
  - [Amazon Redshift](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-redshift)
  - [PostgreSQL](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#postgresql)
  - [MySQL](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#mysql)
  - [SQL Server](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#sqlserver)
  - [Oracle](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#oracle)
  - [Data API Redshift](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#data-api-redshift)
  - [Data API RDS](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#data-api-rds)
  - [OpenSearch](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#opensearch)
  - [AWS Glue Data Quality](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#aws-glue-data-quality)
  - [Amazon Neptune](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-neptune)
  - [DynamoDB](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#dynamodb)
  - [Amazon Timestream](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-timestream)
  - [Amazon EMR](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-emr)
  - [Amazon CloudWatch Logs](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-cloudwatch-logs)
  - [Amazon Chime](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-chime)
  - [Amazon QuickSight](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#amazon-quicksight)
  - [AWS STS](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#aws-sts)
  - [AWS Secrets Manager](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#aws-secrets-manager)
  - [Global Configurations](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#global-configurations)
  - [Distributed - Ray](https://aws-sdk-pandas.readthedocs.io/en/3.11.0/api.html#distributed-ray)
- [**License**](https://github.com/aws/aws-sdk-pandas/blob/main/LICENSE.txt)
- [**Contributing**](https://github.com/aws/aws-sdk-pandas/blob/main/CONTRIBUTING.md)

## Getting Help

The best way to interact with our team is through GitHub. You can open an [issue](https://github.com/aws/aws-sdk-pandas/issues/new/choose) and choose from one of our templates for bug reports, feature requests...
You may also find help on these community resources:
* The #aws-sdk-pandas Slack [channel](https://join.slack.com/t/aws-sdk-pandas/shared_invite/zt-sxdx38sl-E0coRfAds8WdpxXD2Nzfrg)
* Ask a question on [Stack Overflow](https://stackoverflow.com/questions/tagged/awswrangler)
  and tag it with `awswrangler`
* [Runbook](https://github.com/aws/aws-sdk-pandas/discussions/1815) for AWS SDK for pandas with Ray

## Logging

Enabling internal logging examples:

```py3
import logging
logging.basicConfig(level=logging.INFO, format="[%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)
```

Into AWS lambda:

```py3
import logging
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
```
