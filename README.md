# AWS Data Wrangler
*Pandas on AWS*

![AWS Data Wrangler](docs/source/_static/logo2.png?raw=true "AWS Data Wrangler")

[![Release](https://img.shields.io/badge/release-1.4.0-brightgreen.svg)](https://pypi.org/project/awswrangler/)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8-brightgreen.svg)](https://anaconda.org/conda-forge/awswrangler)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen.svg)](https://pypi.org/project/awswrangler/)
![Static Checking](https://github.com/awslabs/aws-data-wrangler/workflows/Static%20Checking/badge.svg?branch=master)
[![Documentation Status](https://readthedocs.org/projects/aws-data-wrangler/badge/?version=latest)](https://aws-data-wrangler.readthedocs.io/?badge=latest)

| Source    | Downloads                                                                                                                       | Page                                                 | Installation Command                       |
|-----------|---------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|--------------------------------------------|
| **PyPi**  | [![PyPI Downloads](https://img.shields.io/pypi/dm/awswrangler.svg)](https://pypi.org/project/awswrangler/)                      | [Link](https://pypi.org/project/awswrangler/)        | `pip install awswrangler`                  |
| **Conda** | [![Conda Downloads](https://img.shields.io/conda/dn/conda-forge/awswrangler.svg)](https://anaconda.org/conda-forge/awswrangler) | [Link](https://anaconda.org/conda-forge/awswrangler) | `conda install -c conda-forge awswrangler` |

## Quick Start

Install the Wrangler with: `pip install awswrangler`

```py3
import awswrangler as wr
import pandas as pd

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

# Getting Redshift connection (SQLAlchemy) from Glue Catalog Connections
# Retrieving the data from Amazon Redshift Spectrum
engine = wr.catalog.get_engine("my-redshift-connection")
df = wr.db.read_sql_query("SELECT * FROM external_schema.my_table", con=engine)

# Creating QuickSight Data Source and Dataset to reflect our new table
wr.quicksight.create_athena_data_source("athena-source", allowed_to_manage=["username"])
wr.quicksight.create_athena_dataset(
    name="my-dataset",
    database="my_db",
    table="my_table",
    data_source_name="athena-source",
    allowed_to_manage=["username"]
)

# Getting MySQL connection (SQLAlchemy) from Glue Catalog Connections
# Load the data into MySQL
engine = wr.catalog.get_engine("my-mysql-connection")
wr.db.to_sql(df, engine, schema="test", name="my_table") 

# Getting PostgreSQL connection (SQLAlchemy) from Glue Catalog Connections
# Load the data into PostgreSQL
engine = wr.catalog.get_engine("my-postgresql-connection")
wr.db.to_sql(df, engine, schema="test", name="my_table") 

```

## [Read The Docs](https://aws-data-wrangler.readthedocs.io/)

- [**What is AWS Data Wrangler?**](https://aws-data-wrangler.readthedocs.io/en/latest/what.html)
- [**Install**](https://aws-data-wrangler.readthedocs.io/en/latest/install.html)
  - [PyPi (pip)](https://aws-data-wrangler.readthedocs.io/en/latest/install.html#pypi-pip)
  - [Conda](https://aws-data-wrangler.readthedocs.io/en/latest/install.html#conda)
  - [AWS Lambda Layer](https://aws-data-wrangler.readthedocs.io/en/latest/install.html#aws-lambda-layer)
  - [AWS Glue Wheel](https://aws-data-wrangler.readthedocs.io/en/latest/install.html#aws-glue-wheel)
  - [Amazon SageMaker Notebook](https://aws-data-wrangler.readthedocs.io/en/latest/install.html#amazon-sagemaker-notebook)
  - [Amazon SageMaker Notebook Lifecycle](https://aws-data-wrangler.readthedocs.io/en/latest/install.html#amazon-sagemaker-notebook-lifecycle)
  - [EMR](https://aws-data-wrangler.readthedocs.io/en/latest/install.html#emr)
  - [From source](https://aws-data-wrangler.readthedocs.io/en/latest/install.html#from-source)
- [**Tutorials**](https://github.com/awslabs/aws-data-wrangler/tree/master/tutorials)
  - [001 - Introduction](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/001%20-%20Introduction.ipynb)
  - [002 - Sessions](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/002%20-%20Sessions.ipynb)
  - [003 - Amazon S3](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/003%20-%20Amazon%20S3.ipynb)
  - [004 - Parquet Datasets](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/004%20-%20Parquet%20Datasets.ipynb)
  - [005 - Glue Catalog](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/005%20-%20Glue%20Catalog.ipynb)
  - [006 - Amazon Athena](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/006%20-%20Amazon%20Athena.ipynb)
  - [007 - Databases (Redshift, MySQL and PostgreSQL)](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/007%20-%20Redshift%2C%20MySQL%2C%20PostgreSQL.ipynb)
  - [008 - Redshift - Copy & Unload.ipynb](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/008%20-%20Redshift%20-%20Copy%20%26%20Unload.ipynb)
  - [009 - Redshift - Append, Overwrite and Upsert](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/009%20-%20Redshift%20-%20Append%2C%20Overwrite%2C%20Upsert.ipynb)
  - [010 - Parquet Crawler](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/010%20-%20Parquet%20Crawler.ipynb)
  - [011 - CSV Datasets](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/011%20-%20CSV%20Datasets.ipynb)
  - [012 - CSV Crawler](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/012%20-%20CSV%20Crawler.ipynb)
  - [013 - Merging Datasets on S3](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/013%20-%20Merging%20Datasets%20on%20S3.ipynb)
  - [014 - Schema Evolution](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/014%20-%20Schema%20Evolution.ipynb)
  - [015 - EMR](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/015%20-%20EMR.ipynb)
  - [016 - EMR & Docker](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/016%20-%20EMR%20%26%20Docker.ipynb)
  - [017 - Partition Projection](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/017%20-%20Partition%20Projection.ipynb)
  - [018 - QuickSight](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/018%20-%20QuickSight.ipynb)
- [**API Reference**](https://aws-data-wrangler.readthedocs.io/en/latest/api.html)
  - [Amazon S3](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#amazon-s3)
  - [AWS Glue Catalog](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#aws-glue-catalog)
  - [Amazon Athena](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#amazon-athena)
  - [Databases (Redshift, PostgreSQL, MySQL)](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#databases-redshift-postgresql-mysql)
  - [EMR Cluster](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#emr-cluster)
  - [CloudWatch Logs](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#cloudwatch-logs)
  - [QuickSight](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#quicksight)
- [**License**](https://github.com/awslabs/aws-data-wrangler/blob/master/LICENSE)
- [**Contributing**](https://github.com/awslabs/aws-data-wrangler/blob/master/CONTRIBUTING.md)
- [**Legacy Docs** (pre-1.0.0)](https://aws-data-wrangler.readthedocs.io/en/legacy/)
