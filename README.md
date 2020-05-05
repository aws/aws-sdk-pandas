# AWS Data Wrangler
*Pandas on AWS*

---

**NOTE**

Due the new major version `1.*.*` with breaking changes, please make sure that all your old projects has dependencies frozen on the desired version (e.g. `pip install awswrangler==0.3.2`).

---

![AWS Data Wrangler](docs/source/_static/logo2.png?raw=true "AWS Data Wrangler")

[![Release](https://img.shields.io/badge/release-1.1.0-brightgreen.svg)](https://pypi.org/project/awswrangler/)
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
engine = wr.catalog.get_engine("my-redshift-connection")

# Retrieving the data from Amazon Redshift Spectrum
df = wr.db.read_sql_query("SELECT * FROM external_schema.my_table", con=engine)
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
  - [01 - Introduction](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/01%20-%20Introduction.ipynb)
  - [02 - Sessions](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/02%20-%20Sessions.ipynb)
  - [03 - Amazon S3](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/03%20-%20Amazon%20S3.ipynb)
  - [04 - Parquet Datasets](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/04%20-%20Parquet%20Datasets.ipynb)
  - [05 - Glue Catalog](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/05%20-%20Glue%20Catalog.ipynb)
  - [06 - Amazon Athena](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/06%20-%20Amazon%20Athena.ipynb)
  - [07 - Databases (Redshift, MySQL and PostgreSQL)](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/07%20-%20Redshift%2C%20MySQL%2C%20PostgreSQL.ipynb)
  - [08 - Redshift - Copy & Unload.ipynb](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/08%20-%20Redshift%20-%20Copy%20%26%20Unload.ipynb)
  - [09 - Redshift - Append, Overwrite and Upsert](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/09%20-%20Redshift%20-%20Append%2C%20Overwrite%2C%20Upsert.ipynb)
  - [10 - Parquet Crawler](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/10%20-%20Parquet%20Crawler.ipynb)
  - [11 - CSV Datasets](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/11%20-%20CSV%20Datasets.ipynb)
  - [12 - CSV Crawler](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/12%20-%20CSV%20Crawler.ipynb)
  - [13 - Merging Datasets on S3](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/13%20-%20Merging%20Datasets%20on%20S3.ipynb)
  - [14 - PyTorch](https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/14%20-%20PyTorch.ipynb)
  - [15 - EMR](https://github.com/awslabs/aws-data-wrangler/blob/dev/tutorials/15%20-%20EMR.ipynb)
  - [16 - EMR & Docker](https://github.com/awslabs/aws-data-wrangler/blob/dev/tutorials/16%20-%20EMR%20%26%20Docker.ipynb)
- [**API Reference**](https://aws-data-wrangler.readthedocs.io/en/latest/api.html)
  - [Amazon S3](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#amazon-s3)
  - [AWS Glue Catalog](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#aws-glue-catalog)
  - [Amazon Athena](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#amazon-athena)
  - [Databases (Redshift, PostgreSQL, MySQL)](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#databases-redshift-postgresql-mysql)
  - [EMR Cluster](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#emr-cluster)
  - [CloudWatch Logs](https://aws-data-wrangler.readthedocs.io/en/latest/api.html#cloudwatch-logs)
- [**License**](https://github.com/awslabs/aws-data-wrangler/blob/master/LICENSE)
- [**Contributing**](https://github.com/awslabs/aws-data-wrangler/blob/master/CONTRIBUTING.md)
