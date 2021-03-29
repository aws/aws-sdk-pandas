# AWS Data Wrangler

*Pandas on AWS*

Easy integration with Athena, Glue, Redshift, Timestream, QuickSight, Chime, CloudWatchLogs, DynamoDB, EMR, SecretManager, PostgreSQL, MySQL, SQLServer and S3 (Parquet, CSV, JSON and EXCEL).

![AWS Data Wrangler](docs/source/_static/logo2.png?raw=true "AWS Data Wrangler")

> An [AWS Professional Service](https://aws.amazon.com/professional-services/) open source initiative | aws-proserve-opensource@amazon.com

[![Release](https://img.shields.io/badge/release-2.6.0-brightgreen.svg)](https://pypi.org/project/awswrangler/)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8%20%7C%203.9-brightgreen.svg)](https://anaconda.org/conda-forge/awswrangler)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Coverage](https://img.shields.io/badge/coverage-91%25-brightgreen.svg)](https://pypi.org/project/awswrangler/)
![Static Checking](https://github.com/awslabs/aws-data-wrangler/workflows/Static%20Checking/badge.svg?branch=main)
![Build Status](https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiaHFXUk91Wks2MGFRMSsxM1R3ZFVjVGg3d0dCT05YTVpoL0VRakRieG43Y3dhdytYZjZtdFVBdG5Sek44anlweDFkM2Z2TWJibVRCRVB5TjlWSnhTdzRBPSIsIml2UGFyYW1ldGVyU3BlYyI6IjUzUllpN295VTUxeFNPQWQiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=main)
[![Documentation Status](https://readthedocs.org/projects/aws-data-wrangler/badge/?version=latest)](https://aws-data-wrangler.readthedocs.io/?badge=latest)

| Source | Downloads | Installation Command |
|--------|-----------|----------------------|
| **[PyPi](https://pypi.org/project/awswrangler/)**  | [![PyPI Downloads](https://pepy.tech/badge/awswrangler)](https://pypi.org/project/awswrangler/) | `pip install awswrangler` |
| **[Conda](https://anaconda.org/conda-forge/awswrangler)** | [![Conda Downloads](https://img.shields.io/conda/dn/conda-forge/awswrangler.svg)](https://anaconda.org/conda-forge/awswrangler) | `conda install -c conda-forge awswrangler` |

> ⚠️ **For platforms without PyArrow 3 support (e.g. [EMR](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#emr-cluster), [Glue PySpark Job](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#aws-glue-pyspark-jobs), MWAA):**<br>
➡️ `pip install pyarrow==2 awswrangler`

Powered By [<img src="https://arrow.apache.org/img/arrow.png" width="200">](https://arrow.apache.org/powered_by/)

## Table of contents

- [Quick Start](#quick-start)
- [Read The Docs](#read-the-docs)
- [Community Resources](#community-resources)
- [Logging](#logging)
- [Who uses AWS Data Wrangler?](#who-uses-aws-data-wrangler)
- [What is Amazon SageMaker Data Wrangler?](#what-is-amazon-sageMaker-data-wrangler)

## Quick Start

Installation command: `pip install awswrangler`

> ⚠️ **For platforms without PyArrow 3 support (e.g. [EMR](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#emr-cluster), [Glue PySpark Job](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#aws-glue-pyspark-jobs), MWAA):**<br>
➡️`pip install pyarrow==2 awswrangler`

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

## [Read The Docs](https://aws-data-wrangler.readthedocs.io/)

- [**What is AWS Data Wrangler?**](https://aws-data-wrangler.readthedocs.io/en/2.6.0/what.html)
- [**Install**](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html)
  - [PyPi (pip)](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#pypi-pip)
  - [Conda](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#conda)
  - [AWS Lambda Layer](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#aws-lambda-layer)
  - [AWS Glue Python Shell Jobs](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#aws-glue-python-shell-jobs)
  - [AWS Glue PySpark Jobs](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#aws-glue-pyspark-jobs)
  - [Amazon SageMaker Notebook](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#amazon-sagemaker-notebook)
  - [Amazon SageMaker Notebook Lifecycle](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#amazon-sagemaker-notebook-lifecycle)
  - [EMR](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#emr)
  - [From source](https://aws-data-wrangler.readthedocs.io/en/2.6.0/install.html#from-source)
- [**Tutorials**](https://github.com/awslabs/aws-data-wrangler/tree/main/tutorials)
  - [001 - Introduction](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/001%20-%20Introduction.ipynb)
  - [002 - Sessions](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/002%20-%20Sessions.ipynb)
  - [003 - Amazon S3](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/003%20-%20Amazon%20S3.ipynb)
  - [004 - Parquet Datasets](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/004%20-%20Parquet%20Datasets.ipynb)
  - [005 - Glue Catalog](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/005%20-%20Glue%20Catalog.ipynb)
  - [006 - Amazon Athena](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/006%20-%20Amazon%20Athena.ipynb)
  - [007 - Databases (Redshift, MySQL, PostgreSQL and SQL Server)](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/007%20-%20Redshift%2C%20MySQL%2C%20PostgreSQL%2C%20SQL%20Server.ipynb)
  - [008 - Redshift - Copy & Unload.ipynb](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/008%20-%20Redshift%20-%20Copy%20%26%20Unload.ipynb)
  - [009 - Redshift - Append, Overwrite and Upsert](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/009%20-%20Redshift%20-%20Append%2C%20Overwrite%2C%20Upsert.ipynb)
  - [010 - Parquet Crawler](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/010%20-%20Parquet%20Crawler.ipynb)
  - [011 - CSV Datasets](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/011%20-%20CSV%20Datasets.ipynb)
  - [012 - CSV Crawler](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/012%20-%20CSV%20Crawler.ipynb)
  - [013 - Merging Datasets on S3](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/013%20-%20Merging%20Datasets%20on%20S3.ipynb)
  - [014 - Schema Evolution](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/014%20-%20Schema%20Evolution.ipynb)
  - [015 - EMR](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/015%20-%20EMR.ipynb)
  - [016 - EMR & Docker](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/016%20-%20EMR%20%26%20Docker.ipynb)
  - [017 - Partition Projection](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/017%20-%20Partition%20Projection.ipynb)
  - [018 - QuickSight](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/018%20-%20QuickSight.ipynb)
  - [019 - Athena Cache](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/019%20-%20Athena%20Cache.ipynb)
  - [020 - Spark Table Interoperability](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/020%20-%20Spark%20Table%20Interoperability.ipynb)
  - [021 - Global Configurations](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb)
  - [022 - Writing Partitions Concurrently](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/022%20-%20Writing%20Partitions%20Concurrently.ipynb)
  - [023 - Flexible Partitions Filter](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/023%20-%20Flexible%20Partitions%20Filter.ipynb)
  - [024 - Athena Query Metadata](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/024%20-%20Athena%20Query%20Metadata.ipynb)
  - [025 - Redshift - Loading Parquet files with Spectrum](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/025%20-%20Redshift%20-%20Loading%20Parquet%20files%20with%20Spectrum.ipynb)
  - [026 - Amazon Timestream](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/026%20-%20Amazon%20Timestream.ipynb)
  - [027 - Amazon Timestream 2](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/027%20-%20Amazon%20Timestream%202.ipynb)
  - [028 - Amazon DynamoDB](https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/028%20-%20DynamoDB.ipynb)
- [**API Reference**](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html)
  - [Amazon S3](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#amazon-s3)
  - [AWS Glue Catalog](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#aws-glue-catalog)
  - [Amazon Athena](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#amazon-athena)
  - [Amazon Redshift](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#amazon-redshift)
  - [PostgreSQL](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#postgresql)
  - [MySQL](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#mysql)
  - [SQL Server](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#sqlserver)
  - [DynamoDB](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#dynamodb)
  - [Amazon Timestream](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#amazon-timestream)
  - [Amazon EMR](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#amazon-emr)
  - [Amazon CloudWatch Logs](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#amazon-cloudwatch-logs)
  - [Amazon Chime](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#amazon-chime)
  - [Amazon QuickSight](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#amazon-quicksight)
  - [AWS STS](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#aws-sts)
  - [AWS Secrets Manager](https://aws-data-wrangler.readthedocs.io/en/2.6.0/api.html#aws-secrets-manager)
- [**License**](https://github.com/awslabs/aws-data-wrangler/blob/main/LICENSE.txt)
- [**Contributing**](https://github.com/awslabs/aws-data-wrangler/blob/main/CONTRIBUTING.md)
- [**Legacy Docs** (pre-1.0.0)](https://aws-data-wrangler.readthedocs.io/en/0.3.3/)

## Community Resources

Please [send a Pull Request](https://github.com/awslabs/aws-data-wrangler/edit/main/README.md) with your resource reference and @githubhandle.

- [Optimize Python ETL by extending Pandas with AWS Data Wrangler](https://aws.amazon.com/blogs/big-data/optimize-python-etl-by-extending-pandas-with-aws-data-wrangler/) [[@igorborgest](https://github.com/igorborgest)]
- [Reading Parquet Files With AWS Lambda](https://aprakash.wordpress.com/2020/04/14/reading-parquet-files-with-aws-lambda/) [[@anand086](https://github.com/anand086)]
- [Transform AWS CloudTrail data using AWS Data Wrangler](https://aprakash.wordpress.com/2020/09/17/transform-aws-cloudtrail-data-using-aws-data-wrangler/) [[@anand086](https://github.com/anand086)]
- [Rename Glue Tables using AWS Data Wrangler](https://ananddatastories.com/rename-glue-tables-using-aws-data-wrangler/) [[@anand086](https://github.com/anand086)]
- [Getting started on AWS Data Wrangler and Athena](https://medium.com/@dheerajsharmainampudi/getting-started-on-aws-data-wrangler-and-athena-7b446c834076) [[@dheerajsharma21](https://github.com/dheerajsharma21)]
- [Simplifying Pandas integration with AWS data related services](https://medium.com/@bv_subhash/aws-data-wrangler-simplifying-pandas-integration-with-aws-data-related-services-2b3325c12188) [[@bvsubhash](https://github.com/bvsubhash)]
- [Build an ETL pipeline using AWS S3, Glue and Athena](https://www.linkedin.com/pulse/build-etl-pipeline-using-aws-s3-glue-athena-data-wrangler-tom-reid/) [[@taupirho](https://github.com/taupirho)]

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

## Who uses AWS Data Wrangler?

Knowing which companies are using this library is important to help prioritize the project internally.

Please [send a Pull Request](https://github.com/awslabs/aws-data-wrangler/edit/main/README.md) with your company name and @githubhandle if you may.

- [Amazon](https://www.amazon.com/)
- [AWS](https://aws.amazon.com/)
- [Cepsa](https://cepsa.com) [[@alvaropc](https://github.com/alvaropc)]
- [Cognitivo](https://www.cognitivo.ai/) [[@msantino](https://github.com/msantino)]
- [Digio](https://www.digio.com.br/) [[@afonsomy](https://github.com/afonsomy)]
- [DNX](https://www.dnx.solutions/) [[@DNXLabs](https://github.com/DNXLabs)]
- [Funcional Health Tech](https://www.funcionalcorp.com.br/) [[@webysther](https://github.com/webysther)]
- [Informa Markets](https://www.informamarkets.com/en/home.html) [[@mateusmorato]](http://github.com/mateusmorato)
- [LINE TV](https://www.linetv.tw/) [[@bryanyang0528](https://github.com/bryanyang0528)]
- [Magnataur](https://magnataur.com) [[@brianmingus2](https://github.com/brianmingus2)]
- [M4U](https://www.m4u.com.br/) [[@Thiago-Dantas](https://github.com/Thiago-Dantas)]
- [NBCUniversal](https://www.nbcuniversal.com/) [[@vibe](https://github.com/vibe)]
- [nrd.io](https://nrd.io/) [[@mrtns](https://github.com/mrtns)]
- [OKRA Technologies](https://okra.ai) [[@JPFrancoia](https://github.com/JPFrancoia), [@schot](https://github.com/schot)]
- [Pier](https://www.pier.digital/) [[@flaviomax](https://github.com/flaviomax)]
- [Pismo](https://www.pismo.io/) [[@msantino](https://github.com/msantino)]
- [ringDNA](https://www.ringdna.com/) [[@msropp](https://github.com/msropp)]
- [Serasa Experian](https://www.serasaexperian.com.br/) [[@andre-marcos-perez](https://github.com/andre-marcos-perez)]
- [Shipwell](https://shipwell.com/) [[@zacharycarter](https://github.com/zacharycarter)]
- [strongDM](https://www.strongdm.com/) [[@mrtns](https://github.com/mrtns)]
- [Thinkbumblebee](https://www.thinkbumblebee.com/) [[@dheerajsharma21]](https://github.com/dheerajsharma21)
- [Zillow](https://www.zillow.com/) [[@nicholas-miles]](https://github.com/nicholas-miles)

## What is Amazon SageMaker Data Wrangler?

**Amazon SageMaker Data Wrangler** is a new SageMaker Studio feature that has a similar name but has a different purpose than the **AWS Data Wrangler** open source project.

- **AWS Data Wrangler** is open source, runs anywhere, and is focused on code.

- **Amazon SageMaker Data Wrangler** is specific for the SageMaker Studio environment and is focused on a visual interface.
