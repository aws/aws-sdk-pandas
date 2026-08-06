---
id: 001-introduction
title: "Introduction"
sidebar_position: 1
sidebar_label: "1 - Introduction"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/001%20-%20Introduction.ipynb
---

# Introduction

> This page is generated from [`tutorials/001 - Introduction.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/001%20-%20Introduction.ipynb). Open it in Jupyter to run it yourself.
## What is AWS SDK for pandas?

An [open-source](https://github.com/aws/aws-sdk-pandas) Python package that extends the power of [Pandas](https://github.com/pandas-dev/pandas) library to AWS connecting **DataFrames** and AWS data related services (**Amazon Redshift**, **AWS Glue**, **Amazon Athena**, **Amazon Timestream**, **Amazon EMR**, etc).

Built on top of other open-source projects like [Pandas](https://github.com/pandas-dev/pandas), [Apache Arrow](https://github.com/apache/arrow) and [Boto3](https://github.com/boto/boto3), it offers abstracted functions to execute usual ETL tasks like load/unload data from **Data Lakes**, **Data Warehouses** and **Databases**.

Check our [list of functionalities](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/api.html).

## How to install?

awswrangler runs almost anywhere over Python 3.8, 3.9 and 3.10, so there are several different ways to install it in the desired environment.

  - [PyPi (pip)](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#pypi-pip)
  - [Conda](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#conda)
  - [AWS Lambda Layer](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#aws-lambda-layer)
  - [AWS Glue Python Shell Jobs](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#aws-glue-python-shell-jobs)
  - [AWS Glue PySpark Jobs](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#aws-glue-pyspark-jobs)
  - [Amazon SageMaker Notebook](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#amazon-sagemaker-notebook)
  - [Amazon SageMaker Notebook Lifecycle](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#amazon-sagemaker-notebook-lifecycle)
  - [EMR Cluster](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#emr-cluster)
  - [From source](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/install.html#from-source)

Some good practices for most of the above methods are:
  - Use new and individual Virtual Environments for each project ([venv](https://docs.python.org/3/library/venv.html))
  - On Notebooks, always restart your kernel after installations.

## Let's Install it!

```python
!pip install awswrangler
```

> Restart your kernel after the installation!

```python
import awswrangler as wr

wr.__version__
```

```text
'2.0.0'
```
