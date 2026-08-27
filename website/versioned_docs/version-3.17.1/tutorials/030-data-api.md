---
id: 030-data-api
title: "Data Api"
sidebar_position: 30
sidebar_label: "30 - Data Api"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/030%20-%20Data%20Api.ipynb
---

# Data Api

> This page is generated from [`tutorials/030 - Data Api.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/030%20-%20Data%20Api.ipynb). Open it in Jupyter to run it yourself.
The Data Api simplifies access to Amazon Redshift and RDS by removing the need to manage database connections and credentials. Instead, you can execute SQL commands to an Amazon Redshift cluster or Amazon Aurora cluster by simply invoking an HTTPS API endpoint provided by the Data API. It takes care of managing database connections and returning data. Since the Data API leverages IAM user credentials or database credentials stored in AWS Secrets Manager, you don’t need to pass credentials in API calls.

## Connect to the cluster
- [wr.data_api.redshift.connect()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.data_api.redshift.connect.html)
- [wr.data_api.rds.connect()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.data_api.rds.connect.html)

```python
con_redshift = wr.data_api.redshift.connect(
    cluster_id="aws-sdk-pandas-1xn5lqxrdxrv3",
    database="test_redshift",
    secret_arn="arn:aws:secretsmanager:us-east-1:111111111111:secret:aws-sdk-pandas/redshift-ewn43d",
)

con_redshift_serverless = wr.data_api.redshift.connect(
    workgroup_name="aws-sdk-pandas",
    database="test_redshift",
    secret_arn="arn:aws:secretsmanager:us-east-1:111111111111:secret:aws-sdk-pandas/redshift-f3en4w",
)

con_mysql = wr.data_api.rds.connect(
    resource_arn="arn:aws:rds:us-east-1:111111111111:cluster:mysql-serverless-cluster-wrangler",
    database="test_rds",
    secret_arn="arn:aws:secretsmanager:us-east-1:111111111111:secret:aws-sdk-pandas/mysql-23df3",
)
```

## Read from database
- [wr.data_api.redshift.read_sql_query()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.data_api.redshift.read_sql_query.html)
- [wr.data_api.rds.read_sql_query()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.data_api.rds.read_sql_query.html)

```python
df = wr.data_api.redshift.read_sql_query(
    sql="SELECT * FROM public.test_table",
    con=con_redshift,
)

df = wr.data_api.rds.read_sql_query(
    sql="SELECT * FROM test.test_table",
    con=con_rds,
)
```
