---
id: about
title: What is AWS SDK for pandas?
sidebar_position: 1
---

# What is AWS SDK for pandas?

An [AWS Professional Service](https://aws.amazon.com/professional-services) [open source](https://github.com/aws/aws-sdk-pandas) Python initiative that extends the power of the [pandas](https://github.com/pandas-dev/pandas) library to AWS, connecting **DataFrames** and AWS data & analytics services.

Easy integration with:

- **Amazon S3** — Parquet, ORC, CSV, JSON and Excel; partitioned datasets; [Delta Lake](https://delta.io/) tables; [S3 Tables](https://aws.amazon.com/s3/features/tables/) (Apache Iceberg); [S3 Vectors](https://aws.amazon.com/s3/features/vectors/) for similarity search with optional Amazon Bedrock embedding
- **Amazon Athena** — SQL queries into DataFrames, [Apache Iceberg](https://iceberg.apache.org/) tables with upsert/delete, Spark on Athena
- **AWS Glue** — Data Catalog, Data Quality rulesets
- **Amazon Redshift** — queries, COPY and UNLOAD, Data API
- **Databases** — PostgreSQL, MySQL, SQL Server, Oracle, RDS Data API
- **NoSQL & search** — DynamoDB, OpenSearch, Neptune (Gremlin, SPARQL, openCypher)
- **And more** — Timestream, EMR & EMR Serverless, AWS Clean Rooms, QuickSight, CloudWatch Logs, Secrets Manager, STS, Chime

Built on top of other open-source projects like [pandas](https://github.com/pandas-dev/pandas), [Apache Arrow](https://github.com/apache/arrow) and [Boto3](https://github.com/boto/boto3), it offers abstracted functions to execute your usual ETL tasks like loading and unloading data from **data lakes**, **data warehouses** and **databases**, even [at scale](scale.md).

Check out our [tutorials](tutorials.md) or the [list of functionalities](api/index.md).
