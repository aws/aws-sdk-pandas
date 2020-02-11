![AWS Data Wrangler](docs/source/_static/logo.png?raw=true "AWS Data Wrangler")

> DataFrames on AWS

[![Release](https://img.shields.io/badge/release-0.3.1-brightgreen.svg)](https://pypi.org/project/awswrangler/)
[![Downloads](https://img.shields.io/pypi/dm/awswrangler.svg)](https://pypi.org/project/awswrangler/)
[![Python Version](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8-brightgreen.svg)](https://pypi.org/project/awswrangler/)
[![Documentation Status](https://readthedocs.org/projects/aws-data-wrangler/badge/?version=latest)](https://aws-data-wrangler.readthedocs.io/en/latest/?badge=latest)
[![Coverage](https://img.shields.io/badge/coverage-89%25-brightgreen.svg)](https://pypi.org/project/awswrangler/)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/awslabs/aws-data-wrangler.svg)](http://isitmaintained.com/project/awslabs/aws-data-wrangler "Average time to resolve an issue")
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## [Read the Docs](https://aws-data-wrangler.readthedocs.io)

## Resources
- [Use Cases](#Use-Cases)
  - [Pandas](#Pandas)
  - [PySpark](#PySpark)
  - [General](#General)
- [Install](https://aws-data-wrangler.readthedocs.io/en/latest/installation.html)
- [Examples](https://aws-data-wrangler.readthedocs.io/en/latest/examples.html)
- [Tutorials](https://aws-data-wrangler.readthedocs.io/en/latest/tutorials.html)
- [API Reference](https://aws-data-wrangler.readthedocs.io/en/latest/api/awswrangler.html)
- [License](https://aws-data-wrangler.readthedocs.io/en/latest/license.html)
- [Contributing](https://aws-data-wrangler.readthedocs.io/en/latest/contributing.html)

## Use Cases

### PySpark

| FROM                        | TO                        | Features                                                                                 |
|-----------------------------|---------------------------|------------------------------------------------------------------------------------------|
| PySpark DataFrame           | Amazon Redshift            | Blazing fast using parallel parquet on S3 behind the scenesAppend/Overwrite/Upsert modes |
| PySpark DataFrame           | Glue Catalog              | Register Parquet or CSV DataFrame on Glue Catalog                                        |
| Nested PySpark<br>DataFrame | Flat PySpark<br>DataFrames| Flatten structs and break up arrays in child tables                                      |


### Pandas

| FROM                     | TO              | Features                                                                                                                                                                                                                           |
|--------------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pandas DataFrame         | Amazon S3       | Parquet, CSV, Partitions, Parallelism, Overwrite/Append/Partitions-Upsert modes,<br>KMS Encryption, Glue Metadata (Athena, Spectrum, Spark, Hive, Presto)                                                                          |
| Amazon S3                | Pandas DataFrame| Parquet (Pushdown filters), CSV, Fixed-width formatted, Partitions, Parallelism,<br>KMS Encryption, Multiple files                                                                                                                                        |
| Amazon Athena            | Pandas DataFrame| Workgroups, S3 output path, Encryption, and two different engines:<br><br>- ctas_approach=False **->** Batching and restrict memory environments<br>- ctas_approach=True  **->** Blazing fast, parallelism and enhanced data types |
| Pandas DataFrame         | Amazon Redshift | Blazing fast using parallel parquet on S3 behind the scenes<br>Append/Overwrite/Upsert modes                                                                                                                                       |
| Amazon Redshift          | Pandas DataFrame| Blazing fast using parallel parquet on S3 behind the scenes                                                                                                                                                                        |
| Pandas DataFrame         | Amazon Aurora   | Supported engines: MySQL, PostgreSQL<br>Blazing fast using parallel CSV on S3 behind the scenes<br>Append/Overwrite modes                                                                                                          |
| Amazon Aurora            | Pandas DataFrame| Supported engines: MySQL<br>Blazing fast using parallel CSV on S3 behind the scenes                                                                                                                                                |
| CloudWatch Logs Insights | Pandas DataFrame| Query results                                                                                                                                                                                                                      |
| Glue Catalog             | Pandas DataFrame| List and get Tables details. Good fit with Jupyter Notebooks.                                                                                                                                                                      |

### General

| Feature                                     | Details                             |
|---------------------------------------------|-------------------------------------|
| List S3 objects                             | e.g. wr.s3.list_objects("s3://...") |
| Delete S3 objects                           | Parallel                            |
| Delete listed S3 objects                    | Parallel                            |
| Delete NOT listed S3 objects                | Parallel                            |
| Copy listed S3 objects                      | Parallel                            |
| Get the size of S3 objects                  | Parallel                            |
| Get CloudWatch Logs Insights query results  |                                     |
| Load partitions on Athena/Glue table        | Through "MSCK REPAIR TABLE"         |
| Create EMR cluster                          | "For humans"                        |
| Terminate EMR cluster                       | "For humans"                        |
| Get EMR cluster state                       | "For humans"                        |
| Submit EMR step(s)                          | "For humans"                        |
| Get EMR step state                          | "For humans"                        |
| Query Athena to receive python primitives   | Returns *Iterable[Dict[str, Any]*   |
| Load and Unzip SageMaker jobs outputs       |                                     |
| Dump Amazon Redshift as Parquet files on S3 |                                     |
| Dump Amazon Aurora as CSV files on S3       | Only for MySQL engine               |
