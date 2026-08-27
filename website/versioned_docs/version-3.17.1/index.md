---
id: index
title: AWS SDK for pandas
sidebar_position: 0
slug: /
---

# AWS SDK for pandas

pandas on AWS — DataFrames in and out of Amazon S3 (Parquet, Iceberg, Delta Lake, CSV, JSON, Excel), Athena, Glue, Redshift, DynamoDB, OpenSearch, Neptune, S3 Vectors and more. An [AWS Professional Services](https://aws.amazon.com/professional-services) open source initiative.

## Quick Start

```bash
pip install awswrangler
```

```bash
# Optional modules are installed with:
pip install 'awswrangler[redshift]'
```

```python
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

# Writing to an Apache Iceberg table with upsert (merge on id)
wr.athena.to_iceberg(
    df=df,
    database="my_db",
    table="my_iceberg_table",
    merge_cols=["id"],
)

# Writing to a Delta Lake table (pip install 'awswrangler[deltalake]')
wr.s3.to_deltalake(df=df, path="s3://bucket/delta/", mode="append")

# Reading from a Redshift data warehouse via a Glue Catalog connection
con = wr.redshift.connect("my-glue-connection")
df = wr.redshift.read_sql_query("SELECT * FROM external_schema.my_table", con=con)
con.close()

# Semantic search with Amazon S3 Vectors: embed with Bedrock, write, query
wr.s3.put_vectors_from_df(
    df=pd.DataFrame({"id": ["a", "b"], "title": ["Dune", "Up"]}),
    key_column="id",
    text_column="title",
    bedrock_model_id="amazon.titan-embed-text-v2:0",
    vector_bucket="my-vector-bucket",
    index="my-index",
)
hits = wr.s3.query_vectors(
    query_text="a touching story about companionship",
    bedrock_model_id="amazon.titan-embed-text-v2:0",
    top_k=3,
    vector_bucket="my-vector-bucket",
    index="my-index",
)
```

## Learn more

- [Install](install.md) — pip, conda, Lambda layers, Glue, SageMaker, EMR
- [At Scale](scale.md) — distributed workflows with Ray and Modin
- [Tutorials](tutorials.md) — hands-on notebooks for every module
- [API Reference](api/index.md) — every function, from the docstrings
