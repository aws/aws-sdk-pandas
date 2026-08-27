---
id: 004-parquet-datasets
title: "Parquet Datasets"
sidebar_position: 4
sidebar_label: "4 - Parquet Datasets"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/004%20-%20Parquet%20Datasets.ipynb
---

# Parquet Datasets

> This page is generated from [`tutorials/004 - Parquet Datasets.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/004%20-%20Parquet%20Datasets.ipynb). Open it in Jupyter to run it yourself.
awswrangler has 3 different write modes to store Parquet Datasets on Amazon S3.

- **append** (Default)

    Only adds new files without any delete.
    
- **overwrite**

    Deletes everything in the target directory and then add new files. If writing new files fails for any reason, old files are _not_ restored.
    
- **overwrite_partitions** (Partition Upsert)

    Only deletes the paths of partitions that should be updated and then writes the new partitions files. It's like a "partition Upsert".

```python
from datetime import date

import pandas as pd

import awswrangler as wr
```

## Enter your bucket name:

```python
import getpass

bucket = getpass.getpass()
path = f"s3://{bucket}/dataset/"
```

```text
 ············
```

## Creating the Dataset

```python
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"], "date": [date(2020, 1, 1), date(2020, 1, 2)]})

wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")

wr.s3.read_parquet(path, dataset=True)
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   boo  2020-01-02
```

## Appending

```python
df = pd.DataFrame({"id": [3], "value": ["bar"], "date": [date(2020, 1, 3)]})

wr.s3.to_parquet(df=df, path=path, dataset=True, mode="append")

wr.s3.read_parquet(path, dataset=True)
```

```text
   id value        date
0   3   bar  2020-01-03
1   1   foo  2020-01-01
2   2   boo  2020-01-02
```

## Overwriting

```python
wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")

wr.s3.read_parquet(path, dataset=True)
```

```text
   id value        date
0   3   bar  2020-01-03
```

## Creating a **Partitioned** Dataset

```python
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"], "date": [date(2020, 1, 1), date(2020, 1, 2)]})

wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", partition_cols=["date"])

wr.s3.read_parquet(path, dataset=True)
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   boo  2020-01-02
```

## Upserting partitions (overwrite_partitions)

```python
df = pd.DataFrame({"id": [2, 3], "value": ["xoo", "bar"], "date": [date(2020, 1, 2), date(2020, 1, 3)]})

wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite_partitions", partition_cols=["date"])

wr.s3.read_parquet(path, dataset=True)
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   xoo  2020-01-02
2   3   bar  2020-01-03
```

## BONUS - Glue/Athena integration

```python
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"], "date": [date(2020, 1, 1), date(2020, 1, 2)]})

wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database="aws_sdk_pandas", table="my_table")

wr.athena.read_sql_query("SELECT * FROM my_table", database="aws_sdk_pandas")
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   boo  2020-01-02
```
