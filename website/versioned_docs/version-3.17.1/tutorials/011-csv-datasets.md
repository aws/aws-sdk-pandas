---
id: 011-csv-datasets
title: "CSV Datasets"
sidebar_position: 11
sidebar_label: "11 - CSV Datasets"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/011%20-%20CSV%20Datasets.ipynb
---

# CSV Datasets

> This page is generated from [`tutorials/011 - CSV Datasets.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/011%20-%20CSV%20Datasets.ipynb). Open it in Jupyter to run it yourself.
awswrangler has 3 different write modes to store CSV Datasets on Amazon S3.

- **append** (Default)

    Only adds new files without any delete.
    
- **overwrite**

    Deletes everything in the target directory and then add new files.
    
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

## Checking/Creating Glue Catalog Databases

```python
if "awswrangler_test" not in wr.catalog.databases().values:
    wr.catalog.create_database("awswrangler_test")
```

## Creating the Dataset

```python
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"], "date": [date(2020, 1, 1), date(2020, 1, 2)]})

wr.s3.to_csv(
    df=df, path=path, index=False, dataset=True, mode="overwrite", database="awswrangler_test", table="csv_dataset"
)

wr.athena.read_sql_table(database="awswrangler_test", table="csv_dataset")
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   boo  2020-01-02
```

## Appending

```python
df = pd.DataFrame({"id": [3], "value": ["bar"], "date": [date(2020, 1, 3)]})

wr.s3.to_csv(
    df=df, path=path, index=False, dataset=True, mode="append", database="awswrangler_test", table="csv_dataset"
)

wr.athena.read_sql_table(database="awswrangler_test", table="csv_dataset")
```

```text
   id value        date
0   3   bar  2020-01-03
1   1   foo  2020-01-01
2   2   boo  2020-01-02
```

## Overwriting

```python
wr.s3.to_csv(
    df=df, path=path, index=False, dataset=True, mode="overwrite", database="awswrangler_test", table="csv_dataset"
)

wr.athena.read_sql_table(database="awswrangler_test", table="csv_dataset")
```

```text
   id value        date
0   3   bar  2020-01-03
```

## Creating a **Partitioned** Dataset

```python
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"], "date": [date(2020, 1, 1), date(2020, 1, 2)]})

wr.s3.to_csv(
    df=df,
    path=path,
    index=False,
    dataset=True,
    mode="overwrite",
    database="awswrangler_test",
    table="csv_dataset",
    partition_cols=["date"],
)

wr.athena.read_sql_table(database="awswrangler_test", table="csv_dataset")
```

```text
   id value        date
0   2   boo  2020-01-02
1   1   foo  2020-01-01
```

## Upserting partitions (overwrite_partitions)

```python
df = pd.DataFrame({"id": [2, 3], "value": ["xoo", "bar"], "date": [date(2020, 1, 2), date(2020, 1, 3)]})

wr.s3.to_csv(
    df=df,
    path=path,
    index=False,
    dataset=True,
    mode="overwrite_partitions",
    database="awswrangler_test",
    table="csv_dataset",
    partition_cols=["date"],
)

wr.athena.read_sql_table(database="awswrangler_test", table="csv_dataset")
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   xoo  2020-01-02
0   3   bar  2020-01-03
```

## BONUS - Glue/Athena integration

```python
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"], "date": [date(2020, 1, 1), date(2020, 1, 2)]})

wr.s3.to_csv(
    df=df,
    path=path,
    dataset=True,
    index=False,
    mode="overwrite",
    database="aws_sdk_pandas",
    table="my_table",
    compression="gzip",
)

wr.athena.read_sql_query("SELECT * FROM my_table", database="aws_sdk_pandas")
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   boo  2020-01-02
```
