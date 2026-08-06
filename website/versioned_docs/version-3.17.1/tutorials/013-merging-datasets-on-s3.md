---
id: 013-merging-datasets-on-s3
title: "Merging Datasets on S3"
sidebar_position: 13
sidebar_label: "13 - Merging Datasets on S3"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/013%20-%20Merging%20Datasets%20on%20S3.ipynb
---

# Merging Datasets on S3

> This page is generated from [`tutorials/013 - Merging Datasets on S3.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/013%20-%20Merging%20Datasets%20on%20S3.ipynb). Open it in Jupyter to run it yourself.
awswrangler has 3 different copy modes to store Parquet Datasets on Amazon S3.

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
path1 = f"s3://{bucket}/dataset1/"
path2 = f"s3://{bucket}/dataset2/"
```

```text
 ············
```

## Creating Dataset 1

```python
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"], "date": [date(2020, 1, 1), date(2020, 1, 2)]})

wr.s3.to_parquet(df=df, path=path1, dataset=True, mode="overwrite", partition_cols=["date"])

wr.s3.read_parquet(path1, dataset=True)
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   boo  2020-01-02
```

## Creating Dataset 2

```python
df = pd.DataFrame({"id": [2, 3], "value": ["xoo", "bar"], "date": [date(2020, 1, 2), date(2020, 1, 3)]})

dataset2_files = wr.s3.to_parquet(df=df, path=path2, dataset=True, mode="overwrite", partition_cols=["date"])["paths"]

wr.s3.read_parquet(path2, dataset=True)
```

```text
   id value        date
0   2   xoo  2020-01-02
1   3   bar  2020-01-03
```

## Merging (Dataset 2 -> Dataset 1) (APPEND)

```python
wr.s3.merge_datasets(source_path=path2, target_path=path1, mode="append")

wr.s3.read_parquet(path1, dataset=True)
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   xoo  2020-01-02
2   2   boo  2020-01-02
3   3   bar  2020-01-03
```

## Merging (Dataset 2 -> Dataset 1) (OVERWRITE_PARTITIONS)

```python
wr.s3.merge_datasets(source_path=path2, target_path=path1, mode="overwrite_partitions")

wr.s3.read_parquet(path1, dataset=True)
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   xoo  2020-01-02
2   3   bar  2020-01-03
```

## Merging (Dataset 2 -> Dataset 1) (OVERWRITE)

```python
wr.s3.merge_datasets(source_path=path2, target_path=path1, mode="overwrite")

wr.s3.read_parquet(path1, dataset=True)
```

```text
   id value        date
0   2   xoo  2020-01-02
1   3   bar  2020-01-03
```

## Cleaning Up

```python
wr.s3.delete_objects(path1)
wr.s3.delete_objects(path2)
```
