---
id: 023-flexible-partitions-filter
title: "Flexible Partitions Filter"
sidebar_position: 23
sidebar_label: "23 - Flexible Partitions Filter"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/023%20-%20Flexible%20Partitions%20Filter.ipynb
---

# Flexible Partitions Filter

> This page is generated from [`tutorials/023 - Flexible Partitions Filter.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/023%20-%20Flexible%20Partitions%20Filter.ipynb). Open it in Jupyter to run it yourself.
* `partition_filter` argument:

        - Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        - This function MUST receive a single argument (Dict[str, str]) where keys are partitions names and values are partitions values.
        - This function MUST return a bool, True to read the partition or False to ignore it.
        - Ignored if `dataset=False`.
        

*P.S. Check the [function API doc](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/api.html) to see it has some argument that can be configured through Global configurations.*

```python
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

## Creating the Dataset (Parquet)

```python
df = pd.DataFrame(
    {
        "id": [1, 2, 3],
        "value": ["foo", "boo", "bar"],
    }
)

wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", partition_cols=["value"])

wr.s3.read_parquet(path, dataset=True)
```

```text
   id value
0   3   bar
1   2   boo
2   1   foo
```

## Parquet Example 1

```python
def my_filter(x):
    return x["value"].endswith("oo")


wr.s3.read_parquet(path, dataset=True, partition_filter=my_filter)
```

```text
   id value
0   2   boo
1   1   foo
```

## Parquet Example 2

```python
from Levenshtein import distance


def my_filter(partitions):
    return distance("boo", partitions["value"]) <= 1


wr.s3.read_parquet(path, dataset=True, partition_filter=my_filter)
```

```text
   id value
0   2   boo
1   1   foo
```

## Creating the Dataset (CSV)

```python
df = pd.DataFrame(
    {
        "id": [1, 2, 3],
        "value": ["foo", "boo", "bar"],
    }
)

wr.s3.to_csv(
    df=df, path=path, dataset=True, mode="overwrite", partition_cols=["value"], compression="gzip", index=False
)

wr.s3.read_csv(path, dataset=True)
```

```text
   id value
0   3   bar
1   2   boo
2   1   foo
```

## CSV Example 1

```python
def my_filter(x):
    return x["value"].endswith("oo")


wr.s3.read_csv(path, dataset=True, partition_filter=my_filter)
```

```text
   id value
0   2   boo
1   1   foo
```

## CSV Example 2

```python
from Levenshtein import distance


def my_filter(partitions):
    return distance("boo", partitions["value"]) <= 1


wr.s3.read_csv(path, dataset=True, partition_filter=my_filter)
```

```text
   id value
0   2   boo
1   1   foo
```
