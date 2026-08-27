---
id: 014-schema-evolution
title: "Schema Evolution"
sidebar_position: 14
sidebar_label: "14 - Schema Evolution"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/014%20-%20Schema%20Evolution.ipynb
---

# Schema Evolution

> This page is generated from [`tutorials/014 - Schema Evolution.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/014%20-%20Schema%20Evolution.ipynb). Open it in Jupyter to run it yourself.
awswrangler supports new **columns** on Parquet and CSV datasets through:

- [wr.s3.to_parquet()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet)
- [wr.s3.store_parquet_metadata()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.s3.store_parquet_metadata.html#awswrangler.s3.store_parquet_metadata) i.e. "Crawler"
- [wr.s3.to_csv()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.s3.to_csv.html#awswrangler.s3.to_csv)

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
 ···········································
```

## Creating the Dataset
### Parquet Create

```python
df = pd.DataFrame(
    {
        "id": [1, 2],
        "value": ["foo", "boo"],
    }
)

wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database="aws_sdk_pandas", table="my_table")

wr.s3.read_parquet(path, dataset=True)
```

```text
   id value
0   1   foo
1   2   boo
```

### CSV Create

```python
df = pd.DataFrame(
    {
        "id": [1, 2],
        "value": ["foo", "boo"],
    }
)

wr.s3.to_csv(df=df, path=path, dataset=True, mode="overwrite", database="aws_sdk_pandas", table="my_table")

wr.s3.read_csv(path, dataset=True)
```

### Schema Version 0 on Glue Catalog (AWS Console)

![Glue Console](/img/tutorials/glue_catalog_version_0.png "Glue Console")

## Appending with NEW COLUMNS
### Parquet Append

```python
df = pd.DataFrame(
    {"id": [3, 4], "value": ["bar", None], "date": [date(2020, 1, 3), date(2020, 1, 4)], "flag": [True, False]}
)

wr.s3.to_parquet(
    df=df,
    path=path,
    dataset=True,
    mode="append",
    database="aws_sdk_pandas",
    table="my_table",
    catalog_versioning=True,  # Optional
)

wr.s3.read_parquet(path, dataset=True, validate_schema=False)
```

```text
   id value        date   flag
0   3   bar  2020-01-03   True
1   4  None  2020-01-04  False
2   1   foo         NaN    NaN
3   2   boo         NaN    NaN
```

### CSV Append

Note: for CSV datasets due to [column ordering](https://docs.aws.amazon.com/athena/latest/ug/types-of-updates.html#updates-add-columns-beginning-middle-of-table), by default, schema evolution is disabled. Enable it by passing `schema_evolution=True` flag

```python
df = pd.DataFrame(
    {"id": [3, 4], "value": ["bar", None], "date": [date(2020, 1, 3), date(2020, 1, 4)], "flag": [True, False]}
)

wr.s3.to_csv(
    df=df,
    path=path,
    dataset=True,
    mode="append",
    database="aws_sdk_pandas",
    table="my_table",
    schema_evolution=True,
    catalog_versioning=True,  # Optional
)

wr.s3.read_csv(path, dataset=True, validate_schema=False)
```

### Schema Version 1 on Glue Catalog (AWS Console)

![Glue Console](/img/tutorials/glue_catalog_version_1.png "Glue Console")

## Reading from Athena

```python
wr.athena.read_sql_table(table="my_table", database="aws_sdk_pandas")
```

```text
   id value        date   flag
0   3   bar  2020-01-03   True
1   4  None  2020-01-04  False
2   1   foo        None   <NA>
3   2   boo        None   <NA>
```

## Cleaning Up

```python
wr.s3.delete_objects(path)
wr.catalog.delete_table_if_exists(table="my_table", database="aws_sdk_pandas")
```

```text
True
```
