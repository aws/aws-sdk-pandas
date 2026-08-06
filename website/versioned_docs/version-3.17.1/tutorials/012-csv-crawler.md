---
id: 012-csv-crawler
title: "CSV Crawler"
sidebar_position: 12
sidebar_label: "12 - CSV Crawler"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/012%20-%20CSV%20Crawler.ipynb
---

# CSV Crawler

> This page is generated from [`tutorials/012 - CSV Crawler.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/012%20-%20CSV%20Crawler.ipynb). Open it in Jupyter to run it yourself.
[awswrangler](https://github.com/aws/aws-sdk-pandas) can extract only the metadata from a Pandas DataFrame and then add it can be added to Glue Catalog as a table.

```python
from datetime import datetime

import pandas as pd

import awswrangler as wr
```

## Enter your bucket name:

```python
import getpass

bucket = getpass.getpass()
path = f"s3://{bucket}/csv_crawler/"
```

```text
 ············
```

### Creating a Pandas DataFrame

```python
ts = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")  # noqa
dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()  # noqa

df = pd.DataFrame(
    {
        "id": [1, 2, 3],
        "string": ["foo", None, "boo"],
        "float": [1.0, None, 2.0],
        "date": [dt("2020-01-01"), None, dt("2020-01-02")],
        "timestamp": [ts("2020-01-01 00:00:00.0"), None, ts("2020-01-02 00:00:01.0")],
        "bool": [True, None, False],
        "par0": [1, 1, 2],
        "par1": ["a", "b", "b"],
    }
)

df
```

```text
   id string  float        date           timestamp   bool  par0 par1
0   1    foo    1.0  2020-01-01 2020-01-01 00:00:00   True     1    a
1   2   None    NaN        None                 NaT   None     1    b
2   3    boo    2.0  2020-01-02 2020-01-02 00:00:01  False     2    b
```

### Extracting the metadata

```python
columns_types, partitions_types = wr.catalog.extract_athena_types(
    df=df, file_format="csv", index=False, partition_cols=["par0", "par1"]
)
```

```python
columns_types
```

```text
{'id': 'bigint',
 'string': 'string',
 'float': 'double',
 'date': 'date',
 'timestamp': 'timestamp',
 'bool': 'boolean'}
```

```python
partitions_types
```

```text
{'par0': 'bigint', 'par1': 'string'}
```

## Creating the table

```python
wr.catalog.create_csv_table(
    table="csv_crawler",
    database="awswrangler_test",
    path=path,
    partitions_types=partitions_types,
    columns_types=columns_types,
)
```

## Checking

```python
wr.catalog.table(database="awswrangler_test", table="csv_crawler")
```

```text
  Column Name       Type  Partition Comment
0          id     bigint      False        
1      string     string      False        
2       float     double      False        
3        date       date      False        
4   timestamp  timestamp      False        
5        bool    boolean      False        
6        par0     bigint       True        
7        par1     string       True
```

## We can still using the extracted metadata to ensure all data types consistence to new data

```python
df = pd.DataFrame(
    {
        "id": [1],
        "string": ["1"],
        "float": [1],
        "date": [ts("2020-01-01 00:00:00.0")],
        "timestamp": [dt("2020-01-02")],
        "bool": [1],
        "par0": [1],
        "par1": ["a"],
    }
)

df
```

```text
   id string  float       date   timestamp  bool  par0 par1
0   1      1      1 2020-01-01  2020-01-02     1     1    a
```

```python
res = wr.s3.to_csv(
    df=df,
    path=path,
    index=False,
    dataset=True,
    database="awswrangler_test",
    table="csv_crawler",
    partition_cols=["par0", "par1"],
    dtype=columns_types,
)
```

## You can also extract the metadata directly from the Catalog if you want

```python
dtype = wr.catalog.get_table_types(database="awswrangler_test", table="csv_crawler")
```

```python
res = wr.s3.to_csv(
    df=df,
    path=path,
    index=False,
    dataset=True,
    database="awswrangler_test",
    table="csv_crawler",
    partition_cols=["par0", "par1"],
    dtype=dtype,
)
```

## Checking out

```python
df = wr.athena.read_sql_table(database="awswrangler_test", table="csv_crawler")

df
```

```text
   id string  float  date  timestamp  bool  par0 par1
0   1      1    1.0  None 2020-01-02  True     1    a
1   1      1    1.0  None 2020-01-02  True     1    a
```

```python
df.dtypes
```

```text
id                    Int64
string               string
float               float64
date                 object
timestamp    datetime64[ns]
bool                boolean
par0                  Int64
par1                 string
dtype: object
```

## Cleaning Up S3

```python
wr.s3.delete_objects(path)
```

## Cleaning Up the Database

```python
wr.catalog.delete_table_if_exists(database="awswrangler_test", table="csv_crawler")
```

```text
True
```
