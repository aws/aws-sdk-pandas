---
id: 017-partition-projection
title: "Partition Projection"
sidebar_position: 17
sidebar_label: "17 - Partition Projection"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/017%20-%20Partition%20Projection.ipynb
---

# Partition Projection

> This page is generated from [`tutorials/017 - Partition Projection.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/017%20-%20Partition%20Projection.ipynb). Open it in Jupyter to run it yourself.
https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html

```python
import getpass
from datetime import datetime

import pandas as pd

import awswrangler as wr
```

## Enter your bucket name:

```python
bucket = getpass.getpass()
```

```text
 ···········································
```

## Integer projection

```python
df = pd.DataFrame({"value": [1, 2, 3], "year": [2019, 2020, 2021], "month": [10, 11, 12], "day": [25, 26, 27]})

df
```

```text
   value  year  month  day
0      1  2019     10   25
1      2  2020     11   26
2      3  2021     12   27
```

```python
wr.s3.to_parquet(
    df=df,
    path=f"s3://{bucket}/table_integer/",
    dataset=True,
    partition_cols=["year", "month", "day"],
    database="default",
    table="table_integer",
    athena_partition_projection_settings={
        "projection_types": {"year": "integer", "month": "integer", "day": "integer"},
        "projection_ranges": {"year": "2000,2025", "month": "1,12", "day": "1,31"},
    },
)
```

```python
wr.athena.read_sql_query("SELECT * FROM table_integer", database="default")
```

```text
   value  year  month  day
0      3  2021     12   27
1      2  2020     11   26
2      1  2019     10   25
```

## Enum projection

```python
df = pd.DataFrame(
    {
        "value": [1, 2, 3],
        "city": ["São Paulo", "Tokio", "Seattle"],
    }
)

df
```

```text
   value       city
0      1  São Paulo
1      2      Tokio
2      3    Seattle
```

```python
wr.s3.to_parquet(
    df=df,
    path=f"s3://{bucket}/table_enum/",
    dataset=True,
    partition_cols=["city"],
    database="default",
    table="table_enum",
    athena_partition_projection_settings={
        "projection_types": {
            "city": "enum",
        },
        "projection_values": {"city": "São Paulo,Tokio,Seattle"},
    },
)
```

```python
wr.athena.read_sql_query("SELECT * FROM table_enum", database="default")
```

```text
   value       city
0      1  São Paulo
1      3    Seattle
2      2      Tokio
```

## Date projection

```python
def ts(x):
    return datetime.strptime(x, "%Y-%m-%d %H:%M:%S")


def dt(x):
    return datetime.strptime(x, "%Y-%m-%d").date()


df = pd.DataFrame(
    {
        "value": [1, 2, 3],
        "dt": [dt("2020-01-01"), dt("2020-01-02"), dt("2020-01-03")],
        "ts": [ts("2020-01-01 00:00:00"), ts("2020-01-01 00:00:01"), ts("2020-01-01 00:00:02")],
    }
)

df
```

```text
   value          dt                  ts
0      1  2020-01-01 2020-01-01 00:00:00
1      2  2020-01-02 2020-01-01 00:00:01
2      3  2020-01-03 2020-01-01 00:00:02
```

```python
wr.s3.to_parquet(
    df=df,
    path=f"s3://{bucket}/table_date/",
    dataset=True,
    partition_cols=["dt", "ts"],
    database="default",
    table="table_date",
    athena_partition_projection_settings={
        "projection_types": {
            "dt": "date",
            "ts": "date",
        },
        "projection_ranges": {"dt": "2020-01-01,2020-01-03", "ts": "2020-01-01 00:00:00,2020-01-01 00:00:02"},
    },
)
```

```python
wr.athena.read_sql_query("SELECT * FROM table_date", database="default")
```

```text
   value          dt                  ts
0      1  2020-01-01 2020-01-01 00:00:00
1      2  2020-01-02 2020-01-01 00:00:01
2      3  2020-01-03 2020-01-01 00:00:02
```

## Injected projection

```python
df = pd.DataFrame(
    {
        "value": [1, 2, 3],
        "uuid": [
            "761e2488-a078-11ea-bb37-0242ac130002",
            "b89ed095-8179-4635-9537-88592c0f6bc3",
            "87adc586-ce88-4f0a-b1c8-bf8e00d32249",
        ],
    }
)

df
```

```text
   value                                  uuid
0      1  761e2488-a078-11ea-bb37-0242ac130002
1      2  b89ed095-8179-4635-9537-88592c0f6bc3
2      3  87adc586-ce88-4f0a-b1c8-bf8e00d32249
```

```python
wr.s3.to_parquet(
    df=df,
    path=f"s3://{bucket}/table_injected/",
    dataset=True,
    partition_cols=["uuid"],
    database="default",
    table="table_injected",
    athena_partition_projection_settings={
        "projection_types": {
            "uuid": "injected",
        }
    },
)
```

```python
wr.athena.read_sql_query(
    sql="SELECT * FROM table_injected WHERE uuid='b89ed095-8179-4635-9537-88592c0f6bc3'", database="default"
)
```

```text
   value                                  uuid
0      2  b89ed095-8179-4635-9537-88592c0f6bc3
```

## Cleaning Up

```python
wr.s3.delete_objects(f"s3://{bucket}/table_integer/")
wr.s3.delete_objects(f"s3://{bucket}/table_enum/")
wr.s3.delete_objects(f"s3://{bucket}/table_date/")
wr.s3.delete_objects(f"s3://{bucket}/table_injected/")
```

```python
wr.catalog.delete_table_if_exists(table="table_integer", database="default")
wr.catalog.delete_table_if_exists(table="table_enum", database="default")
wr.catalog.delete_table_if_exists(table="table_date", database="default")
wr.catalog.delete_table_if_exists(table="table_injected", database="default")
```
