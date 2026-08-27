---
id: 026-amazon-timestream
title: "Amazon Timestream"
sidebar_position: 26
sidebar_label: "26 - Amazon Timestream"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/026%20-%20Amazon%20Timestream.ipynb
---

# Amazon Timestream

> This page is generated from [`tutorials/026 - Amazon Timestream.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/026%20-%20Amazon%20Timestream.ipynb). Open it in Jupyter to run it yourself.
## Creating resources

```python
from datetime import datetime

import pandas as pd

import awswrangler as wr

database = "sampleDB"
table_1 = "sampleTable1"
table_2 = "sampleTable2"
wr.timestream.create_database(database)
wr.timestream.create_table(database, table_1, memory_retention_hours=1, magnetic_retention_days=1)
wr.timestream.create_table(database, table_2, memory_retention_hours=1, magnetic_retention_days=1)
```

## Write

### Single measure WriteRecord

```python
df = pd.DataFrame(
    {
        "time": [datetime.now()] * 3,
        "dim0": ["foo", "boo", "bar"],
        "dim1": [1, 2, 3],
        "measure": [1.0, 1.1, 1.2],
    }
)

rejected_records = wr.timestream.write(
    df=df,
    database=database,
    table=table_1,
    time_col="time",
    measure_col="measure",
    dimensions_cols=["dim0", "dim1"],
)

print(f"Number of rejected records: {len(rejected_records)}")
```

```text
Number of rejected records: 0
```

### Multi measure WriteRecord

```python
df = pd.DataFrame(
    {
        "time": [datetime.now()] * 3,
        "measure_1": ["10", "20", "30"],
        "measure_2": ["100", "200", "300"],
        "measure_3": ["1000", "2000", "3000"],
        "tag": ["tag123", "tag456", "tag789"],
    }
)
rejected_records = wr.timestream.write(
    df=df,
    database=database,
    table=table_2,
    time_col="time",
    measure_col=["measure_1", "measure_2", "measure_3"],
    dimensions_cols=["tag"],
)

print(f"Number of rejected records: {len(rejected_records)}")
```

## Query

```python
wr.timestream.query(
    f'SELECT time, measure_value::double, dim0, dim1 FROM "{database}"."{table_1}" ORDER BY time DESC LIMIT 3'
)
```

```text
                     time  measure_value::double dim0 dim1
0 2020-12-08 19:15:32.468                    1.0  foo    1
1 2020-12-08 19:15:32.468                    1.2  bar    3
2 2020-12-08 19:15:32.468                    1.1  boo    2
```

## Unload

```python
df = wr.timestream.unload(
    sql=f'SELECT time, measure_value, dim0, dim1 FROM "{database}"."{table_1}"',
    path="s3://bucket/extracted_parquet_files/",
    partition_cols=["dim1"],
)
```

## Deleting resources

```python
wr.timestream.delete_table(database, table_1)
wr.timestream.delete_table(database, table_2)
wr.timestream.delete_database(database)
```
