---
id: 022-writing-partitions-concurrently
title: "Writing Partitions Concurrently"
sidebar_position: 22
sidebar_label: "22 - Writing Partitions Concurrently"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/022%20-%20Writing%20Partitions%20Concurrently.ipynb
---

# Writing Partitions Concurrently

> This page is generated from [`tutorials/022 - Writing Partitions Concurrently.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/022%20-%20Writing%20Partitions%20Concurrently.ipynb). Open it in Jupyter to run it yourself.
* `concurrent_partitioning` argument:

        If True will increase the parallelism level during the partitions writing. It will decrease the
        writing time and increase memory usage.

*P.S. Check the [function API doc](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/api.html) to see it has some argument that can be configured through Global configurations.*

```python
%reload_ext memory_profiler

import awswrangler as wr
```

## Enter your bucket name:

```python
import getpass

bucket = getpass.getpass()
path = f"s3://{bucket}/data/"
```

```text
 ············
```

## Reading 4 GB of CSV from NOAA's historical data and creating a year column

```python
noaa_path = "s3://noaa-ghcn-pds/csv/by_year/193"

cols = ["id", "dt", "element", "value", "m_flag", "q_flag", "s_flag", "obs_time"]
dates = ["dt", "obs_time"]
dtype = {x: "category" for x in ["element", "m_flag", "q_flag", "s_flag"]}

df = wr.s3.read_csv(noaa_path, names=cols, parse_dates=dates, dtype=dtype)

df["year"] = df["dt"].dt.year

print(f"Number of rows: {len(df.index)}")
print(f"Number of columns: {len(df.columns)}")
```

```text
Number of rows: 125407761
Number of columns: 9
```

## Default Writing

```python
%%time
%%memit

wr.s3.to_parquet(
    df=df,
    path=path,
    dataset=True,
    mode="overwrite",
    partition_cols=["year"],
)
```

```text
peak memory: 22169.04 MiB, increment: 11119.68 MiB
CPU times: user 49 s, sys: 12.5 s, total: 1min 1s
Wall time: 1min 11s
```

## Concurrent Partitioning (Decreasing writing time, but increasing memory usage)

```python
%%time
%%memit

wr.s3.to_parquet(
    df=df,
    path=path,
    dataset=True,
    mode="overwrite",
    partition_cols=["year"],
    concurrent_partitioning=True  # <-----
)
```

```text
peak memory: 27819.48 MiB, increment: 15743.30 MiB
CPU times: user 52.3 s, sys: 13.6 s, total: 1min 5s
Wall time: 41.6 s
```
