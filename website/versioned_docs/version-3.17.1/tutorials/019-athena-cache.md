---
id: 019-athena-cache
title: "Athena Cache"
sidebar_position: 19
sidebar_label: "19 - Athena Cache"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/019%20-%20Athena%20Cache.ipynb
---

# Athena Cache

> This page is generated from [`tutorials/019 - Athena Cache.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/019%20-%20Athena%20Cache.ipynb). Open it in Jupyter to run it yourself.
[awswrangler](https://github.com/aws/aws-sdk-pandas) has a cache strategy that is disabled by default and can be enabled by passing `max_cache_seconds` bigger than 0 as part of the `athena_cache_settings` parameter. This cache strategy for Amazon Athena can help you to **decrease query times and costs**.

When calling `read_sql_query`, instead of just running the query, we now can verify if the query has been run before. If so, and this last run was within `max_cache_seconds` (a new parameter to `read_sql_query`), we return the same results as last time if they are still available in S3. We have seen this increase performance more than 100x, but the potential is pretty much infinite.

The detailed approach is:
- When `read_sql_query` is called with `max_cache_seconds > 0` (it defaults to 0), we check for the last queries run by the same workgroup (the most we can get without pagination).
- By default it will check the last 50 queries, but you can customize it through the `max_cache_query_inspections` argument.
- We then sort those queries based on CompletionDateTime, descending
- For each of those queries, we check if their CompletionDateTime is still within the `max_cache_seconds` window. If so, we check if the query string is the same as now (with some smart heuristics to guarantee coverage over both `ctas_approach`es). If they are the same, we check if the last one's results are still on S3, and then return them instead of re-running the query.
- During the whole cache resolution phase, if there is anything wrong, the logic falls back to the usual `read_sql_query` path.

*P.S. The `cache scope is bounded for the current workgroup`, so you will be able to reuse queries results from others colleagues running in the same environment.*

```python
import awswrangler as wr
```

## Enter your bucket name:

```python
import getpass

bucket = getpass.getpass()
path = f"s3://{bucket}/data/"
```

## Checking/Creating Glue Catalog Databases

```python
if "awswrangler_test" not in wr.catalog.databases().values:
    wr.catalog.create_database("awswrangler_test")
```

### Creating a Parquet Table from the NOAA's CSV files

[Reference](https://registry.opendata.aws/noaa-ghcn/)

```python
cols = ["id", "dt", "element", "value", "m_flag", "q_flag", "s_flag", "obs_time"]

df = wr.s3.read_csv(path="s3://noaa-ghcn-pds/csv/by_year/1865.csv", names=cols, parse_dates=["dt", "obs_time"])

df
```

```text
                id        dt  element       value  m_flag  q_flag  s_flag  \
0               ID      DATE  ELEMENT  DATA_VALUE  M_FLAG  Q_FLAG  S_FLAG   
1      AGE00135039  18650101     PRCP           0     NaN     NaN       E   
2      ASN00019036  18650101     PRCP           0     NaN     NaN       a   
3      ASN00021001  18650101     PRCP           0     NaN     NaN       a   
4      ASN00021010  18650101     PRCP           0     NaN     NaN       a   
...            ...       ...      ...         ...     ...     ...     ...   
37918  USC00288878  18651231     TMIN         -44     NaN     NaN       6   
37919  USC00288878  18651231     PRCP           0       P     NaN       6   
37920  USC00288878  18651231     SNOW           0       P     NaN       6   
37921  USC00361920  18651231     PRCP           0     NaN     NaN       F   
37922  USP00CA0001  18651231     PRCP           0     NaN     NaN       F   

       obs_time  
0      OBS_TIME  
1           NaN  
2           NaN  
3           NaN  
4           NaN  
...         ...  
37918       NaN  
37919       NaN  
37920       NaN  
37921       NaN  
37922       NaN  

[37923 rows x 8 columns]
```

```python
wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database="awswrangler_test", table="noaa")
```

```python
wr.catalog.table(database="awswrangler_test", table="noaa")
```

```text
  Column Name    Type  Partition Comment
0          id  string      False        
1          dt  string      False        
2     element  string      False        
3       value  string      False        
4      m_flag  string      False        
5      q_flag  string      False        
6      s_flag  string      False        
7    obs_time  string      False
```

## The test query

The more computational resources the query needs, the more the cache will help you. That's why we're doing it using this long running query.

```python
query = """
SELECT
    n1.element,
    count(1) as cnt
FROM
    noaa n1
JOIN
    noaa n2
ON
    n1.id = n2.id
GROUP BY
    n1.element
"""
```

## First execution...

```python
%%time

wr.athena.read_sql_query(query, database="awswrangler_test")
```

```text
CPU times: user 1.59 s, sys: 166 ms, total: 1.75 s
Wall time: 5.62 s
```

```text
    element       cnt
0      PRCP  12044499
1      MDTX      1460
2      DATX      1460
3   ELEMENT         1
4      WT01     22260
5      WT03       840
6      DATN      1460
7      DWPR       490
8      TMIN   7012479
9      MDTN      1460
10     MDPR      2683
11     SNOW   1086762
12     DAPR      1330
13     SNWD    783532
14     TMAX   6533103
```

## Second execution with **CACHE** (400x faster)

```python
%%time

wr.athena.read_sql_query(query, database="awswrangler_test", athena_cache_settings={"max_cache_seconds": 900})
```

```text
CPU times: user 689 ms, sys: 68.1 ms, total: 757 ms
Wall time: 1.11 s
```

```text
    element       cnt
0      PRCP  12044499
1      MDTX      1460
2      DATX      1460
3   ELEMENT         1
4      WT01     22260
5      WT03       840
6      DATN      1460
7      DWPR       490
8      TMIN   7012479
9      MDTN      1460
10     MDPR      2683
11     SNOW   1086762
12     DAPR      1330
13     SNWD    783532
14     TMAX   6533103
```

## Allowing awswrangler to inspect up to 500 historical queries to find same result to reuse.

```python
%%time

wr.athena.read_sql_query(
    query,
    database="awswrangler_test",
    athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 500},
)
```

```text
CPU times: user 715 ms, sys: 44.9 ms, total: 760 ms
Wall time: 1.03 s
```

```text
    element       cnt
0      PRCP  12044499
1      MDTX      1460
2      DATX      1460
3   ELEMENT         1
4      WT01     22260
5      WT03       840
6      DATN      1460
7      DWPR       490
8      TMIN   7012479
9      MDTN      1460
10     MDPR      2683
11     SNOW   1086762
12     DAPR      1330
13     SNWD    783532
14     TMAX   6533103
```

## Cleaning Up S3

```python
wr.s3.delete_objects(path)
```

## Delete table

```python
wr.catalog.delete_table_if_exists(database="awswrangler_test", table="noaa")
```

```text
True
```

## Delete Database

```python
wr.catalog.delete_database("awswrangler_test")
```
