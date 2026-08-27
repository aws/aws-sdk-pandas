---
id: 024-athena-query-metadata
title: "Athena Query Metadata"
sidebar_position: 24
sidebar_label: "24 - Athena Query Metadata"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/024%20-%20Athena%20Query%20Metadata.ipynb
---

# Athena Query Metadata

> This page is generated from [`tutorials/024 - Athena Query Metadata.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/024%20-%20Athena%20Query%20Metadata.ipynb). Open it in Jupyter to run it yourself.
For `wr.athena.read_sql_query()` and `wr.athena.read_sql_table()` the resulting DataFrame (or every DataFrame in the returned Iterator for chunked queries) have a `query_metadata` attribute, which brings the query result metadata returned by Boto3/Athena.

The expected `query_metadata` format is the same returned by:

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_query_execution

## Environment Variables

```python
%env WR_DATABASE=default
```

```text
env: WR_DATABASE=default
```

```python
import awswrangler as wr
```

```python
df = wr.athena.read_sql_query("SELECT 1 AS foo")

df
```

```text
   foo
0    1
```

## Getting statistics from query metadata

```python
print(f"DataScannedInBytes:            {df.query_metadata['Statistics']['DataScannedInBytes']}")
print(f"TotalExecutionTimeInMillis:    {df.query_metadata['Statistics']['TotalExecutionTimeInMillis']}")
print(f"QueryQueueTimeInMillis:        {df.query_metadata['Statistics']['QueryQueueTimeInMillis']}")
print(f"QueryPlanningTimeInMillis:     {df.query_metadata['Statistics']['QueryPlanningTimeInMillis']}")
print(f"ServiceProcessingTimeInMillis: {df.query_metadata['Statistics']['ServiceProcessingTimeInMillis']}")
```

```text
DataScannedInBytes:            0
TotalExecutionTimeInMillis:    2311
QueryQueueTimeInMillis:        121
QueryPlanningTimeInMillis:     250
ServiceProcessingTimeInMillis: 37
```
