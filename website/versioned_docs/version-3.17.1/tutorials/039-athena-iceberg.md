---
id: 039-athena-iceberg
title: "Athena Iceberg"
sidebar_position: 39
sidebar_label: "39 - Athena Iceberg"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/039%20-%20Athena%20Iceberg.ipynb
---

# Athena Iceberg

> This page is generated from [`tutorials/039 - Athena Iceberg.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/039%20-%20Athena%20Iceberg.ipynb). Open it in Jupyter to run it yourself.
Athena supports read, time travel, write, and DDL queries for Apache Iceberg tables that use the Apache Parquet format for data and the AWS Glue catalog for their metastore. More in [User Guide](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html).

### Create Iceberg table

```python
import getpass

bucket_name = getpass.getpass()
```

```python
import awswrangler as wr

glue_database = "aws_sdk_pandas"
glue_table = "iceberg_test"
path = f"s3://{bucket_name}/iceberg_test/"
temp_path = f"s3://{bucket_name}/iceberg_test_temp/"

# Cleanup table before create
wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
```

```text
True
```

### Create table & insert data

It is possible to insert Pandas data frame into Iceberg table using `wr.athena.to_iceberg`. If the table does not exist, it will be created:

```python
import pandas as pd

df = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Lily", "Richard"]})

wr.athena.to_iceberg(
    df=df,
    database=glue_database,
    table=glue_table,
    table_location=path,
    temp_path=temp_path,
)
```

Alternatively, it is also possible to insert by directly running `INSERT INTO ... VALUES`:

```python
wr.athena.start_query_execution(
    sql=f"INSERT INTO {glue_table} VALUES (1,'John'), (2, 'Lily'), (3, 'Richard')",
    database=glue_database,
    wait=True,
)
```

```text
{'QueryExecutionId': 'e339fcd2-9db1-43ac-bb9e-9730e6395b51',
 'Query': "INSERT INTO iceberg_test VALUES (1,'John'), (2, 'Lily'), (3, 'Richard')",
 'StatementType': 'DML',
 'ResultConfiguration': {'OutputLocation': 's3://aws-athena-query-results-...-us-east-1/e339fcd2-9db1-43ac-bb9e-9730e6395b51'},
 'ResultReuseConfiguration': {'ResultReuseByAgeConfiguration': {'Enabled': False}},
 'QueryExecutionContext': {'Database': 'aws_sdk_pandas'},
 'Status': {'State': 'SUCCEEDED',
  'SubmissionDateTime': datetime.datetime(2023, 3, 16, 10, 40, 8, 612000, tzinfo=tzlocal()),
  'CompletionDateTime': datetime.datetime(2023, 3, 16, 10, 40, 11, 143000, tzinfo=tzlocal())},
 'Statistics': {'EngineExecutionTimeInMillis': 2242,
  'DataScannedInBytes': 0,
  'DataManifestLocation': 's3://aws-athena-query-results-...-us-east-1/e339fcd2-9db1-43ac-bb9e-9730e6395b51-manifest.csv',
  'TotalExecutionTimeInMillis': 2531,
  'QueryQueueTimeInMillis': 241,
  'QueryPlanningTimeInMillis': 179,
  'ServiceProcessingTimeInMillis': 48,
  'ResultReuseInformation': {'ReusedPreviousResult': False}},
 'WorkGroup': 'primary',
 'EngineVersion': {'SelectedEngineVersion': 'Athena engine version 3',
  'EffectiveEngineVersion': 'Athena engine version 3'}}
```

```python
wr.athena.start_query_execution(
    sql=f"INSERT INTO {glue_table} VALUES (4,'Anne'), (5, 'Jacob'), (6, 'Leon')",
    database=glue_database,
    wait=True,
)
```

```text
{'QueryExecutionId': '922c8f02-4c00-4050-b4a7-7016809efa2b',
 'Query': "INSERT INTO iceberg_test VALUES (4,'Anne'), (5, 'Jacob'), (6, 'Leon')",
 'StatementType': 'DML',
 'ResultConfiguration': {'OutputLocation': 's3://aws-athena-query-results-...-us-east-1/922c8f02-4c00-4050-b4a7-7016809efa2b'},
 'ResultReuseConfiguration': {'ResultReuseByAgeConfiguration': {'Enabled': False}},
 'QueryExecutionContext': {'Database': 'aws_sdk_pandas'},
 'Status': {'State': 'SUCCEEDED',
  'SubmissionDateTime': datetime.datetime(2023, 3, 16, 10, 40, 24, 582000, tzinfo=tzlocal()),
  'CompletionDateTime': datetime.datetime(2023, 3, 16, 10, 40, 27, 352000, tzinfo=tzlocal())},
 'Statistics': {'EngineExecutionTimeInMillis': 2414,
  'DataScannedInBytes': 0,
  'DataManifestLocation': 's3://aws-athena-query-results-...-us-east-1/922c8f02-4c00-4050-b4a7-7016809efa2b-manifest.csv',
  'TotalExecutionTimeInMillis': 2770,
  'QueryQueueTimeInMillis': 329,
  'QueryPlanningTimeInMillis': 189,
  'ServiceProcessingTimeInMillis': 27,
  'ResultReuseInformation': {'ReusedPreviousResult': False}},
 'WorkGroup': 'primary',
 'EngineVersion': {'SelectedEngineVersion': 'Athena engine version 3',
  'EffectiveEngineVersion': 'Athena engine version 3'}}
```

### Query

```python
wr.athena.read_sql_query(
    sql=f'SELECT * FROM "{glue_table}"',
    database=glue_database,
    ctas_approach=False,
    unload_approach=False,
)
```

```text
   id     name
0   1     John
1   4     Anne
2   2     Lily
3   3  Richard
4   5    Jacob
5   6     Leon
```

### Read query metadata

In a SELECT query, you can use the following properties after `table_name` to query Iceberg table metadata:

- `$files` Shows a table's current data files

- `$manifests` Shows a table's current file manifests

- `$history` Shows a table's history

- `$partitions` Shows a table's current partitions

```python
wr.athena.read_sql_query(
    sql=f'SELECT * FROM "{glue_table}$files"',
    database=glue_database,
    ctas_approach=False,
    unload_approach=False,
)
```

```text
   content                                          file_path file_format  \
0        0  s3://.../iceberg_test/data/089a...     PARQUET   
1        0  s3://.../iceberg_test/data/5736...     PARQUET   

   record_count  file_size_in_bytes  column_sizes value_counts  \
0             3                 360  {1=48, 2=63}   {1=3, 2=3}   
1             3                 355  {1=48, 2=61}   {1=3, 2=3}   

  null_value_counts nan_value_counts   lower_bounds      upper_bounds  \
0        {1=0, 2=0}               {}  {1=1, 2=John}  {1=3, 2=Richard}   
1        {1=0, 2=0}               {}  {1=4, 2=Anne}     {1=6, 2=Leon}   

  key_metadata split_offsets equality_ids  
0         <NA>           NaN          NaN  
1         <NA>           NaN          NaN
```

```python
wr.athena.read_sql_query(
    sql=f'SELECT * FROM "{glue_table}$manifests"',
    database=glue_database,
    ctas_approach=False,
    unload_approach=False,
)
```

```text
                                                path  length  \
0  s3://.../iceberg_test/metadata/...    6538   
1  s3://.../iceberg_test/metadata/...    6548   

   partition_spec_id    added_snapshot_id  added_data_files_count  \
0                  0  4379263637983206651                       1   
1                  0  2934717851675145063                       1   

   added_rows_count  existing_data_files_count  existing_rows_count  \
0                 3                          0                    0   
1                 3                          0                    0   

   deleted_data_files_count  deleted_rows_count partitions  
0                         0                   0         []  
1                         0                   0         []
```

```python
df = wr.athena.read_sql_query(
    sql=f'SELECT * FROM "{glue_table}$history"',
    database=glue_database,
    ctas_approach=False,
    unload_approach=False,
)

# Save snapshot id
snapshot_id = df.snapshot_id[0]

df
```

```text
                   made_current_at          snapshot_id            parent_id  \
0 2023-03-16 09:40:10.438000+00:00  2934717851675145063                 <NA>   
1 2023-03-16 09:40:26.754000+00:00  4379263637983206651  2934717851675144704   

   is_current_ancestor  
0                 True  
1                 True
```

```python
wr.athena.read_sql_query(
    sql=f'SELECT * FROM "{glue_table}$partitions"',
    database=glue_database,
    ctas_approach=False,
    unload_approach=False,
)
```

```text
   record_count  file_count  total_size  \
0             6           2         715   

                                                data  
0  {id={min=1, max=6, null_count=0, nan_count=nul...
```

### Time travel

```python
wr.athena.read_sql_query(
    sql=f"SELECT * FROM {glue_table} FOR TIMESTAMP AS OF (current_timestamp - interval '5' second)",
    database=glue_database,
)
```

```text
   id     name
0   1     John
1   4     Anne
2   2     Lily
3   3  Richard
4   5    Jacob
5   6     Leon
```

### Version travel

```python
wr.athena.read_sql_query(
    sql=f"SELECT * FROM {glue_table} FOR VERSION AS OF {snapshot_id}",
    database=glue_database,
)
```

```text
   id     name
0   1     John
1   2     Lily
2   3  Richard
```

### Optimize

The `OPTIMIZE table REWRITE DATA` compaction action rewrites data files into a more optimized layout based on their size and number of associated delete files. For syntax and table property details, see [OPTIMIZE](https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html).

```python
wr.athena.start_query_execution(
    sql=f"OPTIMIZE {glue_table} REWRITE DATA USING BIN_PACK",
    database=glue_database,
    wait=True,
)
```

```text
{'QueryExecutionId': '94666790-03ae-42d7-850a-fae99fa79a68',
 'Query': 'OPTIMIZE iceberg_test REWRITE DATA USING BIN_PACK',
 'StatementType': 'DDL',
 'ResultConfiguration': {'OutputLocation': 's3://aws-athena-query-results-...-us-east-1/tables/94666790-03ae-42d7-850a-fae99fa79a68'},
 'ResultReuseConfiguration': {'ResultReuseByAgeConfiguration': {'Enabled': False}},
 'QueryExecutionContext': {'Database': 'aws_sdk_pandas'},
 'Status': {'State': 'SUCCEEDED',
  'SubmissionDateTime': datetime.datetime(2023, 3, 16, 10, 49, 42, 857000, tzinfo=tzlocal()),
  'CompletionDateTime': datetime.datetime(2023, 3, 16, 10, 49, 45, 655000, tzinfo=tzlocal())},
 'Statistics': {'EngineExecutionTimeInMillis': 2622,
  'DataScannedInBytes': 220,
  'DataManifestLocation': 's3://aws-athena-query-results-...-us-east-1/tables/94666790-03ae-42d7-850a-fae99fa79a68-manifest.csv',
  'TotalExecutionTimeInMillis': 2798,
  'QueryQueueTimeInMillis': 124,
  'QueryPlanningTimeInMillis': 252,
  'ServiceProcessingTimeInMillis': 52,
  'ResultReuseInformation': {'ReusedPreviousResult': False}},
 'WorkGroup': 'primary',
 'EngineVersion': {'SelectedEngineVersion': 'Athena engine version 3',
  'EffectiveEngineVersion': 'Athena engine version 3'}}
```

### Vacuum

`VACUUM` performs [snapshot expiration](https://iceberg.apache.org/docs/latest/spark-procedures/#expire_snapshots) and [orphan file removal](https://iceberg.apache.org/docs/latest/spark-procedures/#remove_orphan_files). These actions reduce metadata size and remove files not in the current table state that are also older than the retention period specified for the table. For syntax details, see [VACUUM](https://docs.aws.amazon.com/athena/latest/ug/vacuum-statement.html).

```python
wr.athena.start_query_execution(
    sql=f"VACUUM {glue_table}",
    database=glue_database,
    wait=True,
)
```

```text
{'QueryExecutionId': '717a7de6-b873-49c7-b744-1b0b402f24c9',
 'Query': 'VACUUM iceberg_test',
 'StatementType': 'DML',
 'ResultConfiguration': {'OutputLocation': 's3://aws-athena-query-results-...-us-east-1/717a7de6-b873-49c7-b744-1b0b402f24c9.csv'},
 'ResultReuseConfiguration': {'ResultReuseByAgeConfiguration': {'Enabled': False}},
 'QueryExecutionContext': {'Database': 'aws_sdk_pandas'},
 'Status': {'State': 'SUCCEEDED',
  'SubmissionDateTime': datetime.datetime(2023, 3, 16, 10, 50, 41, 14000, tzinfo=tzlocal()),
  'CompletionDateTime': datetime.datetime(2023, 3, 16, 10, 50, 43, 441000, tzinfo=tzlocal())},
 'Statistics': {'EngineExecutionTimeInMillis': 2229,
  'DataScannedInBytes': 0,
  'TotalExecutionTimeInMillis': 2427,
  'QueryQueueTimeInMillis': 153,
  'QueryPlanningTimeInMillis': 30,
  'ServiceProcessingTimeInMillis': 45,
  'ResultReuseInformation': {'ReusedPreviousResult': False}},
 'WorkGroup': 'primary',
 'EngineVersion': {'SelectedEngineVersion': 'Athena engine version 3',
  'EffectiveEngineVersion': 'Athena engine version 3'}}
```
