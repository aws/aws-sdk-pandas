---
id: amazon-timestream
title: "Amazon Timestream"
sidebar_position: 15
---

# Amazon Timestream

Module: `wr.timestream`

### batch_load

```python
wr.timestream.batch_load(
    df: 'pd.DataFrame',
    path: 'str',
    database: 'str',
    table: 'str',
    time_col: 'str',
    dimensions_cols: 'list[str]',
    measure_cols: 'list[str]',
    measure_name_col: 'str',
    report_s3_configuration: 'TimestreamBatchLoadReportS3Configuration',
    time_unit: '_TimeUnitLiteral' = 'MILLISECONDS',
    record_version: 'int' = 1,
    timestream_batch_load_wait_polling_delay: 'float' = 2,
    keep_files: 'bool' = False,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, str] | None' = None
) -> 'dict[str, Any]'
```

Batch load a Pandas DataFrame into a Amazon Timestream table.

:::note
The supplied column names (time, dimension, measure) MUST match those in the Timestream table.
:::
:::note
Only `MultiMeasureMappings` is supported.
See https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-data-model-mappings.html
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- database

- timestream_batch_load_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame.
- **`path`** — S3 prefix to write the data.
- **`database`** — Amazon Timestream database name.
- **`table`** — Amazon Timestream table name.
- **`time_col`** — Column name with the time data. It must be a long data type that represents the time since the Unix epoch.
- **`dimensions_cols`** — List of column names with the dimensions data.
- **`measure_cols`** — List of column names with the measure data.
- **`measure_name_col`** — Column name with the measure name.
- **`report_s3_configuration`** — Dictionary of the configuration for the S3 bucket where the error report is stored. https://docs.aws.amazon.com/timestream/latest/developerguide/API_ReportS3Configuration.html Example: {"BucketName": 'error-report-bucket-name'}
- **`time_unit`** — Time unit for the time column. MILLISECONDS by default.
- **`record_version`** — Record version.
- **`timestream_batch_load_wait_polling_delay`** — Time to wait between two polling attempts.
- **`keep_files`** — Whether to keep the files after the operation.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`s3_additional_kwargs`** — Forwarded to S3 botocore requests.

**Returns**

- A dictionary of the batch load task response.

**Examples**

```python
>>> import awswrangler as wr
```

```python
>>> response = wr.timestream.batch_load(
>>>     df=df,
>>>     path='s3://bucket/path/',
>>>     database='sample_db',
>>>     table='sample_table',
>>>     time_col='time',
>>>     dimensions_cols=['region', 'location'],
>>>     measure_cols=['memory_utilization', 'cpu_utilization'],
>>>     report_s3_configuration={'BucketName': 'error-report-bucket-name'},
>>> )
```

---

### batch_load_from_files

```python
wr.timestream.batch_load_from_files(
    path: 'str',
    database: 'str',
    table: 'str',
    time_col: 'str',
    dimensions_cols: 'list[str]',
    measure_cols: 'list[str]',
    measure_types: 'list[str]',
    measure_name_col: 'str',
    report_s3_configuration: 'TimestreamBatchLoadReportS3Configuration',
    time_unit: '_TimeUnitLiteral' = 'MILLISECONDS',
    record_version: 'int' = 1,
    data_source_csv_configuration: 'dict[str, str | bool] | None' = None,
    timestream_batch_load_wait_polling_delay: 'float' = 2,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Batch load files from S3 into a Amazon Timestream table.

:::note
The supplied column names (time, dimension, measure) MUST match those in the Timestream table.
:::
:::note
Only `MultiMeasureMappings` is supported.
See https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-data-model-mappings.html
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- database

- timestream_batch_load_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`path`** — S3 prefix to write the data.
- **`database`** — Amazon Timestream database name.
- **`table`** — Amazon Timestream table name.
- **`time_col`** — Column name with the time data. It must be a long data type that represents the time since the Unix epoch.
- **`dimensions_cols`** — List of column names with the dimensions data.
- **`measure_cols`** — List of column names with the measure data.
- **`measure_name_col`** — Column name with the measure name.
- **`report_s3_configuration`** — Dictionary of the configuration for the S3 bucket where the error report is stored. https://docs.aws.amazon.com/timestream/latest/developerguide/API_ReportS3Configuration.html Example: {"BucketName": 'error-report-bucket-name'}
- **`time_unit`** — Time unit for the time column. MILLISECONDS by default.
- **`record_version`** — Record version.
- **`data_source_csv_configuration`** — Dictionary of the data source CSV configuration. https://docs.aws.amazon.com/timestream/latest/developerguide/API_CsvConfiguration.html
- **`timestream_batch_load_wait_polling_delay`** — Time to wait between two polling attempts.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- A dictionary of the batch load task response.

**Examples**

```python
>>> import awswrangler as wr
```

```python
>>> response = wr.timestream.batch_load_from_files(
>>>     path='s3://bucket/path/',
>>>     database='sample_db',
>>>     table='sample_table',
>>>     time_col='time',
>>>     dimensions_cols=['region', 'location'],
>>>     measure_cols=['memory_utilization', 'cpu_utilization'],
>>>     report_s3_configuration={'BucketName': 'error-report-bucket-name'},
>>> )
```

---

### create_database

```python
wr.timestream.create_database(
    database: 'str',
    kms_key_id: 'str | None' = None,
    tags: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create a new Timestream database.

:::note
If the KMS key is not specified, the database will be encrypted with a
Timestream managed KMS key located in your account.
:::

**Parameters**

- **`database`** — Database name.
- **`kms_key_id`** — The KMS key for the database. If the KMS key is not specified, the database will be encrypted with a Timestream managed KMS key located in your account.
- **`tags`** — Key/Value dict to put on the database. Tags enable you to categorize databases and/or tables, for example, by purpose, owner, or environment. e.g. {"foo": "boo", "bar": "xoo"})
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- The Amazon Resource Name that uniquely identifies this database. (ARN)

**Examples**

Creating a database.

```python
>>> import awswrangler as wr
>>> arn = wr.timestream.create_database("MyDatabase")
```

---

### create_table

```python
wr.timestream.create_table(
    database: 'str',
    table: 'str',
    memory_retention_hours: 'int',
    magnetic_retention_days: 'int',
    tags: 'dict[str, str] | None' = None,
    timestream_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create a new Timestream database.

:::note
If the KMS key is not specified, the database will be encrypted with a
Timestream managed KMS key located in your account.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`memory_retention_hours`** — The duration for which data must be stored in the memory store.
- **`magnetic_retention_days`** — The duration for which data must be stored in the magnetic store.
- **`tags`** — Key/Value dict to put on the table. Tags enable you to categorize databases and/or tables, for example, by purpose, owner, or environment. e.g. {"foo": "boo", "bar": "xoo"})
- **`timestream_additional_kwargs`** — Forwarded to botocore requests. e.g. timestream_additional_kwargs={'MagneticStoreWriteProperties': {'EnableMagneticStoreWrites': True}}
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- The Amazon Resource Name that uniquely identifies this database. (ARN)

**Examples**

Creating a table.

```python
>>> import awswrangler as wr
>>> arn = wr.timestream.create_table(
...     database="MyDatabase",
...     table="MyTable",
...     memory_retention_hours=3,
...     magnetic_retention_days=7
... )
```

---

### delete_database

```python
wr.timestream.delete_database(database: 'str', boto3_session: 'boto3.Session | None' = None) -> 'None'
```

Delete a given Timestream database. This is an irreversible operation.

After a database is deleted, the time series data from its tables cannot be recovered.

All tables in the database must be deleted first, or a ValidationException error will be thrown.

Due to the nature of distributed retries,
the operation can return either success or a ResourceNotFoundException.
Clients should consider them equivalent.

**Parameters**

- **`database`** — Database name.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

Deleting a database

```python
>>> import awswrangler as wr
>>> arn = wr.timestream.delete_database("MyDatabase")
```

---

### delete_table

```python
wr.timestream.delete_table(
    database: 'str',
    table: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a given Timestream table.

This is an irreversible operation.

After a Timestream database table is deleted, the time series data stored in the table cannot be recovered.

Due to the nature of distributed retries,
the operation can return either success or a ResourceNotFoundException.
Clients should consider them equivalent.

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

Deleting a table

```python
>>> import awswrangler as wr
>>> arn = wr.timestream.delete_table("MyDatabase", "MyTable")
```

---

### list_databases

```python
wr.timestream.list_databases(boto3_session: 'boto3.Session | None' = None) -> 'list[str]'
```

List all databases in timestream.

**Parameters**

- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- a list of available timestream databases.

**Examples**

Querying the list of all available databases

```python
>>> import awswrangler as wr
>>> wr.timestream.list_databases()
["database1", "database2"]
```

---

### list_tables

```python
wr.timestream.list_tables(
    database: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str]'
```

List tables in timestream.

**Parameters**

- **`database`** — Database name. If None, all tables in Timestream will be returned. Otherwise, only the tables inside the given database are returned.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- A list of table names.

**Examples**

Listing all tables in timestream across databases

```python
>>> import awswrangler as wr
>>> wr.timestream.list_tables()
["table1", "table2"]
```

Listing all tables in timestream in a specific database

```python
>>> import awswrangler as wr
>>> wr.timestream.list_tables(DatabaseName="database1")
["table1"]
```

---

### query

```python
wr.timestream.query(
    sql: 'str',
    chunked: 'bool' = False,
    pagination_config: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Run a query and retrieve the result as a Pandas DataFrame.

**Parameters**

- **`sql`** — SQL query.
- **`chunked`** — If True returns DataFrame iterator, and a single DataFrame otherwise. False by default.
- **`pagination_config`** — Pagination configuration dictionary of a form {'MaxItems': 10, 'PageSize': 10, 'StartingToken': '...'}
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_

**Examples**

Run a query and return the result as a Pandas DataFrame or an iterable.

```python
>>> import awswrangler as wr
>>> df = wr.timestream.query('SELECT * FROM "sampleDB"."sampleTable" ORDER BY time DESC LIMIT 10')
```

---

### wait_batch_load_task

```python
wr.timestream.wait_batch_load_task(
    task_id: 'str',
    timestream_batch_load_wait_polling_delay: 'float' = 2,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Wait for the Timestream batch load task to complete.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- timestream_batch_load_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`task_id`** — The ID of the batch load task.
- **`timestream_batch_load_wait_polling_delay`** — Time to wait between two polling attempts.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Dictionary with the describe_batch_load_task response.

**Examples**

```python
>>> import awswrangler as wr
>>> res = wr.timestream.wait_batch_load_task(task_id='task-id')
```

**Raises**

- **`exceptions.TimestreamLoadError`** — Error message raised by failed task.

---

### write

```python
wr.timestream.write(
    df: 'pd.DataFrame',
    database: 'str',
    table: 'str',
    time_col: 'str | None' = None,
    measure_col: 'str | list[str | None] | None' = None,
    dimensions_cols: 'list[str | None] | None' = None,
    version: 'int' = 1,
    time_unit: '_TimeUnitLiteral' = 'MILLISECONDS',
    use_threads: 'bool | int' = True,
    measure_name: 'str | None' = None,
    common_attributes: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, str]]'
```

Store a Pandas DataFrame into an Amazon Timestream table.

:::note
In case `use_threads=True`, the number of threads from os.cpu_count() is used.

If the Timestream service rejects a record(s),
this function will not throw a Python exception.
Instead it will return the rejection information.
:::
:::note
If `time_col` column is supplied, it must be of type timestamp. `time_unit` is set to MILLISECONDS by default.
NANOSECONDS is not supported as python datetime objects are limited to microseconds precision.
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`database`** — Amazon Timestream database name.
- **`table`** — Amazon Timestream table name.
- **`time_col`** — DataFrame column name to be used as time. MUST be a timestamp column.
- **`measure_col`** — DataFrame column name(s) to be used as measure.
- **`dimensions_cols`** — List of DataFrame column names to be used as dimensions.
- **`version`** — Version number used for upserts. Documentation https://docs.aws.amazon.com/timestream/latest/developerguide/API_WriteRecords.html.
- **`time_unit`** — Time unit for the time column. MILLISECONDS by default.
- **`use_threads`** — True to enable concurrent writing, False to disable multiple threads. If enabled, os.cpu_count() is used as the number of threads. If integer is provided, specified number is used.
- **`measure_name`** — Name that represents the data attribute of the time series. Overrides `measure_col` if specified.
- **`common_attributes`** — Dictionary of attributes shared across all records in the request. Using common attributes can optimize the cost of writes by reducing the size of request payloads. Values in `common_attributes` take precedence over all other arguments and data frame values. Dimension attributes are merged with attributes in record objects. Example: `{"Dimensions": [{"Name": "device_id", "Value": "12345"}], "MeasureValueType": "DOUBLE"}`.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Rejected records. Possible reasons for rejection are described here: https://docs.aws.amazon.com/timestream/latest/developerguide/API_RejectedRecord.html

**Examples**

Store a Pandas DataFrame into a Amazon Timestream table.

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> df = pd.DataFrame(
>>>     {
>>>         "time": [datetime.now(), datetime.now(), datetime.now()],
>>>         "dim0": ["foo", "boo", "bar"],
>>>         "dim1": [1, 2, 3],
>>>         "measure": [1.0, 1.1, 1.2],
>>>     }
>>> )
>>> rejected_records = wr.timestream.write(
>>>     df=df,
>>>     database="sampleDB",
>>>     table="sampleTable",
>>>     time_col="time",
>>>     measure_col="measure",
>>>     dimensions_cols=["dim0", "dim1"],
>>> )
>>> assert len(rejected_records) == 0
```

Return value if some records are rejected.

```python
>>> [
>>>     {
>>>         'ExistingVersion': 2,
>>>         'Reason': 'The record version 1 is lower than the existing version 2. A '
>>>                   'higher version is required to update the measure value.',
>>>         'RecordIndex': 0
>>>     }
>>> ]
```

---

### unload_to_files

```python
wr.timestream.unload_to_files(
    sql: 'str',
    path: 'str',
    unload_format: "Literal['CSV', 'PARQUET'] | None" = None,
    compression: "Literal['GZIP', 'NONE'] | None" = None,
    partition_cols: 'list[str] | None' = None,
    encryption: "Literal['SSE_KMS', 'SSE_S3'] | None" = None,
    kms_key_id: 'str | None' = None,
    field_delimiter: 'str | None' = ',',
    escaped_by: 'str | None' = '\\',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Unload query results to Amazon S3.

https://docs.aws.amazon.com/timestream/latest/developerguide/export-unload.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.




:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`sql`** — SQL query
- **`path`** — S3 path to write stage files (e.g. s3://bucket_name/any_name/)
- **`unload_format`** — Format of the unloaded S3 objects from the query. Valid values: "CSV", "PARQUET". Case sensitive. Defaults to "PARQUET"
- **`compression`** — Compression of the unloaded S3 objects from the query. Valid values: "GZIP", "NONE". Defaults to "GZIP"
- **`partition_cols`** — Specifies the partition keys for the unload operation
- **`encryption`** — Encryption of the unloaded S3 objects from the query. Valid values: "SSE_KMS", "SSE_S3". Defaults to "SSE_S3"
- **`kms_key_id`** — Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be used to encrypt data files on Amazon S3
- **`field_delimiter`** — A single ASCII character that is used to separate fields in the output file, such as pipe character (|), a comma (,), or tab (/t). Only used with CSV format
- **`escaped_by`** — The character that should be treated as an escape character in the data file written to S3 bucket. Only used with CSV format
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

Unload and read as Parquet (default).

```python
>>> import awswrangler as wr
>>> wr.timestream.unload_to_files(
...     sql="SELECT time, measure, dimension FROM database.mytable",
...     path="s3://bucket/extracted_parquet_files/",
... )
```

Unload and read partitioned Parquet. Note: partition columns must be at the end of the table.

```python
>>> import awswrangler as wr
>>> wr.timestream.unload_to_files(
...     sql="SELECT time, measure, dim1, dim2 FROM database.mytable",
...     path="s3://bucket/extracted_parquet_files/",
...     partition_cols=["dim2"],
... )
```

Unload and read as CSV.

```python
>>> import awswrangler as wr
>>> wr.timestream.unload_to_files(
...     sql="SELECT time, measure, dimension FROM database.mytable",
...     path="s3://bucket/extracted_parquet_files/",
...     unload_format="CSV",
... )
```

---

### unload

```python
wr.timestream.unload(
    sql: 'str',
    path: 'str',
    unload_format: "Literal['CSV', 'PARQUET'] | None" = None,
    compression: "Literal['GZIP', 'NONE'] | None" = None,
    partition_cols: 'list[str] | None' = None,
    encryption: "Literal['SSE_KMS', 'SSE_S3'] | None" = None,
    kms_key_id: 'str | None' = None,
    field_delimiter: 'str | None' = ',',
    escaped_by: 'str | None' = '\\',
    chunked: 'bool | int' = False,
    keep_files: 'bool' = False,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, str] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Unload query results to Amazon S3 and read the results as Pandas Data Frame.

https://docs.aws.amazon.com/timestream/latest/developerguide/export-unload.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.




:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`sql`** — SQL query
- **`path`** — S3 path to write stage files (e.g. `s3://bucket_name/any_name/`)
- **`unload_format`** — Format of the unloaded S3 objects from the query. Valid values: "CSV", "PARQUET". Case sensitive. Defaults to "PARQUET"
- **`compression`** — Compression of the unloaded S3 objects from the query. Valid values: "GZIP", "NONE". Defaults to "GZIP"
- **`partition_cols`** — Specifies the partition keys for the unload operation
- **`encryption`** — Encryption of the unloaded S3 objects from the query. Valid values: "SSE_KMS", "SSE_S3". Defaults to "SSE_S3"
- **`kms_key_id`** — Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be used to encrypt data files on Amazon S3
- **`field_delimiter`** — A single ASCII character that is used to separate fields in the output file, such as pipe character (|), a comma (,), or tab (/t). Only used with CSV format
- **`escaped_by`** — The character that should be treated as an escape character in the data file written to S3 bucket. Only used with CSV format
- **`chunked`** — If passed will split the data in a Iterable of DataFrames (Memory friendly). If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize. If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
- **`keep_files`** — Should keep stage files?
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`s3_additional_kwargs`** — Forward to botocore requests.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Unload and read as Parquet (default).

```python
>>> import awswrangler as wr
>>> df = wr.timestream.unload(
...     sql="SELECT time, measure, dimension FROM database.mytable",
...     path="s3://bucket/extracted_parquet_files/",
... )
```

Unload and read partitioned Parquet. Note: partition columns must be at the end of the table.

```python
>>> import awswrangler as wr
>>> df = wr.timestream.unload(
...     sql="SELECT time, measure, dim1, dim2 FROM database.mytable",
...     path="s3://bucket/extracted_parquet_files/",
...     partition_cols=["dim2"],
... )
```

Unload and read as CSV.

```python
>>> import awswrangler as wr
>>> df = wr.timestream.unload(
...     sql="SELECT time, measure, dimension FROM database.mytable",
...     path="s3://bucket/extracted_parquet_files/",
...     unload_format="CSV",
... )
```

---
