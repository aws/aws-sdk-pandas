---
id: amazon-redshift
title: "Amazon Redshift"
sidebar_position: 4
---

# Amazon Redshift

Module: `wr.redshift`

### connect

```python
wr.redshift.connect(
    connection: 'str | None' = None,
    secret_id: 'str | None' = None,
    catalog_id: 'str | None' = None,
    dbname: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    ssl: 'bool' = True,
    timeout: 'int | None' = None,
    max_prepared_statements: 'int' = 1000,
    tcp_keepalive: 'bool' = True,
    **kwargs: 'Any'
) -> "'Connection'"
```

Return a redshift_connector connection from a Glue Catalog or Secret Manager.

:::note
You MUST pass a `connection` OR `secret_id`.
Here is an example of the secret structure in Secrets Manager:
{
"host":"my-host.us-east-1.redshift.amazonaws.com",
"username":"test",
"password":"test",
"engine":"redshift",
"port":"5439",
"dbname": "mydb"
}
:::

https://github.com/aws/amazon-redshift-python-driver

**Parameters**

- **`connection`** — Glue Catalog Connection name.
- **`secret_id`** — Specifies the secret containing the connection details that you want to retrieve. You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
- **`catalog_id`** — The ID of the Data Catalog. If none is provided, the AWS account ID is used by default.
- **`dbname`** — Optional database name to overwrite the stored one.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`ssl`** — This governs SSL encryption for TCP/IP sockets. This parameter is forward to redshift_connector. https://github.com/aws/amazon-redshift-python-driver
- **`timeout`** — This is the time in seconds before the connection to the server will time out. The default is None which means no timeout. This parameter is forward to redshift_connector. https://github.com/aws/amazon-redshift-python-driver
- **`max_prepared_statements`** — This parameter is forward to redshift_connector. https://github.com/aws/amazon-redshift-python-driver
- **`tcp_keepalive`** — If True then use TCP keepalive. The default is True. This parameter is forward to redshift_connector. https://github.com/aws/amazon-redshift-python-driver
- **`**kwargs`** — Forwarded to redshift_connector.connect. e.g. `is_serverless=True, serverless_acct_id='...', serverless_work_group='...'`

**Returns**

- `redshift_connector` connection.

**Examples**

Fetching Redshift connection from Glue Catalog

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     with con.cursor() as cursor:
...         cursor.execute("SELECT 1")
...         print(cursor.fetchall())
```

Fetching Redshift connection from Secrets Manager

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect(secret_id="MY_SECRET") as con:
...     with con.cursor() as cursor:
...         cursor.execute("SELECT 1")
...         print(cursor.fetchall())
```

---

### connect_temp

```python
wr.redshift.connect_temp(
    cluster_identifier: 'str',
    user: 'str',
    database: 'str | None' = None,
    duration: 'int' = 900,
    auto_create: 'bool' = True,
    db_groups: 'list[str] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    ssl: 'bool' = True,
    timeout: 'int | None' = None,
    max_prepared_statements: 'int' = 1000,
    tcp_keepalive: 'bool' = True,
    **kwargs: 'Any'
) -> "'Connection'"
```

Return a redshift_connector temporary connection (No password required).

https://github.com/aws/amazon-redshift-python-driver

**Parameters**

- **`cluster_identifier`** — The unique identifier of a cluster. This parameter is case sensitive.
- **`user`** — The name of a database user.
- **`database`** — Database name. If None, the default Database is used.
- **`duration`** — The number of seconds until the returned temporary password expires. Constraint: minimum 900, maximum 3600. Default: 900
- **`auto_create`** — Create a database user with the name specified for the user named in user if one does not exist.
- **`db_groups`** — A list of the names of existing database groups that the user named in user will join for the current session, in addition to any group memberships for an existing user. If not specified, a new user is added only to PUBLIC.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`ssl`** — This governs SSL encryption for TCP/IP sockets. This parameter is forward to redshift_connector. https://github.com/aws/amazon-redshift-python-driver
- **`timeout`** — This is the time in seconds before the connection to the server will time out. The default is None which means no timeout. This parameter is forward to redshift_connector. https://github.com/aws/amazon-redshift-python-driver
- **`max_prepared_statements`** — This parameter is forward to redshift_connector. https://github.com/aws/amazon-redshift-python-driver
- **`tcp_keepalive`** — If True then use TCP keepalive. The default is True. This parameter is forward to redshift_connector. https://github.com/aws/amazon-redshift-python-driver
- **`**kwargs`** — Forwarded to redshift_connector.connect. e.g. is_serverless=True, serverless_acct_id='...', serverless_work_group='...'

**Returns**

- `redshift_connector` connection.

**Examples**

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect_temp(cluster_identifier="my-cluster", user="test") as con:
...     with con.cursor() as cursor:
...         cursor.execute("SELECT 1")
...         print(cursor.fetchall())
```

---

### copy

```python
wr.redshift.copy(
    df: 'pd.DataFrame',
    path: 'str',
    con: "'Connection'",
    table: 'str',
    schema: 'str',
    iam_role: 'str | None' = None,
    aws_access_key_id: 'str | None' = None,
    aws_secret_access_key: 'str | None' = None,
    aws_session_token: 'str | None' = None,
    index: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    mode: '_ToSqlModeLiteral' = 'append',
    overwrite_method: '_ToSqlOverwriteModeLiteral' = 'drop',
    diststyle: '_ToSqlDistStyleLiteral' = 'AUTO',
    distkey: 'str | None' = None,
    sortstyle: '_ToSqlSortStyleLiteral' = 'COMPOUND',
    sortkey: 'list[str] | None' = None,
    primary_keys: 'list[str] | None' = None,
    varchar_lengths_default: 'int' = 256,
    varchar_lengths: 'dict[str, int] | None' = None,
    serialize_to_json: 'bool' = False,
    keep_files: 'bool' = False,
    use_threads: 'bool | int' = True,
    lock: 'bool' = False,
    commit_transaction: 'bool' = True,
    sql_copy_extra_params: 'list[str] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, str] | None' = None,
    max_rows_by_file: 'int | None' = 10000000,
    precombine_key: 'str | None' = None,
    use_column_names: 'bool' = False,
    add_new_columns: 'bool' = False,
    pyarrow_additional_kwargs: 'dict[str, str] | None' = None
) -> 'None'
```

Load Pandas DataFrame as a Table on Amazon Redshift using parquet files on S3 as stage.

This is a **HIGH** latency and **HIGH** throughput alternative to `wr.redshift.to_sql()` to load large
DataFrames into Amazon Redshift through the ** SQL COPY command**.

This strategy has more overhead and requires more IAM privileges
than the regular `wr.redshift.to_sql()` function, so it is only recommended
to inserting +1K rows at once.

https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html

:::note
If the table does not exist yet,
it will be automatically created for you
using the Parquet metadata to
infer the columns data types.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`df`** — Pandas DataFrame.
- **`path`** — S3 path to write stage files (e.g. s3://bucket_name/any_name/). Note: This path must be empty.
- **`con`** — Use redshift_connector.connect() to use " "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
- **`table`** — Table name
- **`schema`** — Schema name
- **`iam_role`** — AWS IAM role with the related permissions.
- **`aws_access_key_id`** — The access key for your AWS account.
- **`aws_secret_access_key`** — The secret key for your AWS account.
- **`aws_session_token`** — The session key for your AWS account. This is only needed when you are using temporary credentials.
- **`index`** — True to store the DataFrame index in file, otherwise False to ignore it.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. Only takes effect if dataset=True. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`mode`** — Append, overwrite or upsert.
- **`overwrite_method`** — Drop, cascade, truncate, or delete. Only applicable in overwrite mode. "drop" - `DROP ... RESTRICT` - drops the table. Fails if there are any views that depend on it. "cascade" - `DROP ... CASCADE` - drops the table, and all views that depend on it. "truncate" - `TRUNCATE ...` - truncates the table, but immediately commits current transaction & starts a new one, hence the overwrite happens in two transactions and is not atomic. "delete" - `DELETE FROM ...` - deletes all rows from the table. Slow relative to the other methods.
- **`diststyle`** — Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"]. https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
- **`distkey`** — Specifies a column name or positional number for the distribution key.
- **`sortstyle`** — Sorting can be "COMPOUND" or "INTERLEAVED". https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
- **`sortkey`** — List of columns to be sorted.
- **`primary_keys`** — Primary keys.
- **`varchar_lengths_default`** — The size that will be set for all VARCHAR columns not specified with varchar_lengths.
- **`varchar_lengths`** — Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
- **`serialize_to_json`** — Should awswrangler add SERIALIZETOJSON parameter into the COPY command? SERIALIZETOJSON is necessary to load nested data https://docs.aws.amazon.com/redshift/latest/dg/ingest-super.html#copy_json
- **`keep_files`** — Should keep stage files?
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`lock`** — True to execute LOCK command inside the transaction to force serializable isolation.
- **`commit_transaction`** — Whether to commit the transaction. True by default.
- **`sql_copy_extra_params`** — Additional copy parameters to pass to the command. For example: ["STATUPDATE ON"]
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
- **`max_rows_by_file`** — Max number of rows in each file. (e.g. 33554432, 268435456)
- **`precombine_key`** — When there is a primary_key match during upsert, this column will change the upsert method, comparing the values of the specified column from source and target, and keeping the larger of the two. Will only work when mode = upsert.
- **`use_column_names`** — If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query. E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be inserted into the database columns `col1` and `col3`.
- **`add_new_columns`** — If True, it automatically adds the new DataFrame columns into the target table.
- **`pyarrow_additional_kwargs`** — Forwarded to pyarrow. e.g. pyarrow_additional_kwargs={'coerce_timestamps': 'us', 'allow_truncated_timestamps': False}

**Examples**

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     wr.redshift.copy(
...         df=pd.DataFrame({'col': [1, 2, 3]}),
...         path="s3://bucket/my_parquet_files/",
...         con=con,
...         table="my_table",
...         schema="public",
...         iam_role="arn:aws:iam::XXX:role/XXX",
...     )
```

---

### copy_from_files

```python
wr.redshift.copy_from_files(
    path: 'str',
    con: "'Connection'",
    table: 'str',
    schema: 'str',
    iam_role: 'str | None' = None,
    aws_access_key_id: 'str | None' = None,
    aws_secret_access_key: 'str | None' = None,
    aws_session_token: 'str | None' = None,
    data_format: '_CopyFromFilesDataFormatLiteral' = 'parquet',
    redshift_column_types: 'dict[str, str] | None' = None,
    parquet_infer_sampling: 'float' = 1.0,
    mode: '_ToSqlModeLiteral' = 'append',
    overwrite_method: '_ToSqlOverwriteModeLiteral' = 'drop',
    diststyle: '_ToSqlDistStyleLiteral' = 'AUTO',
    distkey: 'str | None' = None,
    sortstyle: '_ToSqlSortStyleLiteral' = 'COMPOUND',
    sortkey: 'list[str] | None' = None,
    primary_keys: 'list[str] | None' = None,
    varchar_lengths_default: 'int' = 256,
    varchar_lengths: 'dict[str, int] | None' = None,
    serialize_to_json: 'bool' = False,
    path_suffix: 'str | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    use_threads: 'bool | int' = True,
    lock: 'bool' = False,
    commit_transaction: 'bool' = True,
    manifest: 'bool | None' = False,
    sql_copy_extra_params: 'list[str] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, str] | None' = None,
    precombine_key: 'str | None' = None,
    column_names: 'list[str] | None' = None,
    add_new_columns: 'bool' = False
) -> 'None'
```

Load files from S3 to a Table on Amazon Redshift (Through COPY command).

https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html

:::note
If the table does not exist yet,
it will be automatically created for you
using the Parquet/ORC/CSV metadata to
infer the columns data types.
If the data is in the CSV format,
the Redshift column types need to be
specified manually using `redshift_column_types`.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

**Parameters**

- **`path`** — S3 prefix (e.g. s3://bucket/prefix/)
- **`con`** — Use redshift_connector.connect() to use " "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
- **`table`** — Table name
- **`schema`** — Schema name
- **`iam_role`** — AWS IAM role with the related permissions.
- **`aws_access_key_id`** — The access key for your AWS account.
- **`aws_secret_access_key`** — The secret key for your AWS account.
- **`aws_session_token`** — The session key for your AWS account. This is only needed when you are using temporary credentials.
- **`data_format`** — Data format to be loaded. Supported values are Parquet, ORC, and CSV. Default is Parquet.
- **`redshift_column_types`** — Dictionary with keys as column names and values as Redshift column types. Only used when `data_format` is CSV. e.g. ``{'col1': 'BIGINT', 'col2': 'VARCHAR(256)'}``
- **`parquet_infer_sampling`** — Random sample ratio of files that will have the metadata inspected. Must be `0.0 < sampling <= 1.0`. The higher, the more accurate. The lower, the faster.
- **`mode`** — Append, overwrite or upsert.
- **`overwrite_method`** — Drop, cascade, truncate, or delete. Only applicable in overwrite mode. "drop" - `DROP ... RESTRICT` - drops the table. Fails if there are any views that depend on it. "cascade" - `DROP ... CASCADE` - drops the table, and all views that depend on it. "truncate" - `TRUNCATE ...` - truncates the table, but immediately commits current transaction & starts a new one, hence the overwrite happens in two transactions and is not atomic. "delete" - `DELETE FROM ...` - deletes all rows from the table. Slow relative to the other methods.
- **`diststyle`** — Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"]. https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
- **`distkey`** — Specifies a column name or positional number for the distribution key.
- **`sortstyle`** — Sorting can be "COMPOUND" or "INTERLEAVED". https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
- **`sortkey`** — List of columns to be sorted.
- **`primary_keys`** — Primary keys.
- **`varchar_lengths_default`** — The size that will be set for all VARCHAR columns not specified with varchar_lengths.
- **`varchar_lengths`** — Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
- **`serialize_to_json`** — Should awswrangler add SERIALIZETOJSON parameter into the COPY command? SERIALIZETOJSON is necessary to load nested data https://docs.aws.amazon.com/redshift/latest/dg/ingest-super.html#copy_json
- **`path_suffix`** — Suffix or List of suffixes to be scanned on s3 for the schema extraction (e.g. [".gz.parquet", ".snappy.parquet"]). Only has effect during the table creation. If None, will try to read all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored during the schema extraction. (e.g. [".csv", "_SUCCESS"]). Only has effect during the table creation. If None, will try to read all files. (default)
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`lock`** — True to execute LOCK command inside the transaction to force serializable isolation.
- **`commit_transaction`** — Whether to commit the transaction. True by default.
- **`manifest`** — If set to true path argument accepts a S3 uri to a manifest file.
- **`sql_copy_extra_params`** — Additional copy parameters to pass to the command. For example: ["STATUPDATE ON"]
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
- **`precombine_key`** — When there is a primary_key match during upsert, this column will change the upsert method, comparing the values of the specified column from source and target, and keeping the larger of the two. Will only work when mode = upsert.
- **`column_names`** — List of column names to map source data fields to the target columns.
- **`add_new_columns`** — If True, it automatically adds the new DataFrame columns into the target table.

**Examples**

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     wr.redshift.copy_from_files(
...         path="s3://bucket/my_parquet_files/",
...         con=con,
...         table="my_table",
...         schema="public",
...         iam_role="arn:aws:iam::XXX:role/XXX"
...     )
```

---

### read_sql_query

```python
wr.redshift.read_sql_query(
    sql: 'str',
    con: "'Connection'",
    index_col: 'str | list[str] | None' = None,
    params: 'list[Any] | tuple[Any, ...] | dict[Any, Any] | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    chunksize: 'int | None' = None,
    dtype: 'dict[str, pa.DataType] | None' = None,
    safe: 'bool' = True,
    timestamp_as_object: 'bool' = False
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Return a DataFrame corresponding to the result set of the query string.

:::note
For large extractions (1K+ rows) consider the function **wr.redshift.unload()**.
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- chunksize

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`sql`** — SQL query.
- **`con`** — Use redshift_connector.connect() to use " "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
- **`index_col`** — Column(s) to set as index(MultiIndex).
- **`params`** — List of parameters to pass to execute method. The syntax used to pass parameters is database driver dependent. Check your database driver documentation for which of the five syntax styles, described in PEP 249’s paramstyle, is supported.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`chunksize`** — If specified, return an iterator where chunksize is the number of rows to include in each chunk.
- **`dtype`** — Specifying the datatype for columns. The keys should be the column names and the values should be the PyArrow types.
- **`safe`** — Check for overflows or other unsafe data type conversions.
- **`timestamp_as_object`** — Cast non-nanosecond timestamps (np.datetime64) to objects.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Reading from Redshift using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     df = wr.redshift.read_sql_query(
...         sql="SELECT * FROM public.my_table",
...         con=con
...     )
```

---

### read_sql_table

```python
wr.redshift.read_sql_table(
    table: 'str',
    con: "'Connection'",
    schema: 'str | None' = None,
    index_col: 'str | list[str] | None' = None,
    params: 'list[Any] | tuple[Any, ...] | dict[Any, Any] | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    chunksize: 'int | None' = None,
    dtype: 'dict[str, pa.DataType] | None' = None,
    safe: 'bool' = True,
    timestamp_as_object: 'bool' = False
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Return a DataFrame corresponding the table.

:::note
For large extractions (1K+ rows) consider the function **wr.redshift.unload()**.
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- chunksize

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`table`** — Table name.
- **`con`** — Use redshift_connector.connect() to use " "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
- **`schema`** — Name of SQL schema in database to query (if database flavor supports this). Uses default schema if None (default).
- **`index_col`** — Column(s) to set as index(MultiIndex).
- **`params`** — List of parameters to pass to execute method. The syntax used to pass parameters is database driver dependent. Check your database driver documentation for which of the five syntax styles, described in PEP 249's paramstyle, is supported.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`chunksize`** — If specified, return an iterator where chunksize is the number of rows to include in each chunk.
- **`dtype`** — Specifying the datatype for columns. The keys should be the column names and the values should be the PyArrow types.
- **`safe`** — Check for overflows or other unsafe data type conversions.
- **`timestamp_as_object`** — Cast non-nanosecond timestamps (np.datetime64) to objects.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Reading from Redshift using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     df = wr.redshift.read_sql_table(
...         table="my_table",
...         schema="public",
...         con=con
...     )
```

---

### to_sql

```python
wr.redshift.to_sql(
    df: 'pd.DataFrame',
    con: "'Connection'",
    table: 'str',
    schema: 'str',
    mode: '_ToSqlModeLiteral' = 'append',
    overwrite_method: '_ToSqlOverwriteModeLiteral' = 'drop',
    index: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    diststyle: '_ToSqlDistStyleLiteral' = 'AUTO',
    distkey: 'str | None' = None,
    sortstyle: '_ToSqlSortStyleLiteral' = 'COMPOUND',
    sortkey: 'list[str] | None' = None,
    primary_keys: 'list[str] | None' = None,
    varchar_lengths_default: 'int' = 256,
    varchar_lengths: 'dict[str, int] | None' = None,
    use_column_names: 'bool' = False,
    lock: 'bool' = False,
    chunksize: 'int' = 200,
    commit_transaction: 'bool' = True,
    precombine_key: 'str | None' = None,
    add_new_columns: 'bool' = False
) -> 'None'
```

Write records stored in a DataFrame into Redshift.

:::note
For large DataFrames (1K+ rows) consider the function **wr.redshift.copy()**.
:::


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- chunksize

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`con`** — Use redshift_connector.connect() to use " "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
- **`table`** — Table name
- **`schema`** — Schema name
- **`mode`** — Append, overwrite or upsert.
- **`overwrite_method`** — Drop, cascade, truncate, or delete. Only applicable in overwrite mode. - "drop" - `DROP ... RESTRICT` - drops the table. Fails if there are any views that depend on it. - "cascade" - `DROP ... CASCADE` - drops the table, and all views that depend on it. - "truncate" - `TRUNCATE ...` - truncates the table, but immediately commits current transaction & starts a new one, hence the overwrite happens in two transactions and is not atomic. - "delete" - `DELETE FROM ...` - deletes all rows from the table. Slow relative to the other methods.
- **`index`** — True to store the DataFrame index as a column in the table, otherwise False to ignore it.
- **`dtype`** — Dictionary of columns names and Redshift types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'VARCHAR(10)', 'col2 name': 'FLOAT'})
- **`diststyle`** — Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"]. https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
- **`distkey`** — Specifies a column name or positional number for the distribution key.
- **`sortstyle`** — Sorting can be "COMPOUND" or "INTERLEAVED". https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
- **`sortkey`** — List of columns to be sorted.
- **`primary_keys`** — Primary keys.
- **`varchar_lengths_default`** — The size that will be set for all VARCHAR columns not specified with varchar_lengths.
- **`varchar_lengths`** — Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
- **`use_column_names`** — If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query. E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be inserted into the database columns `col1` and `col3`.
- **`lock`** — True to execute LOCK command inside the transaction to force serializable isolation.
- **`chunksize`** — Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.
- **`commit_transaction`** — Whether to commit the transaction. True by default.
- **`precombine_key`** — When there is a primary_key match during upsert, this column will change the upsert method, comparing the values of the specified column from source and target, and keeping the larger of the two. Will only work when mode = upsert.
- **`add_new_columns`** — If True, it automatically adds the new DataFrame columns into the target table.

**Examples**

Writing to Redshift using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     wr.redshift.to_sql(
...         df=df,
...         table="my_table",
...         schema="public",
...         con=con,
...     )
```

---

### unload

```python
wr.redshift.unload(
    sql: 'str',
    path: 'str',
    con: "'Connection'",
    iam_role: 'str | None' = None,
    aws_access_key_id: 'str | None' = None,
    aws_secret_access_key: 'str | None' = None,
    aws_session_token: 'str | None' = None,
    region: 'str | None' = None,
    max_file_size: 'float | None' = None,
    kms_key_id: 'str | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    chunked: 'bool | int' = False,
    keep_files: 'bool' = False,
    parallel: 'bool' = True,
    cleanpath: 'bool' = False,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, str] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Load Pandas DataFrame from a Amazon Redshift query result using Parquet files on s3 as stage.

This is a **HIGH** latency and **HIGH** throughput alternative to
`wr.redshift.read_sql_query()`/`wr.redshift.read_sql_table()` to extract large
Amazon Redshift data into a Pandas DataFrames through the **UNLOAD command**.

This strategy has more overhead and requires more IAM privileges
than the regular `wr.redshift.read_sql_query()`/`wr.redshift.read_sql_table()` function,
so it is only recommended to fetch 1k+ rows at once.

https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

:::note
`Batching` (`chunked` argument) (Memory Friendly):

Will enable the function to return an Iterable of DataFrames instead of a regular DataFrame.

There are two batching strategies on awswrangler:

- If **chunked=True**, depending on the size of the data, one or more data frames are returned per file.
  Unlike **chunked=INTEGER**, rows from different files are not be mixed in the resulting data frames.

- If **chunked=INTEGER**, awswrangler iterates on the data by number of rows (equal to the received INTEGER).

`P.S.` `chunked=True` is faster and uses less memory while `chunked=INTEGER` is more precise
in the number of rows for each DataFrame.
:::

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`sql`** — SQL query.
- **`path`** — S3 path to write stage files (e.g. s3://bucket_name/any_name/)
- **`con`** — Use redshift_connector.connect() to use " "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
- **`iam_role`** — AWS IAM role with the related permissions.
- **`aws_access_key_id`** — The access key for your AWS account.
- **`aws_secret_access_key`** — The secret key for your AWS account.
- **`aws_session_token`** — The session key for your AWS account. This is only needed when you are using temporary credentials.
- **`region`** — Specifies the AWS Region where the target Amazon S3 bucket is located. REGION is required for UNLOAD to an Amazon S3 bucket that isn't in the same AWS Region as the Amazon Redshift cluster. By default, UNLOAD assumes that the target Amazon S3 bucket is located in the same AWS Region as the Amazon Redshift cluster.
- **`max_file_size`** — Specifies the maximum size (MB) of files that UNLOAD creates in Amazon S3. Specify a decimal value between 5.0 MB and 6200.0 MB. If None, the default maximum file size is 6200.0 MB.
- **`kms_key_id`** — Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be used to encrypt data files on Amazon S3.
- **`keep_files`** — Should keep stage files?
- **`parallel`** — Whether to unload to multiple files in parallel. Defaults to True. By default, UNLOAD writes data in parallel to multiple files, according to the number of slices in the cluster. If parallel is False, UNLOAD writes to one or more data files serially, sorted absolutely according to the ORDER BY clause, if one is used.
- **`cleanpath`** — Use CLEANPATH instead of ALLOWOVERWRITE. When True, uses CLEANPATH to remove existing files located in the Amazon S3 path before unloading files. When False (default), uses ALLOWOVERWRITE to overwrite existing files, including the manifest file. These options are mutually exclusive. ALLOWOVERWRITE: By default, UNLOAD fails if it finds files that it would possibly overwrite. If ALLOWOVERWRITE is specified, UNLOAD overwrites existing files, including the manifest file. CLEANPATH: Removes existing files located in the Amazon S3 path specified in the TO clause before unloading files to the specified location. If you include the PARTITION BY clause, existing files are removed only from the partition folders to receive new files generated by the UNLOAD operation. You must have the s3:DeleteObject permission on the Amazon S3 bucket. Files removed using CLEANPATH are permanently deleted and can't be recovered. For more information, see: https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`chunked`** — If passed will split the data in a Iterable of DataFrames (Memory friendly). If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize. If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`s3_additional_kwargs`** — Forward to botocore requests.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     df = wr.redshift.unload(
...         sql="SELECT * FROM public.mytable",
...         path="s3://bucket/extracted_parquet_files/",
...         con=con,
...         iam_role="arn:aws:iam::XXX:role/XXX"
...     )
>>> # Using CLEANPATH instead of ALLOWOVERWRITE
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     df = wr.redshift.unload(
...         sql="SELECT * FROM public.mytable",
...         path="s3://bucket/extracted_parquet_files/",
...         con=con,
...         iam_role="arn:aws:iam::XXX:role/XXX",
...         cleanpath=True
...     )
```

---

### unload_to_files

```python
wr.redshift.unload_to_files(
    sql: 'str',
    path: 'str',
    con: "'Connection'",
    iam_role: 'str | None' = None,
    aws_access_key_id: 'str | None' = None,
    aws_secret_access_key: 'str | None' = None,
    aws_session_token: 'str | None' = None,
    region: 'str | None' = None,
    unload_format: "Literal['CSV', 'PARQUET'] | None" = None,
    parallel: 'bool' = True,
    max_file_size: 'float | None' = None,
    kms_key_id: 'str | None' = None,
    manifest: 'bool' = False,
    partition_cols: 'list[str] | None' = None,
    cleanpath: 'bool' = False,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Unload Parquet files on s3 from a Redshift query result (Through the UNLOAD command).

https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

**Parameters**

- **`sql`** — SQL query.
- **`path`** — S3 path to write stage files (e.g. s3://bucket_name/any_name/)
- **`con`** — Use redshift_connector.connect() to use " "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
- **`iam_role`** — AWS IAM role with the related permissions.
- **`aws_access_key_id`** — The access key for your AWS account.
- **`aws_secret_access_key`** — The secret key for your AWS account.
- **`aws_session_token`** — The session key for your AWS account. This is only needed when you are using temporary credentials.
- **`region`** — Specifies the AWS Region where the target Amazon S3 bucket is located. REGION is required for UNLOAD to an Amazon S3 bucket that isn't in the same AWS Region as the Amazon Redshift cluster. By default, UNLOAD assumes that the target Amazon S3 bucket is located in the same AWS Region as the Amazon Redshift cluster.
- **`unload_format`** — Format of the unloaded S3 objects from the query. Valid values: "CSV", "PARQUET". Case sensitive. Defaults to PARQUET.
- **`parallel`** — Whether to unload to multiple files in parallel. Defaults to True. By default, UNLOAD writes data in parallel to multiple files, according to the number of slices in the cluster. If parallel is False, UNLOAD writes to one or more data files serially, sorted absolutely according to the ORDER BY clause, if one is used.
- **`max_file_size`** — Specifies the maximum size (MB) of files that UNLOAD creates in Amazon S3. Specify a decimal value between 5.0 MB and 6200.0 MB. If None, the default maximum file size is 6200.0 MB.
- **`kms_key_id`** — Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be used to encrypt data files on Amazon S3.
- **`manifest`** — Unload a manifest file on S3.
- **`partition_cols`** — Specifies the partition keys for the unload operation.
- **`cleanpath`** — Use CLEANPATH instead of ALLOWOVERWRITE. When True, uses CLEANPATH to remove existing files located in the Amazon S3 path before unloading files. When False (default), uses ALLOWOVERWRITE to overwrite existing files, including the manifest file. These options are mutually exclusive. ALLOWOVERWRITE: By default, UNLOAD fails if it finds files that it would possibly overwrite. If ALLOWOVERWRITE is specified, UNLOAD overwrites existing files, including the manifest file. CLEANPATH: Removes existing files located in the Amazon S3 path specified in the TO clause before unloading files to the specified location. If you include the PARTITION BY clause, existing files are removed only from the partition folders to receive new files generated by the UNLOAD operation. You must have the s3:DeleteObject permission on the Amazon S3 bucket. Files removed using CLEANPATH are permanently deleted and can't be recovered. For more information, see: https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     wr.redshift.unload_to_files(
...         sql="SELECT * FROM public.mytable",
...         path="s3://bucket/extracted_parquet_files/",
...         con=con,
...         iam_role="arn:aws:iam::XXX:role/XXX"
...     )
>>> # Using CLEANPATH instead of ALLOWOVERWRITE
>>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
...     wr.redshift.unload_to_files(
...         sql="SELECT * FROM public.mytable",
...         path="s3://bucket/extracted_parquet_files/",
...         con=con,
...         iam_role="arn:aws:iam::XXX:role/XXX",
...         cleanpath=True
...     )
```

---
