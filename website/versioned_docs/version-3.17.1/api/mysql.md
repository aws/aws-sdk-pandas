---
id: mysql
title: "MySQL"
sidebar_position: 6
---

# MySQL

Module: `wr.oracle`

### connect

```python
wr.oracle.connect(
    connection: 'str | None' = None,
    secret_id: 'str | None' = None,
    catalog_id: 'str | None' = None,
    dbname: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    call_timeout: 'int | None' = 0
) -> "'oracledb.Connection'"
```

Return a oracledb connection from a Glue Catalog Connection.

https://github.com/oracle/python-oracledb

:::note
You MUST pass a `connection` OR `secret_id`.
Here is an example of the secret structure in Secrets Manager:
{
"host":"oracle-instance-wrangler.cr4trrvge8rz.us-east-1.rds.amazonaws.com",
"username":"test",
"password":"test",
"engine":"oracle",
"port":"1521",
"dbname": "mydb" # Optional
}
:::

**Parameters**

- **`connection`** — Glue Catalog Connection name.
- **`secret_id`** — Specifies the secret containing the connection details that you want to retrieve. You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
- **`catalog_id`** — The ID of the Data Catalog. If none is provided, the AWS account ID is used by default.
- **`dbname`** — Optional database name to overwrite the stored one.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`call_timeout`** — This is the time in milliseconds that a single round-trip to the database may take before a timeout will occur. The default is None which means no timeout. This parameter is forwarded to oracledb. https://cx-oracle.readthedocs.io/en/latest/api_manual/connection.html#Connection.call_timeout

**Returns**

- oracledb connection.

**Examples**

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     with con.cursor() as cursor:
...         cursor.execute("SELECT 1 FROM DUAL")
...         print(cursor.fetchall())
```

---

### read_sql_query

```python
wr.oracle.read_sql_query(
    sql: 'str',
    con: "'oracledb.Connection'",
    index_col: 'str | list[str] | None' = None,
    params: 'list[Any] | tuple[Any, ...] | dict[Any, Any] | None' = None,
    chunksize: 'int | None' = None,
    dtype: 'dict[str, pa.DataType] | None' = None,
    safe: 'bool' = True,
    timestamp_as_object: 'bool' = False,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Return a DataFrame corresponding to the result set of the query string.

**Parameters**

- **`sql`** — SQL query.
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`index_col`** — Column(s) to set as index(MultiIndex).
- **`params`** — List of parameters to pass to execute method. The syntax used to pass parameters is database driver dependent. Check your database driver documentation for which of the five syntax styles, described in PEP 249’s paramstyle, is supported.
- **`chunksize`** — If specified, return an iterator where chunksize is the number of rows to include in each chunk.
- **`dtype`** — Specifying the datatype for columns. The keys should be the column names and the values should be the PyArrow types.
- **`safe`** — Check for overflows or other unsafe data type conversions.
- **`timestamp_as_object`** — Cast non-nanosecond timestamps (np.datetime64) to objects.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Reading from Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     df = wr.oracle.read_sql_query(
...         sql="SELECT * FROM test.my_table",
...         con=con,
...     )
```

---

### read_sql_table

```python
wr.oracle.read_sql_table(
    table: 'str',
    con: "'oracledb.Connection'",
    schema: 'str | None' = None,
    index_col: 'str | list[str] | None' = None,
    params: 'list[Any] | tuple[Any, ...] | dict[Any, Any] | None' = None,
    chunksize: 'int | None' = None,
    dtype: 'dict[str, pa.DataType] | None' = None,
    safe: 'bool' = True,
    timestamp_as_object: 'bool' = False,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Return a DataFrame corresponding the table.

**Parameters**

- **`table`** — Table name.
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`schema`** — Name of SQL schema in database to query (if database flavor supports this). Uses default schema if None (default).
- **`index_col`** — Column(s) to set as index(MultiIndex).
- **`params`** — List of parameters to pass to execute method. The syntax used to pass parameters is database driver dependent. Check your database driver documentation for which of the five syntax styles, described in PEP 249’s paramstyle, is supported.
- **`chunksize`** — If specified, return an iterator where chunksize is the number of rows to include in each chunk.
- **`dtype`** — Specifying the datatype for columns. The keys should be the column names and the values should be the PyArrow types.
- **`safe`** — Check for overflows or other unsafe data type conversions.
- **`timestamp_as_object`** — Cast non-nanosecond timestamps (np.datetime64) to objects.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Reading from Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     df = wr.oracle.read_sql_table(
...         table="my_table",
...         schema="test",
...         con=con,
...     )
```

---

### to_sql

```python
wr.oracle.to_sql(
    df: 'pd.DataFrame',
    con: "'oracledb.Connection'",
    table: 'str',
    schema: 'str',
    mode: "Literal['append', 'overwrite', 'upsert']" = 'append',
    index: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    varchar_lengths: 'dict[str, int] | None' = None,
    use_column_names: 'bool' = False,
    primary_keys: 'list[str] | None' = None,
    chunksize: 'int' = 200
) -> 'None'
```

Write records stored in a DataFrame into Oracle Database.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- chunksize

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`table`** — Table name
- **`schema`** — Schema name
- **`mode`** — Append, overwrite or upsert.
- **`index`** — True to store the DataFrame index as a column in the table, otherwise False to ignore it.
- **`dtype`** — Dictionary of columns names and Oracle types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'TEXT', 'col2 name': 'FLOAT'})
- **`varchar_lengths`** — Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
- **`use_column_names`** — If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query. E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be inserted into the database columns `col1` and `col3`.
- **`primary_keys`** — Primary keys.
- **`chunksize`** — Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.

**Examples**

Writing to Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     wr.oracle.to_sql(
...         df=df,
...         table="table",
...         schema="ORCL",
...         con=con,
...     )
```

---

### connect

```python
wr.oracle.connect(
    connection: 'str | None' = None,
    secret_id: 'str | None' = None,
    catalog_id: 'str | None' = None,
    dbname: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    call_timeout: 'int | None' = 0
) -> "'oracledb.Connection'"
```

Return a oracledb connection from a Glue Catalog Connection.

https://github.com/oracle/python-oracledb

:::note
You MUST pass a `connection` OR `secret_id`.
Here is an example of the secret structure in Secrets Manager:
{
"host":"oracle-instance-wrangler.cr4trrvge8rz.us-east-1.rds.amazonaws.com",
"username":"test",
"password":"test",
"engine":"oracle",
"port":"1521",
"dbname": "mydb" # Optional
}
:::

**Parameters**

- **`connection`** — Glue Catalog Connection name.
- **`secret_id`** — Specifies the secret containing the connection details that you want to retrieve. You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
- **`catalog_id`** — The ID of the Data Catalog. If none is provided, the AWS account ID is used by default.
- **`dbname`** — Optional database name to overwrite the stored one.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`call_timeout`** — This is the time in milliseconds that a single round-trip to the database may take before a timeout will occur. The default is None which means no timeout. This parameter is forwarded to oracledb. https://cx-oracle.readthedocs.io/en/latest/api_manual/connection.html#Connection.call_timeout

**Returns**

- oracledb connection.

**Examples**

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     with con.cursor() as cursor:
...         cursor.execute("SELECT 1 FROM DUAL")
...         print(cursor.fetchall())
```

---

### read_sql_query

```python
wr.oracle.read_sql_query(
    sql: 'str',
    con: "'oracledb.Connection'",
    index_col: 'str | list[str] | None' = None,
    params: 'list[Any] | tuple[Any, ...] | dict[Any, Any] | None' = None,
    chunksize: 'int | None' = None,
    dtype: 'dict[str, pa.DataType] | None' = None,
    safe: 'bool' = True,
    timestamp_as_object: 'bool' = False,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Return a DataFrame corresponding to the result set of the query string.

**Parameters**

- **`sql`** — SQL query.
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`index_col`** — Column(s) to set as index(MultiIndex).
- **`params`** — List of parameters to pass to execute method. The syntax used to pass parameters is database driver dependent. Check your database driver documentation for which of the five syntax styles, described in PEP 249’s paramstyle, is supported.
- **`chunksize`** — If specified, return an iterator where chunksize is the number of rows to include in each chunk.
- **`dtype`** — Specifying the datatype for columns. The keys should be the column names and the values should be the PyArrow types.
- **`safe`** — Check for overflows or other unsafe data type conversions.
- **`timestamp_as_object`** — Cast non-nanosecond timestamps (np.datetime64) to objects.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Reading from Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     df = wr.oracle.read_sql_query(
...         sql="SELECT * FROM test.my_table",
...         con=con,
...     )
```

---

### read_sql_table

```python
wr.oracle.read_sql_table(
    table: 'str',
    con: "'oracledb.Connection'",
    schema: 'str | None' = None,
    index_col: 'str | list[str] | None' = None,
    params: 'list[Any] | tuple[Any, ...] | dict[Any, Any] | None' = None,
    chunksize: 'int | None' = None,
    dtype: 'dict[str, pa.DataType] | None' = None,
    safe: 'bool' = True,
    timestamp_as_object: 'bool' = False,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Return a DataFrame corresponding the table.

**Parameters**

- **`table`** — Table name.
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`schema`** — Name of SQL schema in database to query (if database flavor supports this). Uses default schema if None (default).
- **`index_col`** — Column(s) to set as index(MultiIndex).
- **`params`** — List of parameters to pass to execute method. The syntax used to pass parameters is database driver dependent. Check your database driver documentation for which of the five syntax styles, described in PEP 249’s paramstyle, is supported.
- **`chunksize`** — If specified, return an iterator where chunksize is the number of rows to include in each chunk.
- **`dtype`** — Specifying the datatype for columns. The keys should be the column names and the values should be the PyArrow types.
- **`safe`** — Check for overflows or other unsafe data type conversions.
- **`timestamp_as_object`** — Cast non-nanosecond timestamps (np.datetime64) to objects.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Reading from Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     df = wr.oracle.read_sql_table(
...         table="my_table",
...         schema="test",
...         con=con,
...     )
```

---

### to_sql

```python
wr.oracle.to_sql(
    df: 'pd.DataFrame',
    con: "'oracledb.Connection'",
    table: 'str',
    schema: 'str',
    mode: "Literal['append', 'overwrite', 'upsert']" = 'append',
    index: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    varchar_lengths: 'dict[str, int] | None' = None,
    use_column_names: 'bool' = False,
    primary_keys: 'list[str] | None' = None,
    chunksize: 'int' = 200
) -> 'None'
```

Write records stored in a DataFrame into Oracle Database.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- chunksize

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`table`** — Table name
- **`schema`** — Schema name
- **`mode`** — Append, overwrite or upsert.
- **`index`** — True to store the DataFrame index as a column in the table, otherwise False to ignore it.
- **`dtype`** — Dictionary of columns names and Oracle types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'TEXT', 'col2 name': 'FLOAT'})
- **`varchar_lengths`** — Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
- **`use_column_names`** — If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query. E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be inserted into the database columns `col1` and `col3`.
- **`primary_keys`** — Primary keys.
- **`chunksize`** — Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.

**Examples**

Writing to Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     wr.oracle.to_sql(
...         df=df,
...         table="table",
...         schema="ORCL",
...         con=con,
...     )
```

---

### connect

```python
wr.oracle.connect(
    connection: 'str | None' = None,
    secret_id: 'str | None' = None,
    catalog_id: 'str | None' = None,
    dbname: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    call_timeout: 'int | None' = 0
) -> "'oracledb.Connection'"
```

Return a oracledb connection from a Glue Catalog Connection.

https://github.com/oracle/python-oracledb

:::note
You MUST pass a `connection` OR `secret_id`.
Here is an example of the secret structure in Secrets Manager:
{
"host":"oracle-instance-wrangler.cr4trrvge8rz.us-east-1.rds.amazonaws.com",
"username":"test",
"password":"test",
"engine":"oracle",
"port":"1521",
"dbname": "mydb" # Optional
}
:::

**Parameters**

- **`connection`** — Glue Catalog Connection name.
- **`secret_id`** — Specifies the secret containing the connection details that you want to retrieve. You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
- **`catalog_id`** — The ID of the Data Catalog. If none is provided, the AWS account ID is used by default.
- **`dbname`** — Optional database name to overwrite the stored one.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`call_timeout`** — This is the time in milliseconds that a single round-trip to the database may take before a timeout will occur. The default is None which means no timeout. This parameter is forwarded to oracledb. https://cx-oracle.readthedocs.io/en/latest/api_manual/connection.html#Connection.call_timeout

**Returns**

- oracledb connection.

**Examples**

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     with con.cursor() as cursor:
...         cursor.execute("SELECT 1 FROM DUAL")
...         print(cursor.fetchall())
```

---

### read_sql_query

```python
wr.oracle.read_sql_query(
    sql: 'str',
    con: "'oracledb.Connection'",
    index_col: 'str | list[str] | None' = None,
    params: 'list[Any] | tuple[Any, ...] | dict[Any, Any] | None' = None,
    chunksize: 'int | None' = None,
    dtype: 'dict[str, pa.DataType] | None' = None,
    safe: 'bool' = True,
    timestamp_as_object: 'bool' = False,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Return a DataFrame corresponding to the result set of the query string.

**Parameters**

- **`sql`** — SQL query.
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`index_col`** — Column(s) to set as index(MultiIndex).
- **`params`** — List of parameters to pass to execute method. The syntax used to pass parameters is database driver dependent. Check your database driver documentation for which of the five syntax styles, described in PEP 249’s paramstyle, is supported.
- **`chunksize`** — If specified, return an iterator where chunksize is the number of rows to include in each chunk.
- **`dtype`** — Specifying the datatype for columns. The keys should be the column names and the values should be the PyArrow types.
- **`safe`** — Check for overflows or other unsafe data type conversions.
- **`timestamp_as_object`** — Cast non-nanosecond timestamps (np.datetime64) to objects.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Reading from Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     df = wr.oracle.read_sql_query(
...         sql="SELECT * FROM test.my_table",
...         con=con,
...     )
```

---

### read_sql_table

```python
wr.oracle.read_sql_table(
    table: 'str',
    con: "'oracledb.Connection'",
    schema: 'str | None' = None,
    index_col: 'str | list[str] | None' = None,
    params: 'list[Any] | tuple[Any, ...] | dict[Any, Any] | None' = None,
    chunksize: 'int | None' = None,
    dtype: 'dict[str, pa.DataType] | None' = None,
    safe: 'bool' = True,
    timestamp_as_object: 'bool' = False,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Return a DataFrame corresponding the table.

**Parameters**

- **`table`** — Table name.
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`schema`** — Name of SQL schema in database to query (if database flavor supports this). Uses default schema if None (default).
- **`index_col`** — Column(s) to set as index(MultiIndex).
- **`params`** — List of parameters to pass to execute method. The syntax used to pass parameters is database driver dependent. Check your database driver documentation for which of the five syntax styles, described in PEP 249’s paramstyle, is supported.
- **`chunksize`** — If specified, return an iterator where chunksize is the number of rows to include in each chunk.
- **`dtype`** — Specifying the datatype for columns. The keys should be the column names and the values should be the PyArrow types.
- **`safe`** — Check for overflows or other unsafe data type conversions.
- **`timestamp_as_object`** — Cast non-nanosecond timestamps (np.datetime64) to objects.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.

**Returns**

- Result as Pandas DataFrame(s).

**Examples**

Reading from Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     df = wr.oracle.read_sql_table(
...         table="my_table",
...         schema="test",
...         con=con,
...     )
```

---

### to_sql

```python
wr.oracle.to_sql(
    df: 'pd.DataFrame',
    con: "'oracledb.Connection'",
    table: 'str',
    schema: 'str',
    mode: "Literal['append', 'overwrite', 'upsert']" = 'append',
    index: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    varchar_lengths: 'dict[str, int] | None' = None,
    use_column_names: 'bool' = False,
    primary_keys: 'list[str] | None' = None,
    chunksize: 'int' = 200
) -> 'None'
```

Write records stored in a DataFrame into Oracle Database.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- chunksize

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`con`** — Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
- **`table`** — Table name
- **`schema`** — Schema name
- **`mode`** — Append, overwrite or upsert.
- **`index`** — True to store the DataFrame index as a column in the table, otherwise False to ignore it.
- **`dtype`** — Dictionary of columns names and Oracle types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'TEXT', 'col2 name': 'FLOAT'})
- **`varchar_lengths`** — Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
- **`use_column_names`** — If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query. E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be inserted into the database columns `col1` and `col3`.
- **`primary_keys`** — Primary keys.
- **`chunksize`** — Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.

**Examples**

Writing to Oracle Database using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.oracle.connect(connection="MY_GLUE_CONNECTION") as con:
...     wr.oracle.to_sql(
...         df=df,
...         table="table",
...         schema="ORCL",
...         con=con,
...     )
```

---
