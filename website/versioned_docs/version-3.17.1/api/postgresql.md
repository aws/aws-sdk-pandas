---
id: postgresql
title: "PostgreSQL"
sidebar_position: 5
---

# PostgreSQL

Module: `wr.postgresql`

### connect

```python
wr.postgresql.connect(
    connection: 'str | None' = None,
    secret_id: 'str | None' = None,
    catalog_id: 'str | None' = None,
    dbname: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    ssl_context: 'bool | SSLContext | None' = None,
    timeout: 'int | None' = None,
    tcp_keepalive: 'bool' = True
) -> "'pg8000.Connection'"
```

Return a pg8000 connection from a Glue Catalog Connection.

https://github.com/tlocke/pg8000

:::note
You MUST pass a `connection` OR `secret_id`.
Here is an example of the secret structure in Secrets Manager:
{
"host":"postgresql-instance-wrangler.dr8vkeyrb9m1.us-east-1.rds.amazonaws.com",
"username":"test",
"password":"test",
"engine":"postgresql",
"port":"3306",
"dbname": "mydb" # Optional
}
:::

**Parameters**

- **`connection`** — Glue Catalog Connection name.
- **`secret_id`** — Specifies the secret containing the connection details that you want to retrieve. You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
- **`catalog_id`** — The ID of the Data Catalog. If none is provided, the AWS account ID is used by default.
- **`dbname`** — Optional database name to overwrite the stored one.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`ssl_context`** — This governs SSL encryption for TCP/IP sockets. This parameter is forward to pg8000. https://github.com/tlocke/pg8000#functions
- **`timeout`** — This is the time in seconds before the connection to the server will time out. The default is None which means no timeout. This parameter is forward to pg8000. https://github.com/tlocke/pg8000#functions
- **`tcp_keepalive`** — If `True` then use TCP keepalive. The default is `True`. This parameter is forwarded to pg8000. https://github.com/tlocke/pg8000#functions

**Returns**

- pg8000 connection.

**Examples**

```python
>>> import awswrangler as wr
>>> with wr.postgresql.connect("MY_GLUE_CONNECTION") as con:
...     with con.cursor() as cursor:
...         cursor.execute("SELECT 1")
...         print(cursor.fetchall())
```

---

### read_sql_query

```python
wr.postgresql.read_sql_query(
    sql: 'str',
    con: "'pg8000.Connection'",
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
- **`con`** — Use pg8000.connect() to use credentials directly or wr.postgresql.connect() to fetch it from the Glue Catalog.
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

Reading from PostgreSQL using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.postgresql.connect("MY_GLUE_CONNECTION") as con:
...     df = wr.postgresql.read_sql_query(
...         sql="SELECT * FROM public.my_table",
...         con=con,
...     )
```

---

### read_sql_table

```python
wr.postgresql.read_sql_table(
    table: 'str',
    con: "'pg8000.Connection'",
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
- **`con`** — Use pg8000.connect() to use credentials directly or wr.postgresql.connect() to fetch it from the Glue Catalog.
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

Reading from PostgreSQL using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.postgresql.connect("MY_GLUE_CONNECTION") as con:
>>>     df = wr.postgresql.read_sql_table(
...         table="my_table",
...         schema="public",
...         con=con,
...     )
```

---

### to_sql

```python
wr.postgresql.to_sql(
    df: 'pd.DataFrame',
    con: "'pg8000.Connection'",
    table: 'str',
    schema: 'str',
    mode: '_ToSqlModeLiteral' = 'append',
    overwrite_method: '_ToSqlOverwriteModeLiteral' = 'drop',
    index: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    varchar_lengths: 'dict[str, int] | None' = None,
    use_column_names: 'bool' = False,
    chunksize: 'int' = 200,
    upsert_conflict_columns: 'list[str] | None' = None,
    insert_conflict_columns: 'list[str] | None' = None,
    commit_transaction: 'bool' = True
) -> 'None'
```

Write records stored in a DataFrame into PostgreSQL.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- chunksize

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
- **`con`** — Use `pg8000.connect()` to use credentials directly or `wr.postgresql.connect()` to fetch it from the Glue Catalog.
- **`table`** — Table name
- **`schema`** — Schema name
- **`mode`** — Append, overwrite or upsert. - append: Inserts new records into table. - overwrite: Drops table and recreates. - upsert: Perform an upsert which checks for conflicts on columns given by `upsert_conflict_columns` and sets the new values on conflicts. Note that `upsert_conflict_columns` is required for this mode.
- **`overwrite_method`** — Drop, cascade, truncate, or truncate cascade. Only applicable in overwrite mode. - "drop" - `DROP ... RESTRICT` - drops the table. Fails if there are any views that depend on it. - "cascade" - `DROP ... CASCADE` - drops the table, and all views that depend on it. - "truncate" - `TRUNCATE ... RESTRICT` - truncates the table. Fails if any of the tables have foreign-key references from tables that are not listed in the command. - "truncate cascade" - `TRUNCATE ... CASCADE` - truncates the table, and all tables that have foreign-key references to any of the named tables.
- **`index`** — True to store the DataFrame index as a column in the table, otherwise False to ignore it.
- **`dtype`** — Dictionary of columns names and PostgreSQL types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. `{'col name': 'TEXT', 'col2 name': 'FLOAT'}`)
- **`varchar_lengths`** — Dict of VARCHAR length by columns. (e.g. `{"col1": 10, "col5": 200}`).
- **`use_column_names`** — If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query. E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be inserted into the database columns `col1` and `col3`.
- **`chunksize`** — Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.
- **`upsert_conflict_columns`** — This parameter is only supported if `mode` is set top `upsert`. In this case conflicts for the given columns are checked for evaluating the upsert.
- **`insert_conflict_columns`** — This parameter is only supported if `mode` is set top `append`. In this case conflicts for the given columns are checked for evaluating the insert 'ON CONFLICT DO NOTHING'.
- **`commit_transaction`** — Whether to commit the transaction. True by default.

**Examples**

Writing to PostgreSQL using a Glue Catalog Connections

```python
>>> import awswrangler as wr
>>> with wr.postgresql.connect("MY_GLUE_CONNECTION") as con:
...     wr.postgresql.to_sql(
...         df=df,
...         table="my_table",
...         schema="public",
...         con=con
...     )
```

---
