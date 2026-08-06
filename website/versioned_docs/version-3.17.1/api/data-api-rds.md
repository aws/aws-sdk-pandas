---
id: data-api-rds
title: "Data API RDS"
sidebar_position: 8
---

# Data API RDS

Module: `wr.data_api.rds`

### RdsDataApi

```python
wr.data_api.rds.RdsDataApi(
    resource_arn: 'str',
    database: 'str',
    secret_arn: 'str' = '',
    sleep: 'float' = 0.5,
    backoff: 'float' = 1.0,
    retries: 'int' = 30,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Provides access to the RDS Data API.

**Parameters**

- **`resource_arn`** — ARN for the RDS resource.
- **`database`** — Target database name.
- **`secret_arn`** — The ARN for the secret to be used for authentication.
- **`sleep`** — Number of seconds to sleep between connection attempts to paused clusters - defaults to 0.5.
- **`backoff`** — Factor by which to increase the sleep between connection attempts to paused clusters - defaults to 1.0.
- **`retries`** — Maximum number of connection attempts to paused clusters - defaults to 10.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

---

### connect

```python
wr.data_api.rds.connect(
    resource_arn: 'str',
    database: 'str',
    secret_arn: 'str' = '',
    boto3_session: 'boto3.Session | None' = None,
    **kwargs: 'Any'
) -> 'RdsDataApi'
```

Create a RDS Data API connection.

**Parameters**

- **`resource_arn`** — ARN for the RDS resource.
- **`database`** — Target database name.
- **`secret_arn`** — The ARN for the secret to be used for authentication.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`**kwargs`** — Any additional kwargs are passed to the underlying RdsDataApi class.

**Returns**

- A RdsDataApi connection instance that can be used with `wr.rds.data_api.read_sql_query`.

---

### read_sql_query

```python
wr.data_api.rds.read_sql_query(
    sql: 'str',
    con: 'RdsDataApi',
    database: 'str | None' = None,
    parameters: 'list[dict[str, Any]] | None' = None
) -> 'pd.DataFrame'
```

Run an SQL query on an RdsDataApi connection and return the result as a DataFrame.

**Parameters**

- **`sql`** — SQL query to run.
- **`con`** — A RdsDataApi connection instance
- **`database`** — Database to run query on - defaults to the database specified by `con`.
- **`parameters`** — A list of named parameters e.g. [{"name": "col", "value": {"stringValue": "val1"}}].

**Returns**

- A Pandas DataFrame containing the query results.

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.data_api.rds.read_sql_query(
>>>     sql="SELECT * FROM public.my_table",
>>>     con=con,
>>> )
```

```python
>>> import awswrangler as wr
>>> df = wr.data_api.rds.read_sql_query(
>>>     sql="SELECT * FROM public.my_table WHERE col = :name",
>>>     con=con,
>>>     parameters=[
>>>        {"name": "col1", "value": {"stringValue": "val1"}}
>>>     ],
>>> )
```

---

### to_sql

```python
wr.data_api.rds.to_sql(
    df: 'pd.DataFrame',
    con: 'RdsDataApi',
    table: 'str',
    database: 'str',
    mode: "Literal['append', 'overwrite']" = 'append',
    index: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    varchar_lengths: 'dict[str, int] | None' = None,
    use_column_names: 'bool' = False,
    chunksize: 'int' = 200,
    sql_mode: 'str' = 'mysql'
) -> 'None'
```

Insert data using an SQL query on a Data API connection.

**Parameters**

- **`df`** — `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
- **`con`** — A RdsDataApi connection instance
- **`database`** — Database to run query on - defaults to the database specified by `con`.
- **`table`** — Table name
- **`mode`** — `append` (inserts new records into table), `overwrite` (drops table and recreates)
- **`index`** — True to store the DataFrame index as a column in the table, otherwise False to ignore it.
- **`dtype`** — Dictionary of columns names and MySQL types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. ``{'col name': 'TEXT', 'col2 name': 'FLOAT'}``)
- **`varchar_lengths`** — Dict of VARCHAR length by columns. (e.g. ``{"col1": 10, "col5": 200}``).
- **`use_column_names`** — If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query. E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be inserted into the database columns `col1` and `col3`.
- **`chunksize`** — Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.
- **`sql_mode`** — "mysql" for default MySQL identifiers (backticks) or "ansi" for ANSI-compatible identifiers (double quotes).

---
