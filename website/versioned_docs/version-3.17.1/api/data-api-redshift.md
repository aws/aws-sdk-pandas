---
id: data-api-redshift
title: "Data API Redshift"
sidebar_position: 7
---

# Data API Redshift

Module: `wr.data_api.redshift`

### RedshiftDataApi

```python
wr.data_api.redshift.RedshiftDataApi(
    cluster_id: 'str' = '',
    database: 'str' = '',
    workgroup_name: 'str' = '',
    secret_arn: 'str' = '',
    db_user: 'str' = '',
    sleep: 'float' = 0.25,
    backoff: 'float' = 1.5,
    retries: 'int' = 15,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Provides access to a Redshift cluster via the Data API.

:::note
When connecting to a standard Redshift cluster, `cluster_id` is used.
When connecting to Redshift Serverless, `workgroup_name` is used. These two arguments are mutually exclusive.
:::

**Parameters**

- **`cluster_id`** — Id for the target Redshift cluster - only required if `workgroup_name` not provided.
- **`database`** — Target database name.
- **`workgroup_name`** — Name for the target serverless Redshift workgroup - only required if `cluster_id` not provided.
- **`secret_arn`** — The ARN for the secret to be used for authentication - only required if `db_user` not provided.
- **`db_user`** — The database user to generate temporary credentials for - only required if `secret_arn` not provided.
- **`sleep: float`** — Number of seconds to sleep between result fetch attempts - defaults to 0.25.
- **`backoff`** — Factor by which to increase the sleep between result fetch attempts - defaults to 1.5.
- **`retries`** — Maximum number of result fetch attempts - defaults to 15.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

---

### connect

```python
wr.data_api.redshift.connect(
    cluster_id: 'str' = '',
    database: 'str' = '',
    workgroup_name: 'str' = '',
    secret_arn: 'str' = '',
    db_user: 'str' = '',
    boto3_session: 'boto3.Session | None' = None,
    **kwargs: 'Any'
) -> 'RedshiftDataApi'
```

Create a Redshift Data API connection.

:::note
When connecting to a standard Redshift cluster, `cluster_id` is used.
When connecting to Redshift Serverless, `workgroup_name` is used. These two arguments are mutually exclusive.
:::

**Parameters**

- **`cluster_id`** — Id for the target Redshift cluster - only required if `workgroup_name` not provided.
- **`database`** — Target database name.
- **`workgroup_name`** — Name for the target serverless Redshift workgroup - only required if `cluster_id` not provided.
- **`secret_arn`** — The ARN for the secret to be used for authentication - only required if `db_user` not provided.
- **`db_user`** — The database user to generate temporary credentials for - only required if `secret_arn` not provided.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`**kwargs`** — Any additional kwargs are passed to the underlying RedshiftDataApi class.

**Returns**

- A RedshiftDataApi connection instance that can be used with `wr.redshift.data_api.read_sql_query`.

---

### read_sql_query

```python
wr.data_api.redshift.read_sql_query(
    sql: 'str',
    con: 'RedshiftDataApi',
    database: 'str | None' = None,
    parameters: 'list[dict[str, Any]] | None' = None
) -> 'pd.DataFrame'
```

Run an SQL query on a RedshiftDataApi connection and return the result as a DataFrame.

**Parameters**

- **`sql`** — SQL query to run.
- **`con`** — A RedshiftDataApi connection instance
- **`database`** — Database to run query on - defaults to the database specified by `con`.
- **`parameters`** — A list of named parameters e.g. [{"name": "id", "value": "42"}].

**Returns**

- A Pandas DataFrame containing the query results.

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.data_api.redshift.read_sql_query(
>>>     sql="SELECT * FROM public.my_table",
>>>     con=con,
>>> )
```

```python
>>> import awswrangler as wr
>>> df = wr.data_api.redshift.read_sql_query(
>>>     sql="SELECT * FROM public.my_table WHERE id >= :id",
>>>     con=con,
>>>     parameters=[
>>>        {"name": "id", "value": "42"},
>>>     ],
>>> )
```

---
