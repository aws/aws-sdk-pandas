---
id: aws-clean-rooms
title: "AWS Clean Rooms"
sidebar_position: 16
---

# AWS Clean Rooms

Module: `wr.cleanrooms`

### read_sql_query

```python
wr.cleanrooms.read_sql_query(
    sql: 'str | None' = None,
    analysis_template_arn: 'str | None' = None,
    membership_id: 'str' = '',
    output_bucket: 'str' = '',
    output_prefix: 'str' = '',
    keep_files: 'bool' = True,
    params: 'dict[str, Any] | None' = None,
    chunksize: 'int | bool | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'Iterator[pd.DataFrame] | pd.DataFrame'
```

Execute Clean Rooms Protected SQL query and return the results as a Pandas DataFrame.

:::note
One of `sql` or `analysis_template_arn` must be supplied, not both.
:::

**Parameters**

- **`sql`** — SQL query
- **`analysis_template_arn`** — ARN of the analysis template
- **`membership_id`** — Membership ID
- **`output_bucket`** — S3 output bucket name
- **`output_prefix`** — S3 output prefix
- **`keep_files`** — Whether files in S3 output bucket/prefix are retained. 'True' by default
- **`params`** — (Client-side) If used in combination with the `sql` parameter, it's the Dict of parameters used for constructing the SQL query. Only named parameters are supported. The dict must be in the form {'name': 'value'} and the SQL query must contain `:name`. Note that for varchar columns and similar, you must surround the value in single quotes. (Server-side) If used in combination with the `analysis_template_arn` parameter, it's the Dict of parameters supplied with the analysis template. It must be a string to string dict in the form {'name': 'value'}.
- **`chunksize`** — If passed, the data is split into an iterable of DataFrames (Memory friendly). If `True` an iterable of DataFrames is returned without guarantee of chunksize. If an `INTEGER` is passed, an iterable of DataFrames is returned with maximum rows equal to the received INTEGER
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() is used as the maximum number of threads. If integer is provided, specified number is used
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}

**Returns**

- Pandas DataFrame or Generator of Pandas DataFrames if chunksize is provided.

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.cleanrooms.read_sql_query(
...     sql='SELECT DISTINCT...',
...     membership_id='membership-id',
...     output_bucket='output-bucket',
...     output_prefix='output-prefix',
... )
```

```python
>>> import awswrangler as wr
>>> df = wr.cleanrooms.read_sql_query(
...     analysis_template_arn='arn:aws:cleanrooms:...',
...     params={'param1': 'value1'},
...     membership_id='membership-id',
...     output_bucket='output-bucket',
...     output_prefix='output-prefix',
... )
```

---

### wait_query

```python
wr.cleanrooms.wait_query(
    membership_id: 'str',
    query_id: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> "'GetProtectedQueryOutputTypeDef'"
```

Wait for the Clean Rooms protected query to end.

**Parameters**

- **`membership_id`** — Membership ID
- **`query_id`** — Protected query execution ID
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- `Dict[str, Any]` Dictionary with the get_protected_query response.

**Raises**

- **`exceptions.QueryFailed`** — Raises exception with error message if protected query is cancelled, times out or fails.

**Examples**

```python
>>> import awswrangler as wr
>>> res = wr.cleanrooms.wait_query(membership_id='membership-id', query_id='query-id')
```

---
