---
id: dynamodb
title: "DynamoDB"
sidebar_position: 12
---

# DynamoDB

Module: `wr.dynamodb`

### delete_items

```python
wr.dynamodb.delete_items(
    items: 'list[dict[str, Any]]',
    table_name: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete all items in the specified DynamoDB table.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`items`** — List which contains the items that will be deleted.
- **`table_name`** — Name of the Amazon DynamoDB table.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

Writing rows of DataFrame

```python
>>> import awswrangler as wr
>>> wr.dynamodb.delete_items(
...     items=[{'key': 1}, {'key': 2, 'value': 'Hello'}],
...     table_name='table'
... )
```

---

### execute_statement

```python
wr.dynamodb.execute_statement(
    statement: 'str',
    parameters: 'list[Any] | None' = None,
    consistent_read: 'bool' = False,
    boto3_session: 'boto3.Session | None' = None
) -> 'Iterator[list[dict[str, Any]]] | None'
```

Run a PartiQL statement against a DynamoDB table.

**Parameters**

- **`statement`** — The PartiQL statement.
- **`parameters`** — The list of PartiQL parameters. These are applied to the statement in the order they are listed.
- **`consistent_read`** — The consistency of a read operation. If `True`, then a strongly consistent read is used. False by default.
- **`boto3_session`** — Boto3 Session. If None, the default boto3 Session is used.

**Returns**

- An iterator of the items from the statement response, if any.

**Examples**

Insert an item

```python
>>> import awswrangler as wr
>>> wr.dynamodb.execute_statement(
...     statement="INSERT INTO movies VALUE {'title': ?, 'year': ?, 'info': ?}",
...     parameters=[title, year, {"plot": plot, "rating": rating}],
... )
```

Select items

```python
>>> wr.dynamodb.execute_statement(
...     statement="SELECT * FROM movies WHERE title=? AND year=?",
...     parameters=[title, year],
... )
```

Update items

```python
>>> wr.dynamodb.execute_statement(
...     statement="UPDATE movies SET info.rating=? WHERE title=? AND year=?",
...     parameters=[rating, title, year],
... )
```

Delete items

```python
>>> wr.dynamodb.execute_statement(
...     statement="DELETE FROM movies WHERE title=? AND year=?",
...     parameters=[title, year],
... )
```

---

### get_table

```python
wr.dynamodb.get_table(table_name: 'str', boto3_session: 'boto3.Session | None' = None) -> "'Table'"
```

Get DynamoDB table object for specified table name.


:::warning
This API is deprecated and will be removed in future AWS SDK for Pandas releases.
:::


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`table_name`** — Name of the Amazon DynamoDB table.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Boto3 DynamoDB.Table object. https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table

---

### put_csv

```python
wr.dynamodb.put_csv(
    path: 'str | Path',
    table_name: 'str',
    boto3_session: 'boto3.Session | None' = None,
    use_threads: 'bool | int' = True,
    **pandas_kwargs: 'Any'
) -> 'None'
```

Write all items from a CSV file to a DynamoDB.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`path`** — Path as str or Path object to the CSV file which contains the items.
- **`table_name`** — Name of the Amazon DynamoDB table.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`use_threads`** — Used for Parallel Write requests. True (default) to enable concurrency, False to disable multiple threads. If enabled os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.
- **`pandas_kwargs`** — KEYWORD arguments forwarded to pandas.read_csv(). You can NOT pass `pandas_kwargs` explicit, just add valid Pandas arguments in the function call and awswrangler will accept it. e.g. wr.dynamodb.put_csv('items.csv', 'my_table', sep='|', na_values=['null', 'none'], skip_blank_lines=True) https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

**Examples**

Writing contents of CSV file

```python
>>> import awswrangler as wr
>>> wr.dynamodb.put_csv(
...     path='items.csv',
...     table_name='table'
... )
```

Writing contents of CSV file using pandas_kwargs

```python
>>> import awswrangler as wr
>>> wr.dynamodb.put_csv(
...     path='items.csv',
...     table_name='table',
...     sep='|',
...     na_values=['null', 'none']
... )
```

---

### put_df

```python
wr.dynamodb.put_df(
    df: 'pd.DataFrame',
    table_name: 'str',
    boto3_session: 'boto3.Session | None' = None,
    use_threads: 'bool | int' = True
) -> 'None'
```

Write all items from a DataFrame to a DynamoDB.


:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`df`** — `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
- **`table_name`** — Name of the Amazon DynamoDB table.
- **`use_threads`** — Used for Parallel Write requests. True (default) to enable concurrency, False to disable multiple threads. If enabled os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

Writing rows of DataFrame

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.dynamodb.put_df(
...     df=pd.DataFrame({'key': [1, 2, 3]}),
...     table_name='table'
... )
```

---

### put_items

```python
wr.dynamodb.put_items(
    items: 'list[dict[str, Any]] | list[Mapping[str, Any]]',
    table_name: 'str',
    boto3_session: 'boto3.Session | None' = None,
    use_threads: 'bool | int' = True
) -> 'None'
```

Insert all items to the specified DynamoDB table.


:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`items`** — List which contains the items that will be inserted.
- **`table_name`** — Name of the Amazon DynamoDB table.
- **`boto3_session`** — Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
- **`use_threads`** — Used for Parallel Write requests. True (default) to enable concurrency, False to disable multiple threads. If enabled os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.

**Examples**

Writing items

```python
>>> import awswrangler as wr
>>> wr.dynamodb.put_items(
...     items=[{'key': 1}, {'key': 2, 'value': 'Hello'}],
...     table_name='table'
... )
```

---

### put_json

```python
wr.dynamodb.put_json(
    path: 'str | Path',
    table_name: 'str',
    boto3_session: 'boto3.Session | None' = None,
    use_threads: 'bool | int' = True
) -> 'None'
```

Write all items from JSON file to a DynamoDB.

The JSON file can either contain a single item which will be inserted in the DynamoDB or an array of items
which all be inserted.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`path`** — Path as str or Path object to the JSON file which contains the items.
- **`table_name`** — Name of the Amazon DynamoDB table.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`use_threads`** — Used for Parallel Write requests. True (default) to enable concurrency, False to disable multiple threads. If enabled os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.

**Examples**

Writing contents of JSON file

```python
>>> import awswrangler as wr
>>> wr.dynamodb.put_json(
...     path='items.json',
...     table_name='table'
... )
```

---

### read_items

```python
wr.dynamodb.read_items(
    table_name: 'str',
    index_name: 'str | None' = None,
    partition_values: 'Sequence[Any] | None' = None,
    sort_values: 'Sequence[Any] | None' = None,
    filter_expression: 'ConditionBase | str | None' = None,
    key_condition_expression: 'ConditionBase | str | None' = None,
    expression_attribute_names: 'dict[str, str] | None' = None,
    expression_attribute_values: 'dict[str, Any] | None' = None,
    consistent: 'bool' = False,
    columns: 'Sequence[str] | None' = None,
    allow_full_scan: 'bool' = False,
    max_items_evaluated: 'int | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    as_dataframe: 'bool' = True,
    chunked: 'bool' = False,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None,
    key_schema: 'list[dict[str, str]] | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame] | _ItemsListType | Iterator[_ItemsListType]'
```

Read items from given DynamoDB table.

This function aims to gracefully handle (some of) the complexity of read actions
available in Boto3 towards a DynamoDB table, abstracting it away while providing
a single, unified entry point.

Under the hood, it wraps all the four available read actions: `get_item`, `batch_get_item`,
`query` and `scan`.

:::warning
To avoid a potentially costly Scan operation, please make sure to pass arguments such as
`partition_values` or `max_items_evaluated`. Note that `filter_expression` is applied AFTER a Scan
:::
:::note
Number of Parallel Scan segments is based on the `use_threads` argument.
A parallel scan with a large number of workers could consume all the provisioned throughput
of the table or index.
See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
:::
:::note
If `max_items_evaluated` is specified, then `use_threads=False` is enforced. This is because
it's not possible to limit the number of items in a Query/Scan operation across threads.
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- dtype_backend
:::

**Parameters**

- **`table_name`** — DynamoDB table name.
- **`index_name`** — Name of the secondary global or local index on the table. Defaults to None.
- **`partition_values`** — Partition key values to retrieve. Defaults to None.
- **`sort_values`** — Sort key values to retrieve. Defaults to None.
- **`filter_expression`** — Filter expression as string or combinations of boto3.dynamodb.conditions.Attr conditions. Defaults to None.
- **`key_condition_expression`** — Key condition expression as string or combinations of boto3.dynamodb.conditions.Key conditions. Defaults to None.
- **`expression_attribute_names`** — Mapping of placeholder and target attributes. Defaults to None.
- **`expression_attribute_values`** — Mapping of placeholder and target values. Defaults to None.
- **`consistent`** — If True, ensure that the performed read operation is strongly consistent, otherwise eventually consistent. Defaults to False.
- **`columns`** — Attributes to retain in the returned items. Defaults to None (all attributes).
- **`allow_full_scan`** — If True, allow full table scan without any filtering. Defaults to False.
- **`max_items_evaluated`** — Limit the number of items evaluated in case of query or scan operations. Defaults to None (all matching items). When set, `use_threads` is enforced to False.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`as_dataframe`** — If True, return items as pd.DataFrame, otherwise as list/dict. Defaults to True.
- **`chunked`** — If `True` an iterable of DataFrames/lists is returned. False by default.
- **`use_threads`** — Used for Parallel Scan requests. True (default) to enable concurrency, False to disable multiple threads. If enabled os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.
- **`key_schema`** — Key schema of the table (e.g. `[{"AttributeName": "key", "KeyType": "HASH"}]`). If provided, the library will bypass the `DescribeTable` API call, which can reduce network latency and prevent API throttling. Defaults to None.

**Raises**

- **`exceptions.InvalidArgumentType`** — When the specified table has also a sort key but only the partition values are specified.
- **`exceptions.InvalidArgumentCombination`** — When both partition and sort values sequences are specified but they have different lengths, or when provided parameters are not enough informative to proceed with a read operation.

**Returns**

- `pd.DataFrame | list[dict[str, Any]] | Iterable[pd.DataFrame] | Iterable[list[dict[str, Any]]]` A Data frame containing the retrieved items, or a dictionary of returned items. Alternatively, the return type can be an iterable of either type when `chunked=True`.

**Examples**

Reading 5 random items from a table

```python
>>> import awswrangler as wr
>>> df = wr.dynamodb.read_items(table_name='my-table', max_items_evaluated=5)
```

Strongly-consistent reading of a given partition value from a table

```python
>>> import awswrangler as wr
>>> df = wr.dynamodb.read_items(table_name='my-table', partition_values=['my-value'], consistent=True)
```

Reading items pairwise-identified by partition and sort values, from a table with a composite primary key

```python
>>> import awswrangler as wr
>>> df = wr.dynamodb.read_items(
...     table_name='my-table',
...     partition_values=['pv_1', 'pv_2'],
...     sort_values=['sv_1', 'sv_2']
... )
```

Reading items while retaining only specified attributes, automatically handling possible collision
with DynamoDB reserved keywords

```python
>>> import awswrangler as wr
>>> df = wr.dynamodb.read_items(
...     table_name='my-table',
...     partition_values=['my-value'],
...     columns=['connection', 'other_col'] # connection is a reserved keyword, managed under the hood!
... )
```

Reading all items from a table explicitly allowing full scan

```python
>>> import awswrangler as wr
>>> df = wr.dynamodb.read_items(table_name='my-table', allow_full_scan=True)
```

Reading items matching a KeyConditionExpression expressed with boto3.dynamodb.conditions.Key

```python
>>> import awswrangler as wr
>>> from boto3.dynamodb.conditions import Key
>>> df = wr.dynamodb.read_items(
...     table_name='my-table',
...     key_condition_expression=(Key('key_1').eq('val_1') & Key('key_2').eq('val_2'))
... )
```

Same as above, but with KeyConditionExpression as string

```python
>>> import awswrangler as wr
>>> df = wr.dynamodb.read_items(
...     table_name='my-table',
...     key_condition_expression='key_1 = :v1 and key_2 = :v2',
...     expression_attribute_values={':v1': 'val_1', ':v2': 'val_2'},
... )
```

Reading items matching a FilterExpression expressed with boto3.dynamodb.conditions.Attr
Note that FilterExpression is applied AFTER a Scan operation

```python
>>> import awswrangler as wr
>>> from boto3.dynamodb.conditions import Attr
>>> df = wr.dynamodb.read_items(
...     table_name='my-table',
...     filter_expression=Attr('my_attr').eq('this-value')
... )
```

Same as above, but with FilterExpression as string

```python
>>> import awswrangler as wr
>>> df = wr.dynamodb.read_items(
...     table_name='my-table',
...     filter_expression='my_attr = :v',
...     expression_attribute_values={':v': 'this-value'}
... )
```

Reading items involving an attribute which collides with DynamoDB reserved keywords

```python
>>> import awswrangler as wr
>>> df = wr.dynamodb.read_items(
...     table_name='my-table',
...     filter_expression='#operator = :v',
...     expression_attribute_names={'#operator': 'operator'},
...     expression_attribute_values={':v': 'this-value'}
... )
```

---

### read_partiql_query

```python
wr.dynamodb.read_partiql_query(
    query: 'str',
    parameters: 'list[Any] | None' = None,
    chunked: 'bool' = False,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Read data from a DynamoDB table via a PartiQL query.

**Parameters**

- **`query`** — The PartiQL statement.
- **`parameters`** — The list of PartiQL parameters. These are applied to the statement in the order they are listed.
- **`chunked`** — If `True` an iterable of DataFrames is returned. False by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Result as Pandas DataFrame.

**Examples**

Select all contents from a table

```python
>>> import awswrangler as wr
>>> wr.dynamodb.read_partiql_query(
...     query="SELECT * FROM my_table WHERE title=? AND year=?",
...     parameters=[title, year],
... )
```

Select specific columns from a table

```python
>>> wr.dynamodb.read_partiql_query(
...     query="SELECT id FROM table"
... )
```

---
