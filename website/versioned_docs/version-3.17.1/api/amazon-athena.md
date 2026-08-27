---
id: amazon-athena
title: "Amazon Athena"
sidebar_position: 3
---

# Amazon Athena

Module: `wr.athena`

### create_athena_bucket

```python
wr.athena.create_athena_bucket(boto3_session: 'boto3.Session | None' = None) -> 'str'
```

Create the default Athena bucket if it doesn't exist.

The bucket name is derived from the caller's account ID and region
(`aws-athena-query-results-{account_id}-{region}`). Because S3 bucket
names are global, this function verifies that the bucket is owned by the
caller's account before returning; if another account owns it, a
:class:`~awswrangler.exceptions.InvalidArgumentValue` is raised to prevent
query results from being written to a bucket controlled by a third party.

**Parameters**

- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Bucket s3 path (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

**Examples**

```python
>>> import awswrangler as wr
>>> wr.athena.create_athena_bucket()
's3://aws-athena-query-results-ACCOUNT-REGION/'
```

---

### create_spark_session

```python
wr.athena.create_spark_session(
    workgroup: 'str',
    coordinator_dpu_size: 'int' = 1,
    max_concurrent_dpus: 'int' = 5,
    default_executor_dpu_size: 'int' = 1,
    additional_configs: 'dict[str, Any] | None' = None,
    spark_properties: 'dict[str, Any] | None' = None,
    notebook_version: 'str | None' = None,
    idle_timeout: 'int' = 15,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create session and wait until ready to accept calculations.

**Parameters**

- **`workgroup`** — Athena workgroup name. Must be Spark-enabled.
- **`coordinator_dpu_size`** — The number of DPUs to use for the coordinator. A coordinator is a special executor that orchestrates processing work and manages other executors in a notebook session. The default is 1.
- **`max_concurrent_dpus`** — The maximum number of DPUs that can run concurrently. The default is 5.
- **`default_executor_dpu_size`** — The default number of DPUs to use for executors. The default is 1.
- **`additional_configs`** — Contains additional engine parameter mappings in the form of key-value pairs.
- **`spark_properties`** — Contains SparkProperties in the form of key-value pairs.Specifies custom jar files and Spark properties for use cases like cluster encryption, table formats, and general Spark tuning.
- **`notebook_version`** — The notebook version. This value is supplied automatically for notebook sessions in the Athena console and is not required for programmatic session access. The only valid notebook version is Athena notebook version 1. If you specify a value for NotebookVersion, you must also specify a value for NotebookId
- **`idle_timeout`** — The idle timeout in minutes for the session. The default is 15.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Session ID

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.athena.create_spark_session(workgroup="...", max_concurrent_dpus=10)
```

---

### create_ctas_table

```python
wr.athena.create_ctas_table(
    sql: 'str',
    database: 'str | None' = None,
    ctas_table: 'str | None' = None,
    ctas_database: 'str | None' = None,
    s3_output: 'str | None' = None,
    storage_format: 'str | None' = None,
    write_compression: 'str | None' = None,
    partitioning_info: 'list[str] | None' = None,
    bucketing_info: 'typing.BucketingInfoTuple | None' = None,
    field_delimiter: 'str | None' = None,
    schema_only: 'bool' = False,
    workgroup: 'str' = 'primary',
    data_source: 'str | None' = None,
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    categories: 'list[str] | None' = None,
    wait: 'bool' = False,
    athena_query_wait_polling_delay: 'float' = 1.0,
    execution_params: 'list[str] | None' = None,
    params: 'dict[str, Any] | list[str] | None' = None,
    paramstyle: "Literal['qmark', 'named']" = 'named',
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, str | _QueryMetadata]'
```

Create a new table populated with the results of a SELECT query.

https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- database

- athena_query_wait_polling_delay

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`sql`** — SELECT SQL query.
- **`database`** — The name of the database where the original table is stored.
- **`ctas_table`** — The name of the CTAS table. If None, a name with a random string is used.
- **`ctas_database`** — The name of the alternative database where the CTAS table should be stored. If None, `database` is used, that is the CTAS table is stored in the same database as the original table.
- **`s3_output`** — The output Amazon S3 path. If None, either the Athena workgroup or client-side location setting is used. If a workgroup enforces a query results location, then it overrides this argument.
- **`storage_format`** — The storage format for the CTAS query results, such as ORC, PARQUET, AVRO, JSON, or TEXTFILE. PARQUET by default.
- **`write_compression`** — The compression type to use for any storage format that allows compression to be specified.
- **`partitioning_info`** — A list of columns by which the CTAS table will be partitioned.
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`field_delimiter`** — The single-character field delimiter for files in CSV, TSV, and text files.
- **`schema_only`** — _description_, by default False
- **`workgroup`** — Athena workgroup. Primary by default.
- **`data_source`** — Data Source / Catalog name. If None, 'AwsDataCatalog' is used.
- **`encryption`** — Valid values: [None, 'SSE_S3', 'SSE_KMS']. Note: 'CSE_KMS' is not supported.
- **`kms_key`** — For SSE-KMS, this is the KMS key ARN or ID.
- **`categories`** — List of columns names that should be returned as pandas.Categorical. Recommended for memory restricted environments.
- **`wait`** — Whether to wait for the query to finish and return a dictionary with the Query metadata.
- **`athena_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the Athena query has completed.
- **`execution_params`** — [**DEPRECATED**] A list of values for the parameters that are used in the SQL query. This parameter is on a deprecation path. Use `params` and `paramstyle` instead.
- **`params`** — Dictionary or list of parameters to pass to execute method. The syntax used to pass parameters depends on the configuration of `paramstyle`.
- **`paramstyle`** — The syntax style to use for the parameters. Supported values are `named` and `qmark`. The default is `named`.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None``.

**Returns**

- A dictionary with the the CTAS database and table names. If `wait` is `False`, the query ID is included, otherwise a Query metadata object is added instead.

**Examples**

Select all into a new table and encrypt the results

```python
>>> import awswrangler as wr
>>> wr.athena.create_ctas_table(
...     sql="select * from table",
...     database="default",
...     encryption="SSE_KMS",
...     kms_key="1234abcd-12ab-34cd-56ef-1234567890ab",
... )
{'ctas_database': 'default', 'ctas_table': 'temp_table_5669340090094....', 'ctas_query_id': 'cc7dfa81-831d-...'}
```

Create a table with schema only

```python
>>> wr.athena.create_ctas_table(
...     sql="select col1, col2 from table",
...     database="default",
...     ctas_table="my_ctas_table",
...     schema_only=True,
...     wait=True,
... )
```

Partition data and save to alternative CTAS database

```python
>>> wr.athena.create_ctas_table(
...     sql="select * from table",
...     database="default",
...     ctas_database="my_ctas_db",
...     storage_format="avro",
...     write_compression="snappy",
...     partitioning_info=["par0", "par1"],
...     wait=True,
... )
```

---

### generate_create_query

```python
wr.athena.generate_create_query(
    table: 'str',
    database: 'str | None' = None,
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Generate the query that created a table(EXTERNAL_TABLE) or a view(VIRTUAL_TABLE).

Analyzes an existing table named table_name to generate the query that created it.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`table`** — Table name.
- **`database`** — Database name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- The query that created the table or view.

**Examples**

```python
>>> import awswrangler as wr
>>> view_create_query: str = wr.athena.generate_create_query(table='my_view', database='default')
```

---

### get_query_columns_types

```python
wr.athena.get_query_columns_types(
    query_execution_id: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, str]'
```

Get the data type of all columns queried.

https://docs.aws.amazon.com/athena/latest/ug/data-types.html

**Parameters**

- **`query_execution_id`** — Athena query execution ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Dictionary with all data types.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.athena.get_query_columns_types('query-execution-id')
{'col0': 'int', 'col1': 'double'}
```

---

### get_query_execution

```python
wr.athena.get_query_execution(
    query_execution_id: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Fetch query execution details.

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_query_execution

**Parameters**

- **`query_execution_id`** — Athena query execution ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Dictionary with the get_query_execution response.

**Examples**

```python
>>> import awswrangler as wr
>>> res = wr.athena.get_query_execution(query_execution_id='query-execution-id')
```

---

### get_query_executions

```python
wr.athena.get_query_executions(
    query_execution_ids: 'list[str]',
    return_unprocessed: 'bool' = False,
    boto3_session: 'boto3.Session | None' = None
) -> 'tuple[pd.DataFrame, pd.DataFrame] | pd.DataFrame'
```

From specified query execution IDs, return a DataFrame of query execution details.

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.batch_get_query_execution

**Parameters**

- **`query_execution_ids`** — Athena query execution IDs.
- **`return_unprocessed`** — True to also return query executions id that are unable to be processed. False to only return DataFrame of query execution details. Default is False
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- DataFrame containing either information about query execution details. Optionally, another DataFrame containing unprocessed query execution IDs.

**Examples**

```python
>>> import awswrangler as wr
>>> query_executions_df, unprocessed_query_executions_df = wr.athena.get_query_executions(
>>>     query_execution_ids=['query-execution-id','query-execution-id1']
>>> )
```

---

### get_query_results

```python
wr.athena.get_query_results(
    query_execution_id: 'str',
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    categories: 'list[str] | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    chunksize: 'int | bool | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None,
    athena_query_wait_polling_delay: 'float' = 1.0
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Get AWS Athena SQL query results as a Pandas DataFrame.


:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- athena_query_wait_polling_delay

- chunksize

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`query_execution_id`** — SQL query's execution_id on AWS Athena.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`categories`** — List of columns names that should be returned as pandas.Categorical. Recommended for memory restricted environments.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`chunksize`** — If passed will split the data in a Iterable of DataFrames (Memory friendly). If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize. If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.
- **`athena_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the Athena query has completed.

**Returns**

- Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

**Examples**

```python
>>> import awswrangler as wr
>>> res = wr.athena.get_query_results(
...     query_execution_id="cbae5b41-8103-4709-95bb-887f88edd4f2"
... )
```

---

### get_named_query_statement

```python
wr.athena.get_named_query_statement(
    named_query_id: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get the named query statement string from a query ID.

**Parameters**

- **`named_query_id`** — The unique ID of the query. Used to get the query statement from a saved query. Requires access to the workgroup where the query is saved.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- The named query statement string

---

### get_work_group

```python
wr.athena.get_work_group(
    workgroup: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Return information about the workgroup with the specified name.

**Parameters**

- **`workgroup`** — Work Group name.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_work_group

**Examples**

```python
>>> import awswrangler as wr
>>> res = wr.athena.get_work_group(workgroup='workgroup_name')
```

---

### list_query_executions

```python
wr.athena.list_query_executions(
    workgroup: 'str | None' = None,
    max_results: 'int | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str]'
```

Fetch list query execution IDs ran in specified workgroup or primary work group if not specified.

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.list_query_executions

**Parameters**

- **`workgroup`** — The name of the workgroup from which the query_id are being returned. If not specified, a list of available query execution IDs for the queries in the primary workgroup is returned.
- **`max_results`** — The maximum number of query execution IDs to return in this request. If not present, all execution IDs will be returned.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- List of query execution IDs.

**Examples**

```python
>>> import awswrangler as wr
>>> res = wr.athena.list_query_executions(workgroup='workgroup-name')
```

---

### read_sql_query

```python
wr.athena.read_sql_query(
    sql: 'str',
    database: 'str',
    ctas_approach: 'bool' = True,
    unload_approach: 'bool' = False,
    ctas_parameters: 'typing.AthenaCTASSettings | None' = None,
    unload_parameters: 'typing.AthenaUNLOADSettings | None' = None,
    categories: 'list[str] | None' = None,
    chunksize: 'int | bool | None' = None,
    s3_output: 'str | None' = None,
    workgroup: 'str' = 'primary',
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    keep_files: 'bool' = True,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    client_request_token: 'str | None' = None,
    athena_cache_settings: 'typing.AthenaCacheSettings | None' = None,
    data_source: 'str | None' = None,
    athena_query_wait_polling_delay: 'float' = 1.0,
    params: 'dict[str, Any] | list[str] | None' = None,
    paramstyle: "Literal['qmark', 'named']" = 'named',
    result_reuse_configuration: 'dict[str, Any] | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Execute any SQL query on AWS Athena and return the results as a Pandas DataFrame.

**Related tutorial:**

- `Amazon Athena <https://aws-sdk-pandas.readthedocs.io/en/3.17.1/
  tutorials/006%20-%20Amazon%20Athena.html>`_
- `Athena Cache <https://aws-sdk-pandas.readthedocs.io/en/3.17.1/
  tutorials/019%20-%20Athena%20Cache.html>`_
- `Global Configurations <https://aws-sdk-pandas.readthedocs.io/en/3.17.1/
  tutorials/021%20-%20Global%20Configurations.html>`_

**There are three approaches available through ctas_approach and unload_approach parameters:**

**1** - ctas_approach=True (Default):

Wrap the query with a CTAS and then reads the table data as parquet directly from s3.

PROS:

- Faster for mid and big result sizes.
- Can handle some level of nested types.

CONS:

- Requires create/delete table permissions on Glue.
- Does not support timestamp with time zone
- Does not support columns with repeated names.
- Does not support columns with undefined data types.
- A temporary table will be created and then deleted immediately.
- Does not support custom data_source/catalog_id.

**2** - unload_approach=True and ctas_approach=False:

Does an UNLOAD query on Athena and parse the Parquet result on s3.

PROS:

- Faster for mid and big result sizes.
- Can handle some level of nested types.
- Does not modify Glue Data Catalog

CONS:

- Output S3 path must be empty.
- Does not support timestamp with time zone.
- Does not support columns with repeated names.
- Does not support columns with undefined data types.

**3** - ctas_approach=False:

Does a regular query on Athena and parse the regular CSV result on s3.

PROS:

- Faster for small result sizes (less latency).
- Does not require create/delete table permissions on Glue
- Supports timestamp with time zone.
- Support custom data_source/catalog_id.

CONS:

- Slower for big results (But stills faster than other libraries that uses the regular Athena's API)
- Does not handle nested types at all.

:::note
The resulting DataFrame (or every DataFrame in the returned Iterator for chunked queries) have a
`query_metadata` attribute, which brings the query result metadata returned by
`Boto3/Athena <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services
/athena.html#Athena.Client.get_query_execution>`_ .

For a practical example check out the
`related tutorial <https://aws-sdk-pandas.readthedocs.io/en/3.17.1/
tutorials/024%20-%20Athena%20Query%20Metadata.html>`_!
:::

:::note
Valid encryption modes: [None, 'SSE_S3', 'SSE_KMS'].

`P.S. 'CSE_KMS' is not supported.`
:::
:::note
Create the default Athena bucket if it doesn't exist and s3_output is None.

(E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)
:::
:::note
`chunksize` argument (Memory Friendly) (i.e batching):

Return an Iterable of DataFrames instead of a regular DataFrame.

There are two batching strategies:

- If **chunksize=True**, depending on the size of the data, one or more data frames are returned per file in the query result.
  Unlike **chunksize=INTEGER**, rows from different files are not mixed in the resulting data frames.

- If **chunksize=INTEGER**, awswrangler iterates on the data by number of rows equal to the received INTEGER.

`P.S.` `chunksize=True` is faster and uses less memory while `chunksize=INTEGER` is more precise
in number of rows for each data frame.

`P.P.S.` If `ctas_approach=False` and `chunksize=True`, you will always receive an iterator with a
single DataFrame because regular Athena queries only produces a single output file.
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



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- ctas_approach

- database

- athena_cache_settings

- athena_query_wait_polling_delay

- workgroup

- chunksize

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`sql`** — SQL query.
- **`database`** — AWS Glue/Athena database name - It is only the origin database from where the query will be launched. You can still using and mixing several databases writing the full table name within the sql (e.g. `database.table`).
- **`ctas_approach`** — Wraps the query using a CTAS, and read the resulted parquet data on S3. If false, read the regular CSV on S3.
- **`unload_approach`** — Wraps the query using UNLOAD, and read the results from S3. Only PARQUET format is supported.
- **`ctas_parameters`** — Parameters of the CTAS such as database, temp_table_name, bucketing_info, and compression.
- **`unload_parameters`** — Parameters of the UNLOAD such as format, compression, field_delimiter, and partitioned_by.
- **`categories`** — List of columns names that should be returned as pandas.Categorical. Recommended for memory restricted environments.
- **`chunksize`** — If passed will split the data in a Iterable of DataFrames (Memory friendly). If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize. If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
- **`s3_output`** — Amazon S3 path. Not required for the regular query path (`ctas_approach=False`, `unload_approach=False`) when the workgroup uses managed query results. Still used for CTAS/UNLOAD paths.
- **`workgroup`** — Athena workgroup. Primary by default.
- **`encryption`** — Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
- **`kms_key`** — For SSE-KMS, this is the KMS key ARN or ID.
- **`keep_files`** — Whether staging files produced by Athena are retained. 'True' by default.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`client_request_token`** — A unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once). If another StartQueryExecution request is received, the same response is returned and another query is not created. If a parameter has changed, for example, the QueryString , an error is returned. If you pass the same client_request_token value with different parameters the query fails with error message "Idempotent parameters do not match". Use this only with ctas_approach=False and unload_approach=False and disabled cache.
- **`athena_cache_settings`** — Parameters of the Athena cache settings such as max_cache_seconds, max_cache_query_inspections, max_remote_cache_entries, and max_local_cache_entries. AthenaCacheSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaCacheSettings or as a regular Python dict. If cached results are valid, awswrangler ignores the `ctas_approach`, `s3_output`, `encryption`, `kms_key`, `keep_files` and `ctas_temp_table_name` params. If reading cached data fails for any reason, execution falls back to the usual query run path.
- **`data_source`** — Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
- **`athena_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the Athena query has completed.
- **`params`** — Parameters that will be used for constructing the SQL query. Only named or question mark parameters are supported. The parameter style needs to be specified in the `paramstyle` parameter. For `paramstyle="named"`, this value needs to be a dictionary. The dict needs to contain the information in the form `{'name': 'value'}` and the SQL query needs to contain `:name`. The formatter will be applied client-side in this scenario. For `paramstyle="qmark"`, this value needs to be a list of strings. The formatter will be applied server-side. The values are applied sequentially to the parameters in the query in the order in which the parameters occur.
- **`paramstyle`** — Determines the style of `params`. Possible values are: - `named` - `qmark`
- **`result_reuse_configuration`** — A structure that contains the configuration settings for reusing query results. This parameter is only valid when both `ctas_approach` and `unload_approach` are set to `False`. See also: https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.

**Returns**

- Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.athena.read_sql_query(sql="...", database="...")
>>> scanned_bytes = df.query_metadata["Statistics"]["DataScannedInBytes"]
```

```python
>>> import awswrangler as wr
>>> df = wr.athena.read_sql_query(
...     sql="SELECT * FROM my_table WHERE name=:name AND city=:city",
...     params={"name": "filtered_name", "city": "filtered_city"}
... )
```

```python
>>> import awswrangler as wr
>>> df = wr.athena.read_sql_query(
...     sql="...",
...     database="...",
...     athena_cache_settings={
...          "max_cache_seconds": 90,
...     },
... )
```

---

### read_sql_table

```python
wr.athena.read_sql_table(
    table: 'str',
    database: 'str',
    unload_approach: 'bool' = False,
    unload_parameters: 'typing.AthenaUNLOADSettings | None' = None,
    ctas_approach: 'bool' = True,
    ctas_parameters: 'typing.AthenaCTASSettings | None' = None,
    categories: 'list[str] | None' = None,
    chunksize: 'int | bool | None' = None,
    s3_output: 'str | None' = None,
    workgroup: 'str' = 'primary',
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    keep_files: 'bool' = True,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    client_request_token: 'str | None' = None,
    athena_cache_settings: 'typing.AthenaCacheSettings | None' = None,
    data_source: 'str | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Extract the full table AWS Athena and return the results as a Pandas DataFrame.

**Related tutorial:**

- `Amazon Athena <https://aws-sdk-pandas.readthedocs.io/en/3.17.1/
  tutorials/006%20-%20Amazon%20Athena.html>`_
- `Athena Cache <https://aws-sdk-pandas.readthedocs.io/en/3.17.1/
  tutorials/019%20-%20Athena%20Cache.html>`_
- `Global Configurations <https://aws-sdk-pandas.readthedocs.io/en/3.17.1/
  tutorials/021%20-%20Global%20Configurations.html>`_

**There are three approaches available through ctas_approach and unload_approach parameters:**

**1** - ctas_approach=True (Default):

Wrap the query with a CTAS and then reads the table data as parquet directly from s3.

PROS:

- Faster for mid and big result sizes.
- Can handle some level of nested types.

CONS:

- Requires create/delete table permissions on Glue.
- Does not support timestamp with time zone
- Does not support columns with repeated names.
- Does not support columns with undefined data types.
- A temporary table will be created and then deleted immediately.
- Does not support custom data_source/catalog_id.

**2** - unload_approach=True and ctas_approach=False:

Does an UNLOAD query on Athena and parse the Parquet result on s3.

PROS:

- Faster for mid and big result sizes.
- Can handle some level of nested types.
- Does not modify Glue Data Catalog

CONS:

- Output S3 path must be empty.
- Does not support timestamp with time zone.
- Does not support columns with repeated names.
- Does not support columns with undefined data types.

**3** - ctas_approach=False:

Does a regular query on Athena and parse the regular CSV result on s3.

PROS:

- Faster for small result sizes (less latency).
- Does not require create/delete table permissions on Glue
- Supports timestamp with time zone.
- Support custom data_source/catalog_id.

CONS:

- Slower for big results (But stills faster than other libraries that uses the regular Athena's API)
- Does not handle nested types at all.

:::note
The resulting DataFrame (or every DataFrame in the returned Iterator for chunked queries) have a
`query_metadata` attribute, which brings the query result metadata returned by
`Boto3/Athena <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services
/athena.html#Athena.Client.get_query_execution>`_ .

For a practical example check out the
`related tutorial <https://aws-sdk-pandas.readthedocs.io/en/3.17.1/
tutorials/024%20-%20Athena%20Query%20Metadata.html>`_!
:::

:::note
Valid encryption modes: [None, 'SSE_S3', 'SSE_KMS'].

`P.S. 'CSE_KMS' is not supported.`
:::
:::note
Create the default Athena bucket if it doesn't exist and s3_output is None.

(E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)
:::
:::note
`chunksize` argument (Memory Friendly) (i.e batching):

Return an Iterable of DataFrames instead of a regular DataFrame.

There are two batching strategies:

- If **chunksize=True**, depending on the size of the data, one or more data frames are returned per file in the query result.
  Unlike **chunksize=INTEGER**, rows from different files are not mixed in the resulting data frames.

- If **chunksize=INTEGER**, awswrangler iterates on the data by number of rows equal to the received INTEGER.

`P.S.` `chunksize=True` is faster and uses less memory while `chunksize=INTEGER` is more precise
in number of rows for each data frame.

`P.P.S.` If `ctas_approach=False` and `chunksize=True`, you will always receive an iterator with a
single DataFrame because regular Athena queries only produces a single output file.
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



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- ctas_approach

- database

- athena_cache_settings

- workgroup

- chunksize

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`table`** — Table name.
- **`database`** — AWS Glue/Athena database name.
- **`ctas_approach`** — Wraps the query using a CTAS, and read the resulted parquet data on S3. If false, read the regular CSV on S3.
- **`unload_approach`** — Wraps the query using UNLOAD, and read the results from S3. Only PARQUET format is supported.
- **`ctas_parameters`** — Parameters of the CTAS such as database, temp_table_name, bucketing_info, and compression.
- **`unload_parameters`** — Parameters of the UNLOAD such as format, compression, field_delimiter, and partitioned_by.
- **`categories`** — List of columns names that should be returned as pandas.Categorical. Recommended for memory restricted environments.
- **`chunksize`** — If passed will split the data in a Iterable of DataFrames (Memory friendly). If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize. If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
- **`s3_output`** — AWS S3 path.
- **`workgroup`** — Athena workgroup. Primary by default.
- **`encryption`** — Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
- **`kms_key`** — For SSE-KMS, this is the KMS key ARN or ID.
- **`keep_files`** — Should awswrangler delete or keep the staging files produced by Athena?
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`client_request_token`** — A unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once). If another StartQueryExecution request is received, the same response is returned and another query is not created. If a parameter has changed, for example, the QueryString , an error is returned. If you pass the same client_request_token value with different parameters the query fails with error message "Idempotent parameters do not match". Use this only with ctas_approach=False and unload_approach=False and disabled cache.
- **`athena_cache_settings`** — Parameters of the Athena cache settings such as max_cache_seconds, max_cache_query_inspections, max_remote_cache_entries, and max_local_cache_entries. AthenaCacheSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaCacheSettings or as a regular Python dict. If cached results are valid, awswrangler ignores the `ctas_approach`, `s3_output`, `encryption`, `kms_key`, `keep_files` and `ctas_temp_table_name` params. If reading cached data fails for any reason, execution falls back to the usual query run path.
- **`data_source`** — Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.

**Returns**

- Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.athena.read_sql_table(table="...", database="...")
>>> scanned_bytes = df.query_metadata["Statistics"]["DataScannedInBytes"]
```

---

### repair_table

```python
wr.athena.repair_table(
    table: 'str',
    database: 'str | None' = None,
    data_source: 'str | None' = None,
    s3_output: 'str | None' = None,
    workgroup: 'str' = 'primary',
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    athena_query_wait_polling_delay: 'float' = 1.0,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Run the Hive's metastore consistency check: 'MSCK REPAIR TABLE table;'.

Recovers partitions and data associated with partitions.
Use this statement when you add partitions to the catalog.
It is possible it will take some time to add all partitions.
If this operation times out, it will be in an incomplete state
where only a few partitions are added to the catalog.

:::note
Create the default Athena bucket if it doesn't exist and s3_output is None.
(E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- database

- athena_query_wait_polling_delay

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`table`** — Table name.
- **`database`** — AWS Glue/Athena database name.
- **`data_source`** — Data Source / Catalog name. If None, 'AwsDataCatalog' is used.
- **`s3_output`** — AWS S3 path.
- **`workgroup`** — Athena workgroup. Primary by default.
- **`encryption`** — None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
- **`kms_key`** — For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
- **`athena_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the Athena query has completed.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Query final state ('SUCCEEDED', 'FAILED', 'CANCELLED').

**Examples**

```python
>>> import awswrangler as wr
>>> query_final_state = wr.athena.repair_table(table='...', database='...')
```

---

### run_spark_calculation

```python
wr.athena.run_spark_calculation(
    code: 'str',
    workgroup: 'str',
    session_id: 'str | None' = None,
    coordinator_dpu_size: 'int' = 1,
    max_concurrent_dpus: 'int' = 5,
    default_executor_dpu_size: 'int' = 1,
    additional_configs: 'dict[str, Any] | None' = None,
    spark_properties: 'dict[str, Any] | None' = None,
    notebook_version: 'str | None' = None,
    idle_timeout: 'int' = 15,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Execute Spark Calculation and wait for completion.

**Parameters**

- **`code`** — A string that contains the code for the calculation.
- **`workgroup`** — Athena workgroup name. Must be Spark-enabled.
- **`session_id`** — The session id. If not passed, a session will be started.
- **`coordinator_dpu_size`** — The number of DPUs to use for the coordinator. A coordinator is a special executor that orchestrates processing work and manages other executors in a notebook session. The default is 1.
- **`max_concurrent_dpus`** — The maximum number of DPUs that can run concurrently. The default is 5.
- **`default_executor_dpu_size`** — The default number of DPUs to use for executors. The default is 1.
- **`additional_configs`** — Contains additional engine parameter mappings in the form of key-value pairs.
- **`spark_properties`** — Contains SparkProperties in the form of key-value pairs.Specifies custom jar files and Spark properties for use cases like cluster encryption, table formats, and general Spark tuning.
- **`notebook_version`** — The notebook version. This value is supplied automatically for notebook sessions in the Athena console and is not required for programmatic session access. The only valid notebook version is Athena notebook version 1. If you specify a value for NotebookVersion, you must also specify a value for NotebookId
- **`idle_timeout`** — The idle timeout in minutes for the session. The default is 15.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Calculation response

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.athena.run_spark_calculation(
...     code="print(spark)",
...     workgroup="...",
... )
```

---

### show_create_table

```python
wr.athena.show_create_table(
    table: 'str',
    database: 'str | None' = None,
    s3_output: 'str | None' = None,
    workgroup: 'str' = 'primary',
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    athena_query_wait_polling_delay: 'float' = 1.0,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Generate the query that created it: 'SHOW CREATE TABLE table;'.

Analyzes an existing table named table_name to generate the query that created it.

:::note
Create the default Athena bucket if it doesn't exist and s3_output is None.
(E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- database

- athena_query_wait_polling_delay

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`table`** — Table name.
- **`database`** — AWS Glue/Athena database name.
- **`s3_output`** — AWS S3 path.
- **`workgroup`** — Athena workgroup. Primary by default.
- **`encryption`** — None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
- **`kms_key`** — For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
- **`athena_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the Athena query has completed.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- The query that created the table.

**Examples**

```python
>>> import awswrangler as wr
>>> df_table = wr.athena.show_create_table(table='my_table', database='default')
```

---

### start_query_execution

```python
wr.athena.start_query_execution(
    sql: 'str',
    database: 'str | None' = None,
    s3_output: 'str | None' = None,
    workgroup: 'str' = 'primary',
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    params: 'dict[str, Any] | list[str] | None' = None,
    paramstyle: "Literal['qmark', 'named']" = 'named',
    result_reuse_configuration: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    client_request_token: 'str | None' = None,
    athena_cache_settings: 'typing.AthenaCacheSettings | None' = None,
    athena_query_wait_polling_delay: 'float' = 1.0,
    data_source: 'str | None' = None,
    wait: 'bool' = False
) -> 'str | dict[str, Any]'
```

Start a SQL Query against AWS Athena.

:::note
Create the default Athena bucket if it doesn't exist and s3_output is None.
Not required when the workgroup uses managed query results.
(E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- database

- athena_cache_settings

- athena_query_wait_polling_delay

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`sql`** — SQL query.
- **`database`** — AWS Glue/Athena database name.
- **`s3_output`** — AWS S3 path. Not required when the workgroup uses managed query results.
- **`workgroup`** — Athena workgroup. Primary by default.
- **`encryption`** — None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
- **`kms_key`** — For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
- **`params`** — Parameters that will be used for constructing the SQL query. Only named or question mark parameters are supported. The parameter style needs to be specified in the `paramstyle` parameter. For `paramstyle="named"`, this value needs to be a dictionary. The dict needs to contain the information in the form `{'name': 'value'}` and the SQL query needs to contain `:name`. The formatter will be applied client-side in this scenario. For `paramstyle="qmark"`, this value needs to be a list of strings. The formatter will be applied server-side. The values are applied sequentially to the parameters in the query in the order in which the parameters occur.
- **`paramstyle`** — Determines the style of `params`. Possible values are: - `named` - `qmark`
- **`result_reuse_configuration`** — A structure that contains the configuration settings for reusing query results. See also: https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`client_request_token`** — A unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once). If another StartQueryExecution request is received, the same response is returned and another query is not created. If a parameter has changed, for example, the QueryString , an error is returned. If you pass the same client_request_token value with different parameters the query fails with error message "Idempotent parameters do not match". Use this only with ctas_approach=False and unload_approach=False and disabled cache.
- **`athena_cache_settings`** — Parameters of the Athena cache settings such as max_cache_seconds, max_cache_query_inspections, max_remote_cache_entries, and max_local_cache_entries. AthenaCacheSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaCacheSettings or as a regular Python dict. If cached results are valid, awswrangler ignores the `ctas_approach`, `s3_output`, `encryption`, `kms_key`, `keep_files` and `ctas_temp_table_name` params. If reading cached data fails for any reason, execution falls back to the usual query run path.
- **`athena_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the Athena query has completed.
- **`data_source`** — Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
- **`wait`** — Indicates whether to wait for the query to finish and return a dictionary with the query execution response.

**Returns**

- Query execution ID if `wait` is set to `False`, dictionary with the get_query_execution response otherwise.

**Examples**

Querying into the default data source (Amazon s3 - 'AwsDataCatalog')

```python
>>> import awswrangler as wr
>>> query_exec_id = wr.athena.start_query_execution(sql='...', database='...')
```

Querying into another data source (PostgreSQL, Redshift, etc)

```python
>>> import awswrangler as wr
>>> query_exec_id = wr.athena.start_query_execution(sql='...', database='...', data_source='...')
```

---

### stop_query_execution

```python
wr.athena.stop_query_execution(
    query_execution_id: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Stop a query execution.

Requires you to have access to the workgroup in which the query ran.

**Parameters**

- **`query_execution_id`** — Athena query execution ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.athena.stop_query_execution(query_execution_id='query-execution-id')
```

---

### to_iceberg

```python
wr.athena.to_iceberg(
    df: 'pd.DataFrame',
    database: 'str',
    table: 'str',
    temp_path: 'str | None' = None,
    index: 'bool' = False,
    table_location: 'str | None' = None,
    partition_cols: 'list[str] | None' = None,
    merge_cols: 'list[str] | None' = None,
    merge_condition: "Literal['update', 'ignore']" = 'update',
    merge_match_nulls: 'bool' = False,
    keep_files: 'bool' = True,
    data_source: 'str | None' = None,
    s3_output: 'str | None' = None,
    workgroup: 'str' = 'primary',
    mode: "Literal['append', 'overwrite', 'overwrite_partitions']" = 'append',
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None,
    additional_table_properties: 'dict[str, Any] | None' = None,
    dtype: 'dict[str, str] | None' = None,
    catalog_id: 'str | None' = None,
    schema_evolution: 'bool' = False,
    fill_missing_columns_in_df: 'bool' = True,
    glue_table_settings: 'GlueTableSettings | None' = None
) -> 'None'
```

Insert into Athena Iceberg table using INSERT INTO ... SELECT. Will create Iceberg table if it does not exist.

Creates temporary external table, writes staged files and inserts via INSERT INTO ... SELECT.


:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame.
- **`database`** — AWS Glue/Athena database name - It is only the origin database from where the query will be launched. You can still using and mixing several databases writing the full table name within the sql (e.g. `database.table`).
- **`table`** — AWS Glue/Athena table name.
- **`temp_path`** — Amazon S3 location to store temporary results. Workgroup config will be used if not provided.
- **`index`** — Should consider the DataFrame index as a column?.
- **`table_location`** — Amazon S3 location for the table. Will only be used to create a new table if it does not exist.
- **`partition_cols`** — List of column names that will be used to create partitions, including support for transform functions (e.g. "day(ts)"). https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-partitioning
- **`merge_cols`** — List of column names that will be used for conditional inserts and updates. https://docs.aws.amazon.com/athena/latest/ug/merge-into-statement.html
- **`merge_condition`** — The condition to be used in the MERGE INTO statement. Valid values: ['update', 'ignore']. Default is `update`.
- **`merge_match_nulls`** — Instruct whether to have nulls in the merge condition match other nulls.
- **`keep_files`** — Whether staging files produced by Athena are retained. Default is `True`.
- **`data_source`** — Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
- **`s3_output`** — Amazon S3 path used for query execution.
- **`workgroup`** — Athena workgroup. Primary by default.
- **`mode`** — `append` (default), `overwrite`, `overwrite_partitions`.
- **`encryption`** — Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
- **`kms_key`** — For SSE-KMS, this is the KMS key ARN or ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`pyarrow_additional_kwargs`** — Additional parameters forwarded to pyarrow. e.g. pyarrow_additional_kwargs={'coerce_timestamps': 'ns', 'use_deprecated_int96_timestamps': False, 'allow_truncated_timestamps'=False}
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`additional_table_properties`** — Additional table properties. e.g. additional_table_properties={'write_target_data_file_size_bytes': '536870912'} https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-table-properties
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. e.g. {'col name': 'bigint', 'col2 name': 'int'}
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`schema_evolution`** — If `True` allows schema evolution for new columns or changes in column types. Columns missing from the DataFrame that are present in the Iceberg schema will throw an error unless `fill_missing_columns_in_df` is set to `True`. Default is `False`.
- **`fill_missing_columns_in_df`** — If `True`, fill columns that was missing in the DataFrame with `NULL` values. Default is `True`.
- **`columns_comments`** — Glue/Athena catalog: Settings for writing to the Glue table. Currently only the 'columns_comments' attribute is supported for this function. Columns comments can only be added with this function when creating a new table.

**Examples**

Insert into an existing Iceberg table

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.athena.to_iceberg(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     database='my_database',
...     table='my_table',
...     temp_path='s3://bucket/temp/',
... )
```

Create Iceberg table and insert data (table doesn't exist, requires table_location)

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.athena.to_iceberg(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     database='my_database',
...     table='my_table2',
...     table_location='s3://bucket/my_table2/',
...     temp_path='s3://bucket/temp/',
... )
```

---

### delete_from_iceberg_table

```python
wr.athena.delete_from_iceberg_table(
    df: 'pd.DataFrame',
    database: 'str',
    table: 'str',
    merge_cols: 'list[str]',
    temp_path: 'str | None' = None,
    keep_files: 'bool' = True,
    data_source: 'str | None' = None,
    s3_output: 'str | None' = None,
    workgroup: 'str' = 'primary',
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    dtype: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    catalog_id: 'str | None' = None
) -> 'None'
```

Delete rows from an Iceberg table.

Creates temporary external table, writes staged files and then deletes any rows which match the contents of the temporary table.


:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame containing the IDs of rows that are to be deleted from the Iceberg table.
- **`database`** — Database name.
- **`table`** — Table name.
- **`merge_cols`** — List of columns to be used to determine which rows of the Iceberg table should be deleted. `MERGE INTO <https://docs.aws.amazon.com/athena/latest/ug/merge-into-statement.html>`_
- **`temp_path`** — S3 path to temporarily store the DataFrame.
- **`keep_files`** — Whether staging files produced by Athena are retained. `True` by default.
- **`data_source`** — The AWS KMS key ID or alias used to encrypt the data.
- **`s3_output`** — Amazon S3 path used for query execution.
- **`workgroup`** — Athena workgroup name.
- **`encryption`** — Valid values: [`None`, `"SSE_S3"`, `"SSE_KMS"`]. Notice: `"CSE_KMS"` is not supported.
- **`kms_key`** — For SSE-KMS, this is the KMS key ARN or ID.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. ``s3_additional_kwargs={"RequestPayer": "requester"}``
- **`catalog_id`** — The ID of the Data Catalog which contains the database and table. If none is provided, the AWS account ID is used by default.

**Examples**

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> df = pd.DataFrame({"id": [1, 2, 3], "col": ["foo", "bar", "baz"]})
>>> wr.athena.to_iceberg(
...     df=df,
...     database="my_database",
...     table="my_table",
...     temp_path="s3://bucket/temp/",
... )
>>> df_delete = pd.DataFrame({"id": [1, 3]})
>>> wr.athena.delete_from_iceberg_table(
...     df=df_delete,
...     database="my_database",
...     table="my_table",
...     merge_cols=["id"],
... )
>>> wr.athena.read_sql_table(table="my_table", database="my_database")
id  col
0   2   bar
```

---

### unload

```python
wr.athena.unload(
    sql: 'str',
    path: 'str',
    database: 'str',
    file_format: 'str' = 'PARQUET',
    compression: 'str | None' = None,
    field_delimiter: 'str | None' = None,
    partitioned_by: 'list[str] | None' = None,
    workgroup: 'str' = 'primary',
    encryption: 'str | None' = None,
    kms_key: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    data_source: 'str | None' = None,
    params: 'dict[str, Any] | list[str] | None' = None,
    paramstyle: "Literal['qmark', 'named']" = 'named',
    athena_query_wait_polling_delay: 'float' = 1.0
) -> '_QueryMetadata'
```

Write query results from a SELECT statement to the specified data format using UNLOAD.

https://docs.aws.amazon.com/athena/latest/ug/unload.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- database

- athena_query_wait_polling_delay

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`sql`** — SQL query.
- **`path`** — Amazon S3 path.
- **`database`** — AWS Glue/Athena database name - It is only the origin database from where the query will be launched. You can still using and mixing several databases writing the full table name within the sql (e.g. `database.table`).
- **`file_format`** — File format of the output. Possible values are ORC, PARQUET, AVRO, JSON, or TEXTFILE
- **`compression`** — This option is specific to the ORC and Parquet formats. For ORC, possible values are lz4, snappy, zlib, or zstd. For Parquet, possible values are gzip or snappy. For ORC, the default is zlib, and for Parquet, the default is gzip.
- **`field_delimiter`** — A single-character field delimiter for files in CSV, TSV, and other text formats.
- **`partitioned_by`** — An array list of columns by which the output is partitioned.
- **`workgroup`** — Athena workgroup. Primary by default.
- **`encryption`** — Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
- **`kms_key`** — For SSE-KMS, this is the KMS key ARN or ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`data_source`** — Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
- **`params`** — Parameters that will be used for constructing the SQL query. Only named or question mark parameters are supported. The parameter style needs to be specified in the `paramstyle` parameter. For `paramstyle="named"`, this value needs to be a dictionary. The dict needs to contain the information in the form `{'name': 'value'}` and the SQL query needs to contain `:name`. The formatter will be applied client-side in this scenario. For `paramstyle="qmark"`, this value needs to be a list of strings. The formatter will be applied server-side. The values are applied sequentially to the parameters in the query in the order in which the parameters occur.
- **`paramstyle`** — Determines the style of `params`. Possible values are: - `named` - `qmark`
- **`athena_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the Athena query has completed.

**Returns**

- Query metadata including query execution id, dtypes, manifest & output location.

**Examples**

```python
>>> import awswrangler as wr
>>> res = wr.athena.unload(
...     sql="SELECT * FROM my_table WHERE name=:name AND city=:city",
...     params={"name": "filtered_name", "city": "filtered_city"}
... )
```

---

### wait_query

```python
wr.athena.wait_query(
    query_execution_id: 'str',
    boto3_session: 'boto3.Session | None' = None,
    athena_query_wait_polling_delay: 'float' = 1.0
) -> 'dict[str, Any]'
```

Wait for the query end.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- athena_query_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`query_execution_id`** — Athena query execution ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`athena_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the Athena query has completed.

**Returns**

- Dictionary with the get_query_execution response.

**Examples**

```python
>>> import awswrangler as wr
>>> res = wr.athena.wait_query(query_execution_id='query-execution-id')
```

---

### create_prepared_statement

```python
wr.athena.create_prepared_statement(
    sql: 'str',
    statement_name: 'str',
    workgroup: 'str' = 'primary',
    mode: "Literal['update', 'error']" = 'update',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Create a SQL statement with the name statement_name to be run at a later time. The statement can include parameters represented by question marks.

https://docs.aws.amazon.com/athena/latest/ug/sql-prepare.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`sql`** — The query string for the prepared statement.
- **`statement_name`** — The name of the prepared statement.
- **`workgroup`** — The name of the workgroup to which the prepared statement belongs. Primary by default.
- **`mode`** — Determines the behaviour if the prepared statement already exists: - `update` - updates statement if already exists - `error` - throws an error if table exists
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.athena.create_prepared_statement(
...     sql="SELECT * FROM my_table WHERE name = ?",
...     statement_name="statement",
... )
```

---

### list_prepared_statements

```python
wr.athena.list_prepared_statements(
    workgroup: 'str' = 'primary',
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List the prepared statements in the specified workgroup.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`workgroup`** — The name of the workgroup to which the prepared statement belongs. Primary by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- List of prepared statements in the workgroup. Each item is a dictionary with the keys `StatementName` and `LastModifiedTime`.

---

### delete_prepared_statement

```python
wr.athena.delete_prepared_statement(
    statement_name: 'str',
    workgroup: 'str' = 'primary',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete the prepared statement with the specified name from the specified workgroup.

https://docs.aws.amazon.com/athena/latest/ug/sql-deallocate-prepare.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- workgroup

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`statement_name`** — The name of the prepared statement.
- **`workgroup`** — The name of the workgroup to which the prepared statement belongs. Primary by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.athena.delete_prepared_statement(
...     statement_name="statement",
... )
```

---
