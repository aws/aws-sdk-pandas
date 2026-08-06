---
id: amazon-neptune
title: "Amazon Neptune"
sidebar_position: 11
---

# Amazon Neptune

Module: `wr.neptune`

### connect

```python
wr.neptune.connect(
    host: 'str',
    port: 'int',
    iam_enabled: 'bool' = False,
    **kwargs: 'Any'
) -> 'NeptuneClient'
```

Create a connection to a Neptune cluster.

**Parameters**

- **`host`** — The host endpoint to connect to
- **`port`** — The port endpoint to connect to
- **`iam_enabled`** — True if IAM is enabled on the cluster. Defaults to False.

**Returns**

- [description]

---

### execute_gremlin

```python
wr.neptune.execute_gremlin(client: 'NeptuneClient', query: 'str') -> 'pd.DataFrame'
```

Return results of a Gremlin traversal as pandas DataFrame.

**Parameters**

- **`client`** — instance of the neptune client to use
- **`query`** — The gremlin traversal to execute

**Returns**

- Results as Pandas DataFrame

**Examples**

Run a Gremlin Query

```python
>>> import awswrangler as wr
>>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
>>> df = wr.neptune.execute_gremlin(client, "g.V().limit(1)")
```

---

### execute_opencypher

```python
wr.neptune.execute_opencypher(client: 'NeptuneClient', query: 'str') -> 'pd.DataFrame'
```

Return results of a openCypher traversal as pandas DataFrame.

**Parameters**

- **`client`** — instance of the neptune client to use
- **`query`** — The openCypher query to execute

**Returns**

- Results as Pandas DataFrame

**Examples**

Run an openCypher query

```python
>>> import awswrangler as wr
>>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
>>> resp = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN n LIMIT 1")
```

---

### execute_sparql

```python
wr.neptune.execute_sparql(client: 'NeptuneClient', query: 'str') -> 'pd.DataFrame'
```

Return results of a SPARQL query as pandas DataFrame.

**Parameters**

- **`client`** — instance of the neptune client to use
- **`query`** — The SPARQL traversal to execute

**Returns**

- Results as Pandas DataFrame

**Examples**

Run a SPARQL query

```python
>>> import awswrangler as wr
>>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
>>> df = wr.neptune.execute_sparql(client, "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
SELECT ?name
WHERE {
?person foaf:name ?name .
```

---

### flatten_nested_df

```python
wr.neptune.flatten_nested_df(
    df: 'pd.DataFrame',
    include_prefix: 'bool' = True,
    separator: 'str' = '_',
    recursive: 'bool' = True
) -> 'pd.DataFrame'
```

Flatten the lists and dictionaries of the input data frame.

**Parameters**

- **`df`** — The input data frame
- **`include_prefix`** — If True, then it will prefix the new column name with the original column name. Defaults to True.
- **`separator`** — The separator to use between field names when a dictionary is exploded. Defaults to "_".
- **`recursive`** — If True, then this will recurse the fields in the data frame. Defaults to True.

**Returns**

- The flattened DataFrame

---

### to_property_graph

```python
wr.neptune.to_property_graph(
    client: 'NeptuneClient',
    df: 'pd.DataFrame',
    batch_size: 'int' = 50,
    use_header_cardinality: 'bool' = True
) -> 'bool'
```

Write records stored in a DataFrame into Amazon Neptune.

If writing to a property graph then DataFrames for vertices and edges must be written separately.

DataFrames for vertices must have a ~label column with the label and a ~id column for the vertex id.
If the ~id column does not exist, the specified id does not exist, or is empty then a new vertex will be added.

DataFrames for edges must have a ~id, ~label, ~to, and ~from column.  If the ~id column does not exist
the specified id does not exist, or is empty then a new edge will be added.

Existing ~id values will be overwritten. If no ~id, ~label, ~to, or ~from column exists,
an InvalidArgumentValue exception will be thrown.

If you would like to save data using `single` cardinality then you can postfix (single) to the column header and
set `use_header_cardinality=True` (default).
e.g. A column named `name(single)` will save the `name` property as single cardinality.
You can disable this by setting `use_header_cardinality=False`.

**Parameters**

- **`client`** — instance of the neptune client to use
- **`df`** — `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
- **`batch_size`** — The number of rows to save at a time. Default 50
- **`use_header_cardinality`** — If True, then the header cardinality will be used to save the data. Default True

**Returns**

- True if records were written

**Examples**

Writing to Amazon Neptune

```python
>>> import awswrangler as wr
>>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
>>> wr.neptune.gremlin.to_property_graph(
...     df=df
... )
```

---

### to_rdf_graph

```python
wr.neptune.to_rdf_graph(
    client: 'NeptuneClient',
    df: 'pd.DataFrame',
    batch_size: 'int' = 50,
    subject_column: 'str' = 's',
    predicate_column: 'str' = 'p',
    object_column: 'str' = 'o',
    graph_column: 'str' = 'g'
) -> 'bool'
```

Write records stored in a DataFrame into Amazon Neptune.

The DataFrame must consist of triples with column names for the subject, predicate, and object specified.
If you want to add data into a named graph then you will also need the graph column.

**Parameters**

- **`client`** — Instance of the neptune client to use.
- **`df`** — `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_.
- **`batch_size`** — The number of rows in the DataFrame (i.e. triples) to write into Amazon Neptune in one query. Defaults to 50.
- **`subject_column`** — The column name in the DataFrame for the subject. Defaults to 's'.
- **`predicate_column`** — The column name in the DataFrame for the predicate. Defaults to 'p'.
- **`object_column`** — The column name in the DataFrame for the object. Defaults to 'o'.
- **`graph_column`** — The column name in the DataFrame for the graph if sending across quads. Defaults to 'g'.

**Returns**

- True if records were written

**Examples**

Writing to Amazon Neptune

```python
>>> import awswrangler as wr
>>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
>>> wr.neptune.gremlin.to_rdf_graph(
...     df=df
... )
```

---

### bulk_load

```python
wr.neptune.bulk_load(
    client: 'NeptuneClient',
    df: 'pd.DataFrame',
    path: 'str',
    iam_role: 'str',
    neptune_load_wait_polling_delay: 'float' = 0.25,
    load_parallelism: "Literal['LOW', 'MEDIUM', 'HIGH', 'OVERSUBSCRIBE']" = 'HIGH',
    parser_configuration: 'BulkLoadParserConfiguration | None' = None,
    update_single_cardinality_properties: "Literal['TRUE', 'FALSE']" = 'FALSE',
    queue_request: "Literal['TRUE', 'FALSE']" = 'FALSE',
    dependencies: 'list[str] | None' = None,
    keep_files: 'bool' = False,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, str] | None' = None
) -> 'None'
```

Write records into Amazon Neptune using the Neptune Bulk Loader.

The DataFrame will be written to S3 and then loaded to Neptune using the
`Bulk Loader <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html>`_.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- neptune_load_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`client`** — Instance of the neptune client to use
- **`df`** — `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_ to write to Neptune.
- **`path`** — S3 Path that the Neptune Bulk Loader will load data from.
- **`iam_role`** — The Amazon Resource Name (ARN) for an IAM role to be assumed by the Neptune DB instance for access to the S3 bucket. For information about creating a role that has access to Amazon S3 and then associating it with a Neptune cluster, see `Prerequisites: IAM Role and Amazon S3 Access <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html>`_.
- **`neptune_load_wait_polling_delay`** — Interval in seconds for how often the function will check if the Neptune bulk load has completed.
- **`load_parallelism`** — Specifies the number of threads used by Neptune's bulk load process.
- **`parser_configuration`** — An optional object with additional parser configuration values. Each of the child parameters is also optional: `namedGraphUri`, `baseUri` and `allowEmptyStrings`.
- **`update_single_cardinality_properties`** — An optional parameter that controls how the bulk loader treats a new value for single-cardinality vertex or edge properties.
- **`queue_request`** — An optional flag parameter that indicates whether the load request can be queued up or not. If omitted or set to `"FALSE"`, the load request will fail if another load job is already running.
- **`dependencies`** — An optional parameter that can make a queued load request contingent on the successful completion of one or more previous jobs in the queue.
- **`keep_files`** — Whether to keep stage files or delete them. False by default.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. `s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}`

**Examples**

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> client = wr.neptune.connect("MY_NEPTUNE_ENDPOINT", 8182)
>>> frame = pd.DataFrame([{"~id": "0", "~labels": ["version"], "~properties": {"type": "version"}}])
>>> wr.neptune.bulk_load(
...     client=client,
...     df=frame,
...     path="s3://my-bucket/stage-files/",
...     iam_role="arn:aws:iam::XXX:role/XXX"
... )
```

---

### bulk_load_from_files

```python
wr.neptune.bulk_load_from_files(
    client: 'NeptuneClient',
    path: 'str',
    iam_role: 'str',
    format: "Literal['csv', 'opencypher', 'ntriples', 'nquads', 'rdfxml', 'turtle']" = 'csv',
    neptune_load_wait_polling_delay: 'float' = 0.25,
    load_parallelism: "Literal['LOW', 'MEDIUM', 'HIGH', 'OVERSUBSCRIBE']" = 'HIGH',
    parser_configuration: 'BulkLoadParserConfiguration | None' = None,
    update_single_cardinality_properties: "Literal['TRUE', 'FALSE']" = 'FALSE',
    queue_request: "Literal['TRUE', 'FALSE']" = 'FALSE',
    dependencies: 'list[str] | None' = None
) -> 'None'
```

Load files from S3 into Amazon Neptune using the Neptune Bulk Loader.

For more information about the Bulk Loader see
`here <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html>`_.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- neptune_load_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`client`** — Instance of the neptune client to use
- **`path`** — S3 Path that the Neptune Bulk Loader will load data from.
- **`iam_role`** — The Amazon Resource Name (ARN) for an IAM role to be assumed by the Neptune DB instance for access to the S3 bucket. For information about creating a role that has access to Amazon S3 and then associating it with a Neptune cluster, see `Prerequisites: IAM Role and Amazon S3 Access <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html>`_.
- **`format`** — The format of the data.
- **`neptune_load_wait_polling_delay`** — Interval in seconds for how often the function will check if the Neptune bulk load has completed.
- **`load_parallelism`** — Specifies the number of threads used by Neptune's bulk load process.
- **`parser_configuration`** — An optional object with additional parser configuration values. Each of the child parameters is also optional: `namedGraphUri`, `baseUri` and `allowEmptyStrings`.
- **`update_single_cardinality_properties`** — An optional parameter that controls how the bulk loader treats a new value for single-cardinality vertex or edge properties.
- **`queue_request`** — An optional flag parameter that indicates whether the load request can be queued up or not. If omitted or set to `"FALSE"`, the load request will fail if another load job is already running.
- **`dependencies`** — An optional parameter that can make a queued load request contingent on the successful completion of one or more previous jobs in the queue.

**Examples**

```python
>>> import awswrangler as wr
>>> client = wr.neptune.connect("MY_NEPTUNE_ENDPOINT", 8182)
>>> wr.neptune.bulk_load_from_files(
...     client=client,
...     path="s3://my-bucket/stage-files/",
...     iam_role="arn:aws:iam::XXX:role/XXX",
...     format="csv",
... )
```

---
