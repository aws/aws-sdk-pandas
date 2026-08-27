---
id: opensearch
title: "OpenSearch"
sidebar_position: 10
---

# OpenSearch

Module: `wr.opensearch`

### connect

```python
wr.opensearch.connect(
    host: 'str',
    port: 'int | None' = 443,
    boto3_session: 'boto3.Session | None' = None,
    region: 'str | None' = None,
    username: 'str | None' = None,
    password: 'str | None' = None,
    service: 'str | None' = None,
    timeout: 'int' = 30,
    max_retries: 'int' = 5,
    retry_on_timeout: 'bool' = True,
    retry_on_status: 'Sequence[int] | None' = None
) -> "'opensearchpy.OpenSearch'"
```

Create a secure connection to the specified Amazon OpenSearch domain.

:::note
We use `opensearch-py <https://github.com/opensearch-project/opensearch-py>`_, an OpenSearch python client.

The username and password are mandatory if the OS Cluster uses `Fine Grained Access Control <https://docs.aws.amazon.com/opensearch-service/latest/developerguide/fgac.html>`_.
If fine grained access control is disabled, session access key and secret keys are used.
:::

**Parameters**

- **`host`** — Amazon OpenSearch domain, for example: my-test-domain.us-east-1.es.amazonaws.com.
- **`port`** — OpenSearch Service only accepts connections over port 80 (HTTP) or 443 (HTTPS)
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`region`** — AWS region of the Amazon OS domain. If not provided will be extracted from boto3_session.
- **`username`** — Fine-grained access control username. Mandatory if OS Cluster uses Fine Grained Access Control.
- **`password`** — Fine-grained access control password. Mandatory if OS Cluster uses Fine Grained Access Control.
- **`service`** — Service id. Supported values are `es`, corresponding to opensearch cluster, and `aoss` for serverless opensearch. By default, service will be parsed from the host URI.
- **`timeout`** — Operation timeout. `30` by default.
- **`max_retries`** — Maximum number of retries before an exception is propagated. `10` by default.
- **`retry_on_timeout`** — Should timeout trigger a retry on different node. `True` by default.
- **`retry_on_status`** — Set of HTTP status codes on which we should retry on a different node. Defaults to [500, 502, 503, 504].

**Returns**

- `OpenSearch low-level client <https://github.com/opensearch-project/opensearch-py/blob/main/opensearchpy/client/__init__.py>`_.

---

### create_collection

```python
wr.opensearch.create_collection(
    name: 'str',
    collection_type: 'str' = 'SEARCH',
    description: 'str' = '',
    encryption_policy: 'dict[str, Any] | list[dict[str, Any]] | None' = None,
    kms_key_arn: 'str | None' = None,
    network_policy: 'dict[str, Any] | list[dict[str, Any]] | None' = None,
    vpc_endpoints: 'list[str] | None' = None,
    data_policy: 'list[dict[str, Any]] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Create Amazon OpenSearch Serverless collection.

Creates Amazon OpenSearch Serverless collection, corresponding encryption and network
policies, and data policy, if `data_policy` provided.

More in `Amazon OpenSearch Serverless (preview) <https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless.html>`_


:::warning
This API is experimental and may change in future AWS SDK for Pandas releases.
:::

**Parameters**

- **`name`** — Collection name.
- **`collection_type`** — Collection type. Allowed values are `SEARCH`, and `TIMESERIES`.
- **`description`** — Collection description.
- **`encryption_policy`** — Encryption policy of a form: { "Rules": [...] } If not provided, default policy using AWS-managed KMS key will be created. To use user-defined key, provide `kms_key_arn`.
- **`kms_key_arn`** — Encryption key.
- **`network_policy`** — Network policy of a form: [{ "Rules": [...] }] If not provided, default network policy allowing public access to the collection will be created. To create the collection in the VPC, provide `vpc_endpoints`.
- **`vpc_endpoints`** — List of VPC endpoints for access to non-public collection.
- **`data_policy`** — Data policy of a form: [{ "Rules": [...] }]
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Collection details

---

### create_index

```python
wr.opensearch.create_index(
    client: "'opensearchpy.OpenSearch'",
    index: 'str',
    doc_type: 'str | None' = None,
    settings: 'dict[str, Any] | None' = None,
    mappings: 'dict[str, Any] | None' = None
) -> 'dict[str, Any]'
```

Create an index.

**Parameters**

- **`client`** — instance of opensearchpy.OpenSearch to use.
- **`index`** — Name of the index.
- **`doc_type`** — Name of the document type (for Elasticsearch versions 5.x and earlier).
- **`settings`** — Index settings https://opensearch.org/docs/opensearch/rest-api/create-index/#index-settings
- **`mappings`** — Index mappings https://opensearch.org/docs/opensearch/rest-api/create-index/#mappings

**Returns**

- OpenSearch rest api response https://opensearch.org/docs/opensearch/rest-api/create-index/#response.

**Examples**

Creating an index.

```python
>>> import awswrangler as wr
>>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
>>> response = wr.opensearch.create_index(
...     client=client,
...     index="sample-index1",
...     mappings={
...        "properties": {
...          "age":  { "type" : "integer" }
...        }
...     },
...     settings={
...         "index": {
...             "number_of_shards": 2,
...             "number_of_replicas": 1
...          }
...     }
... )
```

---

### delete_index

```python
wr.opensearch.delete_index(client: "'opensearchpy.OpenSearch'", index: 'str') -> 'dict[str, Any]'
```

Delete an index.

**Parameters**

- **`client`** — instance of opensearchpy.OpenSearch to use.
- **`index`** — Name of the index.

**Returns**

- OpenSearch rest api response

**Examples**

Deleting an index.

```python
>>> import awswrangler as wr
>>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
>>> response = wr.opensearch.delete_index(
...     client=client,
...     index="sample-index1"
... )
```

---

### index_csv

```python
wr.opensearch.index_csv(
    client: "'opensearchpy.OpenSearch'",
    path: 'str',
    index: 'str',
    doc_type: 'str | None' = None,
    pandas_kwargs: 'dict[str, Any] | None' = None,
    use_threads: 'bool | int' = False,
    **kwargs: 'Any'
) -> 'Any'
```

Index all documents from a CSV file to OpenSearch index.

**Parameters**

- **`client`** — instance of opensearchpy.OpenSearch to use.
- **`path`** — S3 or local path to the CSV file which contains the documents.
- **`index`** — Name of the index.
- **`doc_type`** — Name of the document type (for Elasticsearch versions 5.x and earlier).
- **`pandas_kwargs`** — Dictionary of arguments forwarded to pandas.read_csv(). e.g. pandas_kwargs={'sep': '|', 'na_values': ['null', 'none']} https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html Note: these params values are enforced: `skip_blank_lines=True`
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`**kwargs`** — KEYWORD arguments forwarded to :func:`~awswrangler.opensearch.index_documents` which is used to execute the operation

**Returns**

- Response payload https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.

**Examples**

Writing contents of CSV file

```python
>>> import awswrangler as wr
>>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
>>> wr.opensearch.index_csv(
...     client=client,
...     path='docs.csv',
...     index='sample-index1'
... )
```

Writing contents of CSV file using pandas_kwargs

```python
>>> import awswrangler as wr
>>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
>>> wr.opensearch.index_csv(
...     client=client,
...     path='docs.csv',
...     index='sample-index1',
...     pandas_kwargs={'sep': '|', 'na_values': ['null', 'none']}
... )
```

---

### index_documents

```python
wr.opensearch.index_documents(
    client: "'opensearchpy.OpenSearch'",
    documents: 'Iterable[Mapping[str, Any]]',
    index: 'str',
    doc_type: 'str | None' = None,
    keys_to_write: 'list[str] | None' = None,
    id_keys: 'list[str] | None' = None,
    ignore_status: 'list[Any] | tuple[Any] | None' = None,
    bulk_size: 'int' = 1000,
    chunk_size: 'int | None' = 500,
    max_chunk_bytes: 'int | None' = 104857600,
    max_retries: 'int | None' = None,
    initial_backoff: 'int | None' = None,
    max_backoff: 'int | None' = None,
    use_threads: 'bool | int' = False,
    enable_refresh_interval: 'bool' = True,
    **kwargs: 'Any'
) -> 'dict[str, Any]'
```

Index all documents to OpenSearch index.

:::note
`max_retries`, `initial_backoff`, and `max_backoff` are not supported with parallel bulk
(when `use_threads` is set to True).
:::
:::note
Some of the args are referenced from opensearch-py client library (bulk helpers)
https://opensearch-py.readthedocs.io/en/latest/helpers.html#opensearchpy.helpers.bulk
https://opensearch-py.readthedocs.io/en/latest/helpers.html#opensearchpy.helpers.streaming_bulk

If you receive `Error 429 (Too Many Requests) /_bulk` please to to decrease `bulk_size` value.
Please also consider modifying the cluster size and instance type -
Read more here: https://aws.amazon.com/premiumsupport/knowledge-center/resolve-429-error-es/
:::

**Parameters**

- **`client`** — instance of opensearchpy.OpenSearch to use.
- **`documents`** — List which contains the documents that will be inserted.
- **`index`** — Name of the index.
- **`doc_type`** — Name of the document type (for Elasticsearch versions 5.x and earlier).
- **`keys_to_write`** — list of keys to index. If not provided all keys will be indexed
- **`id_keys`** — list of keys that compound document unique id. If not provided will use `_id` key if exists, otherwise will generate unique identifier for each document.
- **`ignore_status`** — list of HTTP status codes that you want to ignore (not raising an exception)
- **`bulk_size`** — number of docs in each _bulk request (default: 1000)
- **`chunk_size`** — number of docs in one chunk sent to es (default: 500)
- **`max_chunk_bytes`** — the maximum size of the request in bytes (default: 100MB)
- **`max_retries`** — maximum number of times a document will be retried when `429` is received, set to 0 (default) for no retries on `429` (default: 2)
- **`initial_backoff`** — number of seconds we should wait before the first retry. Any subsequent retries will be powers of `initial_backoff*2**retry_number` (default: 2)
- **`max_backoff`** — maximum number of seconds a retry will wait (default: 600)
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`enable_refresh_interval`** — True (default) to enable `refresh_interval` modification to `-1` (disabled) while indexing documents
- **`**kwargs`** — KEYWORD arguments forwarded to bulk operation elasticsearch >= 7.10.2 / opensearch: https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#url-parameters elasticsearch < 7.10.2: https://opendistro.github.io/for-elasticsearch-docs/docs/elasticsearch/rest-api-reference/#url-parameters

**Returns**

- Response payload https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.

**Examples**

Writing documents

```python
>>> import awswrangler as wr
>>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
>>> wr.opensearch.index_documents(
...     documents=[{'_id': '1', 'value': 'foo'}, {'_id': '2', 'value': 'bar'}],
...     index='sample-index1'
... )
```

---

### index_df

```python
wr.opensearch.index_df(
    client: "'opensearchpy.OpenSearch'",
    df: 'pd.DataFrame',
    index: 'str',
    doc_type: 'str | None' = None,
    use_threads: 'bool | int' = False,
    **kwargs: 'Any'
) -> 'Any'
```

Index all documents from a DataFrame to OpenSearch index.

**Parameters**

- **`client`** — instance of opensearchpy.OpenSearch to use.
- **`df`** — `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
- **`index`** — Name of the index.
- **`doc_type`** — Name of the document type (for Elasticsearch versions 5.x and earlier).
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`**kwargs`** — KEYWORD arguments forwarded to :func:`~awswrangler.opensearch.index_documents` which is used to execute the operation

**Returns**

- Response payload https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.

**Examples**

Writing rows of DataFrame

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
>>> wr.opensearch.index_df(
...     client=client,
...     df=pd.DataFrame([{'_id': '1'}, {'_id': '2'}, {'_id': '3'}]),
...     index='sample-index1',
... )
```

---

### index_json

```python
wr.opensearch.index_json(
    client: "'opensearchpy.OpenSearch'",
    path: 'str',
    index: 'str',
    doc_type: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    json_path: 'str | None' = None,
    use_threads: 'bool | int' = False,
    **kwargs: 'Any'
) -> 'Any'
```

Index all documents from JSON file to OpenSearch index.

The JSON file should be in a JSON-Lines text format (newline-delimited JSON) - https://jsonlines.org/
OR if the is a single large JSON please provide `json_path`.

**Parameters**

- **`client`** — instance of opensearchpy.OpenSearch to use.
- **`path`** — s3 or local path to the JSON file which contains the documents.
- **`index`** — Name of the index.
- **`doc_type`** — Name of the document type (for Elasticsearch versions 5.x and earlier).
- **`json_path`** — JsonPath expression to specify explicit path to a single name element in a JSON hierarchical data structure. Read more about `JsonPath <https://jsonpath.com>`_
- **`boto3_session`** — Boto3 Session to be used to access S3 if **path** is provided. The default boto3 session will be used if **boto3_session** is `None`.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`**kwargs`** — KEYWORD arguments forwarded to :func:`~awswrangler.opensearch.index_documents` which is used to execute the operation

**Returns**

- Response payload https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.

**Examples**

Writing contents of JSON file

```python
>>> import awswrangler as wr
>>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
>>> wr.opensearch.index_json(
...     client=client,
...     path='docs.json',
...     index='sample-index1'
... )
```

---

### search

```python
wr.opensearch.search(
    client: "'opensearchpy.OpenSearch'",
    index: 'str | None' = '_all',
    search_body: 'dict[str, Any] | None' = None,
    doc_type: 'str | None' = None,
    is_scroll: 'bool | None' = False,
    filter_path: 'str | Collection[str] | None' = None,
    **kwargs: 'Any'
) -> 'pd.DataFrame'
```

Return results matching query DSL as pandas DataFrame.

**Parameters**

- **`client`** — instance of opensearchpy.OpenSearch to use.
- **`index`** — A comma-separated list of index names to search. use `_all` or empty string to perform the operation on all indices.
- **`search_body`** — The search definition using the `Query DSL <https://opensearch.org/docs/opensearch/query-dsl/full-text/>`_.
- **`doc_type`** — Name of the document type (for Elasticsearch versions 5.x and earlier).
- **`is_scroll`** — Allows to retrieve a large numbers of results from a single search request using `scroll <https://opensearch.org/docs/opensearch/rest-api/scroll/>`_ for example, for machine learning jobs. Because scroll search contexts consume a lot of memory, we suggest you don’t use the scroll operation for frequent user queries.
- **`filter_path`** — Use the filter_path parameter to reduce the size of the OpenSearch Service response (default: ['hits.hits._id','hits.hits._source'])
- **`**kwargs`** — KEYWORD arguments forwarded to `opensearchpy.OpenSearch.search <https://opensearch-py.readthedocs.io/en/latest/api.html#opensearchpy.OpenSearch.search>`_ and also to `opensearchpy.helpers.scan <https://opensearch-py.readthedocs.io/en/master/helpers.html#scan>`_ if `is_scroll=True`

**Returns**

- Results as Pandas DataFrame

**Examples**

Searching an index using query DSL

```python
>>> import awswrangler as wr
>>> client = wr.opensearch.connect(host="DOMAIN-ENDPOINT")
>>> df = wr.opensearch.search(
...     client=client,
...     index="movies",
...     search_body={
...         "query": {
...             "match": {
...                 "title": "wind",
...             },
...         },
...     },
... )
```

---

### search_by_sql

```python
wr.opensearch.search_by_sql(
    client: "'opensearchpy.OpenSearch'",
    sql_query: 'str',
    **kwargs: 'Any'
) -> 'pd.DataFrame'
```

Return results matching `SQL query <https://opensearch.org/docs/search-plugins/sql/index/>`_ as pandas DataFrame.

**Parameters**

- **`client`** — instance of opensearchpy.OpenSearch to use.
- **`sql_query`** — SQL query
- **`**kwargs`** — KEYWORD arguments forwarded to request url (e.g.: filter_path, etc.)

**Returns**

- Results as Pandas DataFrame

**Examples**

Searching an index using SQL query

```python
>>> import awswrangler as wr
>>> client = wr.opensearch.connect(host="DOMAIN-ENDPOINT")
>>> df = wr.opensearch.search_by_sql(
...     client=client,
...     sql_query="SELECT * FROM my-index LIMIT 50",
... )
```

---
