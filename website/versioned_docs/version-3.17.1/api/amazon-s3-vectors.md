---
id: amazon-s3-vectors
title: "Amazon S3 Vectors"
sidebar_position: 14
---

# Amazon S3 Vectors

Module: `wr.s3`

### create_vector_bucket

```python
wr.s3.create_vector_bucket(
    name: 'str',
    *,
    encryption_kms_key_arn: 'str | None' = None,
    sse_type: 'str | None' = None,
    tags: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create an Amazon S3 Vectors bucket.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`name`** — Name of the vector bucket to create. 3-63 chars.
- **`encryption_kms_key_arn`** — Optional KMS key ARN for SSE-KMS encryption. Implies `sse_type='aws:kms'` if not specified.
- **`sse_type`** — Server-side encryption type. `'AES256'` (default if encryption block omitted) or `'aws:kms'`.
- **`tags`** — Resource tags as a dict.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- ARN of the created vector bucket.

**Examples**

```python
>>> import awswrangler as wr
>>> arn = wr.s3.create_vector_bucket("my-vector-bucket")
```

---

### delete_vector_bucket

```python
wr.s3.delete_vector_bucket(
    name: 'str | None' = None,
    *,
    arn: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete an Amazon S3 Vectors bucket. Specify either `name` or `arn`.

---

### list_vector_buckets

```python
wr.s3.list_vector_buckets(
    prefix: 'str | None' = None,
    *,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List all Amazon S3 Vectors buckets in the account/region (paginates internally).


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`prefix`** — Optional name prefix filter.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- List of vector bucket summaries (each a dict with `vectorBucketName`, `vectorBucketArn`, `creationTime`).

---

### get_vector_bucket

```python
wr.s3.get_vector_bucket(
    name: 'str | None' = None,
    *,
    arn: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Get attributes of a vector bucket. Specify either `name` or `arn`.

---

### create_vector_index

```python
wr.s3.create_vector_index(
    *,
    name: 'str',
    dimension: 'int',
    distance_metric: 'str' = 'cosine',
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    data_type: 'str' = 'float32',
    non_filterable_metadata_keys: 'list[str] | None' = None,
    encryption_kms_key_arn: 'str | None' = None,
    sse_type: 'str | None' = None,
    tags: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create a vector index inside an Amazon S3 Vectors bucket.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`name`** — Index name (3-63 chars).
- **`dimension`** — Vector dimension (1-4096). All vectors written to the index must match.
- **`distance_metric`** — `'cosine'` (default) or `'euclidean'`.
- **`vector_bucket / vector_bucket_arn`** — Target vector bucket. Specify exactly one.
- **`data_type`** — Vector element type. Currently only `'float32'` is supported.
- **`non_filterable_metadata_keys`** — Metadata keys excluded from filtering (up to 10). Cannot be changed after index creation.
- **`encryption_kms_key_arn, sse_type`** — Encryption overrides; default is to inherit from the bucket.
- **`tags`** — Resource tags.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- ARN of the created vector index.

**Examples**

```python
>>> import awswrangler as wr
>>> arn = wr.s3.create_vector_index(
...     vector_bucket="my-bucket", name="my-index", dimension=384
... )
```

---

### delete_vector_index

```python
wr.s3.delete_vector_index(
    *,
    name: 'str | None' = None,
    arn: 'str | None' = None,
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a vector index. Specify either `arn`, or `name` together with `vector_bucket`/`vector_bucket_arn`.

---

### list_vector_indexes

```python
wr.s3.list_vector_indexes(
    *,
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    prefix: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List all vector indexes in a bucket (paginates internally).

---

### get_vector_index

```python
wr.s3.get_vector_index(
    *,
    name: 'str | None' = None,
    arn: 'str | None' = None,
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Get attributes of a vector index.

---

### put_vectors

```python
wr.s3.put_vectors(
    *,
    vectors: 'list[dict[str, Any]]',
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    index: 'str | None' = None,
    index_arn: 'str | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Insert one or more vectors into an Amazon S3 Vectors index.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`vectors`** — List of dicts, each shaped `{"key": str, "data": list[float] | dict[str, list[float]] | np.ndarray, "metadata": dict | None}`. `data` is automatically cast to float32. Up to 500 vectors per underlying API call.
- **`vector_bucket / vector_bucket_arn / index / index_arn`** — Target index. See module docstring for resolution rules.
- **`use_threads`** — Concurrency for batched calls.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

---

### put_vectors_from_df

```python
wr.s3.put_vectors_from_df(
    df: 'pd.DataFrame',
    *,
    key_column: 'str',
    vector_column: 'str | None' = None,
    metadata_columns: 'list[str] | None' = None,
    text_column: 'str | None' = None,
    bedrock_model_id: 'str | None' = None,
    bedrock_model_kwargs: 'dict[str, Any] | None' = None,
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    index: 'str | None' = None,
    index_arn: 'str | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Insert all rows of a DataFrame into an Amazon S3 Vectors index.

Either `vector_column` (precomputed embeddings) or `text_column` + `bedrock_model_id`
(embed via Amazon Bedrock on the fly) must be provided.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`df`** — Input DataFrame.
- **`key_column`** — Column containing the per-row vector key (string).
- **`vector_column`** — Column containing the precomputed embedding (list[float] / np.ndarray per row).
- **`metadata_columns`** — Columns to attach as filterable/non-filterable metadata. `None` means "all columns except `key_column`, `vector_column` and `text_column`" — note `text_column` is excluded by default; pass it explicitly here (e.g. for RAG citations) to keep it. NaN / `pd.NA` / `None` cells are dropped per row.
- **`text_column`** — Column containing input text to embed via Bedrock. Mutually exclusive with `vector_column`.
- **`bedrock_model_id, bedrock_model_kwargs`** — Bedrock embedding model and optional model-specific kwargs (e.g. `{"dimensions": 256}`).
- **`vector_bucket / vector_bucket_arn / index / index_arn`** — Target index.
- **`use_threads`** — Concurrency for batched put calls and for parallel Bedrock embedding.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

Pre-computed vectors:

```python
>>> import awswrangler as wr
>>> wr.s3.put_vectors_from_df(
...     df=my_df,
...     key_column="id",
...     vector_column="embedding",
...     vector_bucket="my-bucket",
...     index="my-index",
... )
```

Embed-on-write via Bedrock Titan:

```python
>>> wr.s3.put_vectors_from_df(
...     df=my_df,
...     key_column="id",
...     text_column="content",
...     bedrock_model_id="amazon.titan-embed-text-v2:0",
...     vector_bucket="my-bucket",
...     index="my-index",
... )
```

---

### get_vectors

```python
wr.s3.get_vectors(
    *,
    keys: 'list[str]',
    return_data: 'bool' = False,
    return_metadata: 'bool' = False,
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    index: 'str | None' = None,
    index_arn: 'str | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Retrieve vectors by key. Returns a DataFrame with columns `key` and (optionally) `vector`, `metadata`.

Up to 100 keys per underlying API call (chunked automatically).

---

### delete_vectors

```python
wr.s3.delete_vectors(
    *,
    keys: 'list[str]',
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    index: 'str | None' = None,
    index_arn: 'str | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete vectors by key (chunks at 500 per underlying call).

---

### list_vectors

```python
wr.s3.list_vectors(
    *,
    return_data: 'bool' = False,
    return_metadata: 'bool' = False,
    max_items: 'int | None' = None,
    chunked: 'bool | int' = False,
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    index: 'str | None' = None,
    index_arn: 'str | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

List all vectors in an index. Uses parallel segments (up to 16) when `use_threads` enables it.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`return_data, return_metadata`** — Whether to include each vector's data and metadata.
- **`max_items`** — Optional cap on total vectors returned across all pages/segments.
- **`chunked`** — Batching (memory-friendly). Returns an iterator of DataFrames instead of one frame: - `True` — yield one DataFrame per underlying API page. - `INTEGER` — yield DataFrames of exactly this many rows (final frame may be shorter). Chunked streaming is single-segment (sequential) regardless of `use_threads`.
- **`vector_bucket / vector_bucket_arn / index / index_arn`** — Target index.
- **`use_threads`** — Concurrency for parallel-segment listing. Ignored when `chunked` is truthy.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- DataFrame with columns `key` and (optionally) `vector`, `metadata` — or an iterator of such DataFrames when `chunked` is truthy.

---

### query_vectors

```python
wr.s3.query_vectors(
    *,
    query_vector: 'list[float] | np.ndarray[Any, Any] | None' = None,
    query_text: 'str | None' = None,
    top_k: 'int' = 10,
    filter: 'dict[str, Any] | None' = None,
    return_distance: 'bool' = True,
    return_metadata: 'bool' = True,
    bedrock_model_id: 'str | None' = None,
    bedrock_model_kwargs: 'dict[str, Any] | None' = None,
    vector_bucket: 'str | None' = None,
    vector_bucket_arn: 'str | None' = None,
    index: 'str | None' = None,
    index_arn: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Approximate-nearest-neighbour query against an Amazon S3 Vectors index.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`query_vector`** — Pre-computed query embedding.
- **`query_text`** — Text to embed via Bedrock (requires `bedrock_model_id`).
- **`top_k`** — Number of nearest neighbours to return (1-100).
- **`filter`** — Metadata filter (MongoDB-style operators: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $and, $or).
- **`return_distance, return_metadata`** — Whether to include each result's distance and metadata.
- **`bedrock_model_id, bedrock_model_kwargs`** — Bedrock embedding configuration when using `query_text`.
- **`vector_bucket / vector_bucket_arn / index / index_arn`** — Target index.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- DataFrame with columns `key` and (optionally) `distance`, `metadata`. The configured distance metric is exposed via `df.attrs['distance_metric']`.

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.s3.query_vectors(
...     query_vector=[0.1, 0.2, 0.3],
...     top_k=5,
...     filter={"genre": {"$eq": "documentary"}},
...     vector_bucket="my-bucket",
...     index="my-index",
... )
```

---
