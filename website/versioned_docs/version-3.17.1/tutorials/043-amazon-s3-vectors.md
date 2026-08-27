---
id: 043-amazon-s3-vectors
title: "Amazon S3 Vectors"
sidebar_position: 43
sidebar_label: "43 - Amazon S3 Vectors"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/043%20-%20Amazon%20S3%20Vectors.ipynb
---

# Amazon S3 Vectors

> This page is generated from [`tutorials/043 - Amazon S3 Vectors.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/043%20-%20Amazon%20S3%20Vectors.ipynb). Open it in Jupyter to run it yourself.
[Amazon S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html) provides cost-optimized native vector storage on S3 for similarity search and RAG. The hierarchy is **vector bucket** → **vector index** → **vectors**, where each vector is a tuple of `(key, float32[], metadata)`.

AWS SDK for pandas wraps the `s3vectors` boto3 service and exposes 14 functions on `wr.s3` covering the full bucket / index / data lifecycle, with DataFrame-friendly I/O and optional on-the-fly embedding via Amazon Bedrock.

```python
import getpass

import pandas as pd

import awswrangler as wr
```

```python
bucket_name = getpass.getpass("Enter a vector bucket name:")
index_name = "tutorial"
# Match the embedding model's output dimension. Titan v2 supports 256, 512, or 1024.
dimension = 256
```

## Creating resources

A vector bucket holds many indexes; an index has a fixed dimension and distance metric and holds the actual vectors. All vectors written to an index must match its dimension.

```python
bucket_arn = wr.s3.create_vector_bucket(name=bucket_name)
print(f"Vector bucket ARN: {bucket_arn}")
```

```text
Vector bucket ARN: arn:aws:s3vectors:eu-west-1:123456789012:bucket/test-aws-sdk-pandas-s3-vectors-1
```

```python
index_arn = wr.s3.create_vector_index(
    vector_bucket=bucket_name,
    name=index_name,
    dimension=dimension,
    distance_metric="cosine",
)
print(f"Vector index ARN: {index_arn}")
```

```text
Vector index ARN: arn:aws:s3vectors:eu-west-1:123456789012:bucket/test-aws-sdk-pandas-s3-vectors-1/index/tutorial
```

## Discovery

`list_vector_buckets`, `get_vector_bucket`, `list_vector_indexes`, and `get_vector_index` describe what's in an account / bucket. List operations paginate internally.

```python
print("Buckets:", [b["vectorBucketName"] for b in wr.s3.list_vector_buckets()])
print("This bucket:", wr.s3.get_vector_bucket(name=bucket_name))
print("Indexes:", [i["indexName"] for i in wr.s3.list_vector_indexes(vector_bucket=bucket_name)])
wr.s3.get_vector_index(name=index_name, vector_bucket=bucket_name)
```

```text
Buckets: ['test-aws-sdk-pandas-s3-vectors-1']
This bucket: {'vectorBucketName': 'test-aws-sdk-pandas-s3-vectors-1', 'vectorBucketArn': 'arn:aws:s3vectors:eu-west-1:123456789012:bucket/test-aws-sdk-pandas-s3-vectors-1', 'creationTime': datetime.datetime(2026, 5, 10, 0, 47, 57, tzinfo=tzlocal()), 'encryptionConfiguration': {'sseType': 'AES256'}}
Indexes: ['tutorial']
```

```text
{'vectorBucketName': 'test-aws-sdk-pandas-s3-vectors-1',
 'indexName': 'tutorial',
 'indexArn': 'arn:aws:s3vectors:eu-west-1:123456789012:bucket/test-aws-sdk-pandas-s3-vectors-1/index/tutorial',
 'creationTime': datetime.datetime(2026, 5, 10, 0, 48, tzinfo=tzlocal()),
 'dataType': 'float32',
 'dimension': 256,
 'distanceMetric': 'cosine',
 'encryptionConfiguration': {'sseType': 'AES256'}}
```

## Writing vectors

`put_vectors_from_df` is the DataFrame entry point. The most natural workflow is to pass `text_column` together with a Bedrock embedding model — awswrangler then calls Bedrock for each row and writes the resulting vectors plus any other columns as metadata.

> **Prerequisite:** the calling identity needs `bedrock:InvokeModel` permission and **Bedrock model access** for the chosen embedding model in the current region (request access in the Bedrock console). Supported model id prefixes: `amazon.titan-embed-text-*`, `cohere.embed-*`.

If you already have embeddings (computed by your own model or a different SDK), pass them as a column instead via `vector_column`:

```python
wr.s3.put_vectors_from_df(
    df=my_df,
    key_column="id",
    vector_column="embedding",  # precomputed list[float] / np.ndarray per row
    vector_bucket=bucket_name,
    index=index_name,
)
```

```python
df = pd.DataFrame(
    {
        "id": ["m-1", "m-2", "m-3", "m-4", "m-5"],
        "title": [
            "A wildlife photographer documents the rebirth of a forest after a wildfire.",
            "An investigative reporter uncovers fraud at a multinational bank.",
            "A coming-of-age comedy about siblings inheriting a struggling family bakery.",
            "A heart-warming tale of an elderly widower and a stray dog.",
            "A documentary tracing the history of jazz from New Orleans to Tokyo.",
        ],
        "genre": ["documentary", "drama", "comedy", "drama", "documentary"],
        "year": [2022, 2019, 2024, 2023, 2018],
    }
)
df
```

```text
    id                                              title        genre  year
0  m-1  A wildlife photographer documents the rebirth ...  documentary  2022
1  m-2  An investigative reporter uncovers fraud at a ...        drama  2019
2  m-3  A coming-of-age comedy about siblings inheriti...       comedy  2024
3  m-4  A heart-warming tale of an elderly widower and...        drama  2023
4  m-5  A documentary tracing the history of jazz from...  documentary  2018
```

```python
wr.s3.put_vectors_from_df(
    df=df,
    key_column="id",
    text_column="title",
    bedrock_model_id="amazon.titan-embed-text-v2:0",
    bedrock_model_kwargs={"dimensions": dimension},
    vector_bucket=bucket_name,
    index=index_name,
)
```

## Querying by text

`query_vectors` runs an approximate-nearest-neighbour search. Pass `query_text` + `bedrock_model_id` to embed the query on the fly, or `query_vector` to query with a precomputed embedding (shown later).

```python
wr.s3.query_vectors(
    query_text="a touching story about loneliness and companionship",
    bedrock_model_id="amazon.titan-embed-text-v2:0",
    bedrock_model_kwargs={"dimensions": dimension},
    top_k=3,
    return_distance=True,
    return_metadata=True,
    vector_bucket=bucket_name,
    index=index_name,
)
```

```text
   key  distance                                metadata
0  m-4  0.560630        {'year': 2023, 'genre': 'drama'}
1  m-2  0.701137        {'genre': 'drama', 'year': 2019}
2  m-5  0.751206  {'year': 2018, 'genre': 'documentary'}
```

### Filtering on metadata

Filters use MongoDB-style operators (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`). They are evaluated *during* the search, not as a post-filter.

```python
wr.s3.query_vectors(
    query_text="music history",
    bedrock_model_id="amazon.titan-embed-text-v2:0",
    bedrock_model_kwargs={"dimensions": dimension},
    top_k=5,
    filter={"$and": [{"genre": {"$eq": "documentary"}}, {"year": {"$gte": 2020}}]},
    return_metadata=True,
    vector_bucket=bucket_name,
    index=index_name,
)
```

```text
   key  distance                                metadata
0  m-1  0.776892  {'year': 2022, 'genre': 'documentary'}
```

## Working with vectors directly

`get_vectors` retrieves vectors by key, optionally returning the embedding data and/or metadata. Useful for inspection, re-querying, or exporting subsets.

```python
fetched = wr.s3.get_vectors(
    keys=["m-1", "m-3"],
    return_data=True,
    return_metadata=True,
    vector_bucket=bucket_name,
    index=index_name,
)
fetched
```

```text
   key                                             vector  \
0  m-3  [-0.03277716785669327, 0.10634636878967285, 0....   
1  m-1  [-0.12095249444246292, 0.10887987911701202, 0....   

                                 metadata  
0       {'year': 2024, 'genre': 'comedy'}  
1  {'year': 2022, 'genre': 'documentary'}
```

### Querying with a precomputed vector

Any `list[float]` / `np.ndarray` of the right dimension works. Here we re-use a vector we just fetched — in practice, this is where you'd plug in embeddings from your own model.

```python
wr.s3.query_vectors(
    query_vector=fetched.iloc[0]["vector"],
    top_k=3,
    return_distance=True,
    return_metadata=True,
    vector_bucket=bucket_name,
    index=index_name,
)
```

```text
   key  distance                           metadata
0  m-3  0.000384  {'genre': 'comedy', 'year': 2024}
1  m-2  0.733959   {'genre': 'drama', 'year': 2019}
2  m-4  0.755624   {'year': 2023, 'genre': 'drama'}
```

### Deleting vectors by key

```python
wr.s3.delete_vectors(
    keys=["m-3"],
    vector_bucket=bucket_name,
    index=index_name,
)
```

### Bulk export

`list_vectors` walks the entire index. With `use_threads=True` it parallelises across up to 16 segments under the hood; pass `return_data=True` / `return_metadata=True` to include the full payload.

```python
wr.s3.list_vectors(
    return_metadata=True,
    vector_bucket=bucket_name,
    index=index_name,
    use_threads=4,
)
```

```text
   key                                metadata
0  m-1  {'year': 2022, 'genre': 'documentary'}
1  m-5  {'genre': 'documentary', 'year': 2018}
2  m-4        {'year': 2023, 'genre': 'drama'}
3  m-2        {'genre': 'drama', 'year': 2019}
```

## Cleanup

```python
wr.s3.delete_vector_index(name=index_name, vector_bucket=bucket_name)
wr.s3.delete_vector_bucket(name=bucket_name)
```
