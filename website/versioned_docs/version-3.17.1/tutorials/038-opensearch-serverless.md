---
id: 038-opensearch-serverless
title: "OpenSearch Serverless"
sidebar_position: 38
sidebar_label: "38 - OpenSearch Serverless"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/038%20-%20OpenSearch%20Serverless.ipynb
---

# OpenSearch Serverless

> This page is generated from [`tutorials/038 - OpenSearch Serverless.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/038%20-%20OpenSearch%20Serverless.ipynb). Open it in Jupyter to run it yourself.
Amazon OpenSearch Serverless is an on-demand serverless configuration for Amazon OpenSearch Service.

### Create collection

A collection in Amazon OpenSearch Serverless is a logical grouping of one or more indexes that represent an analytics workload.

Collections must have an assigned encryption policy, network policy, and a matching data access policy that grants permission to its resources.

```python
# Install the optional modules first
!pip install 'awswrangler[opensearch]'
```

```python
import awswrangler as wr
```

```python
data_access_policy = [
    {
        "Rules": [
            {
                "ResourceType": "index",
                "Resource": [
                    "index/my-collection/*",
                ],
                "Permission": [
                    "aoss:*",
                ],
            },
            {
                "ResourceType": "collection",
                "Resource": [
                    "collection/my-collection",
                ],
                "Permission": [
                    "aoss:*",
                ],
            },
        ],
        "Principal": [
            wr.sts.get_current_identity_arn(),
        ],
    }
]
```

AWS SDK for pandas can create default network and encryption policies based on the user input.

By default, the network policy allows public access to the collection, and the encryption policy encrypts the collection using AWS-managed KMS key.

Create a collection, and a corresponding data, network, and access policies:

```python
collection = wr.opensearch.create_collection(
    name="my-collection",
    data_policy=data_access_policy,
)

collection_endpoint = collection["collectionEndpoint"]
```

The call will wait and exit when the collection and corresponding policies are created and active.

To create a collection encrypted with customer KMS key, and attached to a VPC, provide KMS Key ARN and / or VPC endpoints:

```python
kms_key_arn = "arn:aws:kms:..."
vpc_endpoint = "vpce-..."

collection = wr.opensearch.create_collection(
    name="my-secure-collection",
    data_policy=data_access_policy,
    kms_key_arn=kms_key_arn,
    vpc_endpoints=[vpc_endpoint],
)
```

## Connect

Connect to the collection endpoint:

```python
client = wr.opensearch.connect(host=collection_endpoint)
```

## Create index

To create an index, run:

```python
index = "my-index-1"

wr.opensearch.create_index(
    client=client,
    index=index,
)
```

```text
{'acknowledged': True, 'shards_acknowledged': True, 'index': 'my-index-1'}
```

## Index documents

To index documents:

```python
wr.opensearch.index_documents(
    client,
    documents=[{"_id": "1", "name": "John"}, {"_id": "2", "name": "George"}, {"_id": "3", "name": "Julia"}],
    index=index,
)
```

```text
Indexing: 100% (3/3)|####################################|Elapsed Time: 0:00:12
```

```text
{'success': 3, 'errors': []}
```

It is also possible to index Pandas data frames:

```python
import pandas as pd

df = pd.DataFrame(
    [{"_id": "1", "name": "John", "tags": ["foo", "bar"]}, {"_id": "2", "name": "George", "tags": ["foo"]}]
)

wr.opensearch.index_df(
    client,
    df=df,
    index="index-df",
)
```

```text
Indexing: 100% (2/2)|####################################|Elapsed Time: 0:00:12
```

```text
{'success': 2, 'errors': []}
```

AWS SDK for pandas also supports indexing JSON and CSV documents.

For more examples, refer to the [031 - OpenSearch tutorial](https://aws-sdk-pandas.readthedocs.io/en/latest/tutorials/031%20-%20OpenSearch.html)

## Search

Search using search DSL:

```python
wr.opensearch.search(client, index=index, search_body={"query": {"match": {"name": "Julia"}}})
```

```text
  _id   name
0   3  Julia
```

## Delete index

To delete an index, run:

```python
wr.opensearch.delete_index(client=client, index=index)
```
