---
id: 033-amazon-neptune
title: "Amazon Neptune"
sidebar_position: 33
sidebar_label: "33 - Amazon Neptune"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/033%20-%20Amazon%20Neptune.ipynb
---

# Amazon Neptune

> This page is generated from [`tutorials/033 - Amazon Neptune.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/033%20-%20Amazon%20Neptune.ipynb). Open it in Jupyter to run it yourself.
Note: to be able to use SPARQL you must either install `SPARQLWrapper` or install AWS SDK for pandas with `sparql` extra:

```python
!pip install 'awswrangler[gremlin, opencypher, sparql]'
```

## Initialize

The first step to using AWS SDK for pandas with Amazon Neptune is to import the library and create a client connection.

<div style="background-color:#eeeeee; padding:10px; text-align:left; border-radius:10px; margin-top:10px; margin-bottom:10px; "><b>Note</b>: Connecting to Amazon Neptune requires that the application you are running has access to the Private VPC where Neptune is located. Without this access you will not be able to connect using AWS SDK for pandas.</div>

```python
import pandas as pd

import awswrangler as wr

url = "<INSERT CLUSTER ENDPOINT>"  # The Neptune Cluster endpoint
iam_enabled = False  # Set to True/False based on the configuration of your cluster
neptune_port = 8182  # Set to the Neptune Cluster Port, Default is 8182
client = wr.neptune.connect(url, neptune_port, iam_enabled=iam_enabled)
```

## Return the status of the cluster

```python
print(client.status())
```

## Retrieve Data from Neptune using AWS SDK for pandas

AWS SDK for pandas supports querying Amazon Neptune using TinkerPop Gremlin and openCypher for property graph data or SPARQL for RDF data.

### Gremlin

```python
query = "g.E().project('source', 'target').by(outV().id()).by(inV().id()).limit(5)"
df = wr.neptune.execute_gremlin(client, query)
display(df.head(5))
```

### SPARQL

```python
query = """
        PREFIX foaf: <https://xmlns.com/foaf/0.1/>
        PREFIX ex: <https://www.example.com/>
        SELECT ?firstName WHERE { ex:JaneDoe foaf:knows ?person . ?person foaf:firstName ?firstName }"""
df = wr.neptune.execute_sparql(client, query)
display(df.head(5))
```

### openCypher

```python
query = "MATCH (n)-[r]->(d) RETURN id(n) as source, id(d) as target LIMIT 5"
df = wr.neptune.execute_opencypher(client, query)
display(df.head(5))
```

## Saving Data using AWS SDK for pandas

AWS SDK for pandas supports saving Pandas DataFrames into Amazon Neptune using either a property graph or RDF data model.

### Property Graph

If writing to a property graph then DataFrames for vertices and edges must be written separately. DataFrames for vertices must have a `~label` column with the label and a `~id` column for the vertex id.

If the `~id` column does not exist, the specified id does not exists, or is empty then a new vertex will be added.

If no `~label` column exists then writing to the graph will be treated as an update of the element with the specified `~id` value.

DataFrames for edges must have a `~id`, `~label`, `~to`, and `~from` column. If the `~id` column does not exist the specified id does not exists, or is empty then a new edge will be added. If no `~label`, `~to`, or `~from` column exists an exception will be thrown.

#### Add Vertices/Nodes

```python
import random
import string
import uuid


def _create_dummy_vertex():
    data = dict()
    data["~id"] = uuid.uuid4()
    data["~label"] = "foo"
    data["int"] = random.randint(0, 1000)
    data["str"] = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    data["list"] = [random.randint(0, 1000), random.randint(0, 1000)]
    return data


data = [_create_dummy_vertex(), _create_dummy_vertex(), _create_dummy_vertex()]
df = pd.DataFrame(data)
res = wr.neptune.to_property_graph(client, df)
query = f"MATCH (s) WHERE id(s)='{data[0]['~id']}' RETURN s"
df = wr.neptune.execute_opencypher(client, query)
display(df)
```

#### Add Edges

```python
import random
import string
import uuid


def _create_dummy_edge():
    data = dict()
    data["~id"] = uuid.uuid4()
    data["~label"] = "bar"
    data["~to"] = uuid.uuid4()
    data["~from"] = uuid.uuid4()
    data["int"] = random.randint(0, 1000)
    data["str"] = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    return data


data = [_create_dummy_edge(), _create_dummy_edge(), _create_dummy_edge()]
df = pd.DataFrame(data)
res = wr.neptune.to_property_graph(client, df)
query = f"MATCH (s)-[r]->(d) WHERE id(r)='{data[0]['~id']}' RETURN r"
df = wr.neptune.execute_opencypher(client, query)
display(df)
```

#### Update Existing Nodes

```python
idval = uuid.uuid4()
wr.neptune.execute_gremlin(client, f"g.addV().property(T.id, '{str(idval)}')")
query = f"MATCH (s) WHERE id(s)='{idval}' RETURN s"
df = wr.neptune.execute_opencypher(client, query)
print("Before")
display(df)
data = [{"~id": idval, "age": 50}]
df = pd.DataFrame(data)
res = wr.neptune.to_property_graph(client, df)
df = wr.neptune.execute_opencypher(client, query)
print("After")
display(df)
```

#### Setting cardinality based on the header

 If you would like to save data using `single` cardinality then you can postfix (single) to the column header and
    set `use_header_cardinality=True` (default). e.g. A column named `name(single)` will save the `name` property as single cardinality. You can disable this by setting `use_header_cardinality=False`.

```python
data = [_create_dummy_vertex()]
df = pd.DataFrame(data)
# Adding (single) to the column name in the DataFrame will cause it to write that property as `single` cardinality
df.rename(columns={"int": "int(single)"}, inplace=True)
res = wr.neptune.to_property_graph(client, df, use_header_cardinality=True)


# This can be disabled by setting `use_header_cardinality = False`
df.rename(columns={"int": "int(single)"}, inplace=True)
res = wr.neptune.to_property_graph(client, df, use_header_cardinality=False)
```

### RDF

The DataFrame must consist of triples with column names for the subject, predicate, and object specified. If none are provided then `s`, `p`, and `o` are the default.

If you want to add data into a named graph then you will also need the graph column, default is `g`.

#### Write Triples

```python
def _create_dummy_triple(s: str = "foo"):
    return {
        "s": s,
        "p": str(uuid.uuid4()),
        "o": random.randint(0, 1000),
    }


label = f"foo_{uuid.uuid4()}"
data = [_create_dummy_triple(label), _create_dummy_triple(label), _create_dummy_triple(label)]
df = pd.DataFrame(data)
res = wr.neptune.to_rdf_graph(client, df)

query = f"SELECT ?p ?o WHERE {{ <{label}> ?p ?o .}}"
df = wr.neptune.execute_sparql(client, query)
display(df)
```

#### Write Quads

```python
def _create_dummy_quad(s: str):
    data = _create_dummy_triple(s)
    data["g"] = "bar"
    return data


label = f"foo_{uuid.uuid4()}"
query = f"SELECT ?p ?o FROM <bar> WHERE {{ <{label}> ?p ?o .}}"

data = [_create_dummy_quad(label), _create_dummy_quad(label), _create_dummy_quad(label)]
df = pd.DataFrame(data)
res = wr.neptune.to_rdf_graph(client, df)
df = wr.neptune.execute_sparql(client, query)
display(df)
```

## Flatten DataFrames

One of the complexities of working with a row/columns paradigm, such as Pandas, with graph results set is that it is very common for graph results to return complex and nested objects. To help simplify using the results returned from a graph within a more tabular format we have added a method to flatten the returned Pandas DataFrame.

### Flattening the DataFrame

```python
client = wr.neptune.connect(url, 8182, iam_enabled=False)
query = "MATCH (n) RETURN n LIMIT 1"
df = wr.neptune.execute_opencypher(client, query)
print("Original")
display(df)
df_new = wr.neptune.flatten_nested_df(df)
print("Flattened")
display(df_new)
```

### Removing the prefixing of the parent column name

```python
df_new = wr.neptune.flatten_nested_df(df, include_prefix=False)
display(df_new)
```

### Specifying the column header separator

```python
df_new = wr.neptune.flatten_nested_df(df, separator="|")
display(df_new)
```

## Putting it into a workflow

```python
pip install igraph networkx
```

### Running PageRank using NetworkX

```python
import networkx as nx

# Retrieve Data from neptune
client = wr.neptune.connect(url, 8182, iam_enabled=False)
query = "MATCH (n)-[r]->(d) RETURN id(n) as source, id(d) as target LIMIT 100"
df = wr.neptune.execute_opencypher(client, query)

# Run PageRank
G = nx.from_pandas_edgelist(df, edge_attr=True)
pg = nx.pagerank(G)

# Save values back into Neptune
rows = []
for k in pg.keys():
    rows.append({"~id": k, "pageRank_nx(single)": pg[k]})
pg_df = pd.DataFrame(rows, columns=["~id", "pageRank_nx(single)"])
res = wr.neptune.to_property_graph(client, pg_df, use_header_cardinality=True)

# Retrieve newly saved data
query = (
    "MATCH (n:airport) WHERE n.pageRank_nx IS NOT NULL RETURN n.code, n.pageRank_nx ORDER BY n.pageRank_nx DESC LIMIT 5"
)
df = wr.neptune.execute_opencypher(client, query)
display(df)
```

### Running PageRank using iGraph

```python
import igraph as ig

# Retrieve Data from neptune
client = wr.neptune.connect(url, 8182, iam_enabled=False)
query = "MATCH (n)-[r]->(d) RETURN id(n) as source, id(d) as target LIMIT 100"
df = wr.neptune.execute_opencypher(client, query)

# Run PageRank
g = ig.Graph.TupleList(df.itertuples(index=False), directed=True, weights=False)
pg = g.pagerank()

# Save values back into Neptune
rows = []
for idx, v in enumerate(g.vs):
    rows.append({"~id": v["name"], "pageRank_ig(single)": pg[idx]})
pg_df = pd.DataFrame(rows, columns=["~id", "pageRank_ig(single)"])
res = wr.neptune.to_property_graph(client, pg_df, use_header_cardinality=True)

# Retrieve newly saved data
query = (
    "MATCH (n:airport) WHERE n.pageRank_ig IS NOT NULL RETURN n.code, n.pageRank_ig ORDER BY n.pageRank_ig DESC LIMIT 5"
)
df = wr.neptune.execute_opencypher(client, query)
display(df)
```

## Bulk Load

Data can be written using the Neptune Bulk Loader by way of S3.
The Bulk Loader is fast and optimized for large datasets.

For details on the IAM permissions needed to set this up, see [here](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html).

```python
df = pd.DataFrame([_create_dummy_edge() for _ in range(1000)])

wr.neptune.bulk_load(
    client=client,
    df=df,
    path="s3://my-bucket/stage-files/",
    iam_role="arn:aws:iam::XXX:role/XXX",
)
```

Alternatively, if the data is already on S3 in CSV format, you can use the `neptune.bulk_load_from_files` function.
This is also useful if the data is written to S3 as a byproduct of an AWS Athena command, as the example below will show.

```python
sql = """
SELECT
    <col_id> AS "~id"
  , <label_id> AS "~label"
  , *
FROM <database>.<table>
"""

wr.athena.start_query_execution(
    sql=sql,
    s3_output="s3://my-bucket/stage-files-athena/",
    wait=True,
)

wr.neptune.bulk_load_from_files(
    client=client,
    path="s3://my-bucket/stage-files-athena/",
    iam_role="arn:aws:iam::XXX:role/XXX",
)
```

Both the `bulk_load` and `bulk_load_from_files` functions are suitable at scale.
The latter simply invokes the Neptune Bulk Loader on existing data in S3.
The former, however, involves writing CSV data to S3. With `ray` and `modin` installed, this operation can also be distributed across multiple workers in a Ray cluster.
