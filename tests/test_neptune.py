import logging
import random
import string
import uuid
from typing import Any, Dict

import pandas as pd
import pytest  # type: ignore
from gremlin_python.process.traversal import Direction, T

import awswrangler as wr

from ._utils import extract_cloudformation_outputs

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="session")
def cloudformation_outputs():
    return extract_cloudformation_outputs()


@pytest.fixture(scope="session")
def neptune_endpoint(cloudformation_outputs) -> str:
    return cloudformation_outputs["NeptuneClusterEndpoint"]


@pytest.fixture(scope="session")
def neptune_port(cloudformation_outputs) -> int:
    return cloudformation_outputs["NeptunePort"]


@pytest.fixture(scope="session")
def neptune_iam_enabled(cloudformation_outputs) -> int:
    return cloudformation_outputs["NeptuneIAMEnabled"]


def test_connection_neptune_https(neptune_endpoint, neptune_port, neptune_iam_enabled):
    client = wr.neptune.connect(host=neptune_endpoint, port=neptune_port, iam_enabled=neptune_iam_enabled)
    resp = client.status()
    assert resp["status"] == "healthy"


def test_connection_neptune_https_iam(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=True)
    resp = client.status()
    assert resp["status"] == "healthy"


def test_opencypher_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    wr.neptune.execute_opencypher(client, "create (a:Foo { name: 'foo' })-[:TEST]->(b {name : 'bar'})")
    df = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN n LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 1)

    assert isinstance(df, pd.DataFrame)
    df = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN n LIMIT 2")
    assert df.shape == (2, 1)

    df = wr.neptune.execute_opencypher(client, "MATCH p=(n)-[r]->(d) RETURN p LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 1)

    df = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN id(n), labels(n) LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)
    row = df.iloc[0]
    assert row["id(n)"]
    assert row["labels(n)"]


def test_flatten_df(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    wr.neptune.execute_opencypher(client, "create (a:Foo1 { name: 'foo' })-[:TEST]->(b {name : 'bar'})")
    df = wr.neptune.execute_opencypher(client, "MATCH (n:Foo1) RETURN n LIMIT 1")
    df_test = wr.neptune.flatten_nested_df(df)
    assert isinstance(df_test, pd.DataFrame)
    assert df_test.shape == (1, 6)
    row = df_test.iloc[0]
    assert row["n_~properties_name"]

    df_test = wr.neptune.flatten_nested_df(df, include_prefix=False)
    assert isinstance(df_test, pd.DataFrame)
    assert df_test.shape == (1, 6)
    row = df_test.iloc[0]
    assert row["_~properties_name"]

    df_test = wr.neptune.flatten_nested_df(df, seperator="|")
    assert isinstance(df_test, pd.DataFrame)
    assert df_test.shape == (1, 6)
    row = df_test.iloc[0]
    assert row["n|~properties|name"]

    df_new = pd.DataFrame([{"~id": "0", "~labels": ["version"], "~properties": {"type": "version"}}])
    df_test = wr.neptune.flatten_nested_df(df_new)
    assert df_test.shape == (1, 4)
    row = df_test.iloc[0]
    assert row["~properties_type"]


def test_opencypher_malformed_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_opencypher(client, "MATCH (n) LIMIT 2")
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_opencypher(client, "")


def test_gremlin_malformed_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_gremlin(client, "g.V().limit(1")
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_gremlin(client, "")


def test_sparql_malformed_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_sparql(client, "SELECT ?s ?p ?o {?s ?pLIMIT 1")
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_sparql(client, "")


def test_gremlin_query_vertices(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    wr.neptune.execute_gremlin(client, f"g.addV().property(T.id, '{uuid.uuid4()}')")
    df = wr.neptune.execute_gremlin(client, "g.V().limit(1)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)

    df = wr.neptune.execute_gremlin(client, "g.V().limit(2)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)


def test_gremlin_query_edges(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    wr.neptune.execute_gremlin(client, "g.addE('bar').from(addV('foo')).to(addV('foo'))")
    df = wr.neptune.execute_gremlin(client, "g.E().limit(1)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 4)

    df = wr.neptune.execute_gremlin(client, "g.E().limit(2)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 4)


def test_gremlin_query_no_results(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    df = wr.neptune.execute_gremlin(client, "g.V('foo').drop()")
    assert isinstance(df, pd.DataFrame)


def test_sparql_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    df = wr.neptune.execute_sparql(client, "INSERT DATA { <test> <test> <test>}")
    df = wr.neptune.execute_sparql(client, "INSERT DATA { <test1> <test1> <test1>}")
    df = wr.neptune.execute_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 3)

    df = wr.neptune.execute_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 2")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 3)


def test_gremlin_write_updates(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    id = uuid.uuid4()
    wr.neptune.execute_gremlin(client, f"g.addV().property(T.id, '{str(id)}')")

    data = [{"~id": id, "age": 50, "name": "foo"}]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    res = wr.neptune.execute_gremlin(client, f"g.V('{id}').valueMap().with(WithOptions.tokens)")
    saved_row = res.iloc[0]
    assert saved_row["age"] == 50

    final_df = wr.neptune.execute_gremlin(client, f"g.V('{str(id)}').values('age')")
    assert final_df.iloc[0][0] == 50

    # check write cardinality
    data = [{"~id": id, "age(single)": 55, "name": "bar"}]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df, use_header_cardinality=True)
    res = wr.neptune.execute_gremlin(client, f"g.V('{id}').valueMap().with(WithOptions.tokens)")
    saved_row = res.iloc[0]
    assert saved_row["age"] == 55
    assert saved_row["name"] == ["foo", "bar"]
    res = wr.neptune.to_property_graph(client, df, use_header_cardinality=False)
    res = wr.neptune.execute_gremlin(client, f"g.V('{id}').valueMap().with(WithOptions.tokens)")
    saved_row = res.iloc[0]
    assert saved_row["age(single)"] == 55
    assert saved_row["name"] == ["foo", "bar"]


def test_gremlin_write_vertices(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    wr.neptune.execute_gremlin(client, "g.addV('foo')")
    initial_cnt_df = wr.neptune.execute_gremlin(client, "g.V().hasLabel('foo').count()")
    data = [_create_dummy_vertex(), _create_dummy_vertex(), _create_dummy_vertex()]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    original_row = df.iloc[0]
    res = wr.neptune.execute_gremlin(client, f"g.V('{original_row['~id']}').elementMap()")
    saved_row = res.iloc[0]
    assert saved_row[T.id] == original_row["~id"]
    assert saved_row[T.label] == original_row["~label"]
    assert saved_row["int"] == original_row["int"]
    assert saved_row["str"] == original_row["str"]

    final_cnt_df = wr.neptune.execute_gremlin(client, "g.V().hasLabel('foo').count()")
    assert final_cnt_df.iloc[0][0] == initial_cnt_df.iloc[0][0] + 3

    # check to make sure batch addition of vertices works
    data = []
    for i in range(0, 50):
        data.append(_create_dummy_vertex())

    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    batch_cnt_df = wr.neptune.execute_gremlin(client, "g.V().hasLabel('foo').count()")
    assert batch_cnt_df.iloc[0][0] == final_cnt_df.iloc[0][0] + 50

    # check write cardinality
    v = _create_dummy_vertex()
    v2 = _create_dummy_vertex()
    v2["~id"] = v["~id"]
    df = pd.DataFrame([v])
    res = wr.neptune.to_property_graph(client, df)
    original_row = df.iloc[0]

    # save it a second time to make sure it updates correctly when re-adding
    df = pd.DataFrame([v2])
    df.rename(columns={"int": "int(single)"}, inplace=True)
    res = wr.neptune.to_property_graph(client, df, use_header_cardinality=True)
    res = wr.neptune.execute_gremlin(client, f"g.V('{original_row['~id']}').valueMap().with(WithOptions.tokens)")
    saved_row = res.iloc[0]
    assert saved_row[T.id] == original_row["~id"]
    assert saved_row[T.label] == original_row["~label"]
    assert saved_row["int"] == v2["int"]
    assert len(saved_row["str"]) == 2

    # Check that it is respecting the header cardinality
    df = pd.DataFrame([v2])
    df.rename(columns={"int": "int(single)"}, inplace=True)
    res = wr.neptune.to_property_graph(client, df, use_header_cardinality=True)
    res = wr.neptune.execute_gremlin(client, f"g.V('{original_row['~id']}').valueMap().with(WithOptions.tokens)")
    saved_row = res.iloc[0]
    assert saved_row[T.id] == original_row["~id"]
    assert saved_row[T.label] == original_row["~label"]
    assert saved_row["int"] == v2["int"]
    assert len(saved_row["str"]) == 2


def test_gremlin_write_edges(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    initial_cnt_df = wr.neptune.execute_gremlin(client, "g.E().hasLabel('bar').count()")

    data = [_create_dummy_edge(), _create_dummy_edge(), _create_dummy_edge()]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    original_row = df.iloc[0]
    res = wr.neptune.execute_gremlin(client, f"g.E('{original_row['~id']}').elementMap()")
    saved_row = res.iloc[0]
    assert saved_row[T.id] == original_row["~id"]
    assert saved_row[T.label] == original_row["~label"]
    assert saved_row[Direction.IN][T.id] == original_row["~to"]
    assert saved_row[Direction.OUT][T.id] == original_row["~from"]
    assert saved_row["int"] == original_row["int"]
    assert saved_row["str"] == original_row["str"]

    final_cnt_df = wr.neptune.execute_gremlin(client, "g.E().hasLabel('bar').count()")
    assert final_cnt_df.iloc[0][0] == initial_cnt_df.iloc[0][0] + 3

    # check to make sure batch addition of edges works
    data = []
    for i in range(0, 50):
        data.append(_create_dummy_edge())

    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    batch_cnt_df = wr.neptune.execute_gremlin(client, "g.E().hasLabel('bar').count()")
    assert batch_cnt_df.iloc[0][0] == final_cnt_df.iloc[0][0] + 50


def test_sparql_write_triples(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    initial_df = wr.neptune.execute_sparql(client, "SELECT ?p ?o WHERE { <foo> ?p ?o .}")

    data = [_create_dummy_triple(), _create_dummy_triple(), _create_dummy_triple()]
    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    final_df = wr.neptune.execute_sparql(client, "SELECT ?p ?o WHERE { <foo> ?p ?o .}")
    assert len(final_df.index) == len(initial_df.index) + 3

    # check to make sure batch addition of edges works
    data = []
    for i in range(0, 50):
        data.append(_create_dummy_triple())

    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    batch_df = wr.neptune.execute_sparql(client, "SELECT ?p ?o WHERE { <foo> ?p ?o .}")
    assert len(batch_df.index) == len(final_df.index) + 50


def test_sparql_write_quads(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    initial_df = wr.neptune.execute_sparql(client, "SELECT ?p ?o FROM <bar> WHERE { <foo> ?p ?o .}")

    data = [_create_dummy_quad(), _create_dummy_quad(), _create_dummy_quad()]
    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    final_df = wr.neptune.execute_sparql(client, "SELECT ?p ?o  FROM <bar> WHERE { <foo> ?p ?o .}")
    assert len(final_df.index) == len(initial_df.index) + 3

    # check to make sure batch addition of edges works
    data = []
    for i in range(0, 50):
        data.append(_create_dummy_quad())

    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    batch_df = wr.neptune.execute_sparql(client, "SELECT ?p ?o FROM <bar> WHERE { <foo> ?p ?o .}")
    assert len(batch_df.index) == len(final_df.index) + 50


def _create_dummy_vertex() -> Dict[str, Any]:
    data = dict()
    data["~id"] = str(uuid.uuid4())
    data["~label"] = "foo"
    data["int"] = random.randint(0, 1000)
    data["str"] = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    data["list"] = [random.randint(0, 1000), random.randint(0, 1000)]
    return data


def _create_dummy_edge() -> Dict[str, Any]:
    data = dict()
    data["~id"] = str(uuid.uuid4())
    data["~label"] = "bar"
    data["~to"] = str(uuid.uuid4())
    data["~from"] = str(uuid.uuid4())
    data["int"] = random.randint(0, 1000)
    data["str"] = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    return data


def _create_dummy_triple() -> Dict[str, Any]:
    data = dict()
    data["s"] = "foo"
    data["p"] = str(uuid.uuid4())
    data["o"] = random.randint(0, 1000)
    return data


def _create_dummy_quad() -> Dict[str, Any]:
    data = _create_dummy_triple()
    data["g"] = "bar"
    return data
