import logging
import random
import string
import uuid
from typing import Any, Dict

import pandas as pd
import pytest  # type: ignore

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
    wr.neptune.execute_opencypher(client, "create (a { name: 'foo' })-[:TEST]->(b {name : 'bar'})")
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


def test_gremlin_query_vertices(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    wr.neptune.execute_gremlin(client, f"g.addV().property(T.id, '{str(id)}')")
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

    data = [{"~id": id, "age": 50}]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    final_df = wr.neptune.execute_gremlin(client, f"g.V('{str(id)}').values('age')")
    assert final_df.iloc[0][0] == 50


def test_gremlin_write_vertices(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    wr.neptune.execute_gremlin(client, "g.addV('foo')")
    initial_cnt_df = wr.neptune.execute_gremlin(client, "g.V().hasLabel('foo').count()")
    data = [_create_dummy_vertex(), _create_dummy_vertex(), _create_dummy_vertex()]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

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


def test_gremlin_write_edges(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    initial_cnt_df = wr.neptune.execute_gremlin(client, "g.E().hasLabel('bar').count()")

    data = [_create_dummy_edge(), _create_dummy_edge(), _create_dummy_edge()]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

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
    data["~id"] = uuid.uuid4()
    data["~label"] = "foo"
    data["int"] = random.randint(0, 1000)
    data["str"] = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    data["list"] = [random.randint(0, 1000), random.randint(0, 1000)]
    return data


def _create_dummy_edge() -> Dict[str, Any]:
    data = dict()
    data["~id"] = uuid.uuid4()
    data["~label"] = "bar"
    data["~to"] = uuid.uuid4()
    data["~from"] = uuid.uuid4()
    data["int"] = random.randint(0, 1000)
    data["str"] = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    return data


def _create_dummy_triple() -> Dict[str, Any]:
    data = dict()
    data["s"] = "foo"
    data["p"] = uuid.uuid4()
    data["o"] = random.randint(0, 1000)
    return data


def _create_dummy_quad() -> Dict[str, Any]:
    data = _create_dummy_triple()
    data["g"] = "bar"
    return data
