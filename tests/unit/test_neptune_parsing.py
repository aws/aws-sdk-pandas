import logging
from unittest.mock import MagicMock

import pytest  # type: ignore
from gremlin_python.process.traversal import T
from gremlin_python.structure.graph import Edge, Path, Property, Vertex, VertexProperty

import awswrangler as wr
import awswrangler.pandas as pd
from awswrangler import exceptions
from awswrangler.neptune._client import NeptuneClient

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.fixture(scope="session")
def gremlin_parser() -> wr.neptune.GremlinParser:
    c = object.__new__(wr.neptune.GremlinParser)
    return c


# parse Vertex elements
def test_parse_gremlin_vertex_elements(gremlin_parser):
    # parse vertex elements
    v = Vertex("foo")
    input = [v]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 3)
    assert row["id"] == "foo"
    assert row["label"] == "vertex"
    assert not row["properties"]  # gremlinpython <3.8 returns None, >=3.8 returns []

    # parse multiple vertex elements
    v1 = Vertex("bar")
    input = [v, v1]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[1]
    assert df.shape == (2, 3)
    assert row["id"] == "bar"
    assert row["label"] == "vertex"
    assert not row["properties"]  # gremlinpython <3.8 returns None, >=3.8 returns []


# parse Edge elements
def test_parse_gremlin_edge_elements(gremlin_parser):
    # parse edge elements
    v = Edge("foo", "out1", "label", "in1")
    input = [v]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 5)
    assert row["id"] == "foo"
    assert row["outV"] == "out1"
    assert row["label"] == "label"
    assert row["inV"] == "in1"
    assert not row["properties"]  # gremlinpython <3.8 returns None, >=3.8 returns []

    # parse multiple edge elements
    v1 = Edge("bar", "out1", "label", "in2")
    input = [v, v1]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[1]
    assert df.shape == (2, 5)
    assert row["id"] == "bar"
    assert row["outV"] == "out1"
    assert row["label"] == "label"
    assert row["inV"] == "in2"
    assert not row["properties"]  # gremlinpython <3.8 returns None, >=3.8 returns []


# parse Property elements
def test_parse_gremlin_property_elements(gremlin_parser):
    # parse VertexProperty elements
    v = VertexProperty("foo", "name", "bar", "v1")
    input = [v]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 6)
    assert row["id"] == "foo"
    assert row["label"] == "name"
    assert row["value"] == "bar"
    assert row["key"] == "name"
    assert row["vertex"] == "v1"
    assert not row["properties"]  # gremlinpython <3.8 returns None, >=3.8 returns []

    v = Property("foo", "name", "bar")
    input = [v]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 3)
    assert row["element"] == "bar"
    assert row["value"] == "name"
    assert row["key"] == "foo"


# parse Path elements
def _normalize_properties(d: dict) -> dict:
    # gremlinpython <3.8 returns properties=None, >=3.8 returns properties=[].
    return {k: (None if k == "properties" and not v else v) for k, v in d.items()}


def test_parse_gremlin_path_elements(gremlin_parser):
    # parse path with elements
    v = Vertex("foo")
    v2 = Vertex("bar")
    e1 = Edge("e1", "foo", "label", "bar")
    p = Path(labels=["vertex", "label", "vertex"], objects=[v, e1, v2])
    out = gremlin_parser.gremlin_results_to_dict([p])
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 3)
    assert _normalize_properties(row[0]) == {"id": "foo", "label": "vertex", "properties": None}
    assert _normalize_properties(row[1]) == {
        "id": "e1",
        "label": "label",
        "outV": "foo",
        "inV": "bar",
        "properties": None,
    }
    assert _normalize_properties(row[2]) == {"id": "bar", "label": "vertex", "properties": None}

    # parse path with multiple elements
    e2 = Edge("bar", "out1", "label", "in2")
    v3 = Vertex("in2")
    p1 = Path(labels=["vertex", "label", "vertex"], objects=[v2, e2, v3])
    out = gremlin_parser.gremlin_results_to_dict([p, p1])
    df = pd.DataFrame.from_records(out)
    row = df.iloc[1]
    assert df.shape == (2, 3)
    assert _normalize_properties(row[0]) == {"id": "bar", "label": "vertex", "properties": None}
    assert _normalize_properties(row[1]) == {
        "id": "bar",
        "label": "label",
        "outV": "out1",
        "inV": "in2",
        "properties": None,
    }
    assert _normalize_properties(row[2]) == {"id": "in2", "label": "vertex", "properties": None}

    # parse path with maps
    p = Path(
        labels=["vertex", "label", "vertex"],
        objects=[{"name": "foo", "age": 29}, {"dist": 32}, {"name": "bar", "age": 40}],
    )
    out = gremlin_parser.gremlin_results_to_dict([p])
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 3)
    assert row[0]["name"] == "foo"
    assert row[0]["age"] == 29
    assert row[1]["dist"] == 32
    assert row[2]["name"] == "bar"
    assert row[2]["age"] == 40

    # parse path with mixed elements and maps
    p = Path(
        labels=["vertex", "label", "vertex"],
        objects=[{"name": "foo", "age": 29}, Edge("bar", "out1", "label", "in2"), {"name": "bar", "age": 40}],
    )
    out = gremlin_parser.gremlin_results_to_dict([p])
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 3)
    assert row[0]["name"] == "foo"
    assert row[0]["age"] == 29
    assert _normalize_properties(row[1]) == {
        "id": "bar",
        "label": "label",
        "outV": "out1",
        "inV": "in2",
        "properties": None,
    }
    assert row[2]["name"] == "bar"
    assert row[2]["age"] == 40


# parse vertex valueMap
def test_parse_gremlin_maps(gremlin_parser):
    # parse map
    m = {"name": "foo", "age": 29}
    input = [m]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 2)
    assert row["name"] == "foo"
    assert row["age"] == 29

    # parse multiple maps with T
    m1 = {"name": ["foo"], T.id: "2", "age": [40], T.label: "vertex"}
    input = [m, m1]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[1]
    assert df.shape == (2, 4)
    assert row["name"] == "foo"
    assert row["age"] == 40
    assert row[T.id] == "2"
    assert row[T.label] == "vertex"
    m2 = {"name": ["foo", "bar"], T.id: "2", T.label: "vertex"}
    input = [m, m1, m2]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[2]
    assert df.shape == (3, 4)
    assert row["name"] == ["foo", "bar"]
    assert row[T.id] == "2"
    assert row[T.label] == "vertex"


# parse scalar
def test_parse_gremlin_scalar(gremlin_parser):
    # parse map
    m = 12
    n = "Foo"
    input = [m, n]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (2, 1)
    assert row[0] == 12
    row = df.iloc[1]
    assert row[0] == "Foo"


# parse subgraph
def test_parse_gremlin_subgraph(gremlin_parser):
    m = {"@type": "tinker:graph", "@value": {"vertices": ["v[45]", "v[9]"], "edges": ["e[3990][9-route->45]"]}}
    input = [m]
    out = gremlin_parser.gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1, 2)
    assert row["@type"] == "tinker:graph"
    assert row["@value"] == {"vertices": ["v[45]", "v[9]"], "edges": ["e[3990][9-route->45]"]}


# to_rdf_graph IRIREF validation: caller-supplied DataFrame cells must conform to
# the SPARQL IRIREF grammar so they cannot close the <...> token and inject
# arbitrary SPARQL UPDATE syntax (DELETE / DROP / LOAD / ...).


def _rdf_triples_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "s": ["http://example.org/alice", "http://example.org/bob"],
            "p": ["http://xmlns.com/foaf/0.1/name", "http://xmlns.com/foaf/0.1/name"],
            "o": ["http://example.org/AliceName", "http://example.org/BobName"],
        }
    )


def _mock_neptune_client() -> MagicMock:
    client = MagicMock(spec=NeptuneClient)
    client.write_sparql.return_value = True
    return client


def test_to_rdf_graph_accepts_well_formed_iris():
    client = _mock_neptune_client()
    df = _rdf_triples_df()

    assert wr.neptune.to_rdf_graph(client, df) is True
    client.write_sparql.assert_called_once()
    query = client.write_sparql.call_args.args[0]
    assert "<http://example.org/alice>" in query
    assert "<http://example.org/bob>" in query


@pytest.mark.parametrize(
    "malicious_cell, column",
    [
        # Bug-bounty PoC payload: closes IRI, runs DELETE WHERE, reopens INSERT.
        (
            "> . }; DELETE WHERE { ?s ?p ?o }; "
            "INSERT DATA { <http://evil.com/x> <http://evil.com/y> <http://evil.com/z",
            "o",
        ),
        # DROP ALL via the subject slot.
        ("http://x.com/s> <http://x.com/p> <http://x.com/o> . }; DROP ALL ; INSERT DATA { <a", "s"),
        # LOAD via the predicate slot.
        (
            "http://x.com/p> <http://x.com/o> . }; LOAD <http://evil.com/payload.ttl> ; "
            "INSERT DATA { <http://x.com/s> <http://x.com/p2",
            "p",
        ),
        # Whitespace alone is enough to break the IRIREF token.
        ("http://example.org/ has space", "o"),
        ("http://example.org/a\n<http://x>", "o"),
        ("http://example.org/<inner>", "s"),
    ],
)
def test_to_rdf_graph_rejects_malicious_cells(malicious_cell, column):
    client = _mock_neptune_client()
    df = _rdf_triples_df()
    df.loc[0, column] = malicious_cell

    with pytest.raises(exceptions.InvalidArgumentValue, match="not a valid IRI"):
        wr.neptune.to_rdf_graph(client, df)
    # Validation must run before any network call.
    client.write_sparql.assert_not_called()


def test_to_rdf_graph_rejects_malicious_graph_column_for_quads():
    client = _mock_neptune_client()
    df = _rdf_triples_df()
    df["g"] = ["http://example.org/g1", "http://example.org/g2"]
    df.loc[0, "g"] = "http://x> {} }; DROP ALL ; INSERT DATA { GRAPH <http://x> { <a"

    with pytest.raises(exceptions.InvalidArgumentValue, match="'g'"):
        wr.neptune.to_rdf_graph(client, df)
    client.write_sparql.assert_not_called()


def test_to_rdf_graph_error_identifies_row_and_column():
    client = _mock_neptune_client()
    df = _rdf_triples_df()
    df.loc[1, "o"] = "http://example.org/bad value"

    with pytest.raises(exceptions.InvalidArgumentValue) as exc_info:
        wr.neptune.to_rdf_graph(client, df)
    message = str(exc_info.value)
    assert "'o'" in message
    assert "row index 1" in message
