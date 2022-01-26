import logging

import pandas as pd
import pytest  # type: ignore
from gremlin_python.structure.graph import Path
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import VertexProperty
from gremlin_python.structure.graph import Property
from gremlin_python.process.traversal import T

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

@pytest.fixture(scope="session")
def neptune_client() -> wr.neptune.NeptuneClient:
    c = object.__new__(wr.neptune.NeptuneClient)
    return c


#parse Vertex elements
def test_parse_vertex_elements(neptune_client):
    # parse vertex elements
    v = Vertex("foo")
    input = [v]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,2)
    assert row['id'] == 'foo'
    assert row['label'] == 'vertex'

    # parse multiple vertex elements
    v1 = Vertex("bar")
    input = [v, v1]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[1]
    assert df.shape == (2,2)
    assert row['id'] == 'bar'
    assert row['label'] == 'vertex'


#parse Edge elements
def test_parse_edge_elements(neptune_client):
    # parse edge elements
    v = Edge("foo", 'out1', 'label', 'in1')
    input = [v]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,4)
    assert row['id'] == 'foo'
    assert row['outV'] == 'out1'
    assert row['label'] == 'label'
    assert row['inV'] == 'in1'

    # parse multiple edge elements
    v1 = Edge("bar", 'out1', 'label', 'in2')
    input = [v, v1]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[1]
    assert df.shape == (2,4)
    assert row['id'] == 'bar'
    assert row['outV'] == 'out1'
    assert row['label'] == 'label'
    assert row['inV'] == 'in2'

#parse Property elements
def test_parse_property_elements(neptune_client):
    # parse VertexProperty elements
    v = VertexProperty("foo", 'name', 'bar', 'v1')
    input = [v]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,5)
    assert row['id'] == 'foo'
    assert row['label'] == 'name'
    assert row['value'] == 'bar'
    assert row['key'] == 'name'
    assert row['vertex'] == 'v1'

    v = Property("foo", 'name', 'bar')
    input = [v]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,3)
    assert row['element'] == 'bar'
    assert row['value'] == 'name'
    assert row['key'] == 'foo'


#parse Path elements
def test_parse_path_elements(neptune_client):
    #parse path with elements
    v = Vertex("foo")
    v2 = Vertex("bar")
    e1 = Edge("e1", 'foo', 'label', 'bar')
    p = Path(labels=["vertex", "label", "vertex"], objects=[v, e1, v2])
    out = neptune_client._gremlin_results_to_dict([p])
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,3)
    assert row[0] == {'id': 'foo', 'label': 'vertex'}
    assert row[1] == {'id': 'e1', 'label': 'label', 'outV': 'foo', 'inV': 'bar'}
    assert row[2] == {'id': 'bar', 'label': 'vertex'}

    #parse path with multiple elements
    e2 = Edge("bar", 'out1', 'label', 'in2')
    v3 = Vertex("in2")
    p1 = Path(labels=["vertex", "label", "vertex"], objects=[v2, e2, v3])
    out = neptune_client._gremlin_results_to_dict([p, p1])
    df = pd.DataFrame.from_records(out)
    row = df.iloc[1]
    assert df.shape == (2,3)
    assert row[0] == {'id': 'bar', 'label': 'vertex'}
    assert row[1] == {'id': 'bar', 'label': 'label', 'outV': 'out1', 'inV': 'in2'}
    assert row[2] == {'id': 'in2', 'label': 'vertex'}

    #parse path with maps
    p = Path(labels=["vertex", "label", "vertex"], objects=[{'name': 'foo', 'age': 29}, {'dist': 32}, {'name': 'bar', 'age': 40}])
    out = neptune_client._gremlin_results_to_dict([p])
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,3)
    assert row[0]['name'] == 'foo'
    assert row[0]['age'] == 29
    assert row[1]['dist'] == 32
    assert row[2]['name'] == 'bar'
    assert row[2]['age'] == 40

    #parse path with mixed elements and maps
    p = Path(labels=["vertex", "label", "vertex"], objects=[{'name': 'foo', 'age': 29}, 
        Edge("bar", 'out1', 'label', 'in2'), {'name': 'bar', 'age': 40}])
    out = neptune_client._gremlin_results_to_dict([p])
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,3)
    assert row[0]['name'] == 'foo'
    assert row[0]['age'] == 29
    assert row[1] == {'id': 'bar', 'label': 'label', 'outV': 'out1', 'inV': 'in2'}
    assert row[2]['name'] == 'bar'
    assert row[2]['age'] == 40


#parse vertex valueMap
def test_parse_maps(neptune_client):
    # parse map
    m = {'name': 'foo', 'age': 29}
    input = [m]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,2)
    assert row['name'] == 'foo'
    assert row['age'] == 29

    # parse multiple maps with T 
    m1 = {'name': ['foo'], T.id: '2', 'age': [40],  T.label: 'vertex'}
    input = [m, m1]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[1]
    assert df.shape == (2,4)
    assert row['name'] == 'foo'
    assert row['age'] == 40
    assert row[T.id] == '2'
    assert row[T.label] == 'vertex'
    m2 = {'name': ['foo', 'bar'], T.id: '2', T.label: 'vertex'}
    input = [m, m1, m2]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[2]
    assert df.shape == (3,4)
    assert row['name'] == ['foo', 'bar']
    assert row[T.id] == '2'
    assert row[T.label] == 'vertex'

#parse scalar
def test_parse_scalar(neptune_client):
    # parse map
    m = 12
    n = "Foo"
    input = [m, n]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (2,1)
    assert row[0] == 12
    row = df.iloc[1]
    assert row[0] == "Foo"


#parse subgraph
def test_parse_subgraph(neptune_client):
    m = {'@type': 'tinker:graph', '@value': {'vertices': ['v[45]', 'v[9]'], 'edges': ['e[3990][9-route->45]']}}
    input = [m]
    out = neptune_client._gremlin_results_to_dict(input)
    df = pd.DataFrame.from_records(out)
    row = df.iloc[0]
    assert df.shape == (1,2)
    assert row['@type'] ==  'tinker:graph'
    assert row['@value'] ==  {'vertices': ['v[45]', 'v[9]'], 'edges': ['e[3990][9-route->45]']}
