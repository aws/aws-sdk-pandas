import logging
from typing import Any, Dict

import boto3
import pandas as pd
import pytest  # type: ignore

import awswrangler as wr

from ._utils import extract_cloudformation_outputs

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

@pytest.fixture(scope="session")
def cloudformation_outputs():
    outputs = {}
    outputs['cluster_resource_id']='XXX'
    outputs['endpoint'] = 'air-routes-oc.cluster-cei5pmtr7fqq.us-west-2.neptune.amazonaws.com'
    outputs['read_endpoint'] = 'air-routes-oc.cluster-cei5pmtr7fqq.us-west-2.neptune.amazonaws.com'
    outputs['port'] = 8182
    outputs['iam_enabled'] = False
    return outputs


@pytest.fixture(scope="session")
def neptune_endpoint(cloudformation_outputs) -> str:
    return cloudformation_outputs["endpoint"]


@pytest.fixture(scope="session")
def neptune_read_endpoint(cloudformation_outputs) -> str:
    return cloudformation_outputs["read_endpoint"]


@pytest.fixture(scope="session")
def neptune_port(cloudformation_outputs) -> int:
    return cloudformation_outputs["port"]


def test_connection_neptune_https(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(host=neptune_endpoint, port=neptune_port, iam_enabled=False)
    resp = client.status()
    assert resp['status'] == 'healthy'


def test_connection_neptune_https_iam(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=True)
    resp = client.status()
    assert resp['status'] == 'healthy'


def test_opencypher_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    df = wr.neptune.read_opencypher(client, "MATCH (n) RETURN n LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 1)

    assert isinstance(df, pd.DataFrame)
    df = wr.neptune.read_opencypher(client, "MATCH (n) RETURN n LIMIT 2")
    assert df.shape == (2, 1)

    df = wr.neptune.read_opencypher(client, "MATCH p=(n)-[r]->(d) RETURN p LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 1)

    df = wr.neptune.read_opencypher(client, "MATCH (n) RETURN id(n), labels(n) LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)
    row = df.iloc[0]
    assert row['id(n)']
    assert row['labels(n)']


def test_gremlin_query_vertices(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    
    df = wr.neptune.read_gremlin(client, "g.V().limit(1)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)
    
    df = wr.neptune.read_gremlin(client, "g.V().limit(2)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
        
    df = wr.neptune.read_gremlin(client, "g.V().limit(1)", nest_event_loop=True)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)
    
    df = wr.neptune.read_gremlin(client, "g.V().limit(2)", nest_event_loop=True)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)


def test_gremlin_query_edges(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    
    df = wr.neptune.read_gremlin(client, "g.E().limit(1)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 4)
    
    df = wr.neptune.read_gremlin(client, "g.E().limit(2)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 4)

    df = wr.neptune.read_gremlin(client, "g.E().limit(1)", nest_event_loop=True)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 4)
    
    df = wr.neptune.read_gremlin(client, "g.E().limit(2)", nest_event_loop=True)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 4)


def test_gremlin_query_no_results(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    
    df = wr.neptune.read_gremlin(client, "g.V('foo').drop()")
    assert isinstance(df, pd.DataFrame)
        
    df = wr.neptune.read_gremlin(client, "g.V('foo').drop()", nest_event_loop=True)
    assert isinstance(df, pd.DataFrame)


def test_sparql_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    df = wr.neptune.read_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1,3)

    df = wr.neptune.read_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 2")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2,3)