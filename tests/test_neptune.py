import json
import logging
import tempfile
import time
import requests
from typing import Any, Dict
from urllib.error import HTTPError

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
    outputs['endpoint'] = 'air-routes-graph-1509728730.us-west-2.elb.amazonaws.com'
    outputs['read_endpoint'] = 'air-routes-graph-1509728730.us-west-2.elb.amazonaws.com'
    outputs['port'] = 80
    outputs['ssl'] = False
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


def test_connection_neptune(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(neptune_endpoint, neptune_port)
    resp = client.status()
    assert len(resp) > 0


def test_connection_neptune_http(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=False, iam_enabled=False)
    resp = client.status()
    assert len(resp) > 0


@pytest.mark.skip("Need infra")
def test_connection_neptune_https(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=True, iam_enabled=False)
    resp = client.status()
    assert len(resp.text) > 0


@pytest.mark.skip("Need infra")
def test_connection_neptune_http_iam(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=False, iam_enabled=True)
    resp = client.status()
    assert len(resp.text) > 0


@pytest.mark.skip("Need infra")
def test_connection_neptune_https_iam(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=True, iam_enabled=True)
    resp = client.status()
    assert resp.status_code == 200
    assert len(resp.text) > 0


def test_opencypher_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=False, iam_enabled=False)
    df = wr.neptune.read_opencypher(client, "MATCH (n) RETURN n LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert len(df.index) == 1

    assert isinstance(df, pd.DataFrame)
    df = wr.neptune.read_opencypher(client, "MATCH (n) RETURN n LIMIT 2")
    assert len(df.index) == 2

    df = wr.neptune.read_opencypher(client, "MATCH p=(n)-[r]->(d) RETURN p LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert len(df.index) == 1


def test_gremlin_query_vertices(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=False, iam_enabled=False)
    
    df = wr.neptune.read_gremlin(client, "g.V().limit(1)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)
    
    df = wr.neptune.read_gremlin(client, "g.V().limit(2)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)


def test_gremlin_query_edges(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=False, iam_enabled=False)
    
    df = wr.neptune.read_gremlin(client, "g.E().limit(1)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 4)
    
    df = wr.neptune.read_gremlin(client, "g.E().limit(2)")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 4)


def test_gremlin_query_no_results(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=False, iam_enabled=False)
    
    df = wr.neptune.read_gremlin(client, "g.V('foo').drop()")
    assert isinstance(df, pd.DataFrame)


def test_sparql_query(neptune_endpoint, neptune_port) -> Dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=False, iam_enabled=False)
    df = wr.neptune.read_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1,3)

    df = wr.neptune.read_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 2")
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2,3)