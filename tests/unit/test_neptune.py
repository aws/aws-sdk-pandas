from __future__ import annotations

import logging
import random
import string
import uuid
from typing import Any

import numpy as np
import pytest  # type: ignore
from gremlin_python.process.traversal import Direction, T

import awswrangler as wr
import awswrangler.pandas as pd
from awswrangler.neptune._client import BulkLoadParserConfiguration

from .._utils import assert_columns_in_pandas_data_frame, extract_cloudformation_outputs

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


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


@pytest.fixture(scope="session")
def neptune_load_iam_role_arn(cloudformation_outputs: dict[str, Any]) -> str:
    return cloudformation_outputs["NeptuneBulkLoadRole"]


def test_connection_neptune_https(neptune_endpoint, neptune_port, neptune_iam_enabled):
    client = wr.neptune.connect(host=neptune_endpoint, port=neptune_port, iam_enabled=neptune_iam_enabled)
    resp = client.status()
    assert resp["status"] == "healthy"


def test_connection_neptune_https_iam(neptune_endpoint, neptune_port):
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=True)
    resp = client.status()
    assert resp["status"] == "healthy"


def test_opencypher_query(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    wr.neptune.execute_opencypher(client, "create (a:Foo { name: 'foo' })-[:TEST]->(b {name : 'bar'})")
    df = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN n LIMIT 1")
    assert df.shape == (1, 1)

    df = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN n LIMIT 2")
    assert df.shape == (2, 1)

    df = wr.neptune.execute_opencypher(client, "MATCH p=(n)-[r]->(d) RETURN p LIMIT 1")
    assert df.shape == (1, 1)

    df = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN id(n), labels(n) LIMIT 1")
    assert df.shape == (1, 2)
    row = df.iloc[0]
    assert row["id(n)"]
    assert row["labels(n)"]


def test_flatten_df(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    wr.neptune.execute_opencypher(client, "create (a:Foo1 { name: 'foo' })-[:TEST]->(b {name : 'bar'})")
    df = wr.neptune.execute_opencypher(client, "MATCH (n:Foo1) RETURN n LIMIT 1")
    df_test = wr.neptune.flatten_nested_df(df)
    assert df_test.shape == (1, 6)
    row = df_test.iloc[0]
    assert row["n_~properties_name"]

    df_test = wr.neptune.flatten_nested_df(df, include_prefix=False)
    assert df_test.shape == (1, 6)
    row = df_test.iloc[0]
    assert row["_~properties_name"]

    df_test = wr.neptune.flatten_nested_df(df, separator="|")
    assert df_test.shape == (1, 6)
    row = df_test.iloc[0]
    assert row["n|~properties|name"]

    df_new = pd.DataFrame([{"~id": "0", "~labels": ["version"], "~properties": {"type": "version"}}])
    df_test = wr.neptune.flatten_nested_df(df_new)
    assert df_test.shape == (1, 4)
    row = df_test.iloc[0]
    assert row["~properties_type"]


def test_opencypher_malformed_query(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_opencypher(client, "MATCH (n) LIMIT 2")
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_opencypher(client, "")


def test_gremlin_malformed_query(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_gremlin(client, "g.V().limit(1")
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_gremlin(client, "")


def test_sparql_malformed_query(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_sparql(client, "SELECT ?s ?p ?o {?s ?pLIMIT 1")
    with pytest.raises(wr.exceptions.QueryFailed):
        wr.neptune.execute_sparql(client, "")


def test_gremlin_query_vertices(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    wr.neptune.execute_gremlin(client, f"g.addV().property(T.id, '{uuid.uuid4()}')")
    df = wr.neptune.execute_gremlin(client, "g.V().limit(1)")
    assert df.shape == (1, 3)

    df = wr.neptune.execute_gremlin(client, "g.V().limit(2)")
    assert df.shape == (2, 3)


def test_gremlin_query_edges(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    wr.neptune.execute_gremlin(client, "g.addE('bar').from(addV('foo')).to(addV('foo'))")
    df = wr.neptune.execute_gremlin(client, "g.E().limit(1)")
    assert df.shape == (1, 5)

    df = wr.neptune.execute_gremlin(client, "g.E().limit(2)")
    assert df.shape == (2, 5)


def test_gremlin_query_no_results(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    df = wr.neptune.execute_gremlin(client, "g.V('foo').drop()")
    assert df.empty


def test_sparql_query(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    df = wr.neptune.execute_sparql(client, "INSERT DATA { <test> <test> <test>}")
    df = wr.neptune.execute_sparql(client, "INSERT DATA { <test1> <test1> <test1>}")
    df = wr.neptune.execute_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 1")
    assert df.shape == (1, 3)
    assert_columns_in_pandas_data_frame(df, ["s", "p", "o"])

    df = wr.neptune.execute_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 2")
    assert df.shape == (2, 3)
    assert_columns_in_pandas_data_frame(df, ["s", "p", "o"])

    df = wr.neptune.execute_sparql(client, "SELECT ?s ?p ?o {?s ?p ?o} LIMIT 0")
    assert df.shape == (0, 3)
    assert_columns_in_pandas_data_frame(df, ["s", "p", "o"])


def test_write_vertex_property_nan(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    id = uuid.uuid4()
    wr.neptune.execute_gremlin(client, f"g.addV().property(T.id, '{str(id)}')")

    data = [_create_dummy_edge(), _create_dummy_edge()]
    del data[1]["str"]
    data[1]["int"] = np.nan
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res


def test_gremlin_write_different_cols(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    id = uuid.uuid4()
    wr.neptune.execute_gremlin(client, f"g.addV().property(T.id, '{str(id)}')")

    data = [_create_dummy_vertex(), _create_dummy_vertex()]
    del data[1]["str"]
    data[1]["int"] = np.nan
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    data = [_create_dummy_edge(), _create_dummy_edge()]
    del data[1]["str"]
    data[1]["int"] = np.nan
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    data = [{"~id": id, "age(single)": 50, "name": "foo"}, {"~id": id, "age(single)": 55}, {"~id": id, "name": "foo"}]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    res = wr.neptune.execute_gremlin(client, f"g.V('{id}').valueMap().with(WithOptions.tokens)")
    saved_row = res.iloc[0]
    assert saved_row["age"] == 55


@pytest.mark.parametrize("use_threads", [False, True])
@pytest.mark.parametrize("keep_files", [False, True])
def test_gremlin_bulk_load(
    neptune_endpoint: str,
    neptune_port: int,
    neptune_load_iam_role_arn: str,
    path: str,
    use_threads: bool,
    keep_files: bool,
) -> None:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    label = f"foo_{uuid.uuid4()}"
    data = [_create_dummy_vertex(label) for _ in range(10)]
    input_df = pd.DataFrame(data)

    wr.neptune.bulk_load(
        client=client,
        df=input_df,
        path=path,
        iam_role=neptune_load_iam_role_arn,
        use_threads=use_threads,
        keep_files=keep_files,
        queue_request="TRUE",
    )
    res_df = wr.neptune.execute_gremlin(client, f"g.V().hasLabel('{label}').valueMap().with(WithOptions.tokens)")

    assert res_df.shape == input_df.shape

    if keep_files:
        assert len(wr.s3.list_objects(path)) > 0
    else:
        assert len(wr.s3.list_objects(path)) == 0


def test_gremlin_bulk_load_error_when_files_present(
    neptune_endpoint: str,
    neptune_port: int,
    neptune_load_iam_role_arn: str,
    path: str,
) -> None:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    df = pd.DataFrame([_create_dummy_vertex() for _ in range(10)])
    wr.s3.to_csv(df, path, dataset=True, index=False)

    # The received path is not empty
    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.neptune.bulk_load(
            client=client,
            df=df,
            path=path,
            iam_role=neptune_load_iam_role_arn,
        )


DEFAULT_PARSER_CONFIGURATION = BulkLoadParserConfiguration(
    namedGraphUri="http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph",
    baseUri="http://aws.amazon.com/neptune/default",
    allowEmptyStrings=False,
)


@pytest.mark.parametrize("parser_config", [None, DEFAULT_PARSER_CONFIGURATION])
def test_gremlin_bulk_load_from_files(
    neptune_endpoint: str,
    neptune_port: int,
    neptune_load_iam_role_arn: str,
    path: str,
    parser_config: BulkLoadParserConfiguration | None,
) -> None:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    label = f"foo_{uuid.uuid4()}"
    data = [_create_dummy_vertex(label) for _ in range(10)]
    input_df = pd.DataFrame(data)

    wr.s3.to_csv(input_df, path, dataset=True, index=False)

    wr.neptune.bulk_load_from_files(
        client=client,
        path=path,
        iam_role=neptune_load_iam_role_arn,
        queue_request="TRUE",
        parser_configuration=parser_config,
    )
    res_df = wr.neptune.execute_gremlin(client, f"g.V().hasLabel('{label}').valueMap().with(WithOptions.tokens)")

    assert res_df.shape == input_df.shape


def test_gremlin_write_updates(neptune_endpoint, neptune_port) -> dict[str, Any]:
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
    assert "foo" in saved_row["name"]
    assert "bar" in saved_row["name"]
    res = wr.neptune.to_property_graph(client, df, use_header_cardinality=False)
    res = wr.neptune.execute_gremlin(client, f"g.V('{id}').valueMap().with(WithOptions.tokens)")
    saved_row = res.iloc[0]
    assert saved_row["age(single)"] == 55
    assert "foo" in saved_row["name"]
    assert "bar" in saved_row["name"]


def test_gremlin_write_vertices(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    wr.neptune.execute_gremlin(client, "g.addV('foo')")

    initial_cnt_df = wr.neptune.execute_gremlin(client, "g.V().hasLabel('foo').count()")
    data = [_create_dummy_vertex() for _ in range(3)]
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
    data = [_create_dummy_vertex() for _ in range(50)]
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


def test_gremlin_write_edges(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    initial_cnt_df = wr.neptune.execute_gremlin(client, "g.E().hasLabel('bar').count()")

    data = [_create_dummy_edge() for _ in range(3)]
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
    data = [_create_dummy_edge() for _ in range(50)]
    df = pd.DataFrame(data)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    batch_cnt_df = wr.neptune.execute_gremlin(client, "g.E().hasLabel('bar').count()")
    assert batch_cnt_df.iloc[0][0] == final_cnt_df.iloc[0][0] + 50


def test_sparql_write_different_cols(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    data = [_create_dummy_triple(), _create_dummy_triple()]
    del data[1]["o"]
    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    data = [_create_dummy_quad(), _create_dummy_quad()]
    del data[1]["o"]
    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res


def test_sparql_write_triples(neptune_endpoint, neptune_port) -> dict[str, Any]:
    label = f"foo_{uuid.uuid4()}"
    sparkql_query = f"SELECT ?p ?o WHERE {{ <{label}> ?p ?o .}}"

    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    initial_df = wr.neptune.execute_sparql(client, sparkql_query)

    data = [_create_dummy_triple(s=label) for _ in range(3)]
    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    final_df = wr.neptune.execute_sparql(client, sparkql_query)
    assert len(final_df.index) == len(initial_df.index) + 3

    # check to make sure batch addition of edges works
    data = [_create_dummy_triple(s=label) for _ in range(50)]
    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    batch_df = wr.neptune.execute_sparql(client, sparkql_query)
    assert len(batch_df.index) == len(final_df.index) + 50


def test_sparql_write_quads(neptune_endpoint, neptune_port) -> dict[str, Any]:
    label = f"foo_{uuid.uuid4()}"
    sparkql_query = f"SELECT ?p ?o FROM <bar> WHERE {{ <{label}> ?p ?o .}}"

    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    initial_df = wr.neptune.execute_sparql(client, sparkql_query)

    data = [_create_dummy_quad(s=label) for _ in range(3)]
    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    final_df = wr.neptune.execute_sparql(client, sparkql_query)
    assert len(final_df.index) == len(initial_df.index) + 3

    # check to make sure batch addition of edges works
    data = [_create_dummy_quad(s=label) for _ in range(50)]
    df = pd.DataFrame(data)
    res = wr.neptune.to_rdf_graph(client, df)
    assert res

    batch_df = wr.neptune.execute_sparql(client, sparkql_query)
    assert len(batch_df.index) == len(final_df.index) + 50


def test_gremlin_write_empty(neptune_endpoint, neptune_port) -> dict[str, Any]:
    client = wr.neptune.connect(neptune_endpoint, neptune_port)

    with pytest.raises(wr.exceptions.EmptyDataFrame):
        wr.neptune.to_property_graph(client, pd.DataFrame(columns=["~id", "~label"]))


def _create_dummy_vertex(label: str = "foo") -> dict[str, Any]:
    return {
        "~id": str(uuid.uuid4()),
        "~label": label,
        "int": random.randint(0, 1000),
        "str": "".join(random.choice(string.ascii_lowercase) for i in range(10)),
        "list": [random.randint(0, 1000), random.randint(0, 1000)],
    }


def _create_dummy_edge() -> dict[str, Any]:
    return {
        "~id": str(uuid.uuid4()),
        "~label": "bar",
        "~to": str(uuid.uuid4()),
        "~from": str(uuid.uuid4()),
        "int": random.randint(0, 1000),
        "str": "".join(random.choice(string.ascii_lowercase) for i in range(10)),
    }


def _create_dummy_triple(s: str = "foo") -> dict[str, Any]:
    return {
        "s": s,
        "p": str(uuid.uuid4()),
        "o": random.randint(0, 1000),
    }


def _create_dummy_quad(s: str = "foo") -> dict[str, Any]:
    return {
        **_create_dummy_triple(s=s),
        "g": "bar",
    }


def test_gremlin_write_datetime_values(neptune_endpoint, neptune_port) -> dict[str, Any]:
    """Test that datetime values are properly converted when writing to Neptune."""
    from datetime import datetime

    client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)

    # Create a vertex with datetime properties
    test_id = str(uuid.uuid4())
    now = datetime.now()

    # Test with default ISO format
    data = [{"~id": test_id, "~label": "test_datetime", "created_at": now, "name": "test_vertex"}]
    df = pd.DataFrame(data)

    # Verify the DataFrame has datetime type
    assert pd.api.types.is_datetime64_dtype(df["created_at"])

    # Write to Neptune with default datetime handling (ISO format)
    res = wr.neptune.to_property_graph(client, df)
    assert res

    # Retrieve the vertex and check the property
    result_df = wr.neptune.execute_gremlin(client, f"g.V('{test_id}').valueMap()")
    assert not result_df.empty
    assert "created_at" in result_df.iloc[0]

    # Test with timestamp format
    test_id2 = str(uuid.uuid4())
    data = [
        {
            "~id": test_id2,
            "~label": "test_datetime",
            "created_at": now,
            "updated_at": now,
            "name": "test_vertex_timestamp",
        }
    ]
    df = pd.DataFrame(data)

    # Write to Neptune with timestamp format
    res = wr.neptune.to_property_graph(client, df, datetime_format="timestamp")
    assert res

    # Retrieve the vertex and check the property
    result_df = wr.neptune.execute_gremlin(client, f"g.V('{test_id2}').valueMap()")
    assert not result_df.empty
    assert "created_at" in result_df.iloc[0]

    # Test with string format
    test_id3 = str(uuid.uuid4())
    data = [{"~id": test_id3, "~label": "test_datetime", "created_at": now, "name": "test_vertex_string"}]
    df = pd.DataFrame(data)

    # Write to Neptune with string format
    res = wr.neptune.to_property_graph(client, df, datetime_format="string")
    assert res

    # Retrieve the vertex and check the property
    result_df = wr.neptune.execute_gremlin(client, f"g.V('{test_id3}').valueMap()")
    assert not result_df.empty
    assert "created_at" in result_df.iloc[0]
