import json
import logging
import tempfile
import time

import boto3
import pandas as pd
import pytest  # type: ignore

import awswrangler as wr

from ._utils import extract_cloudformation_outputs

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


inspections_documents = [
    {
        "business_address": "315 California St",
        "business_city": "San Francisco",
        "business_id": "24936",
        "business_latitude": "37.793199",
        "business_location": {"lon": -122.400152, "lat": 37.793199},
        "business_longitude": "-122.400152",
        "business_name": "San Francisco Soup Company",
        "business_postal_code": "94104",
        "business_state": "CA",
        "inspection_date": "2016-06-09T00:00:00.000",
        "inspection_id": "24936_20160609",
        "inspection_score": 77,
        "inspection_type": "Routine - Unscheduled",
        "risk_category": "Low Risk",
        "violation_description": "Improper food labeling or menu misrepresentation",
        "violation_id": "24936_20160609_103141",
    },
    {
        "business_address": "10 Mason St",
        "business_city": "San Francisco",
        "business_id": "60354",
        "business_latitude": "37.783527",
        "business_location": {"lon": -122.409061, "lat": 37.783527},
        "business_longitude": "-122.409061",
        "business_name": "Soup Unlimited",
        "business_postal_code": "94102",
        "business_state": "CA",
        "inspection_date": "2016-11-23T00:00:00.000",
        "inspection_id": "60354_20161123",
        "inspection_type": "Routine",
        "inspection_score": 95,
    },
    {
        "business_address": "2872 24th St",
        "business_city": "San Francisco",
        "business_id": "1797",
        "business_latitude": "37.752807",
        "business_location": {"lon": -122.409752, "lat": 37.752807},
        "business_longitude": "-122.409752",
        "business_name": "TIO CHILOS GRILL",
        "business_postal_code": "94110",
        "business_state": "CA",
        "inspection_date": "2016-07-05T00:00:00.000",
        "inspection_id": "1797_20160705",
        "inspection_score": 90,
        "inspection_type": "Routine - Unscheduled",
        "risk_category": "Low Risk",
        "violation_description": "Unclean nonfood contact surfaces",
        "violation_id": "1797_20160705_103142",
    },
    {
        "business_address": "1661 Tennessee St Suite 3B",
        "business_city": "San Francisco Whard Restaurant",
        "business_id": "66198",
        "business_latitude": "37.75072",
        "business_location": {"lon": -122.388478, "lat": 37.75072},
        "business_longitude": "-122.388478",
        "business_name": "San Francisco Restaurant",
        "business_postal_code": "94107",
        "business_state": "CA",
        "inspection_date": "2016-05-27T00:00:00.000",
        "inspection_id": "66198_20160527",
        "inspection_type": "Routine",
        "inspection_score": 56,
    },
    {
        "business_address": "2162 24th Ave",
        "business_city": "San Francisco",
        "business_id": "5794",
        "business_latitude": "37.747228",
        "business_location": {"lon": -122.481299, "lat": 37.747228},
        "business_longitude": "-122.481299",
        "business_name": "Soup House",
        "business_phone_number": "+14155752700",
        "business_postal_code": "94116",
        "business_state": "CA",
        "inspection_date": "2016-09-07T00:00:00.000",
        "inspection_id": "5794_20160907",
        "inspection_score": 96,
        "inspection_type": "Routine - Unscheduled",
        "risk_category": "Low Risk",
        "violation_description": "Unapproved or unmaintained equipment or utensils",
        "violation_id": "5794_20160907_103144",
    },
    {
        "business_address": "2162 24th Ave",
        "business_city": "San Francisco",
        "business_id": "5794",
        "business_latitude": "37.747228",
        "business_location": {"lon": -122.481299, "lat": 37.747228},
        "business_longitude": "-122.481299",
        "business_name": "Soup-or-Salad",
        "business_phone_number": "+14155752700",
        "business_postal_code": "94116",
        "business_state": "CA",
        "inspection_date": "2016-09-07T00:00:00.000",
        "inspection_id": "5794_20160907",
        "inspection_score": 96,
        "inspection_type": "Routine - Unscheduled",
        "risk_category": "Low Risk",
        "violation_description": "Unapproved or unmaintained equipment or utensils",
        "violation_id": "5794_20160907_103144",
    },
]


@pytest.fixture(scope="session")
def cloudformation_outputs():
    return extract_cloudformation_outputs()


@pytest.fixture(scope="session")
def opensearch_password():
    return boto3.client("secretsmanager").get_secret_value(SecretId="aws-sdk-pandas/opensearch_password")[
        "SecretString"
    ]


@pytest.fixture(scope="session")
def domain_endpoint_opensearch_1_0(cloudformation_outputs):
    return cloudformation_outputs["DomainEndpointsdkpandasos1"]


@pytest.fixture(scope="session")
def domain_endpoint_elasticsearch_7_10_fgac(cloudformation_outputs):
    return cloudformation_outputs["DomainEndpointsdkpandases710fgac"]


def test_connection_opensearch_1_0(domain_endpoint_opensearch_1_0):
    client = wr.opensearch.connect(host=domain_endpoint_opensearch_1_0)
    print(client.info())
    assert len(client.info()) > 0


def test_connection_opensearch_1_0_https(domain_endpoint_opensearch_1_0):
    client = wr.opensearch.connect(host=f"https://{domain_endpoint_opensearch_1_0}")
    print(client.info())
    assert len(client.info()) > 0


def test_connection_elasticsearch_7_10_fgac(domain_endpoint_elasticsearch_7_10_fgac, opensearch_password):
    client = wr.opensearch.connect(
        host=domain_endpoint_elasticsearch_7_10_fgac, username="test", password=opensearch_password
    )
    print(client.info())
    assert len(client.info()) > 0


@pytest.fixture(scope="session")
def opensearch_1_0_client(domain_endpoint_opensearch_1_0):
    client = wr.opensearch.connect(host=domain_endpoint_opensearch_1_0)
    return client


@pytest.fixture(scope="session")
def elasticsearch_7_10_fgac_client(domain_endpoint_elasticsearch_7_10_fgac, opensearch_password):
    client = wr.opensearch.connect(
        host=domain_endpoint_elasticsearch_7_10_fgac, username="test", password=opensearch_password
    )
    return client


# testing multiple versions
@pytest.fixture(params=["opensearch_1_0_client", "elasticsearch_7_10_fgac_client"])
def client(request):
    return request.getfixturevalue(request.param)


def test_create_index(client):
    index = "test_create_index"
    wr.opensearch.delete_index(client, index)
    time.sleep(0.5)  # let the cluster clean up
    response = wr.opensearch.create_index(
        client=client,
        index=index,
        mappings={"properties": {"name": {"type": "text"}, "age": {"type": "integer"}}},
        settings={"index": {"number_of_shards": 1, "number_of_replicas": 1}},
    )
    assert response.get("acknowledged", False) is True


def test_delete_index(client):
    index = "test_delete_index"
    wr.opensearch.create_index(client, index=index)
    response = wr.opensearch.delete_index(client, index=index)
    assert response.get("acknowledged", False) is True


def test_index_df(client):
    response = wr.opensearch.index_df(
        client,
        df=pd.DataFrame([{"_id": "1", "name": "John"}, {"_id": "2", "name": "George"}, {"_id": "3", "name": "Julia"}]),
        index="test_index_df1",
    )
    assert response.get("success", 0) == 3


def test_index_df_with_array(client):
    response = wr.opensearch.index_df(
        client,
        df=pd.DataFrame(
            [{"_id": "1", "name": "John", "tags": ["foo", "bar"]}, {"_id": "2", "name": "George", "tags": ["foo"]}]
        ),
        index="test_index_df1",
    )
    assert response.get("success", 0) == 2


def test_index_documents(client):
    response = wr.opensearch.index_documents(
        client,
        documents=[{"_id": "1", "name": "John"}, {"_id": "2", "name": "George"}, {"_id": "3", "name": "Julia"}],
        index="test_index_documents1",
    )
    assert response.get("success", 0) == 3


def test_index_documents_id_keys(client):
    wr.opensearch.index_documents(
        client, documents=inspections_documents, index="test_index_documents_id_keys", id_keys=["inspection_id"]
    )


def test_index_documents_no_id_keys(client):
    wr.opensearch.index_documents(client, documents=inspections_documents, index="test_index_documents_no_id_keys")


def test_search(client):
    index = "test_search"
    wr.opensearch.index_documents(
        client, documents=inspections_documents, index=index, id_keys=["inspection_id"], refresh="wait_for"
    )
    df = wr.opensearch.search(
        client,
        index=index,
        search_body={"query": {"match": {"business_name": "soup"}}},
        _source=["inspection_id", "business_name", "business_location"],
    )
    assert df.shape[0] == 3
    df = wr.opensearch.search(
        client,
        index=index,
        search_body={"query": {"match": {"business_name": "message"}}},
    )
    assert df.shape == (0, 0)


def test_search_filter_path(client):
    index = "test_search"
    wr.opensearch.index_documents(
        client, documents=inspections_documents, index=index, id_keys=["inspection_id"], refresh="wait_for"
    )
    df = wr.opensearch.search(
        client,
        index=index,
        search_body={"query": {"match": {"business_name": "soup"}}},
        _source=["inspection_id", "business_name", "business_location"],
        filter_path=["hits.hits._source"],
    )
    assert df.shape[0] == 3


def test_search_scroll(client):
    index = "test_search_scroll"
    wr.opensearch.index_documents(
        client, documents=inspections_documents, index=index, id_keys=["inspection_id"], refresh="wait_for"
    )
    df = wr.opensearch.search(
        client, index=index, is_scroll=True, _source=["inspection_id", "business_name", "business_location"]
    )
    assert df.shape[0] == 5


def test_search_sql(client):
    index = "test_search_sql"
    wr.opensearch.index_documents(
        client, documents=inspections_documents, index=index, id_keys=["inspection_id"], refresh="wait_for"
    )
    df = wr.opensearch.search_by_sql(client, sql_query=f"select * from {index}")
    assert df.shape[0] == 5


def test_index_json_local(client):
    file_path = f"{tempfile.gettempdir()}/inspections.json"
    with open(file_path, "w") as filehandle:
        for doc in inspections_documents:
            filehandle.write("%s\n" % json.dumps(doc))
    response = wr.opensearch.index_json(client, index="test_index_json_local", path=file_path)
    assert response.get("success", 0) == 6


def test_index_json_s3(client, path):
    file_path = f"{tempfile.gettempdir()}/inspections.json"
    with open(file_path, "w") as filehandle:
        for doc in inspections_documents:
            filehandle.write("%s\n" % json.dumps(doc))
    s3 = boto3.client("s3")
    path = f"{path}opensearch/inspections.json"
    bucket, key = wr._utils.parse_path(path)
    s3.upload_file(file_path, bucket, key)
    response = wr.opensearch.index_json(client, index="test_index_json_s3", path=path)
    assert response.get("success", 0) == 6


def test_index_csv_local(client):
    file_path = f"{tempfile.gettempdir()}/inspections.csv"
    index = "test_index_csv_local"
    df = pd.DataFrame(inspections_documents)
    df.to_csv(file_path, index=False)
    response = wr.opensearch.index_csv(client, path=file_path, index=index)
    assert response.get("success", 0) == 6


def test_index_csv_s3(client, path):
    file_path = f"{tempfile.gettempdir()}/inspections.csv"
    index = "test_index_csv_s3"
    df = pd.DataFrame(inspections_documents)
    df.to_csv(file_path, index=False)
    s3 = boto3.client("s3")
    path = f"{path}opensearch/inspections.csv"
    bucket, key = wr._utils.parse_path(path)
    s3.upload_file(file_path, bucket, key)
    response = wr.opensearch.index_csv(client, path=path, index=index)
    assert response.get("success", 0) == 6


@pytest.mark.skip(reason="takes a long time (~5 mins) since testing against small clusters")
def test_index_json_s3_large_file(client):
    path = "s3://irs-form-990/index_2011.json"
    response = wr.opensearch.index_json(
        client, index="test_index_json_s3_large_file", path=path, json_path="Filings2011", id_keys=["EIN"], bulk_size=20
    )
    assert response.get("success", 0) > 0
