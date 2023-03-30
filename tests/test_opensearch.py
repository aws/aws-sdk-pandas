import json
import logging
import tempfile
import time
import uuid
from typing import Any, Dict, List

import boto3
import botocore
import opensearchpy
import pandas as pd
import pytest  # type: ignore

import awswrangler as wr
from awswrangler.opensearch._utils import _is_serverless

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


def _get_unique_suffix() -> str:
    return str(uuid.uuid4())[:8]


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


def _get_opensearch_data_access_policy() -> List[Dict[str, Any]]:
    return [
        {
            "Rules": [
                {
                    "ResourceType": "index",
                    "Resource": [
                        "index/*/*",
                    ],
                    "Permission": [
                        "aoss:*",
                    ],
                },
                {
                    "ResourceType": "collection",
                    "Resource": [
                        "collection/*",
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


@pytest.fixture(scope="session")
def opensearch_serverless_collection_endpoint(cloudformation_outputs) -> str:
    collection_name: str = cloudformation_outputs["CollectionNamesdkpandasaoss"]
    client: boto3.client = boto3.client(service_name="opensearchserverless")

    try:
        client.create_access_policy(
            name=f"{collection_name}-access",
            type="data",
            policy=json.dumps(_get_opensearch_data_access_policy()),
        )
    except botocore.exceptions.ClientError as error:
        if not error.response["Error"]["Code"] == "ConflictException":
            raise error

    return cloudformation_outputs["CollectionEndpointsdkpandasaoss"]


def test_connection_opensearch_1_0(domain_endpoint_opensearch_1_0):
    client = wr.opensearch.connect(host=domain_endpoint_opensearch_1_0)
    assert len(client.info()) > 0


def test_connection_opensearch_1_0_https(domain_endpoint_opensearch_1_0):
    client = wr.opensearch.connect(host=f"https://{domain_endpoint_opensearch_1_0}")
    assert len(client.info()) > 0


def test_connection_opensearch_serverless(opensearch_serverless_collection_endpoint):
    client = wr.opensearch.connect(host=opensearch_serverless_collection_endpoint)
    # Info endpoint is not available in opensearch serverless
    with pytest.raises(opensearchpy.exceptions.NotFoundError):
        client.info()


def test_connection_elasticsearch_7_10_fgac(domain_endpoint_elasticsearch_7_10_fgac, opensearch_password):
    client = wr.opensearch.connect(
        host=domain_endpoint_elasticsearch_7_10_fgac, username="test", password=opensearch_password
    )
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


@pytest.fixture(scope="session")
def opensearch_serverless_client(opensearch_serverless_collection_endpoint):
    client = wr.opensearch.connect(host=opensearch_serverless_collection_endpoint)
    return client


# testing multiple versions
@pytest.fixture(params=["opensearch_1_0_client", "elasticsearch_7_10_fgac_client", "opensearch_serverless_client"])
def client(request, opensearch_1_0_client, elasticsearch_7_10_fgac_client, opensearch_serverless_client):
    time.sleep(30)  # temp wait until collection is created
    return request.getfixturevalue(request.param)


def test_create_index(client):
    index = f"test_create_index_{_get_unique_suffix()}"
    try:
        response = wr.opensearch.create_index(
            client=client,
            index=index,
            mappings={"properties": {"name": {"type": "text"}, "age": {"type": "integer"}}},
            settings={"index": {"number_of_shards": 1, "number_of_replicas": 1}},
        )
        assert response.get("acknowledged", False) is True
    finally:
        wr.opensearch.delete_index(client, index)


def test_delete_index(client):
    index = f"test_create_index_{_get_unique_suffix()}"
    wr.opensearch.create_index(client, index=index)
    response = wr.opensearch.delete_index(client, index=index)
    assert response.get("acknowledged", False) is True


def test_index_df(client):
    index = f"test_index_df_{_get_unique_suffix()}"
    try:
        response = wr.opensearch.index_df(
            client,
            df=pd.DataFrame(
                [{"_id": "1", "name": "John"}, {"_id": "2", "name": "George"}, {"_id": "3", "name": "Julia"}]
            ),
            index=index,
        )
        assert response.get("success", 0) == 3
    finally:
        wr.opensearch.delete_index(client, index)


def test_index_df_with_array(client):
    index = f"test_index_df_array_{_get_unique_suffix()}"
    try:
        response = wr.opensearch.index_df(
            client,
            df=pd.DataFrame(
                [{"_id": "1", "name": "John", "tags": ["foo", "bar"]}, {"_id": "2", "name": "George", "tags": ["foo"]}]
            ),
            index=index,
        )
        assert response.get("success", 0) == 2
    finally:
        wr.opensearch.delete_index(client, index)


def test_index_documents(client):
    index = f"test_index_documents_{_get_unique_suffix()}"
    try:
        response = wr.opensearch.index_documents(
            client,
            documents=[{"_id": "1", "name": "John"}, {"_id": "2", "name": "George"}, {"_id": "3", "name": "Julia"}],
            index=index,
        )
        assert response.get("success", 0) == 3
    finally:
        wr.opensearch.delete_index(client, index)


def test_index_documents_id_keys(client):
    index = f"test_index_documents_id_keys_{_get_unique_suffix()}"
    try:
        wr.opensearch.index_documents(client, documents=inspections_documents, index=index, id_keys=["inspection_id"])
    finally:
        wr.opensearch.delete_index(client, index)


def test_index_documents_no_id_keys(client):
    index = f"test_index_documents_no_id_keys_{_get_unique_suffix()}"
    try:
        wr.opensearch.index_documents(client, documents=inspections_documents, index=index)
    finally:
        wr.opensearch.delete_index(client, index)


def test_search(client):
    index = f"test_search_{_get_unique_suffix()}"
    kwargs = {} if _is_serverless(client) else {"refresh": "wait_for"}
    try:
        wr.opensearch.index_documents(
            client, documents=inspections_documents, index=index, id_keys=["inspection_id"], **kwargs
        )
        if _is_serverless(client):
            # The refresh interval for serverless OpenSearch is between 10 and 30 seconds
            # depending on the size of the request.
            time.sleep(30)
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
    finally:
        wr.opensearch.delete_index(client, index)


@pytest.mark.parametrize("filter_path", [None, "hits.hits._source", ["hits.hits._source"]])
def test_search_filter_path(client, filter_path):
    index = f"test_search_filter_{_get_unique_suffix()}"
    kwargs = {} if _is_serverless(client) else {"refresh": "wait_for"}
    try:
        wr.opensearch.index_documents(
            client, documents=inspections_documents, index=index, id_keys=["inspection_id"], **kwargs
        )
        if _is_serverless(client):
            # The refresh interval for serverless OpenSearch is between 10 and 30 seconds
            # depending on the size of the request.
            time.sleep(30)
        df = wr.opensearch.search(
            client,
            index=index,
            search_body={"query": {"match": {"business_name": "soup"}}},
            _source=["inspection_id", "business_name", "business_location"],
            filter_path=filter_path,
        )
        assert df.shape[0] == 3
    finally:
        wr.opensearch.delete_index(client, index)


def test_search_scroll(client):
    if _is_serverless(client):
        pytest.skip(reason="Scroll not available for OpenSearch Serverless.")

    index = f"test_search_scroll_{_get_unique_suffix()}"
    try:
        wr.opensearch.index_documents(
            client,
            documents=inspections_documents,
            index=index,
            id_keys=["inspection_id"],
            refresh="wait_for",
        )
        df = wr.opensearch.search(
            client, index=index, is_scroll=True, _source=["inspection_id", "business_name", "business_location"]
        )
        assert df.shape[0] == 5
    finally:
        wr.opensearch.delete_index(client, index)


@pytest.mark.parametrize("fetch_size", [None, 1000, 10000])
@pytest.mark.parametrize("fetch_size_param_name", ["size", "fetch_size"])
def test_search_sql(client, fetch_size, fetch_size_param_name):
    if _is_serverless(client):
        pytest.skip(reason="SQL plugin not available for OpenSearch Serverless.")

    index = f"test_search_sql_{_get_unique_suffix()}"
    try:
        wr.opensearch.index_documents(
            client,
            documents=inspections_documents,
            index=index,
            id_keys=["inspection_id"],
            refresh="wait_for",
        )
        search_kwargs = {fetch_size_param_name: fetch_size} if fetch_size else {}
        df = wr.opensearch.search_by_sql(client, sql_query=f"select * from {index}", **search_kwargs)
        assert df.shape[0] == 5
    finally:
        wr.opensearch.delete_index(client, index)


def test_index_json_local(client):
    index = f"test_index_json_local_{_get_unique_suffix()}"
    file_path = f"{tempfile.gettempdir()}/inspections.json"
    try:
        with open(file_path, "w") as filehandle:
            for doc in inspections_documents:
                filehandle.write("%s\n" % json.dumps(doc))
        response = wr.opensearch.index_json(client, index=index, path=file_path)
        assert response.get("success", 0) == 6
    finally:
        wr.opensearch.delete_index(client, index)


def test_index_json_s3(client, path):
    index = f"test_index_json_s3_{_get_unique_suffix()}"
    file_path = f"{tempfile.gettempdir()}/inspections.json"
    try:
        with open(file_path, "w") as filehandle:
            for doc in inspections_documents:
                filehandle.write("%s\n" % json.dumps(doc))
        s3 = boto3.client("s3")
        path = f"{path}opensearch/inspections.json"
        bucket, key = wr._utils.parse_path(path)
        s3.upload_file(file_path, bucket, key)
        response = wr.opensearch.index_json(client, index=index, path=path)
        assert response.get("success", 0) == 6
    finally:
        wr.opensearch.delete_index(client, index)


def test_index_csv_local(client):
    file_path = f"{tempfile.gettempdir()}/inspections.csv"
    index = f"test_index_csv_local_{_get_unique_suffix()}"
    try:
        df = pd.DataFrame(inspections_documents)
        df.to_csv(file_path, index=False)
        response = wr.opensearch.index_csv(client, path=file_path, index=index)
        assert response.get("success", 0) == 6
    finally:
        wr.opensearch.delete_index(client, index)


def test_index_csv_s3(client, path):
    file_path = f"{tempfile.gettempdir()}/inspections.csv"
    index = f"test_index_csv_s3_{_get_unique_suffix()}"
    try:
        df = pd.DataFrame(inspections_documents)
        df.to_csv(file_path, index=False)
        s3 = boto3.client("s3")
        path = f"{path}opensearch/inspections.csv"
        bucket, key = wr._utils.parse_path(path)
        s3.upload_file(file_path, bucket, key)
        response = wr.opensearch.index_csv(client, path=path, index=index)
        assert response.get("success", 0) == 6
    finally:
        wr.opensearch.delete_index(client, index)


@pytest.mark.skip(reason="takes a long time (~5 mins) since testing against small clusters")
def test_index_json_s3_large_file(client):
    index = f"test_index_json_s3_large_file_{_get_unique_suffix()}"
    path = "s3://irs-form-990/index_2011.json"
    try:
        response = wr.opensearch.index_json(
            client, index=index, path=path, json_path="Filings2011", id_keys=["EIN"], bulk_size=20
        )
        assert response.get("success", 0) > 0
    finally:
        wr.opensearch.delete_index(client, index)


@pytest.mark.skip(reason="Temporary skip until collection cleanup issue is resolved")
def test_opensearch_serverless_create_collection(opensearch_serverless_client) -> None:
    collection_name: str = f"col-{_get_unique_suffix()}"
    client = boto3.client(service_name="opensearchserverless")

    try:
        collection: Dict[str, Any] = wr.opensearch.create_collection(
            name=collection_name,
            data_policy=_get_opensearch_data_access_policy(),
        )
        collection_id: str = collection["id"]

        response = client.batch_get_collection(ids=[collection_id])["collectionDetails"][0]

        assert response["id"] == collection_id
        assert response["status"] == "ACTIVE"
        assert response["type"] == "SEARCH"
    finally:
        # Cleanup collection resources
        client.delete_collection(id=collection_id)
        client.delete_security_policy(name=f"{collection_name}-encryption-policy", type="encryption")
        client.delete_security_policy(name=f"{collection_name}-network-policy", type="network")
        client.delete_access_policy(name=f"{collection_name}-data-policy", type="data")
