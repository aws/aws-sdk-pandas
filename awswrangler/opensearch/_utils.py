"""Amazon OpenSearch Utils Module (PRIVATE)."""

import logging
import re
import time
from typing import Any, Dict, List, Optional

import boto3
import botocore
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_CREATE_COLLECTION_FINAL_STATUSES: List[str] = ["ACTIVE", "FAILED"]
_CREATE_COLLECTION_WAIT_POLLING_DELAY: float = 0.25  # SECONDS


def _get_distribution(client: OpenSearch) -> Any:
    if getattr(client, "_serverless", False):
        return "opensearch"
    return client.info().get("version", {}).get("distribution", "elasticsearch")


def _get_version(client: OpenSearch) -> Any:
    if getattr(client, "_serverless", False):
        return None
    return client.info().get("version", {}).get("number")


def _get_version_major(client: OpenSearch) -> Any:
    version = _get_version(client)
    if version:
        return int(version.split(".")[0])
    return None


def _get_service(endpoint: str) -> str:
    return "aoss" if "aoss" in endpoint else "es"


def _strip_endpoint(endpoint: str) -> str:
    uri_schema = re.compile(r"https?://")
    return uri_schema.sub("", endpoint).strip().strip("/")


def _is_https(port: int) -> bool:
    return port == 443


def connect(
    host: str,
    port: Optional[int] = 443,
    boto3_session: Optional[boto3.Session] = None,
    region: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    service: Optional[str] = None,
) -> OpenSearch:
    """Create a secure connection to the specified Amazon OpenSearch domain.

    Note
    ----
    We use `opensearch-py <https://github.com/opensearch-project/opensearch-py>`_, an OpenSearch python client.

    The username and password are mandatory if the OS Cluster uses `Fine Grained Access Control \
<https://docs.aws.amazon.com/opensearch-service/latest/developerguide/fgac.html>`_.
    If fine grained access control is disabled, session access key and secret keys are used.

    Parameters
    ----------
    host : str
        Amazon OpenSearch domain, for example: my-test-domain.us-east-1.es.amazonaws.com.
    port : int
        OpenSearch Service only accepts connections over port 80 (HTTP) or 443 (HTTPS)
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    region : str, optional
        AWS region of the Amazon OS domain. If not provided will be extracted from boto3_session.
    username : str, optional
        Fine-grained access control username. Mandatory if OS Cluster uses Fine Grained Access Control.
    password : str, optional
        Fine-grained access control password. Mandatory if OS Cluster uses Fine Grained Access Control.
    service : str, optional
        Service id. Supported values are `es`, corresponding to opensearch cluster,
        and `aoss` for serverless opensearch. By default, service will be parsed from the host URI.

    Returns
    -------
    opensearchpy.OpenSearch
        OpenSearch low-level client.
        https://github.com/opensearch-project/opensearch-py/blob/main/opensearchpy/client/__init__.py
    """
    valid_ports = {80, 443}

    if port not in valid_ports:
        raise ValueError(f"results: port must be one of {valid_ports}")

    if not service:
        service = _get_service(host)

    if username and password:
        http_auth = (username, password)
    else:
        if region is None:
            region = _utils.get_region_from_session(boto3_session=boto3_session)
        creds = _utils.get_credentials_from_session(boto3_session=boto3_session)
        if creds.access_key is None or creds.secret_key is None:
            raise exceptions.InvalidArgument(
                "One of IAM Role or AWS ACCESS_KEY_ID and SECRET_ACCESS_KEY must be "
                "given. Unable to find ACCESS_KEY_ID and SECRET_ACCESS_KEY in boto3 "
                "session."
            )
        http_auth = AWS4Auth(creds.access_key, creds.secret_key, region, service, session_token=creds.token)
    try:
        es = OpenSearch(
            host=_strip_endpoint(host),
            port=port,
            http_auth=http_auth,
            use_ssl=_is_https(port),
            verify_certs=_is_https(port),
            connection_class=RequestsHttpConnection,
            timeout=30,
            max_retries=10,
            retry_on_timeout=True,
        )
        es._serverless = True if service == "aoss" else False  # type: ignore
    except Exception as e:
        _logger.error("Error connecting to Opensearch cluster. Please verify authentication details")
        raise e
    return es


def create_collection(
    name: str,
    type: str = "SEARCH",
    description: str = "",
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Create Amazon OpenSearch Serverless collection.

    Parameters
    ----------
    name : str
        Collection name.
    description : str
        Collection description.
    type : str
        Collection type. Allowed values are `SEARCH`, and `TIMESERIES`.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    Collection details : Dict[str, Any]
        Collection details
    """
    if type not in ["SEARCH", "TIMESERIES"]:
        raise exceptions.InvalidArgumentValue("Collection `type` must be either 'SEARCH' or 'TIMESERIES'.")

    client: boto3.client = _utils.client(service_name="opensearchserverless", session=boto3_session)
    try:
        client.create_collection(
            name=name,
            type=type,
            description=description,
        )
        # Get collection details
        status: Optional[str] = None
        response: Optional[Dict[str, Any]] = {}
        while status not in _CREATE_COLLECTION_FINAL_STATUSES:
            time.sleep(_CREATE_COLLECTION_WAIT_POLLING_DELAY)
            response = client.batch_get_collection(names=[name])
            status = response["collectionDetails"][0]["status"]

        if status == "FAILED":
            error_details: str = response.get("collectionErrorDetails")[0]
            raise exceptions.QueryFailed(f"Failed to create collection `{name}`: {error_details}.")

        return response["collectionDetails"][0]
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ConflictException":
            raise exceptions.AlreadyExists(f"A collection with name `{name}` already exists.") from error
        else:
            raise error
