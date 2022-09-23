"""Amazon OpenSearch Utils Module (PRIVATE)."""

import logging
import re
from typing import Any, Optional

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def _get_distribution(client: OpenSearch) -> Any:
    return client.info().get("version", {}).get("distribution", "elasticsearch")


def _get_version(client: OpenSearch) -> Any:
    return client.info().get("version", {}).get("number")


def _get_version_major(client: OpenSearch) -> Any:
    version = _get_version(client)
    if version:
        return int(version.split(".")[0])
    return None


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
    region :
        AWS region of the Amazon OS domain. If not provided will be extracted from boto3_session.
    username :
        Fine-grained access control username. Mandatory if OS Cluster uses Fine Grained Access Control.
    password :
        Fine-grained access control password. Mandatory if OS Cluster uses Fine Grained Access Control.

    Returns
    -------
    opensearchpy.OpenSearch
        OpenSearch low-level client.
        https://github.com/opensearch-project/opensearch-py/blob/main/opensearchpy/client/__init__.py
    """
    valid_ports = {80, 443}

    if port not in valid_ports:
        raise ValueError(f"results: port must be one of {valid_ports}")

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
        http_auth = AWS4Auth(creds.access_key, creds.secret_key, region, "es", session_token=creds.token)
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
    except Exception as e:
        _logger.error("Error connecting to Opensearch cluster. Please verify authentication details")
        raise e
    return es
