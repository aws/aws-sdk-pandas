"""Amazon OpenSearch Utils Module (PRIVATE)."""

from typing import Optional

import boto3
import logging

from awswrangler import _utils, exceptions
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


_logger: logging.Logger = logging.getLogger(__name__)


def _get_distribution(client: Elasticsearch):
    return client.info().get('version', {}).get('distribution', 'elasticsearch')


def _get_version(client: Elasticsearch):
    return client.info().get('version', {}).get('number')


def _get_version_major(client: Elasticsearch):
    version = _get_version(client)
    if version:
        return int(version.split('.')[0])
    return None


def connect(
    host: str,
    port: Optional[int] = 443,
    boto3_session: Optional[boto3.Session] = boto3.Session(),
    region: Optional[str] = None,
    fgac_user: Optional[str] = None,
    fgac_password: Optional[str] = None

) -> Elasticsearch:
    """Creates a secure connection to the specified Amazon OpenSearch domain.

    Note
    ----
    We use [elasticsearch-py](https://elasticsearch-py.readthedocs.io/en/v7.13.4/), an Elasticsearch client for Python,
    version 7.13.4, which is the recommended version for best compatibility Amazon OpenSearch,
    since later versions may reject connections to Amazon OpenSearch clusters.
    In the future will move to a new open source client under the [OpenSearch project](https://www.opensearch.org/)
    You can read more here:
    https://aws.amazon.com/blogs/opensource/keeping-clients-of-opensearch-and-elasticsearch-compatible-with-open-source/
    https://opensearch.org/docs/clients/index/

    The username and password are mandatory if the OS Cluster uses [Fine Grained Access Control](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/fgac.html).
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
    fgac_user :
        Fine-grained access control user. Mandatory if OS Cluster uses Fine Grained Access Control.
    fgac_password :
        Fine-grained access control password. Mandatory if OS Cluster uses Fine Grained Access Control.

    Returns
    -------
    elasticsearch.Elasticsearch
        Elasticsearch low-level client.
        https://elasticsearch-py.readthedocs.io/en/v7.13.4/api.html#elasticsearch
    """

    valid_ports = {80, 443}

    if port not in valid_ports:
        raise ValueError("results: status must be one of %r." % valid_ports)

    if fgac_user and fgac_password:
        http_auth = (fgac_user, fgac_password)
    else:
        if region is None:
            region = boto3_session.region_name
        creds = boto3_session.get_credentials()
        http_auth = AWS4Auth(
            creds.access_key,
            creds.secret_key,
            region,
            'es',
            creds.token
        )
    try:
        es = Elasticsearch(
            host=host,
            port=port,
            http_auth=http_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
    except Exception as e:
        _logger.error("Error connecting to Opensearch cluster. Please verify authentication details")
        raise e
    return es
