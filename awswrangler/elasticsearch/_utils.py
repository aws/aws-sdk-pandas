"""Amazon Elasticsearch Utils Module (PRIVATE)."""

from typing import Optional

import boto3

from awswrangler import _utils, exceptions
from elasticsearch import Elasticsearch


def connect(
    host: str,
    boto3_session: Optional[boto3.Session] = None
) -> Elasticsearch:
    """Establishes a secure connection to the specified Amazon ES domain.

    Note
    ----
    We use [elasticsearch-py](https://elasticsearch-py.readthedocs.io/en/v7.13.4/), an Elasticsearch client for Python,
    version 7.13.4, which is the recommended version for best compatibility Amazon ES,
    since later versions may reject connections to Amazon ES clusters.
    In the future will move to a new open source client under the [OpenSearch project](https://www.opensearch.org/)
    You can read more here:
    https://aws.amazon.com/blogs/opensource/keeping-clients-of-opensearch-and-elasticsearch-compatible-with-open-source/
    https://opensearch.org/docs/clients/index/

    Parameters
    ----------
    host : str
        Amazon Elasticsearch domain, for example: my-test-domain.us-east-1.es.amazonaws.com.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    elasticsearch.Elasticsearch
        Elasticsearch low-level client.
        https://elasticsearch-py.readthedocs.io/en/v7.13.4/api.html#elasticsearch
    """

    pass  # connect to Amazon ES
