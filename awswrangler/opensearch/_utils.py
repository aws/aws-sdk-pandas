# mypy: disable-error-code=name-defined
"""Amazon OpenSearch Utils Module (PRIVATE)."""

import json
import logging
import re
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union, cast

import boto3
import botocore

from awswrangler import _utils, exceptions
from awswrangler.annotations import Experimental

opensearchpy = _utils.import_optional_dependency("opensearchpy")
if opensearchpy:
    from requests_aws4auth import AWS4Auth

if TYPE_CHECKING:
    from mypy_boto3_opensearchserverless.client import OpenSearchServiceServerlessClient
    from mypy_boto3_opensearchserverless.literals import CollectionTypeType, SecurityPolicyTypeType
    from mypy_boto3_opensearchserverless.type_defs import BatchGetCollectionResponseTypeDef

_logger: logging.Logger = logging.getLogger(__name__)

_CREATE_COLLECTION_FINAL_STATUSES: List[str] = ["ACTIVE", "FAILED"]
_CREATE_COLLECTION_WAIT_POLLING_DELAY: float = 1.0  # SECONDS


def _get_distribution(client: "opensearchpy.OpenSearch") -> Any:
    if _is_serverless(client):
        return "opensearch"
    return client.info().get("version", {}).get("distribution", "elasticsearch")


def _get_version(client: "opensearchpy.OpenSearch") -> Any:
    if _is_serverless(client):
        return None
    return client.info().get("version", {}).get("number")


def _get_version_major(client: "opensearchpy.OpenSearch") -> Any:
    version = _get_version(client)
    if version:
        return int(version.split(".")[0])
    return None


def _is_serverless(client: "opensearchpy.OpenSearch") -> bool:
    return getattr(client, "_serverless", False)


def _get_service(endpoint: str) -> str:
    return "aoss" if "aoss.amazonaws.com" in endpoint else "es"


def _strip_endpoint(endpoint: str) -> str:
    uri_schema = re.compile(r"https?://")
    return uri_schema.sub("", endpoint).strip().strip("/")


def _is_https(port: Optional[int]) -> bool:
    return port == 443


def _get_default_encryption_policy(collection_name: str, kms_key_arn: Optional[str]) -> Dict[str, Any]:
    policy: Dict[str, Any] = {
        "Rules": [
            {
                "ResourceType": "collection",
                "Resource": [
                    f"collection/{collection_name}",
                ],
            }
        ],
    }
    if kms_key_arn:
        policy["KmsARN"] = kms_key_arn
    else:
        policy["AWSOwnedKey"] = True
    return policy


def _get_default_network_policy(collection_name: str, vpc_endpoints: Optional[List[str]]) -> List[Dict[str, Any]]:
    policy: List[Dict[str, Any]] = [
        {
            "Rules": [
                {
                    "ResourceType": "dashboard",
                    "Resource": [
                        f"collection/{collection_name}",
                    ],
                },
                {
                    "ResourceType": "collection",
                    "Resource": [
                        f"collection/{collection_name}",
                    ],
                },
            ],
            "Description": f"Default network policy for collection '{collection_name}'.",
        }
    ]
    if vpc_endpoints:
        policy[0]["SourceVPCEs"] = vpc_endpoints
    else:
        policy[0]["AllowFromPublic"] = True
    return policy


def _create_security_policy(
    collection_name: str,
    policy: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]],
    policy_type: "SecurityPolicyTypeType",
    client: "OpenSearchServiceServerlessClient",
    **kwargs: Any,
) -> None:
    if not kwargs:
        kwargs = {}
    if policy_type == "encryption" and not policy:
        policy = _get_default_encryption_policy(collection_name, kwargs.get("kms_key_arn"))
    elif policy_type == "network" and not policy:
        policy = _get_default_network_policy(collection_name, kwargs.get("vpc_endpoints"))
    else:
        raise exceptions.InvalidArgument(f"Invalid policy type '{policy_type}'.")
    try:
        client.create_security_policy(
            name=f"{collection_name}-{policy_type}-policy",
            policy=json.dumps(policy),
            type=policy_type,
            description=f"Default {policy_type} policy for collection '{collection_name}'.",
        )
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ConflictException":
            raise exceptions.PolicyResourceConflict(
                "The policy name or rules conflict with an existing policy."
            ) from error
        raise error


def _create_data_policy(
    collection_name: str, policy: List[Dict[str, Any]], client: "OpenSearchServiceServerlessClient"
) -> None:
    try:
        client.create_access_policy(
            name=f"{collection_name}-data-policy",
            policy=json.dumps(policy),
            type="data",
            description=f"Default data policy for collection '{collection_name}'.",
        )
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ConflictException":
            raise exceptions.PolicyResourceConflict(
                "The policy name or rules conflict with an existing policy."
            ) from error
        raise error


@_utils.check_optional_dependency(opensearchpy, "opensearchpy")
def connect(
    host: str,
    port: Optional[int] = 443,
    boto3_session: Optional[boto3.Session] = None,
    region: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    service: Optional[str] = None,
    timeout: int = 30,
    max_retries: int = 5,
    retry_on_timeout: bool = True,
    retry_on_status: Optional[Sequence[int]] = None,
) -> "opensearchpy.OpenSearch":
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
    timeout : int
        Operation timeout. `30` by default.
    max_retries : int
        Maximum number of retries before an exception is propagated. `10` by default.
    retry_on_timeout : bool
        Should timeout trigger a retry on different node. `True` by default.
    retry_on_status : List[int], optional
        Set of HTTP status codes on which we should retry on a different node. Defaults to [500, 502, 503, 504].

    Returns
    -------
    opensearchpy.OpenSearch
        OpenSearch low-level client.
        https://github.com/opensearch-project/opensearch-py/blob/main/opensearchpy/client/__init__.py
    """
    if not service:
        service = _get_service(host)

    if not retry_on_status:
        # Default retry on (502, 503, 504)
        # Add 500 to retry on BulkIndexError
        retry_on_status = (500, 502, 503, 504)

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
        es = opensearchpy.OpenSearch(
            host=_strip_endpoint(host),
            port=port,
            http_auth=http_auth,
            use_ssl=_is_https(port),
            verify_certs=_is_https(port),
            connection_class=opensearchpy.RequestsHttpConnection,
            timeout=timeout,
            max_retries=max_retries,
            retry_on_timeout=retry_on_timeout,
            retry_on_status=retry_on_status,
        )
        es._serverless = service == "aoss"  # pylint: disable=protected-access
    except Exception as e:
        _logger.error("Error connecting to Opensearch cluster. Please verify authentication details")
        raise e
    return es


@_utils.check_optional_dependency(opensearchpy, "opensearchpy")
@Experimental
def create_collection(
    name: str,
    collection_type: str = "SEARCH",
    description: str = "",
    encryption_policy: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
    kms_key_arn: Optional[str] = None,
    network_policy: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
    vpc_endpoints: Optional[List[str]] = None,
    data_policy: Optional[List[Dict[str, Any]]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Create Amazon OpenSearch Serverless collection.

    Creates Amazon OpenSearch Serverless collection, corresponding encryption and network
    policies, and data policy, if `data_policy` provided.

    More in [Amazon OpenSearch Serverless (preview)]
    (https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless.html)

    Parameters
    ----------
    name : str
        Collection name.
    collection_type : str
        Collection type. Allowed values are `SEARCH`, and `TIMESERIES`.
    description : str
        Collection description.
    encryption_policy : Union[Dict[str, Any], List[Dict[str, Any]]], optional
        Encryption policy of a form: { "Rules": [...] }

        If not provided, default policy using AWS-managed KMS key will be created. To use user-defined key,
        provide `kms_key_arn`.
    kms_key_arn: str, optional
        Encryption key.
    network_policy : Union[Dict[str, Any], List[Dict[str, Any]]], optional
        Network policy of a form: [{ "Rules": [...] }]

        If not provided, default network policy allowing public access to the collection will be created.
        To create the collection in the VPC, provide `vpc_endpoints`.
    vpc_endpoints : List[str], optional
        List of VPC endpoints for access to non-public collection.
    data_policy : Union[Dict[str, Any], List[Dict[str, Any]]], optional
        Data policy of a form: [{ "Rules": [...] }]
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    Collection details : Dict[str, Any]
        Collection details
    """
    if collection_type not in ["SEARCH", "TIMESERIES"]:
        raise exceptions.InvalidArgumentValue("Collection `type` must be either 'SEARCH' or 'TIMESERIES'.")
    collection_type = cast("CollectionTypeType", collection_type)

    client = _utils.client(service_name="opensearchserverless", session=boto3_session)
    # Create encryption and network policies
    _create_security_policy(
        collection_name=name,
        policy=encryption_policy,
        policy_type="encryption",
        client=client,
        kms_key_arn=kms_key_arn,
    )
    _create_security_policy(
        collection_name=name, policy=network_policy, policy_type="network", client=client, vpc_endpoints=vpc_endpoints
    )
    # Create data policy if provided
    if data_policy:
        _create_data_policy(
            collection_name=name,
            policy=data_policy,
            client=client,
        )
    try:
        client.create_collection(
            name=name,
            type=collection_type,
            description=description,
        )
        # Wait for the collection to become active
        status: Optional[str] = None
        response: Optional["BatchGetCollectionResponseTypeDef"] = None
        while status not in _CREATE_COLLECTION_FINAL_STATUSES:
            time.sleep(_CREATE_COLLECTION_WAIT_POLLING_DELAY)
            response = client.batch_get_collection(names=[name])
            status = response["collectionDetails"][0]["status"]

        response = cast("BatchGetCollectionResponseTypeDef", response)
        if status == "FAILED":
            errors = response["collectionErrorDetails"]
            error_details = errors[0] if len(errors) > 0 else "No error details provided"
            raise exceptions.QueryFailed(f"Failed to create collection `{name}`: {error_details}.")

        return response["collectionDetails"][0]  # type: ignore[return-value]
    except botocore.exceptions.ClientError as error:
        if error.response["Error"]["Code"] == "ConflictException":
            raise exceptions.AlreadyExists(f"A collection with name `{name}` already exists.") from error
        raise error
