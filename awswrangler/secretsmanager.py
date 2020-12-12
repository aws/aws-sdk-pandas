"""Secrets Manager module."""

import base64
import json
import logging
from typing import Any, Dict, Optional, Union, cast

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def get_secret(name: str, boto3_session: Optional[boto3.Session] = None) -> Union[str, bytes]:
    """Get secret value.

    Parameters
    ----------
    name: str:
        Specifies the secret containing the version that you want to retrieve.
        You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Union[str, bytes]
        Secret value.

    Examples
    --------
    >>> import awswrangler as wr
    >>> value = wr.secretsmanager.get_secret("my-secret")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client: boto3.client = _utils.client(service_name="secretsmanager", session=session)
    response: Dict[str, Any] = client.get_secret_value(SecretId=name)
    if "SecretString" in response:
        return cast(str, response["SecretString"])
    return base64.b64decode(response["SecretBinary"])


def get_secret_json(name: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, Any]:
    """Get JSON secret value.

    Parameters
    ----------
    name: str:
        Specifies the secret containing the version that you want to retrieve.
        You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Secret JSON value parsed as a dictionary.

    Examples
    --------
    >>> import awswrangler as wr
    >>> value = wr.secretsmanager.get_secret_json("my-secret-with-json-content")

    """
    value: Union[str, bytes] = get_secret(name=name, boto3_session=boto3_session)
    return cast(Dict[str, Any], json.loads(value))
