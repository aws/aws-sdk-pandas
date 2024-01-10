"""Secrets Manager module."""

from __future__ import annotations

import base64
import json
import logging
from typing import Any, Dict, cast

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def get_secret(name: str, boto3_session: boto3.Session | None = None) -> str | bytes:
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
    client = _utils.client(service_name="secretsmanager", session=boto3_session)
    response = client.get_secret_value(SecretId=name)
    if "SecretString" in response:
        return response["SecretString"]
    return base64.b64decode(response["SecretBinary"])


def get_secret_json(name: str, boto3_session: boto3.Session | None = None) -> dict[str, Any]:
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
    value = get_secret(name=name, boto3_session=boto3_session)
    return cast(Dict[str, Any], json.loads(value))
