"""STS module."""

from __future__ import annotations

import logging

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def get_account_id(boto3_session: boto3.Session | None = None) -> str:
    """Get Account ID.

    Parameters
    ----------
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        Account ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> account_id = wr.sts.get_account_id()

    """
    return _utils.client(service_name="sts", session=boto3_session).get_caller_identity()["Account"]


def get_current_identity_arn(boto3_session: boto3.Session | None = None) -> str:
    """Get current user/role ARN.

    Parameters
    ----------
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        User/role ARN.

    Examples
    --------
    >>> import awswrangler as wr
    >>> arn = wr.sts.get_current_identity_arn()

    """
    return _utils.client(service_name="sts", session=boto3_session).get_caller_identity()["Arn"]


def get_current_identity_name(boto3_session: boto3.Session | None = None) -> str:
    """Get current user/role name.

    Parameters
    ----------
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        User/role name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> name = wr.sts.get_current_identity_name()

    """
    arn: str = get_current_identity_arn(boto3_session=boto3_session)
    name: str = arn.rpartition("/")[-1]
    return name
