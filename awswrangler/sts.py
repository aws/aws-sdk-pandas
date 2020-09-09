"""STS module."""

import logging
from typing import Optional, cast

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def get_account_id(boto3_session: Optional[boto3.Session] = None) -> str:
    """Get Account ID.

    Parameters
    ----------
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Account ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> account_id = wr.sts.get_account_id()

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    return cast(str, _utils.client(service_name="sts", session=session).get_caller_identity().get("Account"))


def get_current_identity_arn(boto3_session: Optional[boto3.Session] = None) -> str:
    """Get current user/role ARN.

    Parameters
    ----------
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        User/role ARN.

    Examples
    --------
    >>> import awswrangler as wr
    >>> arn = wr.sts.get_current_identity_arn()

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    return cast(str, _utils.client(service_name="sts", session=session).get_caller_identity().get("Arn"))


def get_current_identity_name(boto3_session: Optional[boto3.Session] = None) -> str:
    """Get current user/role name.

    Parameters
    ----------
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        User/role name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> name = wr.sts.get_current_identity_name()

    """
    arn: str = get_current_identity_arn(boto3_session=boto3_session)
    name: str = arn.rpartition("/")[-1]
    return name
