"""Utilities Module for Amazon Clean Rooms."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import boto3

from awswrangler import _utils, exceptions

if TYPE_CHECKING:
    from mypy_boto3_cleanrooms.type_defs import GetProtectedQueryOutputTypeDef

_QUERY_FINAL_STATES: list[str] = ["CANCELLED", "FAILED", "SUCCESS", "TIMED_OUT"]
_QUERY_WAIT_POLLING_DELAY: float = 2  # SECONDS

_logger: logging.Logger = logging.getLogger(__name__)


def wait_query(
    membership_id: str, query_id: str, boto3_session: boto3.Session | None = None
) -> "GetProtectedQueryOutputTypeDef":
    """Wait for the Clean Rooms protected query to end.

    Parameters
    ----------
    membership_id : str
        Membership ID
    query_id : str
        Protected query execution ID
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used

    Returns
    -------
    Dict[str, Any]
        Dictionary with the get_protected_query response.

    Raises
    ------
    exceptions.QueryFailed
        Raises exception with error message if protected query is cancelled, times out or fails.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.cleanrooms.wait_query(membership_id='membership-id', query_id='query-id')
    """
    client_cleanrooms = _utils.client(service_name="cleanrooms", session=boto3_session)
    state = "SUBMITTED"

    while state not in _QUERY_FINAL_STATES:
        time.sleep(_QUERY_WAIT_POLLING_DELAY)
        response = client_cleanrooms.get_protected_query(
            membershipIdentifier=membership_id, protectedQueryIdentifier=query_id
        )
        state = response["protectedQuery"].get("status")  # type: ignore[assignment]

    _logger.debug("state: %s", state)
    if state != "SUCCESS":
        raise exceptions.QueryFailed(response["protectedQuery"].get("Error"))
    return response
