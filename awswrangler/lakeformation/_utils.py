"""Utilities Module for Amazon Lake Formation."""
import logging
import time
from typing import Any, Dict, List, Optional

import boto3

from awswrangler import _utils, exceptions

_QUERY_FINAL_STATES: List[str] = ["ERROR", "FINISHED"]
_QUERY_WAIT_POLLING_DELAY: float = 2  # SECONDS

_logger: logging.Logger = logging.getLogger(__name__)


def abort_transaction(transaction_id: str, boto3_session: Optional[boto3.Session] = None) -> None:
    """Abort the specified transaction. Returns exception if the transaction was previously committed.

    Parameters
    ----------
    transaction_id : str
        The ID of the transaction.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session received None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.lakeformation.abort_transaction(transaction_id="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)

    client_lakeformation.abort_transaction(TransactionId=transaction_id)


def begin_transaction(read_only: Optional[bool] = False, boto3_session: Optional[boto3.Session] = None) -> str:
    """Start a new transaction and returns its transaction ID.

    Parameters
    ----------
    read_only : bool, optional
        Indicates that that this transaction should be read only.
        Writes made using a read-only transaction ID will be rejected.
        Read-only transactions do not need to be committed.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session received None.

    Returns
    -------
    str
        An opaque identifier for the transaction.

    Examples
    --------
    >>> import awswrangler as wr
    >>> transaction_id = wr.lakeformation.begin_transaction(read_only=False)

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    transaction_id: str = client_lakeformation.begin_transaction(ReadOnly=read_only)["TransactionId"]
    return transaction_id


def commit_transaction(transaction_id: str, boto3_session: Optional[boto3.Session] = None) -> None:
    """Commit the specified transaction. Returns exception if the transaction was previously aborted.

    Parameters
    ----------
    transaction_id : str
        The ID of the transaction.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session received None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.lakeformation.commit_transaction(transaction_id="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)

    client_lakeformation.commit_transaction(TransactionId=transaction_id)


def extend_transaction(transaction_id: str, boto3_session: Optional[boto3.Session] = None) -> None:
    """Indicate to the service that the specified transaction is still active and should not be aborted.

    Parameters
    ----------
    transaction_id : str
        The ID of the transaction.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session received None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.lakeformation.extend_transaction(transaction_id="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)

    client_lakeformation.extend_transaction(TransactionId=transaction_id)


def wait_query(query_id: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, Any]:
    """Wait for the query to end.

    Parameters
    ----------
    query_id : str
        Lake Formation query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session received None.

    Returns
    -------
    Dict[str, Any]
        Dictionary with the get_query_state response.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.lakeformation.wait_query(query_id='query-id')

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)

    response: Dict[str, Any] = client_lakeformation.get_query_state(QueryId=query_id)
    state: str = response["State"]
    while state not in _QUERY_FINAL_STATES:
        time.sleep(_QUERY_WAIT_POLLING_DELAY)
        response = client_lakeformation.get_query_state(QueryId=query_id)
        state = response["State"]
    _logger.debug("state: %s", state)
    if state == "ERROR":
        raise exceptions.QueryFailed(response.get("Error"))
    return response
