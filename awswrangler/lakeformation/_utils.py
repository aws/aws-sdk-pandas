"""Utilities Module for Amazon Lake Formation."""
import logging
import time
from math import inf
from threading import Thread
from typing import Any, Dict, List, Optional, Union

import boto3
import botocore.exceptions

from awswrangler import _utils, exceptions
from awswrangler.catalog._utils import _catalog_id, _transaction_id
from awswrangler.s3._describe import describe_objects

_QUERY_FINAL_STATES: List[str] = ["ERROR", "FINISHED"]
_QUERY_WAIT_POLLING_DELAY: float = 2  # SECONDS
_TRANSACTION_FINAL_STATES: List[str] = ["aborted", "committed"]
_TRANSACTION_WAIT_COMMIT_DELAY: float = 5  # SECONDS
_TRANSACTION_WAIT_POLLING_DELAY: float = 10  # SECONDS

_logger: logging.Logger = logging.getLogger(__name__)


def _without_keys(d: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    return {x: d[x] for x in d if x not in keys}


def _build_partition_predicate(
    partition_cols: List[str],
    partitions_types: Dict[str, str],
    partitions_values: List[str],
) -> str:
    partition_predicates: List[str] = []
    for col, val in zip(partition_cols, partitions_values):
        if partitions_types[col].startswith(("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")):
            partition_predicates.append(f"{col}={str(val)}")
        else:
            partition_predicates.append(f"{col}='{str(val)}'")
    return " AND ".join(partition_predicates)


def _build_table_objects(
    paths: List[str],
    partitions_values: Dict[str, List[str]],
    use_threads: Union[bool, int],
    boto3_session: Optional[boto3.Session],
) -> List[Dict[str, Any]]:
    table_objects: List[Dict[str, Any]] = []
    paths_desc: Dict[str, Dict[str, Any]] = describe_objects(
        path=paths, use_threads=use_threads, boto3_session=boto3_session
    )
    for path, path_desc in paths_desc.items():
        table_object: Dict[str, Any] = {
            "Uri": path,
            "ETag": path_desc["ETag"],
            "Size": path_desc["ContentLength"],
        }
        if partitions_values:
            table_object["PartitionValues"] = partitions_values[f"{path.rsplit('/', 1)[0].rstrip('/')}/"]
        table_objects.append(table_object)
    return table_objects


def _get_table_objects(
    catalog_id: Optional[str],
    database: str,
    table: str,
    transaction_id: str,
    boto3_session: Optional[boto3.Session],
    partition_cols: Optional[List[str]] = None,
    partitions_types: Optional[Dict[str, str]] = None,
    partitions_values: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Get Governed Table Objects from Lake Formation Engine."""
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)

    scan_kwargs: Dict[str, Union[str, int]] = _catalog_id(
        catalog_id=catalog_id,
        **_transaction_id(transaction_id=transaction_id, DatabaseName=database, TableName=table, MaxResults=100),
    )
    if partition_cols and partitions_types and partitions_values:
        scan_kwargs["PartitionPredicate"] = _build_partition_predicate(
            partition_cols=partition_cols, partitions_types=partitions_types, partitions_values=partitions_values
        )

    next_token: str = "init_token"  # Dummy token
    table_objects: List[Dict[str, Any]] = []
    while next_token:
        response = _utils.try_it(
            f=client_lakeformation.get_table_objects,
            ex=botocore.exceptions.ClientError,
            ex_code="ResourceNotReadyException",
            base=1.0,
            max_num_tries=5,
            **scan_kwargs,
        )
        for objects in response["Objects"]:
            for table_object in objects["Objects"]:
                if objects["PartitionValues"]:
                    table_object["PartitionValues"] = objects["PartitionValues"]
                table_objects.append(table_object)
        next_token = response.get("NextToken", None)
        scan_kwargs["NextToken"] = next_token
    return table_objects


def _update_table_objects(
    catalog_id: Optional[str],
    database: str,
    table: str,
    transaction_id: str,
    boto3_session: Optional[boto3.Session],
    add_objects: Optional[List[Dict[str, Any]]] = None,
    del_objects: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Register Governed Table Objects changes to Lake Formation Engine."""
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)

    update_kwargs: Dict[str, Union[str, int, List[Dict[str, Dict[str, Any]]]]] = _catalog_id(
        catalog_id=catalog_id, **_transaction_id(transaction_id=transaction_id, DatabaseName=database, TableName=table)
    )

    write_operations: List[Dict[str, Dict[str, Any]]] = []
    if add_objects:
        write_operations.extend({"AddObject": obj} for obj in add_objects)
    if del_objects:
        write_operations.extend({"DeleteObject": _without_keys(obj, ["Size"])} for obj in del_objects)
    update_kwargs["WriteOperations"] = write_operations

    client_lakeformation.update_table_objects(**update_kwargs)


def _monitor_transaction(transaction_id: str, time_out: float, boto3_session: Optional[boto3.Session] = None) -> None:
    start = time.time()
    elapsed_time = 0.0
    state: str = describe_transaction(transaction_id=transaction_id, boto3_session=boto3_session)
    while (state not in _TRANSACTION_FINAL_STATES) and (time_out > elapsed_time):
        try:
            extend_transaction(transaction_id=transaction_id, boto3_session=boto3_session)
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] in ["TransactionCanceledException", "TransactionCommittedException"]:
                _logger.debug("Transaction: %s was already canceled or committed.", transaction_id)
            else:
                raise ex
        time.sleep(_TRANSACTION_WAIT_POLLING_DELAY)
        elapsed_time = time.time() - start
        state = describe_transaction(transaction_id=transaction_id, boto3_session=boto3_session)
        _logger.debug("Transaction state: %s", state)


def describe_transaction(transaction_id: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Return the status of a single transaction.

    Parameters
    ----------
    transaction_id : str
        The ID of the transaction.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session received None.

    Returns
    -------
    str
        Transaction status (i.e. active|committed|aborted).

    Examples
    --------
    >>> import awswrangler as wr
    >>> status = wr.lakeformation.describe_transaction(transaction_id="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    details: Dict[str, Any] = client_lakeformation.describe_transaction(TransactionId=transaction_id)[
        "TransactionDescription"
    ]
    return details["TransactionStatus"]  # type: ignore


def cancel_transaction(transaction_id: str, boto3_session: Optional[boto3.Session] = None) -> None:
    """Cancel the specified transaction. Returns exception if the transaction was previously committed.

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
    >>> wr.lakeformation.cancel_transaction(transaction_id="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)

    client_lakeformation.cancel_transaction(TransactionId=transaction_id)


def start_transaction(
    read_only: Optional[bool] = False, time_out: Optional[float] = inf, boto3_session: Optional[boto3.Session] = None
) -> str:
    """Start a new transaction and returns its transaction ID.

    The transaction is periodically extended until it's committed, canceled or the defined time-out is reached.

    Parameters
    ----------
    read_only : bool, optional
        Indicates that that this transaction should be read only.
        Writes made using a read-only transaction ID will be rejected.
        Read-only transactions do not need to be committed.
    time_out: float, optional
        Maximum duration over which a transaction is extended.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session received None.

    Returns
    -------
    str
        An opaque identifier for the transaction.

    Examples
    --------
    >>> import awswrangler as wr
    >>> transaction_id = wr.lakeformation.start_transaction(read_only=False)

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    transaction_type: str = "READ_ONLY" if read_only else "READ_AND_WRITE"
    transaction_id: str = client_lakeformation.start_transaction(TransactionType=transaction_type)["TransactionId"]
    # Extend the transaction while in "active" state in a separate thread
    t = Thread(target=_monitor_transaction, args=(transaction_id, time_out, boto3_session))
    t.daemon = True  # Ensures thread is killed when any exception is raised
    t.start()
    return transaction_id


def commit_transaction(transaction_id: str, boto3_session: Optional[boto3.Session] = None) -> None:
    """Commit the specified transaction. Returns exception if the transaction was previously canceled.

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
    committed: bool = False
    # Confirm transaction was committed
    while not committed:
        state: str = describe_transaction(transaction_id=transaction_id, boto3_session=session)
        if state == "committed":
            committed = True
        elif state == "aborted":
            raise exceptions.CommitCancelled(f"Transaction commit with id {transaction_id} was aborted.")
        time.sleep(_TRANSACTION_WAIT_COMMIT_DELAY)


def extend_transaction(transaction_id: str, boto3_session: Optional[boto3.Session] = None) -> None:
    """Indicate to the service that the specified transaction is still active and should not be canceled.

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
