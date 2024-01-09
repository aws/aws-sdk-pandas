"""Utilities Module for Amazon Lake Formation."""
from __future__ import annotations

import logging
import time
from math import inf
from threading import Thread
from typing import Any, Literal

import boto3
import botocore.exceptions

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.catalog._utils import _catalog_id, _transaction_id
from awswrangler.s3._describe import describe_objects

_QUERY_FINAL_STATES: list[str] = ["ERROR", "FINISHED"]
_QUERY_WAIT_POLLING_DELAY: float = 2  # SECONDS
_TRANSACTION_FINAL_STATES: list[str] = ["aborted", "committed"]
_TRANSACTION_WAIT_COMMIT_DELAY: float = 5  # SECONDS
_TRANSACTION_WAIT_POLLING_DELAY: float = 10  # SECONDS

_logger: logging.Logger = logging.getLogger(__name__)


def _without_keys(d: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {x: d[x] for x in d if x not in keys}


def _build_partition_predicate(
    partition_cols: list[str],
    partitions_types: dict[str, str],
    partitions_values: list[str],
) -> str:
    partition_predicates: list[str] = []
    for col, val in zip(partition_cols, partitions_values):
        if partitions_types[col].startswith(("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")):
            partition_predicates.append(f"{col}={str(val)}")
        else:
            partition_predicates.append(f"{col}='{str(val)}'")
    return " AND ".join(partition_predicates)


def _build_table_objects(
    paths: list[str],
    partitions_values: dict[str, list[str]],
    use_threads: bool | int,
    boto3_session: boto3.Session | None,
) -> list[list[dict[str, Any]]]:
    table_objects: list[dict[str, Any]] = []
    paths_desc: dict[str, dict[str, Any]] = describe_objects(
        path=paths, use_threads=use_threads, boto3_session=boto3_session
    )
    for path, path_desc in paths_desc.items():
        table_object: dict[str, Any] = {
            "Uri": path,
            "ETag": path_desc["ETag"],
            "Size": path_desc["ContentLength"],
        }
        if partitions_values:
            table_object["PartitionValues"] = partitions_values[f"{path.rsplit('/', 1)[0].rstrip('/')}/"]
        table_objects.append(table_object)
    return _utils.chunkify(table_objects, max_length=100)  # LF write operations is limited to 100 objects per call


def _get_table_objects(
    catalog_id: str | None,
    database: str,
    table: str,
    transaction_id: str,
    boto3_session: boto3.Session | None,
    partition_cols: list[str] | None = None,
    partitions_types: dict[str, str] | None = None,
    partitions_values: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Get Governed Table Objects from Lake Formation Engine."""
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)

    scan_kwargs: dict[str, str | int | None] = _catalog_id(
        catalog_id=catalog_id,
        **_transaction_id(transaction_id=transaction_id, DatabaseName=database, TableName=table, MaxResults=100),
    )
    if partition_cols and partitions_types and partitions_values:
        scan_kwargs["PartitionPredicate"] = _build_partition_predicate(
            partition_cols=partition_cols, partitions_types=partitions_types, partitions_values=partitions_values
        )

    next_token: str | None = "init_token"  # Dummy token
    table_objects: list[dict[str, Any]] = []
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
                    table_object["PartitionValues"] = objects["PartitionValues"]  # type: ignore[typeddict-unknown-key]
                table_objects.append(table_object)  # type: ignore[arg-type]
        next_token = response.get("NextToken", None)
        scan_kwargs["NextToken"] = next_token
    return table_objects


def _update_table_objects(
    catalog_id: str | None,
    database: str,
    table: str,
    transaction_id: str,
    boto3_session: boto3.Session | None,
    add_objects: list[dict[str, Any]] | None = None,
    del_objects: list[dict[str, Any]] | None = None,
) -> None:
    """Register Governed Table Objects changes to Lake Formation Engine."""
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)

    update_kwargs: dict[str, str | int | list[dict[str, dict[str, Any]]]] = _catalog_id(
        catalog_id=catalog_id, **_transaction_id(transaction_id=transaction_id, DatabaseName=database, TableName=table)
    )

    write_operations: list[dict[str, dict[str, Any]]] = []
    if add_objects:
        write_operations.extend({"AddObject": obj} for obj in add_objects)
    if del_objects:
        write_operations.extend({"DeleteObject": _without_keys(obj, ["Size"])} for obj in del_objects)
    update_kwargs["WriteOperations"] = write_operations

    client_lakeformation.update_table_objects(**update_kwargs)  # type: ignore[arg-type]


def _monitor_transaction(transaction_id: str, time_out: float, boto3_session: boto3.Session | None = None) -> None:
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


def describe_transaction(transaction_id: str, boto3_session: boto3.Session | None = None) -> str:
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
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)
    details = client_lakeformation.describe_transaction(TransactionId=transaction_id)["TransactionDescription"]
    return details["TransactionStatus"]


def cancel_transaction(transaction_id: str, boto3_session: boto3.Session | None = None) -> None:
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
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)

    client_lakeformation.cancel_transaction(TransactionId=transaction_id)


def start_transaction(
    read_only: bool | None = False, time_out: float | None = inf, boto3_session: boto3.Session | None = None
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
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)
    transaction_type: Literal["READ_AND_WRITE", "READ_ONLY"] = "READ_ONLY" if read_only else "READ_AND_WRITE"
    transaction_id: str = client_lakeformation.start_transaction(TransactionType=transaction_type)["TransactionId"]
    # Extend the transaction while in "active" state in a separate thread
    t = Thread(target=_monitor_transaction, args=(transaction_id, time_out, boto3_session))
    t.daemon = True  # Ensures thread is killed when any exception is raised
    t.start()
    return transaction_id


def commit_transaction(transaction_id: str, boto3_session: boto3.Session | None = None) -> None:
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
    client_lakeformation = _utils.client(service_name="lakeformation", session=session)

    client_lakeformation.commit_transaction(TransactionId=transaction_id)
    committed: bool = False
    # Confirm transaction was committed
    while not committed:
        state = describe_transaction(transaction_id=transaction_id, boto3_session=session)
        if state == "committed":
            committed = True
        elif state == "aborted":
            raise exceptions.CommitCancelled(f"Transaction commit with id {transaction_id} was aborted.")
        time.sleep(_TRANSACTION_WAIT_COMMIT_DELAY)


def extend_transaction(transaction_id: str, boto3_session: boto3.Session | None = None) -> None:
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
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)

    client_lakeformation.extend_transaction(TransactionId=transaction_id)


@apply_configs
def wait_query(
    query_id: str,
    boto3_session: boto3.Session | None = None,
    lakeformation_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
) -> dict[str, Any]:
    """Wait for the query to end.

    Parameters
    ----------
    query_id : str
        Lake Formation query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session received None.
    lakeformation_query_wait_polling_delay: float, default: 2 seconds
        Interval in seconds for how often the function will check if the LakeFormation query has completed.


    Returns
    -------
    Dict[str, Any]
        Dictionary with the get_query_state response.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.lakeformation.wait_query(query_id='query-id')

    """
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)

    response = client_lakeformation.get_query_state(QueryId=query_id)
    state = response["State"]
    while state not in _QUERY_FINAL_STATES:
        time.sleep(lakeformation_query_wait_polling_delay)
        response = client_lakeformation.get_query_state(QueryId=query_id)
        state = response["State"]
    _logger.debug("state: %s", state)
    if state == "ERROR":
        raise exceptions.QueryFailed(response.get("Error"))
    return response  # type: ignore[return-value]
