"""Utilities Module for Amazon Lake Formation."""
import logging
import time
from typing import Any, Dict, List, Optional, Union

import boto3

from awswrangler import _utils, exceptions
from awswrangler.catalog._utils import _catalog_id
from awswrangler.s3._describe import describe_objects

_QUERY_FINAL_STATES: List[str] = ["ERROR", "FINISHED"]
_QUERY_WAIT_POLLING_DELAY: float = 2  # SECONDS

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
    use_threads: bool,
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
        catalog_id=catalog_id, TransactionId=transaction_id, DatabaseName=database, TableName=table, MaxResults=100
    )
    if partition_cols and partitions_types and partitions_values:
        scan_kwargs["PartitionPredicate"] = _build_partition_predicate(
            partition_cols=partition_cols, partitions_types=partitions_types, partitions_values=partitions_values
        )

    next_token: str = "init_token"  # Dummy token
    table_objects: List[Dict[str, Any]] = []
    while next_token:
        response = client_lakeformation.get_table_objects(**scan_kwargs)
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
        catalog_id=catalog_id, TransactionId=transaction_id, DatabaseName=database, TableName=table
    )

    write_operations: List[Dict[str, Dict[str, Any]]] = []
    if add_objects:
        write_operations.extend({"AddObject": obj} for obj in add_objects)
    elif del_objects:
        write_operations.extend({"DeleteObject": _without_keys(obj, ["Size"])} for obj in del_objects)
    update_kwargs["WriteOperations"] = write_operations

    client_lakeformation.update_table_objects(**update_kwargs)


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


def test_func() -> int:
    """
    My func.

    description
    """


def get_database_principal_permissions(
    catalog_id: str,
    database_name: str,
    principal_arn: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Dict[str, Any]]:
    """Get the principal permissions associated with a database lakeformation resource.

    Parameters
    ----------
    catalog_id : str
        The catalog id.

    Returns
    -------
    [Dict]
        Returns a list with the resource permissions.

    """
    resource = {"Database": {"CatalogId": catalog_id, "Name": database_name}}

    list_kwargs: Dict[str, Union[str, Dict[str, Any]]] = _catalog_id(catalog_id=catalog_id, Resource=resource)

    if principal_arn is not None:
        list_kwargs["Principal"] = {"DataLakePrincipalIdentifier": principal_arn}

    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    principal_permissions: List[Dict[str, Any]] = []

    next_token: str = "init_token"  # Dummy token
    while next_token:
        result = client_lakeformation.list_permissions(**list_kwargs)
        principal_permissions.extend(result["PrincipalResourcePermissions"])
        next_token = result.get("NextToken", None)
        list_kwargs["NextToken"] = next_token

    return principal_permissions




def revoke_database_permissions(
    resource_catalog_id: str,
    database_name: str,
    principal_arn: str,
    permissions: List[str],
    permissions_with_grant: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Revoke principal permissions in a Lake Formation database resource.

    Parameters
    ----------
    catalog_id : str
        The catalog id.

    Returns
    -------
    None
        Returns None

    """
    resource = {
        "Database": {
            "CatalogId": resource_catalog_id,
            "Name": database_name,
        }
    }

    principal_identifier = {"DataLakePrincipalIdentifier": principal_arn}

    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    if permissions_with_grant is None:
        client_lakeformation.revoke_permissions(
            Principal=principal_identifier, CatalogId=resource_catalog_id, Resource=resource, Permissions=permissions
        )
    else:
        client_lakeformation.revoke_permissions(
            Principal=principal_identifier,
            CatalogId=resource_catalog_id,
            Resource=resource,
            Permissions=permissions,
            PermissionsWithGrantOption=permissions_with_grant,
        )


def grant_database_permissions(
    resource_catalog_id: str,
    database_name: str,
    principal_arn: str,
    permissions: List[str],
    permissions_with_grant: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Grant principal permissions in a Lake Formation database resource.

    Parameters
    ----------
    catalog_id : str
        The catalog id.

    Returns
    -------
    None
        Returns None

    """
    resource = {
        "Database": {
            "CatalogId": resource_catalog_id,
            "Name": database_name,
        }
    }

    principal_identifier = {"DataLakePrincipalIdentifier": principal_arn}

    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    if permissions_with_grant is None:
        client_lakeformation.grant_permissions(
            Principal=principal_identifier, CatalogId=resource_catalog_id, Resource=resource, Permissions=permissions
        )
    else:
        client_lakeformation.grant_permissions(
            Principal=principal_identifier,
            CatalogId=resource_catalog_id,
            Resource=resource,
            Permissions=permissions,
            PermissionsWithGrantOption=permissions_with_grant,
        )
