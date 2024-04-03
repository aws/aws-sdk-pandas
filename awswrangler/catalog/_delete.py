"""AWS Glue Catalog Delete Module."""

from __future__ import annotations

import logging
from typing import Any

import boto3

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.catalog._definitions import _update_table_definition
from awswrangler.catalog._get import _get_partitions
from awswrangler.catalog._utils import _catalog_id

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def delete_database(name: str, catalog_id: str | None = None, boto3_session: boto3.Session | None = None) -> None:
    """Delete a database in AWS Glue Catalog.

    Parameters
    ----------
    name : str
        Database name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.delete_database(
    ...     name='awswrangler_test'
    ... )
    """
    client_glue = _utils.client(service_name="glue", session=boto3_session)
    client_glue.delete_database(**_catalog_id(Name=name, catalog_id=catalog_id))


@apply_configs
def delete_table_if_exists(
    database: str,
    table: str,
    catalog_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> bool:
    """Delete Glue table if exists.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    bool
        True if deleted, otherwise False.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.delete_table_if_exists(database='default', table='my_table')  # deleted
    True
    >>> wr.catalog.delete_table_if_exists(database='default', table='my_table')  # Nothing to be deleted
    False

    """
    client_glue = _utils.client(service_name="glue", session=boto3_session)
    try:
        client_glue.delete_table(**_catalog_id(DatabaseName=database, Name=table, catalog_id=catalog_id))
        return True
    except client_glue.exceptions.EntityNotFoundException:
        return False


@apply_configs
def delete_partitions(
    table: str,
    database: str,
    partitions_values: list[list[str]],
    catalog_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete specified partitions in a AWS Glue Catalog table.

    Parameters
    ----------
    table : str
        Table name.
    database : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    partitions_values : List[List[str]]
        List of lists of partitions values as strings.
        (e.g. [['2020', '10', '25'], ['2020', '11', '16'], ['2020', '12', '19']]).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.delete_partitions(
    ...     table='my_table',
    ...     database='awswrangler_test',
    ...     partitions_values=[['2020', '10', '25'], ['2020', '11', '16'], ['2020', '12', '19']]
    ... )
    """
    client_glue = _utils.client(service_name="glue", session=boto3_session)
    chunks: list[list[list[str]]] = _utils.chunkify(lst=partitions_values, max_length=25)
    for chunk in chunks:
        client_glue.batch_delete_partition(
            **_catalog_id(
                catalog_id=catalog_id,
                DatabaseName=database,
                TableName=table,
                PartitionsToDelete=[{"Values": v} for v in chunk],
            )
        )


@apply_configs
def delete_all_partitions(
    table: str, database: str, catalog_id: str | None = None, boto3_session: boto3.Session | None = None
) -> list[list[str]]:
    """Delete all partitions in a AWS Glue Catalog table.

    Parameters
    ----------
    table : str
        Table name.
    database : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[List[str]]
        Partitions values.

    Examples
    --------
    >>> import awswrangler as wr
    >>> partitions = wr.catalog.delete_all_partitions(
    ...     table='my_table',
    ...     database='awswrangler_test',
    ... )
    """
    _logger.debug("Fetching existing partitions...")
    partitions_values: list[list[str]] = list(
        _get_partitions(database=database, table=table, boto3_session=boto3_session, catalog_id=catalog_id).values()
    )
    _logger.debug("Number of old partitions: %s", len(partitions_values))
    _logger.debug("Deleting existing partitions...")
    delete_partitions(
        table=table,
        database=database,
        catalog_id=catalog_id,
        partitions_values=partitions_values,
        boto3_session=boto3_session,
    )
    return partitions_values


@apply_configs
def delete_column(
    database: str,
    table: str,
    column_name: str,
    boto3_session: boto3.Session | None = None,
    catalog_id: str | None = None,
) -> None:
    """Delete a column in a AWS Glue Catalog table.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    column_name : str
        Column name
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.

    Returns
    -------
    None
        None

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.delete_column(
    ...     database='my_db',
    ...     table='my_table',
    ...     column_name='my_col',
    ... )
    """
    client_glue = _utils.client(service_name="glue", session=boto3_session)
    table_res = client_glue.get_table(**_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table))
    table_input: dict[str, Any] = _update_table_definition(table_res)
    table_input["StorageDescriptor"]["Columns"] = [
        i for i in table_input["StorageDescriptor"]["Columns"] if i["Name"] != column_name
    ]
    res: dict[str, Any] = client_glue.update_table(
        **_catalog_id(catalog_id=catalog_id, DatabaseName=database, TableInput=table_input)
    )
    if ("Errors" in res) and res["Errors"]:
        for error in res["Errors"]:
            if "ErrorDetail" in error:
                if "ErrorCode" in error["ErrorDetail"]:
                    raise exceptions.ServiceApiError(str(res["Errors"]))
