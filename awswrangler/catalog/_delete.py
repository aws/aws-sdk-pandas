"""AWS Glue Catalog Delete Module."""

import logging
from typing import Optional

import boto3  # type: ignore

from awswrangler import _utils
from awswrangler._config import apply_configs
from awswrangler.catalog._utils import _catalog_id

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def delete_database(name: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> None:
    """Create a database in AWS Glue Catalog.

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
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    if catalog_id is not None:
        client_glue.delete_database(CatalogId=catalog_id, Name=name)
    else:
        client_glue.delete_database(Name=name)


@apply_configs
def delete_table_if_exists(
    database: str, table: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
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
    >>> wr.catalog.delete_table_if_exists(database='default', name='my_table')  # deleted
    True
    >>> wr.catalog.delete_table_if_exists(database='default', name='my_table')  # Nothing to be deleted
    False

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        client_glue.delete_table(**_catalog_id(DatabaseName=database, Name=table, catalog_id=catalog_id))
        return True
    except client_glue.exceptions.EntityNotFoundException:
        return False
