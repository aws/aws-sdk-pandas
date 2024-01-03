"""Amazon Timestream Delete Module."""

from __future__ import annotations

import logging

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def delete_database(
    database: str,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete a given Timestream database. This is an irreversible operation.

    After a database is deleted, the time series data from its tables cannot be recovered.

    All tables in the database must be deleted first, or a ValidationException error will be thrown.

    Due to the nature of distributed retries,
    the operation can return either success or a ResourceNotFoundException.
    Clients should consider them equivalent.

    Parameters
    ----------
    database: str
        Database name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    Deleting a database

    >>> import awswrangler as wr
    >>> arn = wr.timestream.delete_database("MyDatabase")

    """
    _logger.info("Deleting Timestream database %s", database)
    client = _utils.client(service_name="timestream-write", session=boto3_session)
    client.delete_database(DatabaseName=database)


def delete_table(
    database: str,
    table: str,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete a given Timestream table.

    This is an irreversible operation.

    After a Timestream database table is deleted, the time series data stored in the table cannot be recovered.

    Due to the nature of distributed retries,
    the operation can return either success or a ResourceNotFoundException.
    Clients should consider them equivalent.

    Parameters
    ----------
    database: str
        Database name.
    table: str
        Table name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    Deleting a table

    >>> import awswrangler as wr
    >>> arn = wr.timestream.delete_table("MyDatabase", "MyTable")

    """
    _logger.info("Deleting Timestream table %s in database %s", table, database)
    client = _utils.client(service_name="timestream-write", session=boto3_session)
    client.delete_table(DatabaseName=database, TableName=table)
