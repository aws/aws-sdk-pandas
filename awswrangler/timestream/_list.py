"""Amazon Timestream List Module."""

from __future__ import annotations

import logging

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def list_databases(
    boto3_session: boto3.Session | None = None,
) -> list[str]:
    """
    List all databases in timestream.

    Parameters
    ----------
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        a list of available timestream databases.

    Examples
    --------
    Querying the list of all available databases

    >>> import awswrangler as wr
    >>> wr.timestream.list_databases()
    ... ["database1", "database2"]


    """
    client = _utils.client(service_name="timestream-write", session=boto3_session)

    response = client.list_databases()
    dbs: list[str] = [db["DatabaseName"] for db in response["Databases"]]
    while "NextToken" in response:
        response = client.list_databases(NextToken=response["NextToken"])
        dbs += [db["DatabaseName"] for db in response["Databases"]]

    return dbs


def list_tables(database: str | None = None, boto3_session: boto3.Session | None = None) -> list[str]:
    """
    List tables in timestream.

    Parameters
    ----------
    database: str
        Database name. If None, all tables in Timestream will be returned. Otherwise, only the tables inside the
        given database are returned.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        A list of table names.

    Examples
    --------
    Listing all tables in timestream across databases

    >>> import awswrangler as wr
    >>> wr.timestream.list_tables()
    ... ["table1", "table2"]

    Listing all tables in timestream in a specific database

    >>> import awswrangler as wr
    >>> wr.timestream.list_tables(DatabaseName="database1")
    ... ["table1"]

    """
    client = _utils.client(service_name="timestream-write", session=boto3_session)
    args = {} if database is None else {"DatabaseName": database}
    response = client.list_tables(**args)  # type: ignore[arg-type]
    tables: list[str] = [tbl["TableName"] for tbl in response["Tables"]]
    while "NextToken" in response:
        response = client.list_tables(**args, NextToken=response["NextToken"])  # type: ignore[arg-type]
        tables += [tbl["TableName"] for tbl in response["Tables"]]

    return tables
