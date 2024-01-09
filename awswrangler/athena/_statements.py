"""Amazon Athena Module gathering all functions related to prepared statements."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Literal, cast

import boto3
from botocore.exceptions import ClientError

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs

if TYPE_CHECKING:
    from mypy_boto3_athena.client import AthenaClient

_logger: logging.Logger = logging.getLogger(__name__)


def _does_statement_exist(
    statement_name: str,
    workgroup: str,
    athena_client: "AthenaClient",
) -> bool:
    try:
        athena_client.get_prepared_statement(StatementName=statement_name, WorkGroup=workgroup)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False

        raise e

    return True


@apply_configs
def create_prepared_statement(
    sql: str,
    statement_name: str,
    workgroup: str = "primary",
    mode: Literal["update", "error"] = "update",
    boto3_session: boto3.Session | None = None,
) -> None:
    """
    Create a SQL statement with the name statement_name to be run at a later time. The statement can include parameters represented by question marks.

    https://docs.aws.amazon.com/athena/latest/ug/sql-prepare.html

    Parameters
    ----------
    sql : str
        The query string for the prepared statement.
    statement_name : str
        The name of the prepared statement.
    workgroup : str
        The name of the workgroup to which the prepared statement belongs. Primary by default.
    mode: str
        Determines the behaviour if the prepared statement already exists:

        - ``update`` - updates statement if already exists
        - ``error`` - throws an error if table exists
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.create_prepared_statement(
    ...     sql="SELECT * FROM my_table WHERE name = ?",
    ...     statement_name="statement",
    ... )
    """
    if mode not in ["update", "error"]:
        raise exceptions.InvalidArgumentValue("`mode` must be one of 'update' or 'error'.")

    athena_client = _utils.client("athena", session=boto3_session)

    already_exists = _does_statement_exist(statement_name, workgroup, athena_client)
    if already_exists and mode == "error":
        raise exceptions.AlreadyExists(f"Prepared statement {statement_name} already exists.")

    if already_exists:
        _logger.info(f"Updating prepared statement {statement_name}")
        athena_client.update_prepared_statement(
            StatementName=statement_name,
            WorkGroup=workgroup,
            QueryStatement=sql,
        )
    else:
        _logger.info(f"Creating prepared statement {statement_name}")
        athena_client.create_prepared_statement(
            StatementName=statement_name,
            WorkGroup=workgroup,
            QueryStatement=sql,
        )


@apply_configs
def list_prepared_statements(workgroup: str = "primary", boto3_session: boto3.Session | None = None) -> list[str]:
    """
    List the prepared statements in the specified workgroup.

    Parameters
    ----------
    workgroup: str
        The name of the workgroup to which the prepared statement belongs. Primary by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        List of prepared statements in the workgroup.
        Each item is a dictionary with the keys ``StatementName`` and ``LastModifiedTime``.
    """
    athena_client = _utils.client("athena", session=boto3_session)

    response = athena_client.list_prepared_statements(WorkGroup=workgroup)
    statements = response["PreparedStatements"]

    while "NextToken" in response:
        response = athena_client.list_prepared_statements(WorkGroup=workgroup, NextToken=response["NextToken"])
        statements += response["PreparedStatements"]

    return cast(List[Dict[str, Any]], statements)


@apply_configs
def delete_prepared_statement(
    statement_name: str,
    workgroup: str = "primary",
    boto3_session: boto3.Session | None = None,
) -> None:
    """
    Delete the prepared statement with the specified name from the specified workgroup.

    https://docs.aws.amazon.com/athena/latest/ug/sql-deallocate-prepare.html

    Parameters
    ----------
    statement_name : str
        The name of the prepared statement.
    workgroup : str, optional
        The name of the workgroup to which the prepared statement belongs. Primary by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.delete_prepared_statement(
    ...     statement_name="statement",
    ... )
    """
    athena_client = _utils.client("athena", session=boto3_session)
    workgroup = workgroup if workgroup else "primary"

    _logger.info(f"Deallocating prepared statement {statement_name}")
    athena_client.delete_prepared_statement(
        StatementName=statement_name,
        WorkGroup=workgroup,
    )
