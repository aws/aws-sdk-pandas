"""Amazon Athena Module gathering all functions related to prepared statements."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, cast

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
def prepare_statement(
    sql: str,
    statement_name: str,
    workgroup: Optional[str] = None,
    mode: Literal["update", "error"] = "update",
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    if mode not in ["update", "error"]:
        raise exceptions.InvalidArgumentValue("`mode` must be one of 'update' or 'error'.")

    athena_client = _utils.client("athena", session=boto3_session)
    workgroup = workgroup if workgroup else "primary"

    already_exists = _does_statement_exist(statement_name, workgroup, athena_client)
    if already_exists and mode == "error":
        raise exceptions.AlreadyExists(f"Prepared statement {statement_name} already exists.")

    if already_exists:
        _logger.info(f"Updating prepared statement {statement_name}")
        return athena_client.update_prepared_statement(
            StatementName=statement_name,
            WorkGroup=workgroup,
            QueryStatement=sql,
        )

    _logger.info(f"Creating prepared statement {statement_name}")
    return athena_client.create_prepared_statement(
        StatementName=statement_name,
        WorkGroup=workgroup,
        QueryStatement=sql,
    )


@apply_configs
def list_prepared_statements(
    workgroup: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[str]:
    athena_client = _utils.client("athena", session=boto3_session)
    workgroup = workgroup if workgroup else "primary"

    response = athena_client.list_prepared_statements(WorkGroup=workgroup)
    statements = response["PreparedStatements"]

    while "NextToken" in response:
        response = athena_client.list_prepared_statements(WorkGroup=workgroup, NextToken=response["NextToken"])
        statements += response["PreparedStatements"]

    return cast(List[Dict[str, Any]], statements)


@apply_configs
def deallocate_prepared_statement(
    statement_name: str,
    workgroup: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    athena_client = _utils.client("athena", session=boto3_session)
    workgroup = workgroup if workgroup else "primary"

    _logger.info(f"Deallocating prepared statement {statement_name}")
    return athena_client.delete_prepared_statement(
        StatementName=statement_name,
        WorkGroup=workgroup,
    )
