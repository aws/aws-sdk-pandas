"""Amazon Athena Module gathering all functions related to prepared statements."""

import logging
from typing import Any, Dict, List, Optional, cast

import boto3

from awswrangler import _utils
from awswrangler._config import apply_configs
from awswrangler.athena._executions import start_query_execution
from awswrangler.athena._utils import (
    _QUERY_WAIT_POLLING_DELAY,
)

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def prepare_statement(
    sql: str,
    statement_name: str,
    workgroup: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
) -> Dict[str, Any]:
    sql_statement = f"""
PREPARE "{statement_name}" FROM
{sql}
    """
    _logger.info(f"Creating prepared statement {statement_name}")
    return start_query_execution(
        sql=sql_statement,
        workgroup=workgroup,
        boto3_session=boto3_session,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        wait=True,
    )


@apply_configs
def list_prepared_statements(workgroup: str, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    athena_client = _utils.client("athena", session=boto3_session)

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
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    data_source: Optional[str] = None,
) -> Dict[str, Any]:
    _logger.info(f"Deallocating prepared statement {statement_name}")
    return start_query_execution(
        sql=f'DEALLOCATE PREPARE "{statement_name}"',
        workgroup=workgroup,
        boto3_session=boto3_session,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        data_source=data_source,
        wait=True,
    )
