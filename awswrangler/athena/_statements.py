"""Amazon Athena Module gathering all functions related to prepared statements."""

import logging
from typing import Any, Dict, Optional

import boto3

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
    database: Optional[str] = None,
    workgroup: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    data_source: Optional[str] = None,
) -> Dict[str, Any]:
    sql_statement = f"""
PREPARE "{statement_name}" FROM
{sql}
    """
    return start_query_execution(
        sql=sql_statement,
        database=database,
        workgroup=workgroup,
        boto3_session=boto3_session,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        data_source=data_source,
        wait=True,
    )


@apply_configs
def deallocate_prepared_statement(
    statement_name: str,
    workgroup: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    data_source: Optional[str] = None,
) -> Dict[str, Any]:
    return start_query_execution(
        sql=f'DEALLOCATE PREPARE "{statement_name}"',
        workgroup=workgroup,
        boto3_session=boto3_session,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        data_source=data_source,
        wait=True,
    )
