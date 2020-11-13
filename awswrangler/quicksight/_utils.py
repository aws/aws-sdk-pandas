"""Internal (private) Amazon QuickSight Utilities Module."""

import logging
from typing import Any, Dict, List, Optional

import boto3

from awswrangler import _data_types, athena, catalog, exceptions
from awswrangler.quicksight._get_list import list_data_sources

_logger: logging.Logger = logging.getLogger(__name__)


def extract_athena_table_columns(database: str, table: str, boto3_session: boto3.Session) -> List[Dict[str, str]]:
    """Extract athena columns data types from table and raising an exception if not exist."""
    dtypes: Optional[Dict[str, str]] = catalog.get_table_types(
        database=database, table=table, boto3_session=boto3_session
    )
    if dtypes is None:
        raise exceptions.InvalidArgument(f"{database}.{table} does not exist on Athena.")
    return [{"Name": name, "Type": _data_types.athena2quicksight(dtype=dtype)} for name, dtype in dtypes.items()]


def extract_athena_query_columns(
    sql: str, data_source_arn: str, account_id: str, boto3_session: boto3.Session
) -> List[Dict[str, str]]:
    """Extract athena columns data types from a SQL query."""
    data_sources: List[Dict[str, Any]] = list_data_sources(account_id=account_id, boto3_session=boto3_session)
    data_source: Dict[str, Any] = [x for x in data_sources if x["Arn"] == data_source_arn][0]
    workgroup: str = data_source["DataSourceParameters"]["AthenaParameters"]["WorkGroup"]
    sql_wrapped: str = f"/* QuickSight */\nSELECT ds.* FROM ( {sql} ) ds LIMIT 0"
    query_id: str = athena.start_query_execution(sql=sql_wrapped, workgroup=workgroup, boto3_session=boto3_session)
    athena.wait_query(query_execution_id=query_id, boto3_session=boto3_session)
    dtypes: Dict[str, str] = athena.get_query_columns_types(query_execution_id=query_id, boto3_session=boto3_session)
    return [{"Name": name, "Type": _data_types.athena2quicksight(dtype=dtype)} for name, dtype in dtypes.items()]
