"""Internal (private) Amazon QuickSight Utilities Module."""

import logging
from typing import Any, Dict, List, Optional

import boto3  # type: ignore

from awswrangler import _data_types, _utils, athena, catalog, exceptions
from awswrangler.quicksight import _get, _list

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
    data_sources: List[Dict[str, Any]] = _list.list_data_sources(account_id=account_id, boto3_session=boto3_session)
    data_source: Dict[str, Any] = [x for x in data_sources if x["Arn"] == data_source_arn][0]
    workgroup: str = data_source["DataSourceParameters"]["AthenaParameters"]["WorkGroup"]
    sql_wrapped: str = f"/* QuickSight */\nSELECT ds.* FROM ( {sql} ) ds LIMIT 0"
    query_id: str = athena.start_query_execution(sql=sql_wrapped, workgroup=workgroup, boto3_session=boto3_session)
    athena.wait_query(query_execution_id=query_id, boto3_session=boto3_session)
    dtypes: Dict[str, str] = athena.get_query_columns_types(query_execution_id=query_id, boto3_session=boto3_session)
    return [{"Name": name, "Type": _data_types.athena2quicksight(dtype=dtype)} for name, dtype in dtypes.items()]


def list_ingestions(
    dataset_name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Dict[str, Any]]:
    """List the history of SPICE ingestions for a dataset.

    Parameters
    ----------
    dataset_name : str, optional
        Dataset name.
    dataset_id : str, optional
        The ID of the dataset used in the ingestion.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        IAM policy assignments.

    Examples
    --------
    >>> import awswrangler as wr
    >>> ingestions = wr.quicksight.list_ingestions()
    """
    if (dataset_name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dataset_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = _utils.get_account_id(boto3_session=session)
    if (dataset_id is None) and (dataset_name is not None):
        dataset_id = _get.get_dataset_id(name=dataset_name, account_id=account_id, boto3_session=session)
    return _list._list(  # pylint: disable=protected-access
        func_name="list_ingestions",
        attr_name="Ingestions",
        account_id=account_id,
        boto3_session=boto3_session,
        DataSetId=dataset_id,
    )
