"""Utilities Module for Amazon Athena."""

import logging
import pprint
import time
from decimal import Decimal
from typing import Any, Dict, List, NamedTuple, Optional

import boto3  # type: ignore

from awswrangler import _data_types, _utils, exceptions, sts
from awswrangler._config import apply_configs

_QUERY_FINAL_STATES: List[str] = ["FAILED", "SUCCEEDED", "CANCELLED"]
_QUERY_WAIT_POLLING_DELAY: float = 0.2  # SECONDS

_logger: logging.Logger = logging.getLogger(__name__)


class _QueryMetadata(NamedTuple):
    execution_id: str
    dtype: Dict[str, str]
    parse_timestamps: List[str]
    parse_dates: List[str]
    converters: Dict[str, Any]
    binaries: List[str]
    output_location: Optional[str]
    manifest_location: Optional[str]


class _WorkGroupConfig(NamedTuple):
    enforced: bool
    s3_output: Optional[str]
    encryption: Optional[str]
    kms_key: Optional[str]


def _get_s3_output(s3_output: Optional[str], wg_config: _WorkGroupConfig, boto3_session: boto3.Session) -> str:
    if wg_config.enforced and wg_config.s3_output is not None:
        return wg_config.s3_output
    if s3_output is not None:
        return s3_output
    if wg_config.s3_output is not None:
        return wg_config.s3_output
    return create_athena_bucket(boto3_session=boto3_session)


def _start_query_execution(
    sql: str,
    wg_config: _WorkGroupConfig,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    args: Dict[str, Any] = {"QueryString": sql}
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    # s3_output
    args["ResultConfiguration"] = {
        "OutputLocation": _get_s3_output(s3_output=s3_output, wg_config=wg_config, boto3_session=session)
    }

    # encryption
    if wg_config.enforced is True:
        if wg_config.encryption is not None:
            args["ResultConfiguration"]["EncryptionConfiguration"] = {"EncryptionOption": wg_config.encryption}
            if wg_config.kms_key is not None:
                args["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"] = wg_config.kms_key
    else:
        if encryption is not None:
            args["ResultConfiguration"]["EncryptionConfiguration"] = {"EncryptionOption": encryption}
            if kms_key is not None:
                args["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"] = kms_key

    # database
    if database is not None:
        args["QueryExecutionContext"] = {"Database": database}

    # workgroup
    if workgroup is not None:
        args["WorkGroup"] = workgroup

    client_athena: boto3.client = _utils.client(service_name="athena", session=session)
    _logger.debug("args: \n%s", pprint.pformat(args))
    response: Dict[str, Any] = client_athena.start_query_execution(**args)
    return response["QueryExecutionId"]


def _get_workgroup_config(session: boto3.Session, workgroup: Optional[str] = None) -> _WorkGroupConfig:
    if workgroup is not None:
        res: Dict[str, Any] = get_work_group(workgroup=workgroup, boto3_session=session)
        enforced: bool = res["WorkGroup"]["Configuration"]["EnforceWorkGroupConfiguration"]
        config: Dict[str, Any] = res["WorkGroup"]["Configuration"]["ResultConfiguration"]
        wg_s3_output: Optional[str] = config.get("OutputLocation")
        encrypt_config: Optional[Dict[str, str]] = config.get("EncryptionConfiguration")
        wg_encryption: Optional[str] = None if encrypt_config is None else encrypt_config.get("EncryptionOption")
        wg_kms_key: Optional[str] = None if encrypt_config is None else encrypt_config.get("KmsKey")
    else:
        enforced, wg_s3_output, wg_encryption, wg_kms_key = False, None, None, None
    wg_config: _WorkGroupConfig = _WorkGroupConfig(
        enforced=enforced, s3_output=wg_s3_output, encryption=wg_encryption, kms_key=wg_kms_key
    )
    _logger.debug("wg_config:\n%s", wg_config)
    return wg_config


def _get_query_metadata(
    query_execution_id: str,
    boto3_session: boto3.Session,
    categories: List[str] = None,
    query_execution_payload: Optional[Dict[str, Any]] = None,
) -> _QueryMetadata:
    """Get query metadata."""
    if (query_execution_payload is not None) and (query_execution_payload["Status"]["State"] in _QUERY_FINAL_STATES):
        if query_execution_payload["Status"]["State"] != "SUCCEEDED":
            reason: str = query_execution_payload["Status"]["StateChangeReason"]
            raise exceptions.QueryFailed(f"Query error: {reason}")
        _query_execution_payload: Dict[str, Any] = query_execution_payload
    else:
        _query_execution_payload = wait_query(query_execution_id=query_execution_id, boto3_session=boto3_session)
    cols_types: Dict[str, str] = get_query_columns_types(
        query_execution_id=query_execution_id, boto3_session=boto3_session
    )
    _logger.debug("cols_types: %s", cols_types)
    dtype: Dict[str, str] = {}
    parse_timestamps: List[str] = []
    parse_dates: List[str] = []
    converters: Dict[str, Any] = {}
    binaries: List[str] = []
    col_name: str
    col_type: str
    for col_name, col_type in cols_types.items():
        if col_type == "array":
            raise exceptions.UnsupportedType(
                "List data type is not support with ctas_approach=False. "
                "Please use ctas_approach=True for List columns."
            )
        if col_type == "row":
            raise exceptions.UnsupportedType(
                "Struct data type is not support with ctas_approach=False. "
                "Please use ctas_approach=True for Struct columns."
            )
        pandas_type: str = _data_types.athena2pandas(dtype=col_type)
        if (categories is not None) and (col_name in categories):
            dtype[col_name] = "category"
        elif pandas_type in ["datetime64", "date"]:
            parse_timestamps.append(col_name)
            if pandas_type == "date":
                parse_dates.append(col_name)
        elif pandas_type == "bytes":
            dtype[col_name] = "string"
            binaries.append(col_name)
        elif pandas_type == "decimal":
            converters[col_name] = lambda x: Decimal(str(x)) if str(x) not in ("", "none", " ", "<NA>") else None
        else:
            dtype[col_name] = pandas_type
    output_location: Optional[str] = None
    if "ResultConfiguration" in _query_execution_payload:
        if "OutputLocation" in _query_execution_payload["ResultConfiguration"]:
            output_location = _query_execution_payload["ResultConfiguration"]["OutputLocation"]
    manifest_location: Optional[str] = None
    if "Statistics" in _query_execution_payload:
        if "DataManifestLocation" in _query_execution_payload["Statistics"]:
            manifest_location = _query_execution_payload["Statistics"]["DataManifestLocation"]
    query_metadata: _QueryMetadata = _QueryMetadata(
        execution_id=query_execution_id,
        dtype=dtype,
        parse_timestamps=parse_timestamps,
        parse_dates=parse_dates,
        converters=converters,
        binaries=binaries,
        output_location=output_location,
        manifest_location=manifest_location,
    )
    _logger.debug("query_metadata:\n%s", query_metadata)
    return query_metadata


def get_query_columns_types(query_execution_id: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, str]:
    """Get the data type of all columns queried.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
        Dictionary with all data types.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.get_query_columns_types('query-execution-id')
    {'col0': 'int', 'col1': 'double'}

    """
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    response: Dict[str, Any] = client_athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=1)
    col_info: List[Dict[str, str]] = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
    return {x["Name"]: x["Type"] for x in col_info}


def create_athena_bucket(boto3_session: Optional[boto3.Session] = None) -> str:
    """Create the default Athena bucket if it doesn't exist.

    Parameters
    ----------
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Bucket s3 path (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.create_athena_bucket()
    's3://aws-athena-query-results-ACCOUNT-REGION/'

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    account_id: str = sts.get_account_id(boto3_session=session)
    region_name: str = str(session.region_name).lower()
    s3_output = f"s3://aws-athena-query-results-{account_id}-{region_name}/"
    s3_resource = session.resource("s3")
    s3_resource.Bucket(s3_output)
    return s3_output


@apply_configs
def start_query_execution(
    sql: str,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Start a SQL Query against AWS Athena.

    Note
    ----
    Create the default Athena bucket if it doesn't exist and s3_output is None.
    (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Parameters
    ----------
    sql : str
        SQL query.
    database : str, optional
        AWS Glue/Athena database name.
    s3_output : str, optional
        AWS S3 path.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key : str, optional
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Query execution ID

    Examples
    --------
    >>> import awswrangler as wr
    >>> query_exec_id = wr.athena.start_query_execution(sql='...', database='...')

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    wg_config: _WorkGroupConfig = _get_workgroup_config(session=session, workgroup=workgroup)
    return _start_query_execution(
        sql=sql,
        wg_config=wg_config,
        database=database,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=session,
    )


@apply_configs
def repair_table(
    table: str,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Run the Hive's metastore consistency check: 'MSCK REPAIR TABLE table;'.

    Recovers partitions and data associated with partitions.
    Use this statement when you add partitions to the catalog.
    It is possible it will take some time to add all partitions.
    If this operation times out, it will be in an incomplete state
    where only a few partitions are added to the catalog.

    Note
    ----
    Create the default Athena bucket if it doesn't exist and s3_output is None.
    (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Parameters
    ----------
    table : str
        Table name.
    database : str, optional
        AWS Glue/Athena database name.
    s3_output : str, optional
        AWS S3 path.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key : str, optional
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Query final state ('SUCCEEDED', 'FAILED', 'CANCELLED').

    Examples
    --------
    >>> import awswrangler as wr
    >>> query_final_state = wr.athena.repair_table(table='...', database='...')

    """
    query = f"MSCK REPAIR TABLE `{table}`;"
    if (database is not None) and (not database.startswith("`")):
        database = f"`{database}`"
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    query_id = start_query_execution(
        sql=query,
        database=database,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=session,
    )
    response: Dict[str, Any] = wait_query(query_execution_id=query_id, boto3_session=session)
    return response["Status"]["State"]


def get_work_group(workgroup: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, Any]:
    """Return information about the workgroup with the specified name.

    Parameters
    ----------
    workgroup : str
        Work Group name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_work_group

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.get_work_group(workgroup='workgroup_name')

    """
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    return client_athena.get_work_group(WorkGroup=workgroup)


def stop_query_execution(query_execution_id: str, boto3_session: Optional[boto3.Session] = None) -> None:
    """Stop a query execution.

    Requires you to have access to the workgroup in which the query ran.

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.stop_query_execution(query_execution_id='query-execution-id')

    """
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    client_athena.stop_query_execution(QueryExecutionId=query_execution_id)


def wait_query(query_execution_id: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, Any]:
    """Wait for the query end.

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Dictionary with the get_query_execution response.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.wait_query(query_execution_id='query-execution-id')

    """
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    response: Dict[str, Any] = client_athena.get_query_execution(QueryExecutionId=query_execution_id)
    state: str = response["QueryExecution"]["Status"]["State"]
    while state not in _QUERY_FINAL_STATES:
        time.sleep(_QUERY_WAIT_POLLING_DELAY)
        response = client_athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]
    _logger.debug("state: %s", state)
    _logger.debug("StateChangeReason: %s", response["QueryExecution"]["Status"].get("StateChangeReason"))
    if state == "FAILED":
        raise exceptions.QueryFailed(response["QueryExecution"]["Status"].get("StateChangeReason"))
    if state == "CANCELLED":
        raise exceptions.QueryCancelled(response["QueryExecution"]["Status"].get("StateChangeReason"))
    return response["QueryExecution"]


def get_query_execution(query_execution_id: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, Any]:
    """Fetch query execution details.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_query_execution

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Dictionary with the get_query_execution response.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.get_query_execution(query_execution_id='query-execution-id')

    """
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    response: Dict[str, Any] = client_athena.get_query_execution(QueryExecutionId=query_execution_id)
    return response["QueryExecution"]
