"""Utilities Module for Amazon Athena."""
import csv
import datetime
import logging
import pprint
import time
import warnings
from decimal import Decimal
from heapq import heappop, heappush
from typing import Any, Dict, Generator, List, NamedTuple, Optional, Tuple, Union, cast

import boto3
import botocore.exceptions
import pandas as pd

from awswrangler import _data_types, _utils, exceptions, s3, sts
from awswrangler._config import apply_configs

_QUERY_FINAL_STATES: List[str] = ["FAILED", "SUCCEEDED", "CANCELLED"]
_QUERY_WAIT_POLLING_DELAY: float = 0.25  # SECONDS

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
    raw_payload: Dict[str, Any]


class _WorkGroupConfig(NamedTuple):
    enforced: bool
    s3_output: Optional[str]
    encryption: Optional[str]
    kms_key: Optional[str]


class _LocalMetadataCacheManager:
    def __init__(self) -> None:
        self._cache: Dict[str, Any] = dict()
        self._pqueue: List[Tuple[datetime.datetime, str]] = []
        self._max_cache_size = 100

    def update_cache(self, items: List[Dict[str, Any]]) -> None:
        """
        Update the local metadata cache with new query metadata.

        Parameters
        ----------
        items : List[Dict[str, Any]]
            List of query execution metadata which is returned by boto3 `batch_get_query_execution()`.

        Returns
        -------
        None
            None.
        """
        if self._pqueue:
            oldest_item = self._cache[self._pqueue[0][1]]
            items = list(
                filter(lambda x: x["Status"]["SubmissionDateTime"] > oldest_item["Status"]["SubmissionDateTime"], items)
            )

        cache_oversize = len(self._cache) + len(items) - self._max_cache_size
        for _ in range(cache_oversize):
            _, query_execution_id = heappop(self._pqueue)
            del self._cache[query_execution_id]

        for item in items[: self._max_cache_size]:
            heappush(self._pqueue, (item["Status"]["SubmissionDateTime"], item["QueryExecutionId"]))
            self._cache[item["QueryExecutionId"]] = item

    def sorted_successful_generator(self) -> List[Dict[str, Any]]:
        """
        Sorts the entries in the local cache based on query Completion DateTime.

        This is useful to guarantee LRU caching rules.

        Returns
        -------
        List[Dict[str, Any]]
            Returns successful DDL and DML queries sorted by query completion time.
        """
        filtered: List[Dict[str, Any]] = []
        for query in self._cache.values():
            if (query["Status"].get("State") == "SUCCEEDED") and (query.get("StatementType") in ["DDL", "DML"]):
                filtered.append(query)
        return sorted(filtered, key=lambda e: str(e["Status"]["CompletionDateTime"]), reverse=True)

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    @property
    def max_cache_size(self) -> int:
        """Property max_cache_size."""
        return self._max_cache_size

    @max_cache_size.setter
    def max_cache_size(self, value: int) -> None:
        self._max_cache_size = value


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
    data_source: Optional[str] = None,
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
        if data_source is not None:
            args["QueryExecutionContext"]["Catalog"] = data_source

    # workgroup
    if workgroup is not None:
        args["WorkGroup"] = workgroup

    client_athena: boto3.client = _utils.client(service_name="athena", session=session)
    _logger.debug("args: \n%s", pprint.pformat(args))
    response: Dict[str, Any] = _utils.try_it(
        f=client_athena.start_query_execution,
        ex=botocore.exceptions.ClientError,
        ex_code="ThrottlingException",
        max_num_tries=5,
        **args,
    )
    return cast(str, response["QueryExecutionId"])


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


def _fetch_txt_result(
    query_metadata: _QueryMetadata,
    keep_files: bool,
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
) -> pd.DataFrame:
    if query_metadata.output_location is None or query_metadata.output_location.endswith(".txt") is False:
        return pd.DataFrame()
    path: str = query_metadata.output_location
    _logger.debug("Start TXT reading from %s", path)
    df = s3.read_csv(
        path=[path],
        dtype=query_metadata.dtype,
        parse_dates=query_metadata.parse_timestamps,
        converters=query_metadata.converters,
        quoting=csv.QUOTE_ALL,
        keep_default_na=False,
        skip_blank_lines=True,
        na_values=[],
        use_threads=False,
        boto3_session=boto3_session,
        names=list(query_metadata.dtype.keys()),
        sep="\t",
    )
    if keep_files is False:
        s3.delete_objects(
            path=[path, f"{path}.metadata"],
            use_threads=False,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
    return df


def _parse_describe_table(df: pd.DataFrame) -> pd.DataFrame:
    origin_df_dict = df.to_dict()
    target_df_dict: Dict[str, List[Union[str, bool]]] = {"Column Name": [], "Type": [], "Partition": [], "Comment": []}
    for index, col_name in origin_df_dict["col_name"].items():
        col_name = col_name.strip()
        if col_name.startswith("#") or col_name == "":
            pass
        elif col_name in target_df_dict["Column Name"]:
            index_col_name = target_df_dict["Column Name"].index(col_name)
            target_df_dict["Partition"][index_col_name] = True
        else:
            target_df_dict["Column Name"].append(col_name)
            target_df_dict["Type"].append(origin_df_dict["data_type"][index].strip())
            target_df_dict["Partition"].append(False)
            target_df_dict["Comment"].append(origin_df_dict["comment"][index].strip())
    return pd.DataFrame(data=target_df_dict)


def _get_query_metadata(  # pylint: disable=too-many-statements
    query_execution_id: str,
    boto3_session: boto3.Session,
    categories: Optional[List[str]] = None,
    query_execution_payload: Optional[Dict[str, Any]] = None,
    metadata_cache_manager: Optional[_LocalMetadataCacheManager] = None,
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
        output_location = _query_execution_payload["ResultConfiguration"].get("OutputLocation")

    athena_statistics: Dict[str, Union[int, str]] = _query_execution_payload.get("Statistics", {})
    manifest_location: Optional[str] = str(athena_statistics.get("DataManifestLocation"))

    if metadata_cache_manager is not None and query_execution_id not in metadata_cache_manager:
        metadata_cache_manager.update_cache(items=[_query_execution_payload])
    query_metadata: _QueryMetadata = _QueryMetadata(
        execution_id=query_execution_id,
        dtype=dtype,
        parse_timestamps=parse_timestamps,
        parse_dates=parse_dates,
        converters=converters,
        binaries=binaries,
        output_location=output_location,
        manifest_location=manifest_location,
        raw_payload=_query_execution_payload,
    )
    _logger.debug("query_metadata:\n%s", query_metadata)
    return query_metadata


def _empty_dataframe_response(
    chunked: bool, query_metadata: _QueryMetadata
) -> Union[pd.DataFrame, Generator[None, None, None]]:
    """Generate an empty dataframe response."""
    if chunked is False:
        df = pd.DataFrame()
        df = _apply_query_metadata(df=df, query_metadata=query_metadata)
        return df
    return _utils.empty_generator()


def _apply_query_metadata(df: pd.DataFrame, query_metadata: _QueryMetadata) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        df.query_metadata = query_metadata.raw_payload
    return df


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
    s3_resource = _utils.resource(service_name="s3", session=session)
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
    data_source: Optional[str] = None,
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
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.

    Returns
    -------
    str
        Query execution ID

    Examples
    --------
    Querying into the default data source (Amazon s3 - 'AwsDataCatalog')

    >>> import awswrangler as wr
    >>> query_exec_id = wr.athena.start_query_execution(sql='...', database='...')

    Querying into another data source (PostgreSQL, Redshift, etc)

    >>> import awswrangler as wr
    >>> query_exec_id = wr.athena.start_query_execution(sql='...', database='...', data_source='...')

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    wg_config: _WorkGroupConfig = _get_workgroup_config(session=session, workgroup=workgroup)
    return _start_query_execution(
        sql=sql,
        wg_config=wg_config,
        database=database,
        data_source=data_source,
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
    return cast(str, response["Status"]["State"])


@apply_configs
def describe_table(
    table: str,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    """Show the list of columns, including partition columns: 'DESCRIBE table;'.

    Shows the list of columns, including partition columns, for the named column.
    The result of this function will be equal to `wr.catalog.table`.

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
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "RequestPayer".
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame filled by formatted infos.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_table = wr.athena.describe_table(table='my_table', database='default')

    """
    query = f"DESCRIBE `{table}`;"
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
    query_metadata: _QueryMetadata = _get_query_metadata(query_execution_id=query_id, boto3_session=session)
    raw_result = _fetch_txt_result(
        query_metadata=query_metadata, keep_files=True, boto3_session=session, s3_additional_kwargs=s3_additional_kwargs
    )
    return _parse_describe_table(raw_result)


@apply_configs
def show_create_table(
    table: str,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Generate the query that created it: 'SHOW CREATE TABLE table;'.

    Analyzes an existing table named table_name to generate the query that created it.

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
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "RequestPayer".
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        The query that created the table.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_table = wr.athena.show_create_table(table='my_table', database='default')

    """
    query = f"SHOW CREATE TABLE `{table}`;"
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
    query_metadata: _QueryMetadata = _get_query_metadata(query_execution_id=query_id, boto3_session=session)
    raw_result = _fetch_txt_result(
        query_metadata=query_metadata, keep_files=True, boto3_session=session, s3_additional_kwargs=s3_additional_kwargs
    )
    return cast(str, raw_result.createtab_stmt.str.strip().str.cat(sep=" "))


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
    return cast(
        Dict[str, Any],
        _utils.try_it(
            f=client_athena.get_work_group,
            ex=botocore.exceptions.ClientError,
            ex_code="ThrottlingException",
            max_num_tries=5,
            WorkGroup=workgroup,
        ),
    )


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
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    response: Dict[str, Any] = get_query_execution(query_execution_id=query_execution_id, boto3_session=session)
    state: str = response["Status"]["State"]
    while state not in _QUERY_FINAL_STATES:
        time.sleep(_QUERY_WAIT_POLLING_DELAY)
        response = get_query_execution(query_execution_id=query_execution_id, boto3_session=session)
        state = response["Status"]["State"]
    _logger.debug("state: %s", state)
    _logger.debug("StateChangeReason: %s", response["Status"].get("StateChangeReason"))
    if state == "FAILED":
        raise exceptions.QueryFailed(response["Status"].get("StateChangeReason"))
    if state == "CANCELLED":
        raise exceptions.QueryCancelled(response["Status"].get("StateChangeReason"))
    return response


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
    response: Dict[str, Any] = _utils.try_it(
        f=client_athena.get_query_execution,
        ex=botocore.exceptions.ClientError,
        ex_code="ThrottlingException",
        max_num_tries=5,
        QueryExecutionId=query_execution_id,
    )
    return cast(Dict[str, Any], response["QueryExecution"])
