"""Utilities Module for Amazon Athena."""
import base64
import csv
import json
import logging
import pprint
import uuid
import warnings
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    Union,
    cast,
)

import boto3
import botocore.exceptions
import pandas as pd
from typing_extensions import Literal

from awswrangler import _data_types, _utils, catalog, exceptions, s3, sts, typing
from awswrangler._config import apply_configs
from awswrangler._sql_formatter import _process_sql_params
from awswrangler.catalog._utils import _catalog_id, _transaction_id

from . import _executions
from ._cache import _cache_manager, _LocalMetadataCacheManager

if TYPE_CHECKING:
    from mypy_boto3_glue.type_defs import ColumnTypeDef

_QUERY_FINAL_STATES: List[str] = ["FAILED", "SUCCEEDED", "CANCELLED"]
_QUERY_WAIT_POLLING_DELAY: float = 1.0  # SECONDS

_logger: logging.Logger = logging.getLogger(__name__)


class _QueryMetadata(NamedTuple):
    execution_id: str
    dtype: Dict[str, str]
    parse_timestamps: List[str]
    parse_dates: List[str]
    parse_geometry: List[str]
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


def _get_s3_output(
    s3_output: Optional[str], wg_config: _WorkGroupConfig, boto3_session: Optional[boto3.Session] = None
) -> str:
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
    execution_params: Optional[List[str]] = None,
    client_request_token: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    args: Dict[str, Any] = {"QueryString": sql}

    # s3_output
    args["ResultConfiguration"] = {
        "OutputLocation": _get_s3_output(s3_output=s3_output, wg_config=wg_config, boto3_session=boto3_session)
    }

    # encryption
    if wg_config.enforced is True:
        if wg_config.encryption is not None:
            args["ResultConfiguration"]["EncryptionConfiguration"] = {"EncryptionOption": wg_config.encryption}
            if wg_config.kms_key is not None:
                args["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"] = wg_config.kms_key
    elif encryption is not None:
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

    if client_request_token:
        args["ClientRequestToken"] = client_request_token

    if execution_params:
        args["ExecutionParameters"] = execution_params

    client_athena = _utils.client(service_name="athena", session=boto3_session)
    _logger.debug("Starting query execution with args: \n%s", pprint.pformat(args))
    response = _utils.try_it(
        f=client_athena.start_query_execution,
        ex=botocore.exceptions.ClientError,
        ex_code="ThrottlingException",
        max_num_tries=5,
        **args,
    )
    _logger.debug("Query response:\n%s", response)
    return response["QueryExecutionId"]


def _get_workgroup_config(session: Optional[boto3.Session] = None, workgroup: Optional[str] = None) -> _WorkGroupConfig:
    enforced: bool
    wg_s3_output: Optional[str]
    wg_encryption: Optional[str]
    wg_kms_key: Optional[str]

    enforced, wg_s3_output, wg_encryption, wg_kms_key = False, None, None, None
    if workgroup is not None:
        res = get_work_group(workgroup=workgroup, boto3_session=session)
        enforced = res["WorkGroup"]["Configuration"]["EnforceWorkGroupConfiguration"]
        config: Dict[str, Any] = res["WorkGroup"]["Configuration"].get("ResultConfiguration")
        if config is not None:
            wg_s3_output = config.get("OutputLocation")
            encrypt_config: Optional[Dict[str, str]] = config.get("EncryptionConfiguration")
            wg_encryption = None if encrypt_config is None else encrypt_config.get("EncryptionOption")
            wg_kms_key = None if encrypt_config is None else encrypt_config.get("KmsKey")
    wg_config: _WorkGroupConfig = _WorkGroupConfig(
        enforced=enforced, s3_output=wg_s3_output, encryption=wg_encryption, kms_key=wg_kms_key
    )
    _logger.debug("Workgroup config:\n%s", wg_config)
    return wg_config


def _fetch_txt_result(
    query_metadata: _QueryMetadata,
    keep_files: bool,
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
) -> pd.DataFrame:
    if query_metadata.output_location is None or query_metadata.output_location.endswith(".txt") is False:
        return pd.DataFrame()
    path: str = query_metadata.output_location
    _logger.debug("Reading TXT result from %s", path)
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
        col_name = col_name.strip()  # ruff: noqa: PLW2901
        if col_name.startswith("#") or not col_name:
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
    boto3_session: Optional[boto3.Session] = None,
    categories: Optional[List[str]] = None,
    query_execution_payload: Optional[Dict[str, Any]] = None,
    metadata_cache_manager: Optional[_LocalMetadataCacheManager] = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    execution_params: Optional[List[str]] = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> _QueryMetadata:
    """Get query metadata."""
    if (query_execution_payload is not None) and (query_execution_payload["Status"]["State"] in _QUERY_FINAL_STATES):
        if query_execution_payload["Status"]["State"] != "SUCCEEDED":
            reason: str = query_execution_payload["Status"]["StateChangeReason"]
            raise exceptions.QueryFailed(f"Query error: {reason}")
        _query_execution_payload: Dict[str, Any] = query_execution_payload
    else:
        _query_execution_payload = _executions.wait_query(
            query_execution_id=query_execution_id,
            boto3_session=boto3_session,
            athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        )
    cols_types: Dict[str, str] = get_query_columns_types(
        query_execution_id=query_execution_id, boto3_session=boto3_session
    )
    _logger.debug("Casting query column types: %s", cols_types)
    dtype: Dict[str, str] = {}
    parse_timestamps: List[str] = []
    parse_dates: List[str] = []
    parse_geometry: List[str] = []
    converters: Dict[str, Any] = {}
    binaries: List[str] = []
    col_name: str
    col_type: str
    for col_name, col_type in cols_types.items():
        pandas_type: str = _data_types.athena2pandas(dtype=col_type, dtype_backend=dtype_backend)
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
        elif col_type == "geometry" and pandas_type == "string":
            parse_geometry.append(col_name)
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
        parse_geometry=parse_geometry,
        converters=converters,
        binaries=binaries,
        output_location=output_location,
        manifest_location=manifest_location,
        raw_payload=_query_execution_payload,
    )
    _logger.debug("Query metadata:\n%s", query_metadata)
    return query_metadata


def _empty_dataframe_response(
    chunked: bool, query_metadata: _QueryMetadata
) -> Union[pd.DataFrame, Generator[None, None, None]]:
    """Generate an empty DataFrame response."""
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


class _FormatterTypeQMark(TypedDict):
    params: List[str]
    paramstyle: Literal["qmark"]


class _FormatterTypeNamed(TypedDict):
    params: Dict[str, Any]
    paramstyle: Literal["named"]


_FormatterType = Union[_FormatterTypeQMark, _FormatterTypeNamed, None]


def _verify_formatter(
    params: Union[Dict[str, Any], List[str], None],
    paramstyle: Literal["qmark", "named"],
) -> _FormatterType:
    if params is None:
        return None

    if paramstyle == "named":
        if not isinstance(params, dict):
            raise exceptions.InvalidArgumentCombination(
                f"`params` must be a dict when paramstyle is `named`. Instead, found type {type(params)}."
            )

        return {
            "paramstyle": "named",
            "params": params,
        }

    if paramstyle == "qmark":
        if not isinstance(params, list):
            raise exceptions.InvalidArgumentCombination(
                f"`params` must be a list when paramstyle is `qmark`. Instead, found type {type(params)}."
            )

        return {
            "paramstyle": "qmark",
            "params": params,
        }

    raise exceptions.InvalidArgumentValue(f"`paramstyle` must be either `qmark` or `named`. Found: {paramstyle}.")


def _apply_formatter(
    sql: str,
    params: Union[Dict[str, Any], List[str], None],
    paramstyle: Literal["qmark", "named"],
) -> Tuple[str, Optional[List[str]]]:
    formatter_settings = _verify_formatter(params, paramstyle)

    if formatter_settings is None:
        return sql, None

    if formatter_settings["paramstyle"] == "named":
        # Substitute query parameters]
        sql = _process_sql_params(sql, formatter_settings["params"])

        return sql, None

    return sql, formatter_settings["params"]


def get_named_query_statement(
    named_query_id: str,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """
    Get the named query statement string from a query ID.

    Parameters
    ----------
    named_query_id: str
        The unique ID of the query. Used to get the query statement from a saved query.
        Requires access to the workgroup where the query is saved.
    boto3_session : boto3.Session(), optional
        Boto3 Session. If none, the default boto3 session is used.

    Returns
    -------
    str
        The named query statement string
    """
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    return client_athena.get_named_query(NamedQueryId=named_query_id)["NamedQuery"]["QueryString"]


def get_query_columns_types(query_execution_id: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, str]:
    """Get the data type of all columns queried.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. If none, the default boto3 session is used.

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
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    response = client_athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=1)
    col_info = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
    return dict(
        (c["Name"], f"{c['Type']}({c['Precision']},{c.get('Scale', 0)})")
        if c["Type"] in ["decimal"]
        else (c["Name"], c["Type"])
        for c in col_info
    )


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
    account_id: str = sts.get_account_id(boto3_session=boto3_session)
    region_name: str = _utils.get_region_from_session(boto3_session=boto3_session).lower()
    bucket_name = f"aws-athena-query-results-{account_id}-{region_name}"
    path = f"s3://{bucket_name}/"
    client_s3 = _utils.client(service_name="s3", session=boto3_session)
    args = {} if region_name == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": region_name}}
    try:
        client_s3.create_bucket(Bucket=bucket_name, **args)  # type: ignore[arg-type]
    except (client_s3.exceptions.BucketAlreadyExists, client_s3.exceptions.BucketAlreadyOwnedByYou) as err:
        _logger.debug("Bucket %s already exists.", err.response["Error"]["BucketName"])
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "OperationAborted":
            _logger.debug("A conflicting conditional operation is currently in progress against this resource.")
    client_s3.get_waiter("bucket_exists").wait(Bucket=bucket_name)
    return path


@apply_configs
def repair_table(
    table: str,
    database: Optional[str] = None,
    data_source: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
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
    data_source : Optional[str], optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' is used.
    s3_output : str, optional
        AWS S3 path.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key : str, optional
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.
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
    query_id = _executions.start_query_execution(
        sql=query,
        database=database,
        data_source=data_source,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=boto3_session,
    )
    response: Dict[str, Any] = _executions.wait_query(
        query_execution_id=query_id,
        boto3_session=boto3_session,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
    )
    return cast(str, response["Status"]["State"])


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def describe_table(
    table: str,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
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
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame filled by formatted table information.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_table = wr.athena.describe_table(table='my_table', database='default')

    """
    query = f"DESCRIBE `{table}`;"
    if (database is not None) and (not database.startswith("`")):
        database = f"`{database}`"
    query_id = _executions.start_query_execution(
        sql=query,
        database=database,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=boto3_session,
    )
    query_metadata: _QueryMetadata = _get_query_metadata(
        query_execution_id=query_id,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        boto3_session=boto3_session,
    )
    raw_result = _fetch_txt_result(
        query_metadata=query_metadata,
        keep_files=True,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    return _parse_describe_table(raw_result)


@apply_configs
def create_ctas_table(  # pylint: disable=too-many-locals
    sql: str,
    database: Optional[str] = None,
    ctas_table: Optional[str] = None,
    ctas_database: Optional[str] = None,
    s3_output: Optional[str] = None,
    storage_format: Optional[str] = None,
    write_compression: Optional[str] = None,
    partitioning_info: Optional[List[str]] = None,
    bucketing_info: Optional[typing.BucketingInfoTuple] = None,
    field_delimiter: Optional[str] = None,
    schema_only: bool = False,
    workgroup: Optional[str] = None,
    data_source: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    categories: Optional[List[str]] = None,
    wait: bool = False,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    execution_params: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Union[str, _QueryMetadata]]:
    """Create a new table populated with the results of a SELECT query.

    https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html

    Parameters
    ----------
    sql : str
        SELECT SQL query.
    database : Optional[str], optional
        The name of the database where the original table is stored.
    ctas_table : Optional[str], optional
        The name of the CTAS table.
        If None, a name with a random string is used.
    ctas_database : Optional[str], optional
        The name of the alternative database where the CTAS table should be stored.
        If None, `database` is used, that is the CTAS table is stored in the same database as the original table.
    s3_output : Optional[str], optional
        The output Amazon S3 path.
        If None, either the Athena workgroup or client-side location setting is used.
        If a workgroup enforces a query results location, then it overrides this argument.
    storage_format : Optional[str], optional
        The storage format for the CTAS query results, such as ORC, PARQUET, AVRO, JSON, or TEXTFILE.
        PARQUET by default.
    write_compression : Optional[str], optional
        The compression type to use for any storage format that allows compression to be specified.
    partitioning_info : Optional[List[str]], optional
        A list of columns by which the CTAS table will be partitioned.
    bucketing_info : Optional[Tuple[List[str], int]], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    field_delimiter : Optional[str], optional
        The single-character field delimiter for files in CSV, TSV, and text files.
    schema_only : bool, optional
        _description_, by default False
    workgroup : Optional[str], optional
        Athena workgroup.
    data_source : Optional[str], optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' is used.
    encryption : str, optional
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Note: 'CSE_KMS' is not supported.
    kms_key : str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    wait : bool, default False
        Whether to wait for the query to finish and return a dictionary with the Query metadata.
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.
    boto3_session : Optional[boto3.Session], optional
        Boto3 Session. The default boto3 session is used if boto3_session is None.

    Returns
    -------
    Dict[str, Union[str, _QueryMetadata]]
        A dictionary with the the CTAS database and table names.
        If `wait` is `False`, the query ID is included, otherwise a Query metadata object is added instead.

    Examples
    --------
    Select all into a new table and encrypt the results

    >>> import awswrangler as wr
    >>> wr.athena.create_ctas_table(
    ...     sql="select * from table",
    ...     database="default",
    ...     encryption="SSE_KMS",
    ...     kms_key="1234abcd-12ab-34cd-56ef-1234567890ab",
    ... )
    {'ctas_database': 'default', 'ctas_table': 'temp_table_5669340090094....', 'ctas_query_id': 'cc7dfa81-831d-...'}

    Create a table with schema only

    >>> wr.athena.create_ctas_table(
    ...     sql="select col1, col2 from table",
    ...     database="default",
    ...     ctas_table="my_ctas_table",
    ...     schema_only=True,
    ...     wait=True,
    ... )

    Partition data and save to alternative CTAS database

    >>> wr.athena.create_ctas_table(
    ...     sql="select * from table",
    ...     database="default",
    ...     ctas_database="my_ctas_db",
    ...     storage_format="avro",
    ...     write_compression="snappy",
    ...     partitioning_info=["par0", "par1"],
    ...     wait=True,
    ... )

    """
    ctas_table = catalog.sanitize_table_name(ctas_table) if ctas_table else f"temp_table_{uuid.uuid4().hex}"
    ctas_database = ctas_database if ctas_database else database

    if ctas_database is None:
        raise exceptions.InvalidArgumentCombination("Either ctas_database or database must be defined.")

    fully_qualified_name = f'"{ctas_database}"."{ctas_table}"'

    wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
    s3_output = _get_s3_output(s3_output=s3_output, wg_config=wg_config, boto3_session=boto3_session)
    s3_output = s3_output[:-1] if s3_output[-1] == "/" else s3_output
    # If the workgroup enforces an external location, then it overrides the user supplied argument
    external_location_str: str = (
        f"    external_location = '{s3_output}/{ctas_table}',\n" if (not wg_config.enforced) and (s3_output) else ""
    )

    # At least one property must be specified within `WITH()` in the query. We default to `PARQUET` for `storage_format`
    storage_format_str: str = f"""    format = '{storage_format.upper() if storage_format else "PARQUET"}'"""
    write_compression_str: str = (
        f"    write_compression = '{write_compression.upper()}',\n" if write_compression else ""
    )
    partitioning_str: str = f"    partitioned_by = ARRAY{partitioning_info},\n" if partitioning_info else ""
    bucketing_str: str = (
        f"    bucketed_by = ARRAY{bucketing_info[0]},\n    bucket_count = {bucketing_info[1]},\n"
        if bucketing_info
        else ""
    )
    field_delimiter_str: str = f"    field_delimiter = '{field_delimiter}',\n" if field_delimiter else ""
    schema_only_str: str = "\nWITH NO DATA" if schema_only else ""

    ctas_sql = (
        f"CREATE TABLE {fully_qualified_name}\n"
        f"WITH(\n"
        f"{external_location_str}"
        f"{partitioning_str}"
        f"{bucketing_str}"
        f"{field_delimiter_str}"
        f"{write_compression_str}"
        f"{storage_format_str}"
        f")\n"
        f"AS {sql}"
        f"{schema_only_str}"
    )
    _logger.debug("ctas sql: %s", ctas_sql)

    try:
        query_execution_id: str = _start_query_execution(
            sql=ctas_sql,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            s3_output=s3_output,
            workgroup=workgroup,
            encryption=encryption,
            kms_key=kms_key,
            boto3_session=boto3_session,
            execution_params=execution_params,
        )
    except botocore.exceptions.ClientError as ex:
        error = ex.response["Error"]
        if error["Code"] == "InvalidRequestException" and "Exception parsing query" in error["Message"]:
            raise exceptions.InvalidCtasApproachQuery(
                f"It is not possible to wrap this query into a CTAS statement. Root error message: {error['Message']}"
            )
        if error["Code"] == "InvalidRequestException" and "extraneous input" in error["Message"]:
            raise exceptions.InvalidCtasApproachQuery(
                f"It is not possible to wrap this query into a CTAS statement. Root error message: {error['Message']}"
            )
        raise ex

    response: Dict[str, Union[str, _QueryMetadata]] = {"ctas_database": ctas_database, "ctas_table": ctas_table}
    if wait:
        try:
            response["ctas_query_metadata"] = _get_query_metadata(
                query_execution_id=query_execution_id,
                boto3_session=boto3_session,
                categories=categories,
                metadata_cache_manager=_cache_manager,
                athena_query_wait_polling_delay=athena_query_wait_polling_delay,
            )
        except exceptions.QueryFailed as ex:
            msg: str = str(ex)
            if "Column name" in msg and "specified more than once" in msg:
                raise exceptions.InvalidCtasApproachQuery(
                    f"Please, define distinct names for your columns. Root error message: {msg}"
                )
            if "Column name not specified" in msg:
                raise exceptions.InvalidArgumentValue(
                    "Please, define all columns names in your query. (E.g. 'SELECT MAX(col1) AS max_col1, ...')"
                )
            if "Column type is unknown" in msg:
                raise exceptions.InvalidArgumentValue(
                    "Please, don't leave undefined columns types in your query. You can cast to ensure it. "
                    "(E.g. 'SELECT CAST(NULL AS INTEGER) AS MY_COL, ...')"
                )
            raise ex
    else:
        response["ctas_query_id"] = query_execution_id
    _logger.info("Created CTAS table %s", fully_qualified_name)
    return response


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def show_create_table(
    table: str,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
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
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
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
    query_id = _executions.start_query_execution(
        sql=query,
        database=database,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=boto3_session,
    )
    query_metadata: _QueryMetadata = _get_query_metadata(
        query_execution_id=query_id,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        boto3_session=boto3_session,
    )
    raw_result = _fetch_txt_result(
        query_metadata=query_metadata,
        keep_files=True,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    return cast(str, raw_result.createtab_stmt.str.strip().str.cat(sep=" "))


@apply_configs
def generate_create_query(
    table: str,
    database: Optional[str] = None,
    transaction_id: Optional[str] = None,
    query_as_of_time: Optional[str] = None,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Generate the query that created a table(EXTERNAL_TABLE) or a view(VIRTUAL_TABLE).

    Analyzes an existing table named table_name to generate the query that created it.

    Parameters
    ----------
    table : str
        Table name.
    database : str
        Database name.
    transaction_id: str, optional
        The ID of the transaction.
    query_as_of_time: str, optional
        The time as of when to read the table contents. Must be a valid Unix epoch timestamp.
        Cannot be specified alongside transaction_id.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        The query that created the table or view.

    Examples
    --------
    >>> import awswrangler as wr
    >>> view_create_query: str = wr.athena.generate_create_query(table='my_view', database='default')

    """

    def parse_columns(columns_description: Sequence["ColumnTypeDef"]) -> str:
        columns_str: List[str] = []
        for column in columns_description:
            column_str = f"  `{column['Name']}` {column['Type']}"
            if "Comment" in column:
                column_str += f" COMMENT '{column['Comment']}'"
            columns_str.append(column_str)
        return ", \n".join(columns_str)

    def parse_properties(parameters: Dict[str, str]) -> str:
        properties_str: List[str] = []
        for key, value in parameters.items():
            if key == "EXTERNAL":
                continue
            property_key_value = f"  '{key}'='{value}'"
            properties_str.append(property_key_value)
        return ", \n".join(properties_str)

    client_glue = _utils.client(service_name="glue", session=boto3_session)
    table_detail = client_glue.get_table(
        **_catalog_id(
            catalog_id=catalog_id,
            **_transaction_id(
                transaction_id=transaction_id, query_as_of_time=query_as_of_time, DatabaseName=database, Name=table
            ),
        )
    )["Table"]
    if table_detail["TableType"] == "VIRTUAL_VIEW":
        glue_base64_query: str = table_detail["ViewOriginalText"].replace("/* Presto View: ", "").replace(" */", "")
        glue_query: str = json.loads(base64.b64decode(glue_base64_query))["originalSql"]
        return f"""CREATE OR REPLACE VIEW "{table}" AS \n{glue_query}"""
    if table_detail["TableType"] == "EXTERNAL_TABLE":
        columns: str = parse_columns(columns_description=table_detail["StorageDescriptor"]["Columns"])
        query_parts: List[str] = [f"""CREATE EXTERNAL TABLE `{table}`(\n{columns})"""]
        partitioned_columns: str = parse_columns(columns_description=table_detail["PartitionKeys"])
        if partitioned_columns:
            query_parts.append(f"""PARTITIONED BY ( \n{partitioned_columns})""")
        tblproperties: str = parse_properties(parameters=table_detail["Parameters"])

        query_parts += [
            """ROW FORMAT SERDE """,
            f"""  '{table_detail['StorageDescriptor']['SerdeInfo']['SerializationLibrary']}' """,
            """STORED AS INPUTFORMAT """,
            f"""  '{table_detail['StorageDescriptor']['InputFormat']}' """,
            """OUTPUTFORMAT """,
            f"""  '{table_detail['StorageDescriptor']['OutputFormat']}'""",
            """LOCATION""",
            f"""  '{table_detail['StorageDescriptor']['Location']}'""",
            f"""TBLPROPERTIES (\n{tblproperties})""",
        ]
        sql = "\n".join(query_parts)
        _logger.debug("Generated create query:\n%s", sql)
        return sql
    raise NotImplementedError()


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
    client_athena = _utils.client(service_name="athena", session=boto3_session)
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


def get_query_executions(
    query_execution_ids: List[str], return_unprocessed: bool = False, boto3_session: Optional[boto3.Session] = None
) -> Union[Tuple[pd.DataFrame, pd.DataFrame], pd.DataFrame]:
    """From specified query execution IDs, return a DataFrame of query execution details.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.batch_get_query_execution

    Parameters
    ----------
    query_execution_ids : List[str]
        Athena query execution IDs.
    return_unprocessed: bool.
        True to also return query executions id that are unable to be processed.
        False to only return DataFrame of query execution details.
        Default is False
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    DataFrame
        DataFrame contain information about query execution details.

    DataFrame
        DataFrame contain information about unprocessed query execution ids.

    Examples
    --------
    >>> import awswrangler as wr
    >>> query_executions_df, unprocessed_query_executions_df = wr.athena.get_query_executions(
            query_execution_ids=['query-execution-id','query-execution-id1']
        )
    """
    chunked_size: int = 50
    query_executions = []
    unprocessed_query_execution = []
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    for i in range(0, len(query_execution_ids), chunked_size):
        response = client_athena.batch_get_query_execution(QueryExecutionIds=query_execution_ids[i : i + chunked_size])
        query_executions += response["QueryExecutions"]
        unprocessed_query_execution += response["UnprocessedQueryExecutionIds"]
    if unprocessed_query_execution and not return_unprocessed:
        _logger.warning(
            "Some of query execution ids are unable to be processed."
            "Set return_unprocessed to True to get unprocessed query execution ids"
        )
    if return_unprocessed:
        return pd.json_normalize(query_executions), pd.json_normalize(unprocessed_query_execution)
    return pd.json_normalize(query_executions)


def list_query_executions(workgroup: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    """Fetch list query execution IDs ran in specified workgroup or primary work group if not specified.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.list_query_executions

    Parameters
    ----------
    workgroup : str
        The name of the workgroup from which the query_id are being returned.
        If not specified, a list of available query execution IDs for the queries in the primary workgroup is returned.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        List of query execution IDs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.list_query_executions(workgroup='workgroup-name')

    """
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    kwargs: Dict[str, Any] = {"base": 1}
    if workgroup:
        kwargs["WorkGroup"] = workgroup
    query_list: List[str] = []
    response = _utils.try_it(
        f=client_athena.list_query_executions,
        ex=botocore.exceptions.ClientError,
        ex_code="ThrottlingException",
        max_num_tries=5,
        **kwargs,
    )
    query_list += response["QueryExecutionIds"]
    while "NextToken" in response:
        kwargs["NextToken"] = response["NextToken"]
        response = _utils.try_it(
            f=client_athena.list_query_executions,
            ex=botocore.exceptions.ClientError,
            ex_code="ThrottlingException",
            max_num_tries=5,
            **kwargs,
        )
        query_list += response["QueryExecutionIds"]
    _logger.debug("Running %d query executions", len(query_list))
    return query_list
