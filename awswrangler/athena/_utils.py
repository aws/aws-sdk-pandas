"""Utilities Module for Amazon Athena."""

from __future__ import annotations

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
    NamedTuple,
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
from awswrangler.catalog._utils import _catalog_id

from . import _executions
from ._cache import _cache_manager, _LocalMetadataCacheManager

if TYPE_CHECKING:
    from mypy_boto3_athena.type_defs import QueryExecutionTypeDef
    from mypy_boto3_glue.type_defs import ColumnOutputTypeDef

_QUERY_FINAL_STATES: list[str] = ["FAILED", "SUCCEEDED", "CANCELLED"]
_QUERY_WAIT_POLLING_DELAY: float = 1.0  # SECONDS

_logger: logging.Logger = logging.getLogger(__name__)


class _QueryMetadata(NamedTuple):
    execution_id: str
    dtype: dict[str, str]
    parse_timestamps: list[str]
    parse_dates: list[str]
    parse_geometry: list[str]
    converters: dict[str, Any]
    binaries: list[str]
    output_location: str | None
    manifest_location: str | None
    raw_payload: "QueryExecutionTypeDef"


class _WorkGroupConfig(NamedTuple):
    enforced: bool
    s3_output: str | None
    encryption: str | None
    kms_key: str | None


def _get_s3_output(
    s3_output: str | None, wg_config: _WorkGroupConfig, boto3_session: boto3.Session | None = None
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
    database: str | None = None,
    data_source: str | None = None,
    s3_output: str | None = None,
    workgroup: str | None = None,
    encryption: str | None = None,
    kms_key: str | None = None,
    execution_params: list[str] | None = None,
    client_request_token: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    args: dict[str, Any] = {"QueryString": sql}

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


def _get_workgroup_config(session: boto3.Session | None = None, workgroup: str = "primary") -> _WorkGroupConfig:
    enforced: bool
    wg_s3_output: str | None
    wg_encryption: str | None
    wg_kms_key: str | None

    enforced, wg_s3_output, wg_encryption, wg_kms_key = False, None, None, None
    if workgroup is not None:
        res = get_work_group(workgroup=workgroup, boto3_session=session)
        enforced = res["WorkGroup"]["Configuration"]["EnforceWorkGroupConfiguration"]
        config: dict[str, Any] = res["WorkGroup"]["Configuration"].get("ResultConfiguration")
        if config is not None:
            wg_s3_output = config.get("OutputLocation")
            encrypt_config: dict[str, str] | None = config.get("EncryptionConfiguration")
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
    boto3_session: boto3.Session | None,
    s3_additional_kwargs: dict[str, str] | None,
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
    target_df_dict: dict[str, list[str | bool]] = {"Column Name": [], "Type": [], "Partition": [], "Comment": []}
    for index, col_name in origin_df_dict["col_name"].items():
        col_name = col_name.strip()  # noqa: PLW2901
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


def _get_query_metadata(
    query_execution_id: str,
    boto3_session: boto3.Session | None = None,
    categories: list[str] | None = None,
    query_execution_payload: "QueryExecutionTypeDef" | None = None,
    metadata_cache_manager: _LocalMetadataCacheManager | None = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    execution_params: list[str] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> _QueryMetadata:
    """Get query metadata."""
    if (query_execution_payload is not None) and (query_execution_payload["Status"]["State"] in _QUERY_FINAL_STATES):
        if query_execution_payload["Status"]["State"] != "SUCCEEDED":
            reason: str = query_execution_payload["Status"]["StateChangeReason"]
            raise exceptions.QueryFailed(f"Query error: {reason}")
        _query_execution_payload = query_execution_payload
    else:
        _query_execution_payload = cast(
            "QueryExecutionTypeDef",
            _executions.wait_query(
                query_execution_id=query_execution_id,
                boto3_session=boto3_session,
                athena_query_wait_polling_delay=athena_query_wait_polling_delay,
            ),
        )
    cols_types: dict[str, str] = get_query_columns_types(
        query_execution_id=query_execution_id, boto3_session=boto3_session
    )
    _logger.debug("Casting query column types: %s", cols_types)
    dtype: dict[str, str] = {}
    parse_timestamps: list[str] = []
    parse_dates: list[str] = []
    parse_geometry: list[str] = []
    converters: dict[str, Any] = {}
    binaries: list[str] = []
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

    output_location: str | None = None
    if "ResultConfiguration" in _query_execution_payload:
        output_location = _query_execution_payload["ResultConfiguration"].get("OutputLocation")

    athena_statistics = _query_execution_payload.get("Statistics", {})
    manifest_location: str | None = athena_statistics.get("DataManifestLocation")

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
) -> pd.DataFrame | Generator[None, None, None]:
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
    params: list[str]
    paramstyle: Literal["qmark"]


class _FormatterTypeNamed(TypedDict):
    params: dict[str, Any]
    paramstyle: Literal["named"]


_FormatterType = Union[_FormatterTypeQMark, _FormatterTypeNamed, None]


def _verify_formatter(
    params: dict[str, Any] | list[str] | None,
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
    params: dict[str, Any] | list[str] | None,
    paramstyle: Literal["qmark", "named"],
) -> tuple[str, list[str] | None]:
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
    boto3_session: boto3.Session | None = None,
) -> str:
    """
    Get the named query statement string from a query ID.

    Parameters
    ----------
    named_query_id
        The unique ID of the query. Used to get the query statement from a saved query.
        Requires access to the workgroup where the query is saved.
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Returns
    -------
        The named query statement string
    """
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    return client_athena.get_named_query(NamedQueryId=named_query_id)["NamedQuery"]["QueryString"]


def get_query_columns_types(query_execution_id: str, boto3_session: boto3.Session | None = None) -> dict[str, str]:
    """Get the data type of all columns queried.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    query_execution_id
        Athena query execution ID.
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Returns
    -------
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


def create_athena_bucket(boto3_session: boto3.Session | None = None) -> str:
    """Create the default Athena bucket if it doesn't exist.

    Parameters
    ----------
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Returns
    -------
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
    except (client_s3.exceptions.BucketAlreadyExists, client_s3.exceptions.BucketAlreadyOwnedByYou):
        _logger.debug("Bucket %s already exists.", bucket_name)
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "OperationAborted":
            _logger.debug("A conflicting conditional operation is currently in progress against this resource.")
    client_s3.get_waiter("bucket_exists").wait(Bucket=bucket_name)
    return path


@apply_configs
def repair_table(
    table: str,
    database: str | None = None,
    data_source: str | None = None,
    s3_output: str | None = None,
    workgroup: str = "primary",
    encryption: str | None = None,
    kms_key: str | None = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    boto3_session: boto3.Session | None = None,
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
    table
        Table name.
    database
        AWS Glue/Athena database name.
    data_source
        Data Source / Catalog name. If None, 'AwsDataCatalog' is used.
    s3_output
        AWS S3 path.
    workgroup
        Athena workgroup. Primary by default.
    encryption
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    athena_query_wait_polling_delay
        Interval in seconds for how often the function will check if the Athena query has completed.
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Returns
    -------
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
    response: dict[str, Any] = _executions.wait_query(
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
    database: str | None = None,
    s3_output: str | None = None,
    workgroup: str = "primary",
    encryption: str | None = None,
    kms_key: str | None = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    s3_additional_kwargs: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
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
    table
        Table name.
    database
        AWS Glue/Athena database name.
    s3_output
        AWS S3 path.
    workgroup
        Athena workgroup. Primary by default.
    encryption
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    athena_query_wait_polling_delay
        Interval in seconds for how often the function will check if the Athena query has completed.
    s3_additional_kwargs
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Returns
    -------
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
def create_ctas_table(
    sql: str,
    database: str | None = None,
    ctas_table: str | None = None,
    ctas_database: str | None = None,
    s3_output: str | None = None,
    storage_format: str | None = None,
    write_compression: str | None = None,
    partitioning_info: list[str] | None = None,
    bucketing_info: typing.BucketingInfoTuple | None = None,
    field_delimiter: str | None = None,
    schema_only: bool = False,
    workgroup: str = "primary",
    data_source: str | None = None,
    encryption: str | None = None,
    kms_key: str | None = None,
    categories: list[str] | None = None,
    wait: bool = False,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    execution_params: list[str] | None = None,
    params: dict[str, Any] | list[str] | None = None,
    paramstyle: Literal["qmark", "named"] = "named",
    boto3_session: boto3.Session | None = None,
) -> dict[str, str | _QueryMetadata]:
    """Create a new table populated with the results of a SELECT query.

    https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html

    Parameters
    ----------
    sql
        SELECT SQL query.
    database
        The name of the database where the original table is stored.
    ctas_table
        The name of the CTAS table.
        If None, a name with a random string is used.
    ctas_database
        The name of the alternative database where the CTAS table should be stored.
        If None, `database` is used, that is the CTAS table is stored in the same database as the original table.
    s3_output
        The output Amazon S3 path.
        If None, either the Athena workgroup or client-side location setting is used.
        If a workgroup enforces a query results location, then it overrides this argument.
    storage_format
        The storage format for the CTAS query results, such as ORC, PARQUET, AVRO, JSON, or TEXTFILE.
        PARQUET by default.
    write_compression
        The compression type to use for any storage format that allows compression to be specified.
    partitioning_info
        A list of columns by which the CTAS table will be partitioned.
    bucketing_info
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    field_delimiter
        The single-character field delimiter for files in CSV, TSV, and text files.
    schema_only
        _description_, by default False
    workgroup
        Athena workgroup. Primary by default.
    data_source
        Data Source / Catalog name. If None, 'AwsDataCatalog' is used.
    encryption
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Note: 'CSE_KMS' is not supported.
    kms_key
        For SSE-KMS, this is the KMS key ARN or ID.
    categories
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    wait
        Whether to wait for the query to finish and return a dictionary with the Query metadata.
    athena_query_wait_polling_delay
        Interval in seconds for how often the function will check if the Athena query has completed.
    execution_params
        [**DEPRECATED**]
        A list of values for the parameters that are used in the SQL query.
        This parameter is on a deprecation path.
        Use ``params`` and `paramstyle`` instead.
    params
        Dictionary or list of parameters to pass to execute method.
        The syntax used to pass parameters depends on the configuration of ``paramstyle``.
    paramstyle
        The syntax style to use for the parameters.
        Supported values are ``named`` and ``qmark``.
        The default is ``named``.
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Returns
    -------
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

    # Substitute execution_params with params
    if execution_params:
        if params:
            raise exceptions.InvalidArgumentCombination("`execution_params` and `params` are mutually exclusive.")

        params = execution_params
        paramstyle = "qmark"
        raise DeprecationWarning(
            '`execution_params` is being deprecated. Use `params` and `paramstyle="qmark"` instead.'
        )

    # Substitute query parameters if applicable
    sql, execution_params = _apply_formatter(sql, params, paramstyle)

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

    response: dict[str, str | _QueryMetadata] = {"ctas_database": ctas_database, "ctas_table": ctas_table}
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
    database: str | None = None,
    s3_output: str | None = None,
    workgroup: str = "primary",
    encryption: str | None = None,
    kms_key: str | None = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    s3_additional_kwargs: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Generate the query that created it: 'SHOW CREATE TABLE table;'.

    Analyzes an existing table named table_name to generate the query that created it.

    Note
    ----
    Create the default Athena bucket if it doesn't exist and s3_output is None.
    (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Parameters
    ----------
    table
        Table name.
    database
        AWS Glue/Athena database name.
    s3_output
        AWS S3 path.
    workgroup
        Athena workgroup. Primary by default.
    encryption
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    athena_query_wait_polling_delay
        Interval in seconds for how often the function will check if the Athena query has completed.
    s3_additional_kwargs
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Returns
    -------
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
    database: str | None = None,
    catalog_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Generate the query that created a table(EXTERNAL_TABLE) or a view(VIRTUAL_TABLE).

    Analyzes an existing table named table_name to generate the query that created it.

    Parameters
    ----------
    table
        Table name.
    database
        Database name.
    catalog_id
        The ID of the Data Catalog from which to retrieve Databases.
        If ``None`` is provided, the AWS account ID is used by default.
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Returns
    -------
        The query that created the table or view.

    Examples
    --------
    >>> import awswrangler as wr
    >>> view_create_query: str = wr.athena.generate_create_query(table='my_view', database='default')

    """

    def parse_columns(columns_description: list["ColumnOutputTypeDef"]) -> str:
        columns_str: list[str] = []
        for column in columns_description:
            column_str = f"  `{column['Name']}` {column['Type']}"
            if "Comment" in column:
                column_str += f" COMMENT '{column['Comment']}'"
            columns_str.append(column_str)
        return ", \n".join(columns_str)

    def parse_properties(parameters: dict[str, str]) -> str:
        properties_str: list[str] = []
        for key, value in parameters.items():
            if key == "EXTERNAL":
                continue
            property_key_value = f"  '{key}'='{value}'"
            properties_str.append(property_key_value)
        return ", \n".join(properties_str)

    client_glue = _utils.client(service_name="glue", session=boto3_session)
    table_detail = client_glue.get_table(**_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table))[
        "Table"
    ]
    if table_detail["TableType"] == "VIRTUAL_VIEW":
        glue_base64_query: str = table_detail["ViewOriginalText"].replace("/* Presto View: ", "").replace(" */", "")
        glue_query: str = json.loads(base64.b64decode(glue_base64_query))["originalSql"]
        return f"""CREATE OR REPLACE VIEW "{table}" AS \n{glue_query}"""
    if table_detail["TableType"] == "EXTERNAL_TABLE":
        columns: str = parse_columns(columns_description=table_detail["StorageDescriptor"]["Columns"])
        query_parts: list[str] = [f"""CREATE EXTERNAL TABLE `{table}`(\n{columns})"""]
        partitioned_columns: str = parse_columns(columns_description=table_detail["PartitionKeys"])
        if partitioned_columns:
            query_parts.append(f"""PARTITIONED BY ( \n{partitioned_columns})""")
        tblproperties: str = parse_properties(parameters=table_detail["Parameters"])

        query_parts += [
            """ROW FORMAT SERDE """,
            f"""  '{table_detail["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"]}' """,
            """STORED AS INPUTFORMAT """,
            f"""  '{table_detail["StorageDescriptor"]["InputFormat"]}' """,
            """OUTPUTFORMAT """,
            f"""  '{table_detail["StorageDescriptor"]["OutputFormat"]}'""",
            """LOCATION""",
            f"""  '{table_detail["StorageDescriptor"]["Location"]}'""",
            f"""TBLPROPERTIES (\n{tblproperties})""",
        ]
        sql = "\n".join(query_parts)
        _logger.debug("Generated create query:\n%s", sql)
        return sql
    raise NotImplementedError()


def get_work_group(workgroup: str, boto3_session: boto3.Session | None = None) -> dict[str, Any]:
    """Return information about the workgroup with the specified name.

    Parameters
    ----------
    workgroup
        Work Group name.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
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
    query_execution_ids: list[str], return_unprocessed: bool = False, boto3_session: boto3.Session | None = None
) -> tuple[pd.DataFrame, pd.DataFrame] | pd.DataFrame:
    """From specified query execution IDs, return a DataFrame of query execution details.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.batch_get_query_execution

    Parameters
    ----------
    query_execution_ids
        Athena query execution IDs.
    return_unprocessed
        True to also return query executions id that are unable to be processed.
        False to only return DataFrame of query execution details.
        Default is False
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        DataFrame containing either information about query execution details.
        Optionally, another DataFrame containing unprocessed query execution IDs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> query_executions_df, unprocessed_query_executions_df = wr.athena.get_query_executions(
    >>>     query_execution_ids=['query-execution-id','query-execution-id1']
    >>> )
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


def list_query_executions(
    workgroup: str | None = None,
    max_results: int | None = None,
    boto3_session: boto3.Session | None = None,
) -> list[str]:
    """Fetch list query execution IDs ran in specified workgroup or primary work group if not specified.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.list_query_executions

    Parameters
    ----------
    workgroup
        The name of the workgroup from which the query_id are being returned.
        If not specified, a list of available query execution IDs for the queries in the primary workgroup is returned.
    max_results
        The maximum number of query execution IDs to return in this request.
        If not present, all execution IDs will be returned.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        List of query execution IDs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.list_query_executions(workgroup='workgroup-name')

    """
    client_athena = _utils.client(service_name="athena", session=boto3_session)

    kwargs: dict[str, Any] = {}
    if workgroup:
        kwargs["WorkGroup"] = workgroup

        if max_results is not None:
            kwargs["MaxResults"] = min(max_results, 50)

    query_list: list[str] = []
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

        if max_results is not None:
            if len(query_list) >= max_results:
                break

            kwargs["MaxResults"] = min(max_results - len(query_list), 50)

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
