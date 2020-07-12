"""Amazon Athena Module."""

import csv
import datetime
import logging
import pprint
import re
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3  # type: ignore
import pandas as pd  # type: ignore

from awswrangler import _data_types, _utils, catalog, exceptions, s3, sts

_logger: logging.Logger = logging.getLogger(__name__)

_QUERY_WAIT_POLLING_DELAY: float = 0.2  # SECONDS


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
    response: Dict = client_athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=1)
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
    wg_config: Dict[str, Union[bool, Optional[str]]] = _get_workgroup_config(session=session, workgroup=workgroup)
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


def _start_query_execution(
    sql: str,
    wg_config: Dict[str, Union[Optional[bool], Optional[str]]],
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
    if wg_config["enforced"] is True:
        if wg_config["encryption"] is not None:
            args["ResultConfiguration"]["EncryptionConfiguration"] = {"EncryptionOption": wg_config["encryption"]}
            if wg_config["kms_key"] is not None:
                args["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"] = wg_config["kms_key"]
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
    response = client_athena.start_query_execution(**args)
    return response["QueryExecutionId"]


def _get_s3_output(
    s3_output: Optional[str], wg_config: Dict[str, Union[bool, Optional[str]]], boto3_session: boto3.Session
) -> str:
    if s3_output is None:
        _s3_output: Optional[str] = wg_config["s3_output"]  # type: ignore
        if _s3_output is not None:
            s3_output = _s3_output
        else:
            s3_output = create_athena_bucket(boto3_session=boto3_session)
    elif wg_config["enforced"] is True:
        s3_output = wg_config["s3_output"]  # type: ignore
    return s3_output


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
    final_states: List[str] = ["FAILED", "SUCCEEDED", "CANCELLED"]
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    response: Dict[str, Any] = client_athena.get_query_execution(QueryExecutionId=query_execution_id)
    state: str = response["QueryExecution"]["Status"]["State"]
    while state not in final_states:
        time.sleep(_QUERY_WAIT_POLLING_DELAY)
        response = client_athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]
    _logger.debug("state: %s", state)
    _logger.debug("StateChangeReason: %s", response["QueryExecution"]["Status"].get("StateChangeReason"))
    if state == "FAILED":
        raise exceptions.QueryFailed(response["QueryExecution"]["Status"].get("StateChangeReason"))
    if state == "CANCELLED":
        raise exceptions.QueryCancelled(response["QueryExecution"]["Status"].get("StateChangeReason"))
    return response


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
    return response["QueryExecution"]["Status"]["State"]


def _extract_ctas_manifest_paths(path: str, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    """Get the list of paths of the generated files."""
    bucket_name, key_path = _utils.parse_path(path)
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    body: bytes = client_s3.get_object(Bucket=bucket_name, Key=key_path)["Body"].read()
    return [x for x in body.decode("utf-8").split("\n") if x != ""]


def _get_query_metadata(
    query_execution_id: str, categories: List[str] = None, boto3_session: Optional[boto3.Session] = None
) -> Tuple[Dict[str, str], List[str], List[str], Dict[str, Any], List[str]]:
    """Get query metadata."""
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
    _logger.debug("dtype: %s", dtype)
    _logger.debug("parse_timestamps: %s", parse_timestamps)
    _logger.debug("parse_dates: %s", parse_dates)
    _logger.debug("converters: %s", converters)
    _logger.debug("binaries: %s", binaries)
    return dtype, parse_timestamps, parse_dates, converters, binaries


def _fix_csv_types_generator(
    dfs: Iterator[pd.DataFrame], parse_dates: List[str], binaries: List[str]
) -> Iterator[pd.DataFrame]:
    """Apply data types cast to a Pandas DataFrames Generator."""
    for df in dfs:
        yield _fix_csv_types(df=df, parse_dates=parse_dates, binaries=binaries)


def _fix_csv_types(df: pd.DataFrame, parse_dates: List[str], binaries: List[str]) -> pd.DataFrame:
    """Apply data types cast to a Pandas DataFrames."""
    if len(df.index) > 0:
        for col in parse_dates:
            df[col] = df[col].dt.date.replace(to_replace={pd.NaT: None})
        for col in binaries:
            df[col] = df[col].str.encode(encoding="utf-8")
    return df


def read_sql_query(  # pylint: disable=too-many-branches,too-many-locals,too-many-return-statements,too-many-statements
    sql: str,
    database: str,
    ctas_approach: bool = True,
    categories: Optional[List[str]] = None,
    chunksize: Optional[Union[int, bool]] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    keep_files: bool = True,
    ctas_temp_table_name: Optional[str] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    max_cache_seconds: int = 0,
    max_cache_query_inspections: int = 50,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Execute any SQL query on AWS Athena and return the results as a Pandas DataFrame.

    There are two approaches to be defined through ctas_approach parameter:

    1 - `ctas_approach=True` (`Default`):
    Wrap the query with a CTAS and then reads the table data as parquet directly from s3.
    PROS: Faster and can handle some level of nested types.
    CONS: Requires create/delete table permissions on Glue and Does not support timestamp with time zone
    (A temporary table will be created and then deleted immediately).

    2 - `ctas_approach False`:
    Does a regular query on Athena and parse the regular CSV result on s3.
    PROS: Does not require create/delete table permissions on Glue and supports timestamp with time zone.
    CONS: Slower (But stills faster than other libraries that uses the regular Athena API)
    and does not handle nested types at all.

    Note
    ----
    Valid encryption modes: [None, 'SSE_S3', 'SSE_KMS'].

    `P.S. 'CSE_KMS' is not supported.`

    Note
    ----
    Create the default Athena bucket if it doesn't exist and s3_output is None.

    (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Note
    ----
    ``Batching`` (`chunksize` argument) (Memory Friendly):

    Will anable the function to return a Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies on Wrangler:

    - If **chunksize=True**, a new DataFrame will be returned for each file in the query result.

    - If **chunked=INTEGER**, Wrangler will iterate on the data by number of rows igual the received INTEGER.

    `P.S.` `chunksize=True` if faster and uses less memory while `chunksize=INTEGER` is more precise
    in number of rows for each Dataframe.

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    sql : str
        SQL query.
    database : str
        AWS Glue/Athena database name.
    ctas_approach: bool
        Wraps the query using a CTAS, and read the resulted parquet data on S3.
        If false, read the regular CSV on S3.
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    chunksize : Union[int, bool], optional
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` wrangler will iterate on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed Wrangler will iterate on the data by number of rows igual the received INTEGER.
    s3_output : str, optional
        Amazon S3 path.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
    kms_key : str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    keep_files : bool
        Should Wrangler delete or keep the staging files produced by Athena?
    ctas_temp_table_name : str, optional
        The name of the temporary table and also the directory name on S3 where the CTAS result is stored.
        If None, it will use the follow random pattern: `f"temp_table_{pyarrow.compat.guid()}"`.
        On S3 this directory will be under under the pattern: `f"{s3_output}/{ctas_temp_table_name}/"`.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    max_cache_seconds : int
        Wrangler can look up in Athena's history if this query has been run before.
        If so, and its completion time is less than `max_cache_seconds` before now, wrangler
        skips query execution and just returns the same results as last time.
        If cached results are valid, wrangler ignores the `ctas_approach`, `s3_output`, `encryption`, `kms_key`,
        `keep_files` and `ctas_temp_table_name` params.
        If reading cached data fails for any reason, execution falls back to the usual query run path.
    max_cache_query_inspections : int
        Max number of queries that will be inspected from the history to try to find some result to reuse.
        The bigger the number of inspection, the bigger will be the latency for not cached queries.
        Only takes effect if max_cache_seconds > 0.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.athena.read_sql_query(sql='...', database='...')

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    # check for cached results
    cache_info: Dict[str, Any] = _check_for_cached_results(
        sql=sql,
        session=session,
        workgroup=workgroup,
        max_cache_seconds=max_cache_seconds,
        max_cache_query_inspections=max_cache_query_inspections,
    )
    _logger.debug("cache_info: %s", cache_info)

    if cache_info["has_valid_cache"] is True:
        _logger.debug("Valid cache found. Retrieving...")
        cache_result: Union[pd.DataFrame, Iterator[pd.DataFrame], None] = None
        try:
            cache_result = _resolve_query_with_cache(
                cache_info=cache_info,
                categories=categories,
                chunksize=chunksize,
                use_threads=use_threads,
                session=session,
            )
        # pylint: disable=broad-except
        except Exception as e:  # pragma: no cover
            _logger.error(e)
            # if there is anything wrong with the cache, just fallback to the usual path
        if cache_result is not None:
            return cache_result
        _logger.debug("Corrupt cache. Continuing to execute query...")  # pragma: no cover

    return _resolve_query_without_cache(
        sql=sql,
        database=database,
        ctas_approach=ctas_approach,
        categories=categories,
        chunksize=chunksize,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        keep_files=keep_files,
        ctas_temp_table_name=ctas_temp_table_name,
        use_threads=use_threads,
        session=session,
    )


def _resolve_query_without_cache(
    # pylint: disable=too-many-branches,too-many-locals,too-many-return-statements,too-many-statements
    sql: str,
    database: str,
    ctas_approach: bool,
    categories: Optional[List[str]],
    chunksize: Optional[Union[int, bool]],
    s3_output: Optional[str],
    workgroup: Optional[str],
    encryption: Optional[str],
    kms_key: Optional[str],
    keep_files: bool,
    ctas_temp_table_name: Optional[str],
    use_threads: bool,
    session: Optional[boto3.Session],
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """
    Execute any query in Athena and returns results as Dataframe, back to `read_sql_query`.

    Usually called by `read_sql_query` when using cache is not possible.
    """
    wg_config: Dict[str, Union[bool, Optional[str]]] = _get_workgroup_config(session=session, workgroup=workgroup)
    _s3_output: str = _get_s3_output(s3_output=s3_output, wg_config=wg_config, boto3_session=session)
    _s3_output = _s3_output[:-1] if _s3_output[-1] == "/" else _s3_output

    name: str = ""
    if ctas_approach is True:
        if ctas_temp_table_name is not None:
            name = catalog.sanitize_table_name(ctas_temp_table_name)
        else:
            name = f"temp_table_{uuid.uuid4().hex}"
        path: str = f"{_s3_output}/{name}"
        ext_location: str = "\n" if wg_config["enforced"] is True else f",\n    external_location = '{path}'\n"
        sql = (
            f'CREATE TABLE "{name}"\n'
            f"WITH(\n"
            f"    format = 'Parquet',\n"
            f"    parquet_compression = 'SNAPPY'"
            f"{ext_location}"
            f") AS\n"
            f"{sql}"
        )
    _logger.debug("sql: %s", sql)
    query_id: str = _start_query_execution(
        sql=sql,
        wg_config=wg_config,
        database=database,
        s3_output=_s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=session,
    )
    _logger.debug("query_id: %s", query_id)
    try:
        query_response: Dict[str, Any] = wait_query(query_execution_id=query_id, boto3_session=session)
    except exceptions.QueryFailed as ex:
        if ctas_approach is True:
            if "Column name not specified" in str(ex):
                raise exceptions.InvalidArgumentValue(
                    "Please, define all columns names in your query. (E.g. 'SELECT MAX(col1) AS max_col1, ...')"
                )
            if "Column type is unknown" in str(ex):
                raise exceptions.InvalidArgumentValue(
                    "Please, define all columns types in your query. "
                    "(E.g. 'SELECT CAST(NULL AS INTEGER) AS MY_COL, ...')"
                )
        raise ex  # pragma: no cover
    if query_response["QueryExecution"]["Status"]["State"] in ["FAILED", "CANCELLED"]:  # pragma: no cover
        reason: str = query_response["QueryExecution"]["Status"]["StateChangeReason"]
        message_error: str = f"Query error: {reason}"
        raise exceptions.AthenaQueryError(message_error)
    ret: Union[pd.DataFrame, Iterator[pd.DataFrame]]
    if ctas_approach is True:
        catalog.delete_table_if_exists(database=database, table=name, boto3_session=session)
        manifest_path: str = f"{_s3_output}/tables/{query_id}-manifest.csv"
        metadata_path: str = f"{_s3_output}/tables/{query_id}.metadata"
        _logger.debug("manifest_path: %s", manifest_path)
        _logger.debug("metadata_path: %s", metadata_path)
        s3.wait_objects_exist(paths=[manifest_path, metadata_path], use_threads=False, boto3_session=session)
        paths: List[str] = _extract_ctas_manifest_paths(path=manifest_path, boto3_session=session)
        chunked: Union[bool, int] = False if chunksize is None else chunksize
        _logger.debug("chunked: %s", chunked)
        if not paths:
            if chunked is False:
                return pd.DataFrame()
            return _utils.empty_generator()
        s3.wait_objects_exist(paths=paths, use_threads=False, boto3_session=session)
        ret = s3.read_parquet(
            path=paths, use_threads=use_threads, boto3_session=session, chunked=chunked, categories=categories
        )
        paths_delete: List[str] = paths + [manifest_path, metadata_path]
        _logger.debug(type(ret))
        if chunked is False:
            if keep_files is False:
                s3.delete_objects(path=paths_delete, use_threads=use_threads, boto3_session=session)
            return ret
        if keep_files is False:
            return _delete_after_iterate(dfs=ret, paths=paths_delete, use_threads=use_threads, boto3_session=session)
        return ret
    dtype, parse_timestamps, parse_dates, converters, binaries = _get_query_metadata(
        query_execution_id=query_id, categories=categories, boto3_session=session
    )
    path = f"{_s3_output}/{query_id}.csv"
    s3.wait_objects_exist(paths=[path], use_threads=False, boto3_session=session)
    _logger.debug("Start CSV reading from %s", path)
    _chunksize: Optional[int] = chunksize if isinstance(chunksize, int) else None
    _logger.debug("_chunksize: %s", _chunksize)
    ret = s3.read_csv(
        path=[path],
        dtype=dtype,
        parse_dates=parse_timestamps,
        converters=converters,
        quoting=csv.QUOTE_ALL,
        keep_default_na=False,
        na_values=[""],
        chunksize=_chunksize,
        skip_blank_lines=False,
        use_threads=False,
        boto3_session=session,
    )
    _logger.debug("Start type casting...")
    _logger.debug(type(ret))
    if chunksize is None:
        df = _fix_csv_types(df=ret, parse_dates=parse_dates, binaries=binaries)
        if keep_files is False:
            s3.delete_objects(path=[path, f"{path}.metadata"], use_threads=use_threads, boto3_session=session)
        return df
    dfs = _fix_csv_types_generator(dfs=ret, parse_dates=parse_dates, binaries=binaries)
    if keep_files is False:
        return _delete_after_iterate(
            dfs=dfs, paths=[path, f"{path}.metadata"], use_threads=use_threads, boto3_session=session
        )
    return dfs


def _resolve_query_with_cache(  # pylint: disable=too-many-return-statements
    cache_info,
    categories: Optional[List[str]],
    chunksize: Optional[Union[int, bool]],
    use_threads: bool,
    session: Optional[boto3.Session],
):
    """Fetch cached data and return it as a pandas Dataframe (or list of Dataframes)."""
    _logger.debug("cache_info: %s", cache_info)
    if cache_info["data_type"] == "parquet":
        manifest_path = cache_info["query_execution_info"]["Statistics"]["DataManifestLocation"]
        # this is needed just so we can access boto's modeled exceptions
        client_s3: boto3.client = _utils.client(service_name="s3", session=session)
        try:
            paths: List[str] = _extract_ctas_manifest_paths(path=manifest_path, boto3_session=session)
        except (client_s3.exceptions.NoSuchBucket, client_s3.exceptions.NoSuchKey):  # pragma: no cover
            return None
        if all([s3.does_object_exist(path) for path in paths]):
            chunked: Union[bool, int] = False if chunksize is None else chunksize
            _logger.debug("chunked: %s", chunked)
            if not paths:  # pragma: no cover
                if chunked is False:
                    return pd.DataFrame()
                return _utils.empty_generator()
            ret = s3.read_parquet(
                path=paths, use_threads=use_threads, boto3_session=session, chunked=chunked, categories=categories
            )
            _logger.debug(type(ret))
            return ret
    elif cache_info["data_type"] == "csv":
        dtype, parse_timestamps, parse_dates, converters, binaries = _get_query_metadata(
            query_execution_id=cache_info["query_execution_info"]["QueryExecutionId"],
            categories=categories,
            boto3_session=session,
        )
        path = cache_info["query_execution_info"]["ResultConfiguration"]["OutputLocation"]
        if s3.does_object_exist(path=path, boto3_session=session):
            _logger.debug("Start CSV reading from %s", path)
            _chunksize: Optional[int] = chunksize if isinstance(chunksize, int) else None
            _logger.debug("_chunksize: %s", _chunksize)
            ret = s3.read_csv(
                path=[path],
                dtype=dtype,
                parse_dates=parse_timestamps,
                converters=converters,
                quoting=csv.QUOTE_ALL,
                keep_default_na=False,
                na_values=[""],
                chunksize=_chunksize,
                skip_blank_lines=False,
                use_threads=False,
                boto3_session=session,
            )
            _logger.debug("Start type casting...")
            _logger.debug(type(ret))
            if chunksize is None:
                df = _fix_csv_types(df=ret, parse_dates=parse_dates, binaries=binaries)
                return df
            dfs = _fix_csv_types_generator(dfs=ret, parse_dates=parse_dates, binaries=binaries)
            return dfs
    raise exceptions.InvalidArgumentValue(f"Invalid data type: {cache_info['data_type']}.")  # pragma: no cover


def _delete_after_iterate(
    dfs: Iterator[pd.DataFrame], paths: List[str], use_threads: bool, boto3_session: boto3.Session
) -> Iterator[pd.DataFrame]:
    for df in dfs:
        yield df
    s3.delete_objects(path=paths, use_threads=use_threads, boto3_session=boto3_session)


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


def _get_workgroup_config(
    session: boto3.Session, workgroup: Optional[str] = None
) -> Dict[str, Union[bool, Optional[str]]]:
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
    wg_config: Dict[str, Union[bool, Optional[str]]] = {
        "enforced": enforced,
        "s3_output": wg_s3_output,
        "encryption": wg_encryption,
        "kms_key": wg_kms_key,
    }
    _logger.debug("wg_config: \n%s", pprint.pformat(wg_config))
    return wg_config


def read_sql_table(
    table: str,
    database: str,
    ctas_approach: bool = True,
    categories: List[str] = None,
    chunksize: Optional[Union[int, bool]] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    keep_files: bool = True,
    ctas_temp_table_name: Optional[str] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    max_cache_seconds: int = 0,
    max_cache_query_inspections: int = 50,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Extract the full table AWS Athena and return the results as a Pandas DataFrame.

    There are two approaches to be defined through ctas_approach parameter:

    1 - `ctas_approach=True` (`Default`):
    Wrap the query with a CTAS and then reads the table data as parquet directly from s3.
    PROS: Faster and can handle some level of nested types
    CONS: Requires create/delete table permissions on Glue and Does not support timestamp with time zone
    (A temporary table will be created and then deleted immediately).

    2 - `ctas_approach False`:
    Does a regular query on Athena and parse the regular CSV result on s3.
    PROS: Does not require create/delete table permissions on Glue and give support timestamp with time zone.
    CONS: Slower (But stills faster than other libraries that uses the regular Athena API)
    and does not handle nested types at all

    Note
    ----
    Valid encryption modes: [None, 'SSE_S3', 'SSE_KMS'].

    `P.S. 'CSE_KMS' is not supported.`

    Note
    ----
    Create the default Athena bucket if it doesn't exist and s3_output is None.

    (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Note
    ----
    ``Batching`` (`chunksize` argument) (Memory Friendly):

    Will anable the function to return a Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies on Wrangler:

    - If **chunksize=True**, a new DataFrame will be returned for each file in the query result.

    - If **chunked=INTEGER**, Wrangler will iterate on the data by number of rows igual the received INTEGER.

    `P.S.` `chunksize=True` if faster and uses less memory while `chunksize=INTEGER` is more precise
    in number of rows for each Dataframe.

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    table : str
        Table name.
    database : str
        AWS Glue/Athena database name.
    ctas_approach: bool
        Wraps the query using a CTAS, and read the resulted parquet data on S3.
        If false, read the regular CSV on S3.
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    chunksize : Union[int, bool], optional
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` wrangler will iterate on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed Wrangler will iterate on the data by number of rows igual the received INTEGER.
    s3_output : str, optional
        AWS S3 path.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key : str, optional
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    keep_files : bool
        Should Wrangler delete or keep the staging files produced by Athena?
    ctas_temp_table_name : str, optional
        The name of the temporary table and also the directory name on S3 where the CTAS result is stored.
        If None, it will use the follow random pattern: `f"temp_table_{pyarrow.compat.guid()}"`.
        On S3 this directory will be under under the pattern: `f"{s3_output}/{ctas_temp_table_name}/"`.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    max_cache_seconds: int
        Wrangler can look up in Athena's history if this table has been read before.
        If so, and its completion time is less than `max_cache_seconds` before now, wrangler
        skips query execution and just returns the same results as last time.
        If cached results are valid, wrangler ignores the `ctas_approach`, `s3_output`, `encryption`, `kms_key`,
        `keep_files` and `ctas_temp_table_name` params.
        If reading cached data fails for any reason, execution falls back to the usual query run path.
    max_cache_query_inspections : int
        Max number of queries that will be inspected from the history to try to find some result to reuse.
        The bigger the number of inspection, the bigger will be the latency for not cached queries.
        Only takes effect if max_cache_seconds > 0.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.athena.read_sql_table(table='...', database='...')

    """
    table = catalog.sanitize_table_name(table=table)
    return read_sql_query(
        sql=f'SELECT * FROM "{table}"',
        database=database,
        ctas_approach=ctas_approach,
        categories=categories,
        chunksize=chunksize,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        keep_files=keep_files,
        ctas_temp_table_name=ctas_temp_table_name,
        use_threads=use_threads,
        boto3_session=boto3_session,
        max_cache_seconds=max_cache_seconds,
        max_cache_query_inspections=max_cache_query_inspections,
    )


def _prepare_query_string_for_comparison(query_string: str) -> str:
    """To use cached data, we need to compare queries. Returns a query string in canonical form."""
    # for now this is a simple complete strip, but it could grow into much more sophisticated
    # query comparison data structures
    query_string = "".join(query_string.split()).strip("()").lower()
    query_string = query_string[:-1] if query_string.endswith(";") is True else query_string
    return query_string


def _get_last_query_executions(
    boto3_session: Optional[boto3.Session] = None, workgroup: Optional[str] = None
) -> Iterator[List[Dict[str, Any]]]:
    """Return an iterator of `query_execution_info`s run by the workgroup in Athena."""
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    args: Dict[str, str] = {}
    if workgroup is not None:
        args["WorkGroup"] = workgroup
    paginator = client_athena.get_paginator("list_query_executions")
    for page in paginator.paginate(**args):
        _logger.debug("paginating Athena's queries history...")
        query_execution_id_list: List[str] = page["QueryExecutionIds"]
        execution_data = client_athena.batch_get_query_execution(QueryExecutionIds=query_execution_id_list)
        yield execution_data.get("QueryExecutions")


def _sort_successful_executions_data(query_executions: List[Dict[str, Any]]):
    """
    Sorts `_get_last_query_executions`'s results based on query Completion DateTime.

    This is useful to guarantee LRU caching rules.
    """
    filtered = [query for query in query_executions if query["Status"].get("State") == "SUCCEEDED"]
    return sorted(filtered, key=lambda e: e["Status"]["CompletionDateTime"], reverse=True)


def _parse_select_query_from_possible_ctas(possible_ctas: str) -> Optional[str]:
    """Check if `possible_ctas` is a valid parquet-generating CTAS and returns the full SELECT statement."""
    possible_ctas = possible_ctas.lower()
    parquet_format_regex = r"format\s*=\s*\'parquet\'\s*,"
    is_parquet_format = re.search(parquet_format_regex, possible_ctas)
    if is_parquet_format is not None:
        unstripped_select_statement_regex = r"\s+as\s+\(*(select|with).*"
        unstripped_select_statement_match = re.search(unstripped_select_statement_regex, possible_ctas, re.DOTALL)
        if unstripped_select_statement_match is not None:
            stripped_select_statement_match = re.search(
                r"(select|with).*", unstripped_select_statement_match.group(0), re.DOTALL
            )
            if stripped_select_statement_match is not None:
                return stripped_select_statement_match.group(0)
    return None  # pragma: no cover


def _check_for_cached_results(
    sql: str, session: boto3.Session, workgroup: Optional[str], max_cache_seconds: int, max_cache_query_inspections: int
) -> Dict[str, Any]:
    """
    Check whether `sql` has been run before, within the `max_cache_seconds` window, by the `workgroup`.

    If so, returns a dict with Athena's `query_execution_info` and the data format.
    """
    num_executions_inspected: int = 0
    if max_cache_seconds > 0:  # pylint: disable=too-many-nested-blocks
        current_timestamp = datetime.datetime.now(datetime.timezone.utc)
        for query_executions in _get_last_query_executions(boto3_session=session, workgroup=workgroup):

            _logger.debug("len(query_executions): %s", len(query_executions))
            cached_queries: List[Dict[str, Any]] = _sort_successful_executions_data(query_executions=query_executions)
            comparable_sql: str = _prepare_query_string_for_comparison(sql)
            _logger.debug("len(cached_queries): %s", len(cached_queries))

            # this could be mapreduced, but it is only 50 items long, tops
            for query_info in cached_queries:

                query_timestamp: datetime.datetime = query_info["Status"]["CompletionDateTime"]
                _logger.debug("current_timestamp: %s", current_timestamp)
                _logger.debug("query_timestamp: %s", query_timestamp)
                if (current_timestamp - query_timestamp).total_seconds() > max_cache_seconds:
                    return {"has_valid_cache": False}  # pragma: no cover

                comparison_query: Optional[str]
                if query_info["StatementType"] == "DDL" and query_info["Query"].startswith("CREATE TABLE"):
                    parsed_query: Optional[str] = _parse_select_query_from_possible_ctas(query_info["Query"])
                    if parsed_query is not None:
                        comparison_query = _prepare_query_string_for_comparison(query_string=parsed_query)
                        _logger.debug("DDL - comparison_query: %s", comparison_query)
                        _logger.debug("DDL - comparable_sql: %s", comparable_sql)
                        if comparison_query == comparable_sql:
                            data_type = "parquet"
                            return {"has_valid_cache": True, "data_type": data_type, "query_execution_info": query_info}

                elif query_info["StatementType"] == "DML" and not query_info["Query"].startswith("INSERT"):
                    comparison_query = _prepare_query_string_for_comparison(query_string=query_info["Query"])
                    _logger.debug("DML - comparison_query: %s", comparison_query)
                    _logger.debug("DML - comparable_sql: %s", comparable_sql)
                    if comparison_query == comparable_sql:
                        data_type = "csv"
                        return {"has_valid_cache": True, "data_type": data_type, "query_execution_info": query_info}

                num_executions_inspected += 1
                _logger.debug("num_executions_inspected: %s", num_executions_inspected)
                _logger.debug("max_cache_query_inspections: %s", max_cache_query_inspections)
                if num_executions_inspected >= max_cache_query_inspections:
                    return {"has_valid_cache": False}  # pragma: no cover

    return {"has_valid_cache": False}
