"""Amazon Athena Module gathering all read_sql_* function."""

import csv
import datetime
import logging
import re
import uuid
from typing import Any, Dict, Iterator, List, Match, NamedTuple, Optional, Union

import boto3
import botocore.exceptions
import pandas as pd

from awswrangler import _utils, catalog, exceptions, s3
from awswrangler._config import apply_configs
from awswrangler.athena._utils import (
    _apply_query_metadata,
    _empty_dataframe_response,
    _get_query_metadata,
    _get_s3_output,
    _get_workgroup_config,
    _QueryMetadata,
    _start_query_execution,
    _WorkGroupConfig,
)

_logger: logging.Logger = logging.getLogger(__name__)


class _CacheInfo(NamedTuple):
    has_valid_cache: bool
    file_format: Optional[str] = None
    query_execution_id: Optional[str] = None
    query_execution_payload: Optional[Dict[str, Any]] = None


def _extract_ctas_manifest_paths(path: str, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    """Get the list of paths of the generated files."""
    bucket_name, key_path = _utils.parse_path(path)
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    body: bytes = client_s3.get_object(Bucket=bucket_name, Key=key_path)["Body"].read()
    return [x for x in body.decode("utf-8").split("\n") if x != ""]


def _fix_csv_types_generator(
    dfs: Iterator[pd.DataFrame], parse_dates: List[str], binaries: List[str]
) -> Iterator[pd.DataFrame]:
    """Apply data types cast to a Pandas DataFrames Generator."""
    for df in dfs:
        yield _fix_csv_types(df=df, parse_dates=parse_dates, binaries=binaries)


def _add_query_metadata_generator(
    dfs: Iterator[pd.DataFrame], query_metadata: _QueryMetadata
) -> Iterator[pd.DataFrame]:
    """Add Query Execution metadata to every DF in iterator."""
    for df in dfs:
        df = _apply_query_metadata(df=df, query_metadata=query_metadata)
        yield df


def _fix_csv_types(df: pd.DataFrame, parse_dates: List[str], binaries: List[str]) -> pd.DataFrame:
    """Apply data types cast to a Pandas DataFrames."""
    if len(df.index) > 0:
        for col in parse_dates:
            df[col] = df[col].dt.date.replace(to_replace={pd.NaT: None})
        for col in binaries:
            df[col] = df[col].str.encode(encoding="utf-8")
    return df


def _delete_after_iterate(
    dfs: Iterator[pd.DataFrame], paths: List[str], use_threads: bool, boto3_session: boto3.Session
) -> Iterator[pd.DataFrame]:
    for df in dfs:
        yield df
    s3.delete_objects(path=paths, use_threads=use_threads, boto3_session=boto3_session)


def _prepare_query_string_for_comparison(query_string: str) -> str:
    """To use cached data, we need to compare queries. Returns a query string in canonical form."""
    # for now this is a simple complete strip, but it could grow into much more sophisticated
    # query comparison data structures
    query_string = "".join(query_string.split()).strip("()").lower()
    query_string = query_string[:-1] if query_string.endswith(";") else query_string
    return query_string


def _compare_query_string(sql: str, other: str) -> bool:
    comparison_query = _prepare_query_string_for_comparison(query_string=other)
    _logger.debug("sql: %s", sql)
    _logger.debug("comparison_query: %s", comparison_query)
    if sql == comparison_query:
        return True
    return False


def _get_last_query_executions(
    boto3_session: Optional[boto3.Session] = None, workgroup: Optional[str] = None
) -> Iterator[List[Dict[str, Any]]]:
    """Return an iterator of `query_execution_info`s run by the workgroup in Athena."""
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    args: Dict[str, Union[str, Dict[str, int]]] = {"PaginationConfig": {"MaxItems": 50, "PageSize": 50}}
    if workgroup is not None:
        args["WorkGroup"] = workgroup
    paginator = client_athena.get_paginator("list_query_executions")
    for page in paginator.paginate(**args):
        _logger.debug("paginating Athena's queries history...")
        query_execution_id_list: List[str] = page["QueryExecutionIds"]
        execution_data = client_athena.batch_get_query_execution(QueryExecutionIds=query_execution_id_list)
        yield execution_data.get("QueryExecutions")


def _sort_successful_executions_data(query_executions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sorts `_get_last_query_executions`'s results based on query Completion DateTime.

    This is useful to guarantee LRU caching rules.
    """
    filtered: List[Dict[str, Any]] = []
    for query in query_executions:
        if (query["Status"].get("State") == "SUCCEEDED") and (query.get("StatementType") in ["DDL", "DML"]):
            filtered.append(query)
    return sorted(filtered, key=lambda e: str(e["Status"]["CompletionDateTime"]), reverse=True)


def _parse_select_query_from_possible_ctas(possible_ctas: str) -> Optional[str]:
    """Check if `possible_ctas` is a valid parquet-generating CTAS and returns the full SELECT statement."""
    possible_ctas = possible_ctas.lower()
    parquet_format_regex: str = r"format\s*=\s*\'parquet\'\s*,"
    is_parquet_format: Optional[Match[str]] = re.search(pattern=parquet_format_regex, string=possible_ctas)
    if is_parquet_format is not None:
        unstripped_select_statement_regex: str = r"\s+as\s+\(*(select|with).*"
        unstripped_select_statement_match: Optional[Match[str]] = re.search(
            unstripped_select_statement_regex, possible_ctas, re.DOTALL
        )
        if unstripped_select_statement_match is not None:
            stripped_select_statement_match: Optional[Match[str]] = re.search(
                r"(select|with).*", unstripped_select_statement_match.group(0), re.DOTALL
            )
            if stripped_select_statement_match is not None:
                return stripped_select_statement_match.group(0)
    return None


def _check_for_cached_results(
    sql: str,
    boto3_session: boto3.Session,
    workgroup: Optional[str],
    max_cache_seconds: int,
    max_cache_query_inspections: int,
) -> _CacheInfo:
    """
    Check whether `sql` has been run before, within the `max_cache_seconds` window, by the `workgroup`.

    If so, returns a dict with Athena's `query_execution_info` and the data format.
    """
    if max_cache_seconds <= 0:
        return _CacheInfo(has_valid_cache=False)
    num_executions_inspected: int = 0
    comparable_sql: str = _prepare_query_string_for_comparison(sql)
    current_timestamp: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
    _logger.debug("current_timestamp: %s", current_timestamp)
    for query_executions in _get_last_query_executions(boto3_session=boto3_session, workgroup=workgroup):
        _logger.debug("len(query_executions): %s", len(query_executions))
        cached_queries: List[Dict[str, Any]] = _sort_successful_executions_data(query_executions=query_executions)
        _logger.debug("len(cached_queries): %s", len(cached_queries))
        for query_info in cached_queries:
            query_execution_id: str = query_info["QueryExecutionId"]
            query_timestamp: datetime.datetime = query_info["Status"]["CompletionDateTime"]
            _logger.debug("query_timestamp: %s", query_timestamp)

            if (current_timestamp - query_timestamp).total_seconds() > max_cache_seconds:
                return _CacheInfo(
                    has_valid_cache=False, query_execution_id=query_execution_id, query_execution_payload=query_info
                )

            statement_type: Optional[str] = query_info.get("StatementType")
            if statement_type == "DDL" and query_info["Query"].startswith("CREATE TABLE"):
                parsed_query: Optional[str] = _parse_select_query_from_possible_ctas(possible_ctas=query_info["Query"])
                if parsed_query is not None:
                    if _compare_query_string(sql=comparable_sql, other=parsed_query):
                        return _CacheInfo(
                            has_valid_cache=True,
                            file_format="parquet",
                            query_execution_id=query_execution_id,
                            query_execution_payload=query_info,
                        )
            elif statement_type == "DML" and not query_info["Query"].startswith("INSERT"):
                if _compare_query_string(sql=comparable_sql, other=query_info["Query"]):
                    return _CacheInfo(
                        has_valid_cache=True,
                        file_format="csv",
                        query_execution_id=query_execution_id,
                        query_execution_payload=query_info,
                    )

            num_executions_inspected += 1
            _logger.debug("num_executions_inspected: %s", num_executions_inspected)
            if num_executions_inspected >= max_cache_query_inspections:
                return _CacheInfo(has_valid_cache=False)

    return _CacheInfo(has_valid_cache=False)


def _fetch_parquet_result(
    query_metadata: _QueryMetadata,
    keep_files: bool,
    categories: Optional[List[str]],
    chunksize: Optional[int],
    use_threads: bool,
    boto3_session: boto3.Session,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    ret: Union[pd.DataFrame, Iterator[pd.DataFrame]]
    chunked: Union[bool, int] = False if chunksize is None else chunksize
    _logger.debug("chunked: %s", chunked)
    if query_metadata.manifest_location is None:
        return _empty_dataframe_response(bool(chunked), query_metadata)
    manifest_path: str = query_metadata.manifest_location
    metadata_path: str = manifest_path.replace("-manifest.csv", ".metadata")
    _logger.debug("manifest_path: %s", manifest_path)
    _logger.debug("metadata_path: %s", metadata_path)
    s3.wait_objects_exist(paths=[manifest_path], use_threads=False, boto3_session=boto3_session)
    paths: List[str] = _extract_ctas_manifest_paths(path=manifest_path, boto3_session=boto3_session)
    if not paths:
        return _empty_dataframe_response(bool(chunked), query_metadata)
    s3.wait_objects_exist(paths=paths, use_threads=False, boto3_session=boto3_session)
    ret = s3.read_parquet(
        path=paths, use_threads=use_threads, boto3_session=boto3_session, chunked=chunked, categories=categories
    )
    if chunked is False:
        ret = _apply_query_metadata(df=ret, query_metadata=query_metadata)
    else:
        ret = _add_query_metadata_generator(dfs=ret, query_metadata=query_metadata)
    paths_delete: List[str] = paths + [manifest_path, metadata_path]
    _logger.debug("type(ret): %s", type(ret))
    if chunked is False:
        if keep_files is False:
            s3.delete_objects(path=paths_delete, use_threads=use_threads, boto3_session=boto3_session)
        return ret
    if keep_files is False:
        return _delete_after_iterate(dfs=ret, paths=paths_delete, use_threads=use_threads, boto3_session=boto3_session)
    return ret


def _fetch_csv_result(
    query_metadata: _QueryMetadata,
    keep_files: bool,
    chunksize: Optional[int],
    use_threads: bool,
    boto3_session: boto3.Session,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    _chunksize: Optional[int] = chunksize if isinstance(chunksize, int) else None
    _logger.debug("_chunksize: %s", _chunksize)
    if query_metadata.output_location is None or query_metadata.output_location.endswith(".csv") is False:
        chunked = _chunksize is not None
        return _empty_dataframe_response(chunked, query_metadata)
    path: str = query_metadata.output_location
    s3.wait_objects_exist(paths=[path], use_threads=False, boto3_session=boto3_session)
    _logger.debug("Start CSV reading from %s", path)
    ret = s3.read_csv(
        path=[path],
        dtype=query_metadata.dtype,
        parse_dates=query_metadata.parse_timestamps,
        converters=query_metadata.converters,
        quoting=csv.QUOTE_ALL,
        keep_default_na=False,
        na_values=["", "NaN"],
        chunksize=_chunksize,
        skip_blank_lines=False,
        use_threads=False,
        boto3_session=boto3_session,
    )
    _logger.debug("Start type casting...")
    _logger.debug(type(ret))
    if _chunksize is None:
        df = _fix_csv_types(df=ret, parse_dates=query_metadata.parse_dates, binaries=query_metadata.binaries)
        df = _apply_query_metadata(df=df, query_metadata=query_metadata)
        if keep_files is False:
            s3.delete_objects(path=[path, f"{path}.metadata"], use_threads=use_threads, boto3_session=boto3_session)
        return df
    dfs = _fix_csv_types_generator(dfs=ret, parse_dates=query_metadata.parse_dates, binaries=query_metadata.binaries)
    dfs = _add_query_metadata_generator(dfs=dfs, query_metadata=query_metadata)
    if keep_files is False:
        return _delete_after_iterate(
            dfs=dfs, paths=[path, f"{path}.metadata"], use_threads=use_threads, boto3_session=boto3_session
        )
    return dfs


def _resolve_query_with_cache(
    cache_info: _CacheInfo,
    categories: Optional[List[str]],
    chunksize: Optional[Union[int, bool]],
    use_threads: bool,
    session: Optional[boto3.Session],
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Fetch cached data and return it as a pandas DataFrame (or list of DataFrames)."""
    _logger.debug("cache_info:\n%s", cache_info)
    if cache_info.query_execution_id is None:
        raise RuntimeError("Trying to resolve with cache but w/o any query execution ID.")
    query_metadata: _QueryMetadata = _get_query_metadata(
        query_execution_id=cache_info.query_execution_id,
        boto3_session=session,
        categories=categories,
        query_execution_payload=cache_info.query_execution_payload,
    )
    if cache_info.file_format == "parquet":
        return _fetch_parquet_result(
            query_metadata=query_metadata,
            keep_files=True,
            categories=categories,
            chunksize=chunksize,
            use_threads=use_threads,
            boto3_session=session,
        )
    if cache_info.file_format == "csv":
        return _fetch_csv_result(
            query_metadata=query_metadata,
            keep_files=True,
            chunksize=chunksize,
            use_threads=use_threads,
            boto3_session=session,
        )
    raise exceptions.InvalidArgumentValue(f"Invalid data type: {cache_info.file_format}.")


def _resolve_query_without_cache_ctas(
    sql: str,
    database: Optional[str],
    data_source: Optional[str],
    s3_output: Optional[str],
    keep_files: bool,
    chunksize: Union[int, bool, None],
    categories: Optional[List[str]],
    encryption: Optional[str],
    workgroup: Optional[str],
    kms_key: Optional[str],
    wg_config: _WorkGroupConfig,
    name: Optional[str],
    use_threads: bool,
    boto3_session: boto3.Session,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    path: str = f"{s3_output}/{name}"
    ext_location: str = "\n" if wg_config.enforced is True else f",\n    external_location = '{path}'\n"
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
    try:
        query_id: str = _start_query_execution(
            sql=sql,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            s3_output=s3_output,
            workgroup=workgroup,
            encryption=encryption,
            kms_key=kms_key,
            boto3_session=boto3_session,
        )
    except botocore.exceptions.ClientError as ex:
        error: Dict[str, Any] = ex.response["Error"]
        if error["Code"] == "InvalidRequestException" and "Exception parsing query" in error["Message"]:
            raise exceptions.InvalidCtasApproachQuery(
                "Is not possible to wrap this query into a CTAS statement. Please use ctas_approach=False."
            )
        if error["Code"] == "InvalidRequestException" and "extraneous input" in error["Message"]:
            raise exceptions.InvalidCtasApproachQuery(
                "Is not possible to wrap this query into a CTAS statement. Please use ctas_approach=False."
            )
        raise ex
    _logger.debug("query_id: %s", query_id)
    try:
        query_metadata: _QueryMetadata = _get_query_metadata(
            query_execution_id=query_id, boto3_session=boto3_session, categories=categories,
        )
    except exceptions.QueryFailed as ex:
        msg: str = str(ex)
        if "Column name" in msg and "specified more than once" in msg:
            raise exceptions.InvalidCtasApproachQuery(
                f"Please, define distinct names for your columns OR pass ctas_approach=False. Root error message: {msg}"
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
    return _fetch_parquet_result(
        query_metadata=query_metadata,
        keep_files=keep_files,
        categories=categories,
        chunksize=chunksize,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )


def _resolve_query_without_cache_regular(
    sql: str,
    database: Optional[str],
    data_source: Optional[str],
    s3_output: Optional[str],
    keep_files: bool,
    chunksize: Union[int, bool, None],
    categories: Optional[List[str]],
    encryption: Optional[str],
    workgroup: Optional[str],
    kms_key: Optional[str],
    wg_config: _WorkGroupConfig,
    use_threads: bool,
    boto3_session: boto3.Session,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    _logger.debug("sql: %s", sql)
    query_id: str = _start_query_execution(
        sql=sql,
        wg_config=wg_config,
        database=database,
        data_source=data_source,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=boto3_session,
    )
    _logger.debug("query_id: %s", query_id)
    query_metadata: _QueryMetadata = _get_query_metadata(
        query_execution_id=query_id, boto3_session=boto3_session, categories=categories,
    )
    return _fetch_csv_result(
        query_metadata=query_metadata,
        keep_files=keep_files,
        chunksize=chunksize,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )


def _resolve_query_without_cache(
    # pylint: disable=too-many-branches,too-many-locals,too-many-return-statements,too-many-statements
    sql: str,
    database: str,
    data_source: Optional[str],
    ctas_approach: bool,
    categories: Optional[List[str]],
    chunksize: Union[int, bool, None],
    s3_output: Optional[str],
    workgroup: Optional[str],
    encryption: Optional[str],
    kms_key: Optional[str],
    keep_files: bool,
    ctas_temp_table_name: Optional[str],
    use_threads: bool,
    boto3_session: boto3.Session,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """
    Execute a query in Athena and returns results as DataFrame, back to `read_sql_query`.

    Usually called by `read_sql_query` when using cache is not possible.
    """
    wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
    _s3_output: str = _get_s3_output(s3_output=s3_output, wg_config=wg_config, boto3_session=boto3_session)
    _s3_output = _s3_output[:-1] if _s3_output[-1] == "/" else _s3_output
    if ctas_approach is True:
        if ctas_temp_table_name is not None:
            name: str = catalog.sanitize_table_name(ctas_temp_table_name)
        else:
            name = f"temp_table_{uuid.uuid4().hex}"
        try:
            return _resolve_query_without_cache_ctas(
                sql=sql,
                database=database,
                data_source=data_source,
                s3_output=_s3_output,
                keep_files=keep_files,
                chunksize=chunksize,
                categories=categories,
                encryption=encryption,
                workgroup=workgroup,
                kms_key=kms_key,
                wg_config=wg_config,
                name=name,
                use_threads=use_threads,
                boto3_session=boto3_session,
            )
        finally:
            catalog.delete_table_if_exists(database=database, table=name, boto3_session=boto3_session)
    return _resolve_query_without_cache_regular(
        sql=sql,
        database=database,
        data_source=data_source,
        s3_output=_s3_output,
        keep_files=keep_files,
        chunksize=chunksize,
        categories=categories,
        encryption=encryption,
        workgroup=workgroup,
        kms_key=kms_key,
        wg_config=wg_config,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )


@apply_configs
def read_sql_query(
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
    data_source: Optional[str] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Execute any SQL query on AWS Athena and return the results as a Pandas DataFrame.

    **Related tutorial:**

    - `Amazon Athena <https://github.com/awslabs/aws-data-wrangler/blob/
      master/tutorials/006%20-%20Amazon%20Athena.ipynb>`_
    - `Athena Cache <https://github.com/awslabs/aws-data-wrangler/blob/
      master/tutorials/019%20-%20Athena%20Cache.ipynb>`_
    - `Global Configurations <https://github.com/awslabs/aws-data-wrangler/blob/
      master/tutorials/021%20-%20Global%20Configurations.ipynb>`_

    **There are two approaches to be defined through ctas_approach parameter:**

    **1** - ctas_approach=True (Default):

    Wrap the query with a CTAS and then reads the table data as parquet directly from s3.

    PROS:

    - Faster for mid and big result sizes.
    - Can handle some level of nested types.

    CONS:

    - Requires create/delete table permissions on Glue.
    - Does not support timestamp with time zone
    - Does not support columns with repeated names.
    - Does not support columns with undefined data types.
    - A temporary table will be created and then deleted immediately.
    - Does not support custom data_source/catalog_id.

    **2** - ctas_approach=False:

    Does a regular query on Athena and parse the regular CSV result on s3.

    PROS:

    - Faster for small result sizes (less latency).
    - Does not require create/delete table permissions on Glue
    - Supports timestamp with time zone.
    - Support custom data_source/catalog_id.

    CONS:

    - Slower for big results (But stills faster than other libraries that uses the regular Athena's API)
    - Does not handle nested types at all.


    Note
    ----
    The resulting DataFrame (or every DataFrame in the returned Iterator for chunked queries) have a
    `query_metadata` attribute, which brings the query result metadata returned by
    `Boto3/Athena <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services
    /athena.html#Athena.Client.get_query_execution>`_ .

    For a practical example check out the
    `related tutorial <https://github.com/awslabs/aws-data-wrangler/blob/
    master/tutorials/024%20-%20Athena%20Query%20Metadata.ipynb>`_!


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
    `chunksize` argument (Memory Friendly) (i.e batching):

    Enable the function to return an Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies on Wrangler:

    - If **chunksize=True**, a new DataFrame will be returned for each file in the query result.

    - If **chunked=INTEGER**, Wrangler will iterate on the data by number of rows igual the received INTEGER.

    `P.S.` `chunksize=True` if faster and uses less memory while `chunksize=INTEGER` is more precise
    in number of rows for each Dataframe.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    sql : str
        SQL query.
    database : str
        AWS Glue/Athena database name - It is only the origin database from where the query will be launched.
        You can still using and mixing several databases writing the full table name within the sql
        (e.g. `database.table`).
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
        If None, it will use the follow random pattern: `f"temp_table_{uuid.uuid4().hex()}"`.
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
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.athena.read_sql_query(sql="...", database="...")
    >>> scanned_bytes = df.query_metadata["Statistics"]["DataScannedInBytes"]

    """
    if ctas_approach and data_source not in (None, "AwsDataCatalog"):
        raise exceptions.InvalidArgumentCombination(
            "Queries with ctas_approach=True (default) does not support "
            "data_source values different than None and 'AwsDataCatalog'. "
            "Please check the related tutorial for more details "
            "(https://github.com/awslabs/aws-data-wrangler/blob/master/"
            "tutorials/006%20-%20Amazon%20Athena.ipynb)"
        )
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    cache_info: _CacheInfo = _check_for_cached_results(
        sql=sql,
        boto3_session=session,
        workgroup=workgroup,
        max_cache_seconds=max_cache_seconds,
        max_cache_query_inspections=max_cache_query_inspections,
    )
    _logger.debug("cache_info:\n%s", cache_info)
    if cache_info.has_valid_cache is True:
        _logger.debug("Valid cache found. Retrieving...")
        try:
            return _resolve_query_with_cache(
                cache_info=cache_info,
                categories=categories,
                chunksize=chunksize,
                use_threads=use_threads,
                session=session,
            )
        except Exception as e:  # pylint: disable=broad-except
            _logger.error(e)  # if there is anything wrong with the cache, just fallback to the usual path
            _logger.debug("Corrupted cache. Continuing to execute query...")
    return _resolve_query_without_cache(
        sql=sql,
        database=database,
        data_source=data_source,
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
        boto3_session=session,
    )


@apply_configs
def read_sql_table(
    table: str,
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
    data_source: Optional[str] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Extract the full table AWS Athena and return the results as a Pandas DataFrame.

    **Related tutorial:**

    - `Amazon Athena <https://github.com/awslabs/aws-data-wrangler/blob/
      master/tutorials/006%20-%20Amazon%20Athena.ipynb>`_
    - `Athena Cache <https://github.com/awslabs/aws-data-wrangler/blob/
      master/tutorials/019%20-%20Athena%20Cache.ipynb>`_
    - `Global Configurations <https://github.com/awslabs/aws-data-wrangler/blob/
      master/tutorials/021%20-%20Global%20Configurations.ipynb>`_

    **There are two approaches to be defined through ctas_approach parameter:**

    **1** - ctas_approach=True (Default):

    Wrap the query with a CTAS and then reads the table data as parquet directly from s3.

    PROS:

    - Faster for mid and big result sizes.
    - Can handle some level of nested types.

    CONS:

    - Requires create/delete table permissions on Glue.
    - Does not support timestamp with time zone
    - Does not support columns with repeated names.
    - Does not support columns with undefined data types.
    - A temporary table will be created and then deleted immediately.

    **2** - ctas_approach=False:

    Does a regular query on Athena and parse the regular CSV result on s3.

    PROS:

    - Faster for small result sizes (less latency).
    - Does not require create/delete table permissions on Glue
    - Supports timestamp with time zone.

    CONS:

    - Slower for big results (But stills faster than other libraries that uses the regular Athena's API)
    - Does not handle nested types at all.

    Note
    ----
    The resulting DataFrame (or every DataFrame in the returned Iterator for chunked queries) have a
    `query_metadata` attribute, which brings the query result metadata returned by
    `Boto3/Athena <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services
    /athena.html#Athena.Client.get_query_execution>`_ .

    For a practical example check out the
    `related tutorial <https://github.com/awslabs/aws-data-wrangler/blob/master/
    tutorials/024%20-%20Athena%20Query%20Metadata.ipynb>`_!


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
    `chunksize` argument (Memory Friendly) (i.e batching):

    Enable the function to return an Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies on Wrangler:

    - If **chunksize=True**, a new DataFrame will be returned for each file in the query result.

    - If **chunked=INTEGER**, Wrangler will iterate on the data by number of rows igual the received INTEGER.

    `P.S.` `chunksize=True` if faster and uses less memory while `chunksize=INTEGER` is more precise
    in number of rows for each Dataframe.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

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
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
    kms_key : str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    keep_files : bool
        Should Wrangler delete or keep the staging files produced by Athena?
    ctas_temp_table_name : str, optional
        The name of the temporary table and also the directory name on S3 where the CTAS result is stored.
        If None, it will use the follow random pattern: `f"temp_table_{uuid.uuid4().hex}"`.
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
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.athena.read_sql_table(table="...", database="...")
    >>> scanned_bytes = df.query_metadata["Statistics"]["DataScannedInBytes"]

    """
    table = catalog.sanitize_table_name(table=table)
    return read_sql_query(
        sql=f'SELECT * FROM "{table}"',
        database=database,
        data_source=data_source,
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
