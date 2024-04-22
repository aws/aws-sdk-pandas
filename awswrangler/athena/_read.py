"""Amazon Athena Module gathering all read_sql_* function."""

from __future__ import annotations

import csv
import logging
import sys
import uuid
from datetime import date
from typing import Any, Iterator, cast

import boto3
import botocore.exceptions
import pandas as pd
from typing_extensions import Literal

from awswrangler import _utils, catalog, exceptions, s3, typing
from awswrangler._config import apply_configs
from awswrangler._data_types import cast_pandas_with_athena_types
from awswrangler.athena._utils import (
    _QUERY_WAIT_POLLING_DELAY,
    _apply_formatter,
    _apply_query_metadata,
    _empty_dataframe_response,
    _get_query_metadata,
    _get_s3_output,
    _get_workgroup_config,
    _QueryMetadata,
    _start_query_execution,
    _WorkGroupConfig,
    create_ctas_table,
)

from ._cache import _cache_manager, _CacheInfo, _check_for_cached_results

shapely_wkt = _utils.import_optional_dependency("shapely.wkt")
geopandas = _utils.import_optional_dependency("geopandas")

_logger: logging.Logger = logging.getLogger(__name__)


@_utils.check_optional_dependency(shapely_wkt, "shapely")
@_utils.check_optional_dependency(geopandas, "geopandas")
def _cast_geometry(df: pd.DataFrame, parse_geometry: list[str] = None):
    def load_geom_wkt(x):
        """Load geometry from well-known text."""
        return shapely_wkt.loads(x)

    for col in parse_geometry:
        df[col] = geopandas.GeoSeries(df[col].apply(load_geom_wkt))

    return geopandas.GeoDataFrame(df)


def _extract_ctas_manifest_paths(path: str, boto3_session: boto3.Session | None = None) -> list[str]:
    """Get the list of paths of the generated files."""
    bucket_name, key_path = _utils.parse_path(path)
    client_s3 = _utils.client(service_name="s3", session=boto3_session)
    body: bytes = client_s3.get_object(Bucket=bucket_name, Key=key_path)["Body"].read()
    paths = [x for x in body.decode("utf-8").split("\n") if x]
    _logger.debug("Read %d paths from manifest file in: %s", len(paths), path)
    return paths


def _fix_csv_types_generator(
    dfs: Iterator[pd.DataFrame], parse_dates: list[str], binaries: list[str], parse_geometry: list[str]
) -> Iterator[pd.DataFrame]:
    """Apply data types cast to a Pandas DataFrames Generator."""
    for df in dfs:
        yield _fix_csv_types(df=df, parse_dates=parse_dates, binaries=binaries, parse_geometry=parse_geometry)


def _add_query_metadata_generator(
    dfs: Iterator[pd.DataFrame], query_metadata: _QueryMetadata
) -> Iterator[pd.DataFrame]:
    """Add Query Execution metadata to every DF in iterator."""
    for df in dfs:
        df = _apply_query_metadata(df=df, query_metadata=query_metadata)  # noqa: PLW2901
        yield df


def _fix_csv_types(
    df: pd.DataFrame, parse_dates: list[str], binaries: list[str], parse_geometry: list[str]
) -> pd.DataFrame:
    """Apply data types cast to a Pandas DataFrames."""
    if len(df.index) > 0:
        for col in parse_dates:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.date.replace(to_replace={pd.NaT: None})
            else:
                df[col] = (
                    df[col].replace(to_replace={pd.NaT: None}).apply(lambda x: date.fromisoformat(x) if x else None)
                )
        for col in binaries:
            df[col] = df[col].str.encode(encoding="utf-8")

    if geopandas and parse_geometry:
        df = _cast_geometry(df, parse_geometry=parse_geometry)

    return df


def _delete_after_iterate(
    dfs: Iterator[pd.DataFrame],
    paths: list[str],
    use_threads: bool | int,
    boto3_session: boto3.Session | None,
    s3_additional_kwargs: dict[str, str] | None,
) -> Iterator[pd.DataFrame]:
    yield from dfs
    s3.delete_objects(
        path=paths, use_threads=use_threads, boto3_session=boto3_session, s3_additional_kwargs=s3_additional_kwargs
    )


def _fetch_parquet_result(
    query_metadata: _QueryMetadata,
    keep_files: bool,
    categories: list[str] | None,
    chunksize: int | None,
    use_threads: bool | int,
    boto3_session: boto3.Session | None,
    s3_additional_kwargs: dict[str, Any] | None,
    temp_table_fqn: str | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    ret: pd.DataFrame | Iterator[pd.DataFrame]
    chunked: bool | int = False if chunksize is None else chunksize
    _logger.debug("Chunked: %s", chunked)
    if query_metadata.manifest_location is None:
        return _empty_dataframe_response(bool(chunked), query_metadata)
    manifest_path: str = query_metadata.manifest_location
    metadata_path: str = manifest_path.replace("-manifest.csv", ".metadata")
    _logger.debug("Manifest path: %s", manifest_path)
    _logger.debug("Metadata path: %s", metadata_path)
    paths: list[str] = _extract_ctas_manifest_paths(path=manifest_path, boto3_session=boto3_session)
    if not paths:
        if not temp_table_fqn:
            raise exceptions.EmptyDataFrame("Query would return untyped, empty dataframe.")
        database, temp_table_name = map(lambda x: x.replace('"', ""), temp_table_fqn.split("."))
        dtype_dict = catalog.get_table_types(database=database, table=temp_table_name, boto3_session=boto3_session)
        if dtype_dict is None:
            raise exceptions.ResourceDoesNotExist(f"Temp table {temp_table_fqn} not found.")
        df = pd.DataFrame(columns=list(dtype_dict.keys()))
        df = cast_pandas_with_athena_types(df=df, dtype=dtype_dict, dtype_backend=dtype_backend)
        df = _apply_query_metadata(df=df, query_metadata=query_metadata)
        if chunked:
            return (df,)
        return df
    if not pyarrow_additional_kwargs:
        pyarrow_additional_kwargs = {}
    if categories:
        pyarrow_additional_kwargs["categories"] = categories
    _logger.debug("Reading Parquet result from %d paths", len(paths))
    ret = s3.read_parquet(
        path=paths,
        use_threads=use_threads,
        boto3_session=boto3_session,
        chunked=chunked,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        dtype_backend=dtype_backend,
    )

    if chunked is False:
        ret = _apply_query_metadata(df=ret, query_metadata=query_metadata)
    else:
        ret = _add_query_metadata_generator(dfs=ret, query_metadata=query_metadata)
    paths_delete: list[str] = paths + [manifest_path, metadata_path]
    if chunked is False:
        if keep_files is False:
            s3.delete_objects(
                path=paths_delete,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
        return ret
    if keep_files is False:
        return _delete_after_iterate(
            dfs=ret,
            paths=paths_delete,
            use_threads=use_threads,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
    return ret


def _fetch_csv_result(
    query_metadata: _QueryMetadata,
    keep_files: bool,
    chunksize: int | None,
    use_threads: bool | int,
    boto3_session: boto3.Session | None,
    s3_additional_kwargs: dict[str, Any] | None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    _chunksize: int | None = chunksize if isinstance(chunksize, int) else None
    _logger.debug("Chunksize: %s", _chunksize)
    if query_metadata.output_location is None or query_metadata.output_location.endswith(".csv") is False:
        chunked = _chunksize is not None
        return _empty_dataframe_response(chunked, query_metadata)
    path: str = query_metadata.output_location
    _logger.debug("Reading CSV result from %s", path)
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
        dtype_backend=dtype_backend,
    )
    _logger.debug("Start type casting...")
    if _chunksize is None:
        df = _fix_csv_types(
            df=ret,
            parse_dates=query_metadata.parse_dates,
            binaries=query_metadata.binaries,
            parse_geometry=query_metadata.parse_geometry,
        )
        df = _apply_query_metadata(df=df, query_metadata=query_metadata)
        if keep_files is False:
            s3.delete_objects(
                path=[path, f"{path}.metadata"],
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
        return df
    dfs = _fix_csv_types_generator(
        dfs=ret,
        parse_dates=query_metadata.parse_dates,
        binaries=query_metadata.binaries,
        parse_geometry=query_metadata.parse_geometry,
    )
    dfs = _add_query_metadata_generator(dfs=dfs, query_metadata=query_metadata)
    if keep_files is False:
        return _delete_after_iterate(
            dfs=dfs,
            paths=[path, f"{path}.metadata"],
            use_threads=use_threads,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
    return dfs


def _resolve_query_with_cache(
    cache_info: _CacheInfo,
    categories: list[str] | None,
    chunksize: int | bool | None,
    use_threads: bool | int,
    athena_query_wait_polling_delay: float,
    session: boto3.Session | None,
    s3_additional_kwargs: dict[str, Any] | None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """Fetch cached data and return it as a pandas DataFrame (or list of DataFrames)."""
    _logger.debug("cache_info:\n%s", cache_info)
    if cache_info.query_execution_id is None:
        raise RuntimeError("Trying to resolve with cache but w/o any query execution ID.")
    query_metadata: _QueryMetadata = _get_query_metadata(
        query_execution_id=cache_info.query_execution_id,
        boto3_session=session,
        categories=categories,
        query_execution_payload=cache_info.query_execution_payload,
        metadata_cache_manager=_cache_manager,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
    )
    if cache_info.file_format == "parquet":
        return _fetch_parquet_result(
            query_metadata=query_metadata,
            keep_files=True,
            categories=categories,
            chunksize=chunksize,
            use_threads=use_threads,
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            dtype_backend=dtype_backend,
        )
    if cache_info.file_format == "csv":
        return _fetch_csv_result(
            query_metadata=query_metadata,
            keep_files=True,
            chunksize=chunksize,
            use_threads=use_threads,
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
    raise exceptions.InvalidArgumentValue(f"Invalid data type: {cache_info.file_format}.")


def _resolve_query_without_cache_ctas(
    sql: str,
    database: str | None,
    data_source: str | None,
    s3_output: str | None,
    keep_files: bool,
    chunksize: int | bool | None,
    categories: list[str] | None,
    encryption: str | None,
    workgroup: str | None,
    kms_key: str | None,
    alt_database: str | None,
    name: str | None,
    ctas_bucketing_info: typing.BucketingInfoTuple | None,
    ctas_write_compression: str | None,
    athena_query_wait_polling_delay: float,
    use_threads: bool | int,
    s3_additional_kwargs: dict[str, Any] | None,
    boto3_session: boto3.Session | None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
    execution_params: list[str] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    ctas_query_info: dict[str, str | _QueryMetadata] = create_ctas_table(
        sql=sql,
        database=database,
        ctas_table=name,
        ctas_database=alt_database,
        bucketing_info=ctas_bucketing_info,
        data_source=data_source,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        write_compression=ctas_write_compression,
        kms_key=kms_key,
        wait=True,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        boto3_session=boto3_session,
        execution_params=execution_params,
    )
    fully_qualified_name: str = f'"{ctas_query_info["ctas_database"]}"."{ctas_query_info["ctas_table"]}"'
    ctas_query_metadata = cast(_QueryMetadata, ctas_query_info["ctas_query_metadata"])
    _logger.debug("CTAS query metadata: %s", ctas_query_metadata)
    return _fetch_parquet_result(
        query_metadata=ctas_query_metadata,
        keep_files=keep_files,
        categories=categories,
        chunksize=chunksize,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
        temp_table_fqn=fully_qualified_name,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        dtype_backend=dtype_backend,
    )


def _resolve_query_without_cache_unload(
    sql: str,
    file_format: str,
    compression: str | None,
    field_delimiter: str | None,
    partitioned_by: list[str] | None,
    database: str | None,
    data_source: str | None,
    s3_output: str | None,
    keep_files: bool,
    chunksize: int | bool | None,
    categories: list[str] | None,
    encryption: str | None,
    kms_key: str | None,
    workgroup: str | None,
    use_threads: bool | int,
    athena_query_wait_polling_delay: float,
    s3_additional_kwargs: dict[str, Any] | None,
    boto3_session: boto3.Session | None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
    execution_params: list[str] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    query_metadata = _unload(
        sql=sql,
        path=s3_output,
        file_format=file_format,
        compression=compression,
        field_delimiter=field_delimiter,
        partitioned_by=partitioned_by,
        workgroup=workgroup,
        database=database,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=boto3_session,
        data_source=data_source,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        execution_params=execution_params,
    )
    if file_format == "PARQUET":
        return _fetch_parquet_result(
            query_metadata=query_metadata,
            keep_files=keep_files,
            categories=categories,
            chunksize=chunksize,
            use_threads=use_threads,
            s3_additional_kwargs=s3_additional_kwargs,
            boto3_session=boto3_session,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            dtype_backend=dtype_backend,
        )
    raise exceptions.InvalidArgumentValue("Only PARQUET file format is supported when unload_approach=True.")


def _resolve_query_without_cache_regular(
    sql: str,
    database: str | None,
    data_source: str | None,
    s3_output: str | None,
    keep_files: bool,
    chunksize: int | bool | None,
    categories: list[str] | None,
    encryption: str | None,
    workgroup: str | None,
    kms_key: str | None,
    use_threads: bool | int,
    athena_query_wait_polling_delay: float,
    s3_additional_kwargs: dict[str, Any] | None,
    boto3_session: boto3.Session | None,
    execution_params: list[str] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    client_request_token: str | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
    s3_output = _get_s3_output(s3_output=s3_output, wg_config=wg_config, boto3_session=boto3_session)
    s3_output = s3_output[:-1] if s3_output[-1] == "/" else s3_output
    _logger.debug("Executing sql: %s", sql)
    query_id: str = _start_query_execution(
        sql=sql,
        wg_config=wg_config,
        database=database,
        data_source=data_source,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        execution_params=execution_params,
        client_request_token=client_request_token,
        boto3_session=boto3_session,
    )
    _logger.debug("Query id: %s", query_id)
    query_metadata: _QueryMetadata = _get_query_metadata(
        query_execution_id=query_id,
        boto3_session=boto3_session,
        categories=categories,
        metadata_cache_manager=_cache_manager,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        dtype_backend=dtype_backend,
    )
    return _fetch_csv_result(
        query_metadata=query_metadata,
        keep_files=keep_files,
        chunksize=chunksize,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        dtype_backend=dtype_backend,
    )


def _resolve_query_without_cache(
    sql: str,
    database: str,
    data_source: str | None,
    ctas_approach: bool,
    unload_approach: bool,
    unload_parameters: typing.AthenaUNLOADSettings | None,
    categories: list[str] | None,
    chunksize: int | bool | None,
    s3_output: str | None,
    workgroup: str | None,
    encryption: str | None,
    kms_key: str | None,
    keep_files: bool,
    ctas_database: str | None,
    ctas_temp_table_name: str | None,
    ctas_bucketing_info: typing.BucketingInfoTuple | None,
    ctas_write_compression: str | None,
    athena_query_wait_polling_delay: float,
    use_threads: bool | int,
    s3_additional_kwargs: dict[str, Any] | None,
    boto3_session: boto3.Session | None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
    execution_params: list[str] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    client_request_token: str | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """
    Execute a query in Athena and returns results as DataFrame, back to `read_sql_query`.

    Usually called by `read_sql_query` when using cache is not possible.
    """
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
                s3_output=s3_output,
                keep_files=keep_files,
                chunksize=chunksize,
                categories=categories,
                encryption=encryption,
                workgroup=workgroup,
                kms_key=kms_key,
                alt_database=ctas_database,
                name=name,
                ctas_bucketing_info=ctas_bucketing_info,
                ctas_write_compression=ctas_write_compression,
                athena_query_wait_polling_delay=athena_query_wait_polling_delay,
                use_threads=use_threads,
                s3_additional_kwargs=s3_additional_kwargs,
                boto3_session=boto3_session,
                pyarrow_additional_kwargs=pyarrow_additional_kwargs,
                execution_params=execution_params,
                dtype_backend=dtype_backend,
            )
        finally:
            catalog.delete_table_if_exists(database=ctas_database or database, table=name, boto3_session=boto3_session)
    elif unload_approach is True:
        if unload_parameters is None:
            unload_parameters = {}
        return _resolve_query_without_cache_unload(
            sql=sql,
            file_format=unload_parameters.get("file_format") or "PARQUET",
            compression=unload_parameters.get("compression"),
            field_delimiter=unload_parameters.get("field_delimiter"),
            partitioned_by=unload_parameters.get("partitioned_by"),
            database=database,
            data_source=data_source,
            s3_output=s3_output,
            keep_files=keep_files,
            chunksize=chunksize,
            categories=categories,
            encryption=encryption,
            kms_key=kms_key,
            workgroup=workgroup,
            use_threads=use_threads,
            athena_query_wait_polling_delay=athena_query_wait_polling_delay,
            s3_additional_kwargs=s3_additional_kwargs,
            boto3_session=boto3_session,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            execution_params=execution_params,
            dtype_backend=dtype_backend,
        )
    return _resolve_query_without_cache_regular(
        sql=sql,
        database=database,
        data_source=data_source,
        s3_output=s3_output,
        keep_files=keep_files,
        chunksize=chunksize,
        categories=categories,
        encryption=encryption,
        workgroup=workgroup,
        kms_key=kms_key,
        use_threads=use_threads,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
        execution_params=execution_params,
        dtype_backend=dtype_backend,
        client_request_token=client_request_token,
    )


def _unload(
    sql: str,
    path: str | None,
    file_format: str,
    compression: str | None,
    field_delimiter: str | None,
    partitioned_by: list[str] | None,
    workgroup: str | None,
    database: str | None,
    encryption: str | None,
    kms_key: str | None,
    boto3_session: boto3.Session | None,
    data_source: str | None,
    athena_query_wait_polling_delay: float,
    execution_params: list[str] | None,
) -> _QueryMetadata:
    wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
    s3_output: str = _get_s3_output(s3_output=path, wg_config=wg_config, boto3_session=boto3_session)
    s3_output = s3_output[:-1] if s3_output[-1] == "/" else s3_output
    # Athena does not enforce a Query Result Location for UNLOAD. Thus, the workgroup output location
    # is only used if no path is supplied.
    if not path:
        path = s3_output

    # Set UNLOAD parameters
    unload_parameters = f"  format='{file_format}'"
    if compression:
        unload_parameters += f"  , compression='{compression}'"
    if field_delimiter:
        unload_parameters += f"  , field_delimiter='{field_delimiter}'"
    if partitioned_by:
        unload_parameters += f"  , partitioned_by=ARRAY{partitioned_by}"

    sql = f"UNLOAD ({sql}) " f"TO '{path}' " f"WITH ({unload_parameters})"
    _logger.debug("Executing unload query: %s", sql)
    try:
        query_id: str = _start_query_execution(
            sql=sql,
            workgroup=workgroup,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            s3_output=s3_output,
            encryption=encryption,
            kms_key=kms_key,
            boto3_session=boto3_session,
            execution_params=execution_params,
        )
    except botocore.exceptions.ClientError as ex:
        msg: str = str(ex)
        error = ex.response["Error"]
        if error["Code"] == "InvalidRequestException":
            raise exceptions.InvalidArgumentValue(f"Exception parsing query. Root error message: {msg}")
        raise ex
    _logger.debug("query_id: %s", query_id)
    try:
        query_metadata: _QueryMetadata = _get_query_metadata(
            query_execution_id=query_id,
            boto3_session=boto3_session,
            metadata_cache_manager=_cache_manager,
            athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        )
    except exceptions.QueryFailed as ex:
        msg = str(ex)
        if "Column name" in msg and "specified more than once" in msg:
            raise exceptions.InvalidArgumentValue(
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
    return query_metadata


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def get_query_results(
    query_execution_id: str,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    categories: list[str] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    chunksize: int | bool | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """Get AWS Athena SQL query results as a Pandas DataFrame.

    Parameters
    ----------
    query_execution_id : str
        SQL query's execution_id on AWS Athena.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    chunksize: Union[int, bool], optional
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    pyarrow_additional_kwargs: dict[str, Any]], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.get_query_results(
    ...     query_execution_id="cbae5b41-8103-4709-95bb-887f88edd4f2"
    ... )

    """
    query_metadata: _QueryMetadata = _get_query_metadata(
        query_execution_id=query_execution_id,
        boto3_session=boto3_session,
        categories=categories,
        metadata_cache_manager=_cache_manager,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
    )
    _logger.debug("Query metadata:\n%s", query_metadata)
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    query_info = client_athena.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]
    _logger.debug("Query info:\n%s", query_info)
    statement_type: str | None = query_info.get("StatementType")
    if (statement_type == "DDL" and query_info["Query"].startswith("CREATE TABLE")) or (
        statement_type == "DML" and query_info["Query"].startswith("UNLOAD")
    ):
        return _fetch_parquet_result(
            query_metadata=query_metadata,
            keep_files=True,
            categories=categories,
            chunksize=chunksize,
            use_threads=use_threads,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            dtype_backend=dtype_backend,
        )
    if statement_type == "DML" and not query_info["Query"].startswith("INSERT"):
        return _fetch_csv_result(
            query_metadata=query_metadata,
            keep_files=True,
            chunksize=chunksize,
            use_threads=use_threads,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            dtype_backend=dtype_backend,
        )
    raise exceptions.UndetectedType(f"""Unable to get results for: {query_info["Query"]}.""")


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def read_sql_query(
    sql: str,
    database: str,
    ctas_approach: bool = True,
    unload_approach: bool = False,
    ctas_parameters: typing.AthenaCTASSettings | None = None,
    unload_parameters: typing.AthenaUNLOADSettings | None = None,
    categories: list[str] | None = None,
    chunksize: int | bool | None = None,
    s3_output: str | None = None,
    workgroup: str = "primary",
    encryption: str | None = None,
    kms_key: str | None = None,
    keep_files: bool = True,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    client_request_token: str | None = None,
    athena_cache_settings: typing.AthenaCacheSettings | None = None,
    data_source: str | None = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    params: dict[str, Any] | list[str] | None = None,
    paramstyle: Literal["qmark", "named"] = "named",
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    s3_additional_kwargs: dict[str, Any] | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """Execute any SQL query on AWS Athena and return the results as a Pandas DataFrame.

    **Related tutorial:**

    - `Amazon Athena <https://aws-sdk-pandas.readthedocs.io/en/3.7.3/
      tutorials/006%20-%20Amazon%20Athena.html>`_
    - `Athena Cache <https://aws-sdk-pandas.readthedocs.io/en/3.7.3/
      tutorials/019%20-%20Athena%20Cache.html>`_
    - `Global Configurations <https://aws-sdk-pandas.readthedocs.io/en/3.7.3/
      tutorials/021%20-%20Global%20Configurations.html>`_

    **There are three approaches available through ctas_approach and unload_approach parameters:**

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

    **2** - unload_approach=True and ctas_approach=False:

    Does an UNLOAD query on Athena and parse the Parquet result on s3.

    PROS:

    - Faster for mid and big result sizes.
    - Can handle some level of nested types.
    - Does not modify Glue Data Catalog

    CONS:

    - Output S3 path must be empty.
    - Does not support timestamp with time zone.
    - Does not support columns with repeated names.
    - Does not support columns with undefined data types.

    **3** - ctas_approach=False:

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
    `related tutorial <https://aws-sdk-pandas.readthedocs.io/en/3.7.3/
    tutorials/024%20-%20Athena%20Query%20Metadata.html>`_!


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

    Return an Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies:

    - If **chunksize=True**, depending on the size of the data, one or more data frames are returned per file in the query result.
      Unlike **chunksize=INTEGER**, rows from different files are not mixed in the resulting data frames.

    - If **chunksize=INTEGER**, awswrangler iterates on the data by number of rows equal to the received INTEGER.

    `P.S.` `chunksize=True` is faster and uses less memory while `chunksize=INTEGER` is more precise
    in number of rows for each data frame.

    `P.P.S.` If `ctas_approach=False` and `chunksize=True`, you will always receive an iterator with a
    single DataFrame because regular Athena queries only produces a single output file.

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
    unload_approach: bool
        Wraps the query using UNLOAD, and read the results from S3.
        Only PARQUET format is supported.
    ctas_parameters: typing.AthenaCTASSettings, optional
        Parameters of the CTAS such as database, temp_table_name, bucketing_info, and compression.
    unload_parameters : typing.AthenaUNLOADSettings, optional
        Parameters of the UNLOAD such as format, compression, field_delimiter, and partitioned_by.
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    chunksize : Union[int, bool], optional
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
    s3_output : str, optional
        Amazon S3 path.
    workgroup : str
        Athena workgroup. Primary by default.
    encryption : str, optional
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
    kms_key : str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    keep_files : bool
        Whether staging files produced by Athena are retained. 'True' by default.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    client_request_token : str, optional
        A unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once).
        If another StartQueryExecution request is received, the same response is returned and another query is not created.
        If a parameter has changed, for example, the QueryString , an error is returned.
        If you pass the same client_request_token value with different parameters the query fails with error
        message "Idempotent parameters do not match". Use this only with ctas_approach=False and unload_approach=False
        and disabled cache.
    athena_cache_settings: typing.AthenaCacheSettings, optional
        Parameters of the Athena cache settings such as max_cache_seconds, max_cache_query_inspections,
        max_remote_cache_entries, and max_local_cache_entries.
        AthenaCacheSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an
        instance of AthenaCacheSettings or as a regular Python dict.
        If cached results are valid, awswrangler ignores the `ctas_approach`, `s3_output`, `encryption`, `kms_key`,
        `keep_files` and `ctas_temp_table_name` params.
        If reading cached data fails for any reason, execution falls back to the usual query run path.
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.
    params: Dict[str, any] | List[str], optional
        Parameters that will be used for constructing the SQL query.
        Only named or question mark parameters are supported.
        The parameter style needs to be specified in the ``paramstyle`` parameter.

        For ``paramstyle="named"``, this value needs to be a dictionary.
        The dict needs to contain the information in the form ``{'name': 'value'}`` and the SQL query needs to contain
        ``:name``.
        The formatter will be applied client-side in this scenario.

        For ``paramstyle="qmark"``, this value needs to be a list of strings.
        The formatter will be applied server-side.
        The values are applied sequentially to the parameters in the query in the order in which the parameters occur.
    paramstyle: str, optional
        Determines the style of ``params``.
        Possible values are:

        - ``named``
        - ``qmark``
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    pyarrow_additional_kwargs: dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.athena.read_sql_query(sql="...", database="...")
    >>> scanned_bytes = df.query_metadata["Statistics"]["DataScannedInBytes"]

    >>> import awswrangler as wr
    >>> df = wr.athena.read_sql_query(
    ...     sql="SELECT * FROM my_table WHERE name=:name AND city=:city",
    ...     params={"name": "filtered_name", "city": "filtered_city"}
    ... )

    >>> import awswrangler as wr
    >>> df = wr.athena.read_sql_query(
    ...     sql="...",
    ...     database="...",
    ...     athena_cache_settings={
    ...          "max_cache_seconds": 90,
    ...     },
    ... )

    """
    if ctas_approach and data_source not in (None, "AwsDataCatalog"):
        raise exceptions.InvalidArgumentCombination(
            "Queries with ctas_approach=True (default) does not support "
            "data_source values different than None and 'AwsDataCatalog'. "
            "Please check the related tutorial for more details "
            "(https://github.com/aws/aws-sdk-pandas/blob/main/"
            "tutorials/006%20-%20Amazon%20Athena.ipynb)"
        )
    if ctas_approach and unload_approach:
        raise exceptions.InvalidArgumentCombination("Only one of ctas_approach=True or unload_approach=True is allowed")
    if unload_parameters and unload_parameters.get("file_format") not in (None, "PARQUET"):
        raise exceptions.InvalidArgumentCombination("Only PARQUET file format is supported if unload_approach=True")
    if client_request_token and athena_cache_settings:
        raise exceptions.InvalidArgumentCombination(
            "Only one of `client_request_token` or `athena_cache_settings` is allowed."
        )
    if client_request_token and (ctas_approach or unload_approach):
        raise exceptions.InvalidArgumentCombination(
            "Using `client_request_token` is only allowed when `ctas_approach=False` and `unload_approach=False`."
        )
    chunksize = sys.maxsize if ctas_approach is False and chunksize is True else chunksize

    # Substitute query parameters if applicable
    sql, execution_params = _apply_formatter(sql, params, paramstyle)

    if not client_request_token:
        cache_info: _CacheInfo = _check_for_cached_results(
            sql=sql,
            boto3_session=boto3_session,
            workgroup=workgroup,
            athena_cache_settings=athena_cache_settings,
        )
        _logger.debug("Cache info:\n%s", cache_info)
        if cache_info.has_valid_cache is True:
            _logger.debug("Valid cache found. Retrieving...")
            try:
                return _resolve_query_with_cache(
                    cache_info=cache_info,
                    categories=categories,
                    chunksize=chunksize,
                    use_threads=use_threads,
                    session=boto3_session,
                    athena_query_wait_polling_delay=athena_query_wait_polling_delay,
                    s3_additional_kwargs=s3_additional_kwargs,
                    pyarrow_additional_kwargs=pyarrow_additional_kwargs,
                    dtype_backend=dtype_backend,
                )
            except Exception as e:
                _logger.error(e)  # if there is anything wrong with the cache, just fallback to the usual path
                _logger.debug("Corrupted cache. Continuing to execute query...")

    ctas_parameters = ctas_parameters if ctas_parameters else {}
    ctas_database = ctas_parameters.get("database")
    ctas_temp_table_name = ctas_parameters.get("temp_table_name")
    ctas_bucketing_info = ctas_parameters.get("bucketing_info")
    ctas_write_compression = ctas_parameters.get("compression")

    return _resolve_query_without_cache(
        sql=sql,
        database=database,
        data_source=data_source,
        ctas_approach=ctas_approach,
        unload_approach=unload_approach,
        unload_parameters=unload_parameters,
        categories=categories,
        chunksize=chunksize,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        keep_files=keep_files,
        ctas_database=ctas_database,
        ctas_temp_table_name=ctas_temp_table_name,
        ctas_bucketing_info=ctas_bucketing_info,
        ctas_write_compression=ctas_write_compression,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        execution_params=execution_params,
        dtype_backend=dtype_backend,
        client_request_token=client_request_token,
    )


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def read_sql_table(
    table: str,
    database: str,
    unload_approach: bool = False,
    unload_parameters: typing.AthenaUNLOADSettings | None = None,
    ctas_approach: bool = True,
    ctas_parameters: typing.AthenaCTASSettings | None = None,
    categories: list[str] | None = None,
    chunksize: int | bool | None = None,
    s3_output: str | None = None,
    workgroup: str = "primary",
    encryption: str | None = None,
    kms_key: str | None = None,
    keep_files: bool = True,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    client_request_token: str | None = None,
    athena_cache_settings: typing.AthenaCacheSettings | None = None,
    data_source: str | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    s3_additional_kwargs: dict[str, Any] | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """Extract the full table AWS Athena and return the results as a Pandas DataFrame.

    **Related tutorial:**

    - `Amazon Athena <https://aws-sdk-pandas.readthedocs.io/en/3.7.3/
      tutorials/006%20-%20Amazon%20Athena.html>`_
    - `Athena Cache <https://aws-sdk-pandas.readthedocs.io/en/3.7.3/
      tutorials/019%20-%20Athena%20Cache.html>`_
    - `Global Configurations <https://aws-sdk-pandas.readthedocs.io/en/3.7.3/
      tutorials/021%20-%20Global%20Configurations.html>`_

    **There are three approaches available through ctas_approach and unload_approach parameters:**

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

    **2** - unload_approach=True and ctas_approach=False:

    Does an UNLOAD query on Athena and parse the Parquet result on s3.

    PROS:

    - Faster for mid and big result sizes.
    - Can handle some level of nested types.
    - Does not modify Glue Data Catalog

    CONS:

    - Output S3 path must be empty.
    - Does not support timestamp with time zone.
    - Does not support columns with repeated names.
    - Does not support columns with undefined data types.

    **3** - ctas_approach=False:

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
    `related tutorial <https://aws-sdk-pandas.readthedocs.io/en/3.7.3/
    tutorials/024%20-%20Athena%20Query%20Metadata.html>`_!


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

    Return an Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies:

    - If **chunksize=True**, depending on the size of the data, one or more data frames are returned per file in the query result.
      Unlike **chunksize=INTEGER**, rows from different files are not mixed in the resulting data frames.

    - If **chunksize=INTEGER**, awswrangler iterates on the data by number of rows equal to the received INTEGER.

    `P.S.` `chunksize=True` is faster and uses less memory while `chunksize=INTEGER` is more precise
    in number of rows for each data frame.

    `P.P.S.` If `ctas_approach=False` and `chunksize=True`, you will always receive an iterator with a
    single DataFrame because regular Athena queries only produces a single output file.

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
    unload_approach: bool
        Wraps the query using UNLOAD, and read the results from S3.
        Only PARQUET format is supported.
    ctas_parameters: typing.AthenaCTASSettings, optional
        Parameters of the CTAS such as database, temp_table_name, bucketing_info, and compression.
    unload_parameters : typing.AthenaUNLOADSettings, optional
        Parameters of the UNLOAD such as format, compression, field_delimiter, and partitioned_by.
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    chunksize : Union[int, bool], optional
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
    s3_output : str, optional
        AWS S3 path.
    workgroup : str
        Athena workgroup. Primary by default.
    encryption : str, optional
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
    kms_key : str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    keep_files : bool
        Should awswrangler delete or keep the staging files produced by Athena?
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    client_request_token : str, optional
        A unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once).
        If another StartQueryExecution request is received, the same response is returned and another query is not created.
        If a parameter has changed, for example, the QueryString , an error is returned.
        If you pass the same client_request_token value with different parameters the query fails with error
        message "Idempotent parameters do not match". Use this only with ctas_approach=False and unload_approach=False
        and disabled cache.
    athena_cache_settings: typing.AthenaCacheSettings, optional
        Parameters of the Athena cache settings such as max_cache_seconds, max_cache_query_inspections,
        max_remote_cache_entries, and max_local_cache_entries.
        AthenaCacheSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an
        instance of AthenaCacheSettings or as a regular Python dict.
        If cached results are valid, awswrangler ignores the `ctas_approach`, `s3_output`, `encryption`, `kms_key`,
        `keep_files` and `ctas_temp_table_name` params.
        If reading cached data fails for any reason, execution falls back to the usual query run path.
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    pyarrow_additional_kwargs: dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

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
        ctas_approach=ctas_approach,
        unload_approach=unload_approach,
        ctas_parameters=ctas_parameters,
        unload_parameters=unload_parameters,
        categories=categories,
        chunksize=chunksize,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        keep_files=keep_files,
        use_threads=use_threads,
        boto3_session=boto3_session,
        client_request_token=client_request_token,
        athena_cache_settings=athena_cache_settings,
        data_source=data_source,
        s3_additional_kwargs=s3_additional_kwargs,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        dtype_backend=dtype_backend,
    )


@apply_configs
def unload(
    sql: str,
    path: str,
    database: str,
    file_format: str = "PARQUET",
    compression: str | None = None,
    field_delimiter: str | None = None,
    partitioned_by: list[str] | None = None,
    workgroup: str = "primary",
    encryption: str | None = None,
    kms_key: str | None = None,
    boto3_session: boto3.Session | None = None,
    data_source: str | None = None,
    params: dict[str, Any] | list[str] | None = None,
    paramstyle: Literal["qmark", "named"] = "named",
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
) -> _QueryMetadata:
    """Write query results from a SELECT statement to the specified data format using UNLOAD.

    https://docs.aws.amazon.com/athena/latest/ug/unload.html

    Parameters
    ----------
    sql : str
        SQL query.
    path : str, optional
        Amazon S3 path.
    database : str
        AWS Glue/Athena database name - It is only the origin database from where the query will be launched.
        You can still using and mixing several databases writing the full table name within the sql
        (e.g. `database.table`).
    file_format : str
        File format of the output. Possible values are ORC, PARQUET, AVRO, JSON, or TEXTFILE
    compression: str, optional
        This option is specific to the ORC and Parquet formats. For ORC, possible values are lz4, snappy, zlib, or zstd.
        For Parquet, possible values are gzip or snappy. For ORC, the default is zlib, and for Parquet,
        the default is gzip.
    field_delimiter : str
        A single-character field delimiter for files in CSV, TSV, and other text formats.
    partitioned_by: list[str], optional
        An array list of columns by which the output is partitioned.
    workgroup : str
        Athena workgroup. Primary by default.
    encryption : str, optional
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
    kms_key : str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
    params: Dict[str, any] | List[str], optional
        Parameters that will be used for constructing the SQL query.
        Only named or question mark parameters are supported.
        The parameter style needs to be specified in the ``paramstyle`` parameter.

        For ``paramstyle="named"``, this value needs to be a dictionary.
        The dict needs to contain the information in the form ``{'name': 'value'}`` and the SQL query needs to contain
        ``:name``.
        The formatter will be applied client-side in this scenario.

        For ``paramstyle="qmark"``, this value needs to be a list of strings.
        The formatter will be applied server-side.
        The values are applied sequentially to the parameters in the query in the order in which the parameters occur.
    paramstyle: str, optional
        Determines the style of ``params``.
        Possible values are:

        - ``named``
        - ``qmark``
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.

    Returns
    -------
    _QueryMetadata
        Query metadata including query execution id, dtypes, manifest & output location.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.unload(
    ...     sql="SELECT * FROM my_table WHERE name=:name AND city=:city",
    ...     params={"name": "filtered_name", "city": "filtered_city"}
    ... )

    """
    # Substitute query parameters if applicable
    sql, execution_params = _apply_formatter(sql, params, paramstyle)
    return _unload(
        sql=sql,
        path=path,
        file_format=file_format,
        compression=compression,
        field_delimiter=field_delimiter,
        partitioned_by=partitioned_by,
        workgroup=workgroup,
        database=database,
        encryption=encryption,
        kms_key=kms_key,
        athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        boto3_session=boto3_session,
        data_source=data_source,
        execution_params=execution_params,
    )
