"""Amazon S3 Read PARQUET Module (PRIVATE)."""

import concurrent.futures
import datetime
import functools
import itertools
import json
import logging
import pprint
import warnings
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union, cast

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet

from awswrangler import _data_types, _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._list import _path2list
from awswrangler.s3._read import (
    _apply_partition_filter,
    _apply_partitions,
    _extract_partitions_dtypes_from_table_details,
    _extract_partitions_metadata_from_paths,
    _get_path_ignore_suffix,
    _get_path_root,
    _read_dfs_from_multiple_paths,
    _union,
)

_logger: logging.Logger = logging.getLogger(__name__)


def _pyarrow_parquet_file_wrapper(
    source: Any, read_dictionary: Optional[List[str]] = None
) -> pyarrow.parquet.ParquetFile:
    try:
        return pyarrow.parquet.ParquetFile(source=source, read_dictionary=read_dictionary)
    except pyarrow.ArrowInvalid as ex:
        if str(ex) == "Parquet file size is 0 bytes":
            _logger.warning("Ignoring empty file...xx")
            return None
        raise


def _read_parquet_metadata_file(
    path: str,
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_id: Optional[str] = None,
) -> Optional[Dict[str, str]]:
    with open_s3_object(
        path=path,
        mode="rb",
        version_id=version_id,
        use_threads=use_threads,
        s3_block_size=131_072,  # 128 KB (128 * 2**10)
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
    ) as f:
        pq_file: Optional[pyarrow.parquet.ParquetFile] = _pyarrow_parquet_file_wrapper(source=f)
        if pq_file is None:
            return None
        return _data_types.athena_types_from_pyarrow_schema(schema=pq_file.schema.to_arrow_schema(), partitions=None)[0]


def _read_schemas_from_files(
    paths: List[str],
    sampling: float,
    use_threads: Union[bool, int],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_ids: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], ...]:
    paths = _utils.list_sampling(lst=paths, sampling=sampling)
    schemas: Tuple[Optional[Dict[str, str]], ...] = tuple()
    n_paths: int = len(paths)
    cpus: int = _utils.ensure_cpu_count(use_threads)
    if cpus == 1 or n_paths == 1:
        schemas = tuple(
            _read_parquet_metadata_file(
                path=p,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
                use_threads=use_threads,
                version_id=version_ids.get(p) if isinstance(version_ids, dict) else None,
            )
            for p in paths
        )
    elif n_paths > 1:
        versions = [version_ids.get(p) if isinstance(version_ids, dict) else None for p in paths]
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            schemas = tuple(
                executor.map(
                    _read_parquet_metadata_file,
                    paths,
                    itertools.repeat(_utils.boto3_to_primitives(boto3_session=boto3_session)),  # Boto3.Session
                    itertools.repeat(s3_additional_kwargs),
                    itertools.repeat(use_threads),
                    versions,
                )
            )
    schemas = cast(Tuple[Dict[str, str], ...], tuple(x for x in schemas if x is not None))
    _logger.debug("schemas: %s", schemas)
    return schemas


def _validate_schemas(schemas: Tuple[Dict[str, str], ...]) -> None:
    if len(schemas) < 2:
        return None
    first: Dict[str, str] = schemas[0]
    for schema in schemas[1:]:
        if first != schema:
            raise exceptions.InvalidSchemaConvergence(
                f"Was detect at least 2 different schemas:\n    1 - {first}\n    2 - {schema}."
            )
    return None


def _validate_schemas_from_files(
    paths: List[str],
    sampling: float,
    use_threads: Union[bool, int],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_ids: Optional[Dict[str, str]] = None,
) -> None:
    schemas: Tuple[Dict[str, str], ...] = _read_schemas_from_files(
        paths=paths,
        sampling=sampling,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        version_ids=version_ids,
    )
    _validate_schemas(schemas=schemas)


def _merge_schemas(schemas: Tuple[Dict[str, str], ...]) -> Dict[str, str]:
    columns_types: Dict[str, str] = {}
    for schema in schemas:
        for column, dtype in schema.items():
            if (column in columns_types) and (columns_types[column] != dtype):
                raise exceptions.InvalidSchemaConvergence(
                    f"Was detect at least 2 different types in column {column} ({columns_types[column]} and {dtype})."
                )
            columns_types[column] = dtype
    return columns_types


def _read_parquet_metadata(
    path: Union[str, List[str]],
    path_suffix: Optional[str],
    path_ignore_suffix: Optional[str],
    ignore_empty: bool,
    dtype: Optional[Dict[str, str]],
    sampling: float,
    dataset: bool,
    use_threads: Union[bool, int],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_id: Optional[Union[str, Dict[str, str]]] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Handle wr.s3.read_parquet_metadata internally."""
    path_root: Optional[str] = _get_path_root(path=path, dataset=dataset)
    paths: List[str] = _path2list(
        path=path,
        boto3_session=boto3_session,
        suffix=path_suffix,
        ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
        ignore_empty=ignore_empty,
        s3_additional_kwargs=s3_additional_kwargs,
    )

    # Files
    schemas: Tuple[Dict[str, str], ...] = _read_schemas_from_files(
        paths=paths,
        sampling=sampling,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        version_ids=version_id
        if isinstance(version_id, dict)
        else {paths[0]: version_id}
        if isinstance(version_id, str)
        else None,
    )
    columns_types: Dict[str, str] = _merge_schemas(schemas=schemas)

    # Partitions
    partitions_types: Optional[Dict[str, str]] = None
    partitions_values: Optional[Dict[str, List[str]]] = None
    if (dataset is True) and (path_root is not None):
        partitions_types, partitions_values = _extract_partitions_metadata_from_paths(path=path_root, paths=paths)

    # Casting
    if dtype:
        for k, v in dtype.items():
            if columns_types and k in columns_types:
                columns_types[k] = v
            if partitions_types and k in partitions_types:
                partitions_types[k] = v

    return columns_types, partitions_types, partitions_values


def _apply_index(df: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
    index_columns: List[Any] = metadata["index_columns"]
    ignore_index: bool = True
    _logger.debug("df.columns: %s", df.columns)

    if index_columns:
        if isinstance(index_columns[0], str):
            indexes: List[str] = [i for i in index_columns if i in df.columns]
            if indexes:
                df = df.set_index(keys=indexes, drop=True, inplace=False, verify_integrity=False)
                ignore_index = False
        elif isinstance(index_columns[0], dict) and index_columns[0]["kind"] == "range":
            col = index_columns[0]
            if col["kind"] == "range":
                df.index = pd.RangeIndex(start=col["start"], stop=col["stop"], step=col["step"])
                ignore_index = False
                col_name: Optional[str] = None
                if "name" in col and col["name"] is not None:
                    col_name = str(col["name"])
                elif "field_name" in col and col["field_name"] is not None:
                    col_name = str(col["field_name"])
                if col_name is not None and col_name.startswith("__index_level_") is False:
                    df.index.name = col_name

        df.index.names = [None if n is not None and n.startswith("__index_level_") else n for n in df.index.names]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        df._awswrangler_ignore_index = ignore_index  # pylint: disable=protected-access
    return df


def _apply_timezone(df: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
    for c in metadata["columns"]:
        if "field_name" in c and c["field_name"] is not None:
            col_name = str(c["field_name"])
        elif "name" in c and c["name"] is not None:
            col_name = str(c["name"])
        else:
            continue
        if col_name in df.columns and c["pandas_type"] == "datetimetz":
            timezone: datetime.tzinfo = pa.lib.string_to_tzinfo(c["metadata"]["timezone"])
            _logger.debug("applying timezone (%s) on column %s", timezone, col_name)
            if hasattr(df[col_name].dtype, "tz") is False:
                df[col_name] = df[col_name].dt.tz_localize(tz="UTC")
            df[col_name] = df[col_name].dt.tz_convert(tz=timezone)
    return df


def _arrowtable2df(
    table: pa.Table,
    categories: Optional[List[str]],
    safe: bool,
    map_types: bool,
    use_threads: Union[bool, int],
    dataset: bool,
    path: str,
    path_root: Optional[str],
) -> pd.DataFrame:
    metadata: Dict[str, Any] = {}
    if table.schema.metadata is not None and b"pandas" in table.schema.metadata:
        metadata = json.loads(table.schema.metadata[b"pandas"])
    if type(use_threads) == int:  # pylint: disable=unidiomatic-typecheck
        use_threads = bool(use_threads > 1)
    df: pd.DataFrame = _apply_partitions(
        df=table.to_pandas(
            use_threads=use_threads,
            split_blocks=True,
            self_destruct=True,
            integer_object_nulls=False,
            date_as_object=True,
            ignore_metadata=True,
            strings_to_categorical=False,
            safe=safe,
            categories=categories,
            types_mapper=_data_types.pyarrow2pandas_extension if map_types else None,
        ),
        dataset=dataset,
        path=path,
        path_root=path_root,
    )
    df = _utils.ensure_df_is_mutable(df=df)
    if metadata:
        _logger.debug("metadata: %s", metadata)
        df = _apply_timezone(df=df, metadata=metadata)
        df = _apply_index(df=df, metadata=metadata)
    return df


def _pyarrow_chunk_generator(
    pq_file: pyarrow.parquet.ParquetFile,
    chunked: Union[bool, int],
    columns: Optional[List[str]],
    use_threads_flag: bool,
) -> Iterator[pa.RecordBatch]:
    if chunked is True:
        batch_size = 65_536
    elif isinstance(chunked, int) and chunked > 0:
        batch_size = chunked
    else:
        raise exceptions.InvalidArgument(f"chunked: {chunked}")

    chunks = pq_file.iter_batches(
        batch_size=batch_size, columns=columns, use_threads=use_threads_flag, use_pandas_metadata=False
    )

    for chunk in chunks:
        yield chunk


def _row_group_chunk_generator(
    pq_file: pyarrow.parquet.ParquetFile,
    columns: Optional[List[str]],
    use_threads_flag: bool,
    num_row_groups: int,
) -> Iterator[pa.Table]:
    for i in range(num_row_groups):
        _logger.debug("Reading Row Group %s...", i)
        yield pq_file.read_row_group(i=i, columns=columns, use_threads=use_threads_flag, use_pandas_metadata=False)


def _read_parquet_chunked(  # pylint: disable=too-many-branches
    paths: List[str],
    chunked: Union[bool, int],
    validate_schema: bool,
    ignore_index: Optional[bool],
    columns: Optional[List[str]],
    categories: Optional[List[str]],
    safe: bool,
    map_types: bool,
    boto3_session: boto3.Session,
    dataset: bool,
    path_root: Optional[str],
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_ids: Optional[Dict[str, str]] = None,
) -> Iterator[pd.DataFrame]:
    next_slice: Optional[pd.DataFrame] = None
    last_schema: Optional[Dict[str, str]] = None
    last_path: str = ""
    for path in paths:
        with open_s3_object(
            path=path,
            version_id=version_ids.get(path) if version_ids else None,
            mode="rb",
            use_threads=use_threads,
            s3_block_size=10_485_760,  # 10 MB (10 * 2**20)
            s3_additional_kwargs=s3_additional_kwargs,
            boto3_session=boto3_session,
        ) as f:
            pq_file: Optional[pyarrow.parquet.ParquetFile] = _pyarrow_parquet_file_wrapper(
                source=f, read_dictionary=categories
            )
            if pq_file is None:
                continue
            if validate_schema is True:
                schema: Dict[str, str] = _data_types.athena_types_from_pyarrow_schema(
                    schema=pq_file.schema.to_arrow_schema(), partitions=None
                )[0]
                if last_schema is not None:
                    if schema != last_schema:
                        raise exceptions.InvalidSchemaConvergence(
                            f"Was detect at least 2 different schemas:\n"
                            f"    - {last_path} -> {last_schema}\n"
                            f"    - {path} -> {schema}"
                        )
                last_schema = schema
                last_path = path
            num_row_groups: int = pq_file.num_row_groups
            _logger.debug("num_row_groups: %s", num_row_groups)
            use_threads_flag: bool = use_threads if isinstance(use_threads, bool) else bool(use_threads > 1)
            # iter_batches is only available for pyarrow >= 3.0.0
            if callable(getattr(pq_file, "iter_batches", None)):
                chunk_generator = _pyarrow_chunk_generator(
                    pq_file=pq_file, chunked=chunked, columns=columns, use_threads_flag=use_threads_flag
                )
            else:
                chunk_generator = _row_group_chunk_generator(
                    pq_file=pq_file, columns=columns, use_threads_flag=use_threads_flag, num_row_groups=num_row_groups
                )

            for chunk in chunk_generator:
                df: pd.DataFrame = _arrowtable2df(
                    table=chunk,
                    categories=categories,
                    safe=safe,
                    map_types=map_types,
                    use_threads=use_threads,
                    dataset=dataset,
                    path=path,
                    path_root=path_root,
                )
                if chunked is True:
                    yield df
                elif isinstance(chunked, int) and chunked > 0:
                    if next_slice is not None:
                        df = _union(dfs=[next_slice, df], ignore_index=ignore_index)
                    while len(df.index) >= chunked:
                        yield df.iloc[:chunked, :].copy()
                        df = df.iloc[chunked:, :]
                    if df.empty:
                        next_slice = None
                    else:
                        next_slice = df
                else:
                    raise exceptions.InvalidArgument(f"chunked: {chunked}")
    if next_slice is not None:
        yield next_slice


def _read_parquet_file(
    path: str,
    columns: Optional[List[str]],
    categories: Optional[List[str]],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_id: Optional[str] = None,
) -> pa.Table:
    s3_block_size: int = 20_971_520 if columns else -1  # One shot for a full read otherwise 20 MB (20 * 2**20)
    with open_s3_object(
        path=path,
        mode="rb",
        version_id=version_id,
        use_threads=use_threads,
        s3_block_size=s3_block_size,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
    ) as f:
        pq_file: Optional[pyarrow.parquet.ParquetFile] = _pyarrow_parquet_file_wrapper(
            source=f, read_dictionary=categories
        )
        if pq_file is None:
            raise exceptions.InvalidFile(f"Invalid Parquet file: {path}")
        return pq_file.read(columns=columns, use_threads=False, use_pandas_metadata=False)


def _count_row_groups(
    path: str,
    categories: Optional[List[str]],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
) -> int:
    _logger.debug("Counting row groups...")
    with open_s3_object(
        path=path,
        mode="rb",
        use_threads=use_threads,
        s3_block_size=131_072,  # 128 KB (128 * 2**10)
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
    ) as f:
        pq_file: Optional[pyarrow.parquet.ParquetFile] = _pyarrow_parquet_file_wrapper(
            source=f, read_dictionary=categories
        )
        if pq_file is None:
            return 0
        n: int = cast(int, pq_file.num_row_groups)
        _logger.debug("Row groups count: %d", n)
        return n


def _read_parquet(
    path: str,
    version_id: Optional[str],
    columns: Optional[List[str]],
    categories: Optional[List[str]],
    safe: bool,
    map_types: bool,
    boto3_session: Union[boto3.Session, _utils.Boto3PrimitivesType],
    dataset: bool,
    path_root: Optional[str],
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
) -> pd.DataFrame:
    boto3_session = _utils.ensure_session(boto3_session)
    return _arrowtable2df(
        table=_read_parquet_file(
            path=path,
            columns=columns,
            categories=categories,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=use_threads,
            version_id=version_id,
        ),
        categories=categories,
        safe=safe,
        map_types=map_types,
        use_threads=use_threads,
        dataset=dataset,
        path=path,
        path_root=path_root,
    )


def read_parquet(
    path: Union[str, List[str]],
    path_suffix: Union[str, List[str], None] = None,
    path_ignore_suffix: Union[str, List[str], None] = None,
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    ignore_empty: bool = True,
    ignore_index: Optional[bool] = None,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    columns: Optional[List[str]] = None,
    validate_schema: bool = False,
    chunked: Union[bool, int] = False,
    dataset: bool = False,
    categories: Optional[List[str]] = None,
    safe: bool = True,
    map_types: bool = True,
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read Apache Parquet file(s) from from a received S3 prefix or list of S3 objects paths.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    ``Batching`` (`chunked` argument) (Memory Friendly):

    Will anable the function to return a Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies on Wrangler:

    - If **chunked=True**, a new DataFrame will be returned for each file in your path/dataset.

    - If **chunked=INTEGER**, Wrangler will iterate on the data by number of rows igual the received INTEGER.

    `P.S.` `chunked=True` if faster and uses less memory while `chunked=INTEGER` is more precise
    in number of rows for each Dataframe.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Note
    ----
    The filter by last_modified begin last_modified end is applied after list all S3 files

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]).
        If None, will try to read all files. (default)
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.(e.g. [".csv", "_SUCCESS"]).
        If None, will try to read all files. (default)
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    ignore_empty: bool
        Ignore files with 0 bytes.
    ignore_index: Optional[bool]
        Ignore index when combining multiple parquet files to one DataFrame.
    partition_filter: Optional[Callable[[Dict[str, str]], bool]]
        Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        This function MUST receive a single argument (Dict[str, str]) where keys are partitions
        names and values are partitions values. Partitions values will be always strings extracted from S3.
        This function MUST return a bool, True to read the partition or False to ignore it.
        Ignored if `dataset=False`.
        E.g ``lambda x: True if x["year"] == "2020" and x["month"] == "1" else False``
    columns : List[str], optional
        Names of columns to read from the file(s).
    validate_schema:
        Check that individual file schemas are all the same / compatible. Schemas within a
        folder prefix should all be the same. Disable if you have schemas that are different
        and want to disable this check.
    chunked : Union[int, bool]
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` wrangler will iterate on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed Wrangler will iterate on the data by number of rows igual the received INTEGER.
    dataset: bool
        If `True` read a parquet dataset instead of simple file(s) loading all the related partitions as columns.
    categories: Optional[List[str]], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    safe : bool, default True
        For certain data types, a cast is needed in order to store the
        data in a pandas DataFrame or Series (e.g. timestamps are always
        stored as nanoseconds in pandas). This option controls whether it
        is a safe cast or not.
    map_types : bool, default True
        True to convert pyarrow DataTypes to pandas ExtensionDtypes. It is
        used to override the default pandas type for conversion of built-in
        pyarrow types or in absence of pandas_metadata in the Table schema.
    use_threads : Union[bool, int]
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If given an int will use the given amount of threads.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.

    Returns
    -------
    Union[pandas.DataFrame, Generator[pandas.DataFrame, None, None]]
        Pandas DataFrame or a Generator in case of `chunked=True`.

    Examples
    --------
    Reading all Parquet files under a prefix

    >>> import awswrangler as wr
    >>> df = wr.s3.read_parquet(path='s3://bucket/prefix/')

    Reading all Parquet files from a list

    >>> import awswrangler as wr
    >>> df = wr.s3.read_parquet(path=['s3://bucket/filename0.parquet', 's3://bucket/filename1.parquet'])

    Reading in chunks (Chunk by file)

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_parquet(path=['s3://bucket/filename0.csv', 's3://bucket/filename1.csv'], chunked=True)
    >>> for df in dfs:
    >>>     print(df)  # Smaller Pandas DataFrame

    Reading in chunks (Chunk by 1MM rows)

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_parquet(path=['s3://bucket/filename0.csv', 's3://bucket/filename1.csv'], chunked=1_000_000)
    >>> for df in dfs:
    >>>     print(df)  # 1MM Pandas DataFrame

    Reading Parquet Dataset with PUSH-DOWN filter over partitions

    >>> import awswrangler as wr
    >>> my_filter = lambda x: True if x["city"].startswith("new") else False
    >>> df = wr.s3.read_parquet(path, dataset=True, partition_filter=my_filter)

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    paths: List[str] = _path2list(
        path=path,
        boto3_session=session,
        suffix=path_suffix,
        ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_empty=ignore_empty,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    versions: Optional[Dict[str, str]] = (
        version_id if isinstance(version_id, dict) else {paths[0]: version_id} if isinstance(version_id, str) else None
    )
    path_root: Optional[str] = _get_path_root(path=path, dataset=dataset)
    if path_root is not None:
        paths = _apply_partition_filter(path_root=path_root, paths=paths, filter_func=partition_filter)
    if len(paths) < 1:
        raise exceptions.NoFilesFound(f"No files Found on: {path}.")
    _logger.debug("paths:\n%s", paths)
    args: Dict[str, Any] = {
        "columns": columns,
        "categories": categories,
        "safe": safe,
        "map_types": map_types,
        "boto3_session": session,
        "dataset": dataset,
        "path_root": path_root,
        "s3_additional_kwargs": s3_additional_kwargs,
        "use_threads": use_threads,
    }
    _logger.debug("args:\n%s", pprint.pformat(args))
    if chunked is not False:
        return _read_parquet_chunked(
            paths=paths,
            chunked=chunked,
            validate_schema=validate_schema,
            ignore_index=ignore_index,
            version_ids=versions,
            **args,
        )
    if len(paths) == 1:
        return _read_parquet(
            path=paths[0], version_id=versions[paths[0]] if isinstance(versions, dict) else None, **args
        )
    if validate_schema is True:
        _validate_schemas_from_files(
            paths=paths,
            version_ids=versions,
            sampling=1.0,
            use_threads=True,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
    return _union(
        dfs=_read_dfs_from_multiple_paths(
            read_func=_read_parquet, paths=paths, version_ids=versions, use_threads=use_threads, kwargs=args
        ),
        ignore_index=ignore_index,
    )


@apply_configs
def read_parquet_table(
    table: str,
    database: str,
    filename_suffix: Union[str, List[str], None] = None,
    filename_ignore_suffix: Union[str, List[str], None] = None,
    catalog_id: Optional[str] = None,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    columns: Optional[List[str]] = None,
    validate_schema: bool = True,
    categories: Optional[List[str]] = None,
    safe: bool = True,
    map_types: bool = True,
    chunked: Union[bool, int] = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read Apache Parquet table registered on AWS Glue Catalog.

    Note
    ----
    ``Batching`` (`chunked` argument) (Memory Friendly):

    Will anable the function to return a Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies on Wrangler:

    - If **chunked=True**, a new DataFrame will be returned for each file in your path/dataset.

    - If **chunked=INTEGER**, Wrangler will paginate through files slicing and concatenating
      to return DataFrames with the number of row igual the received INTEGER.

    `P.S.` `chunked=True` if faster and uses less memory while `chunked=INTEGER` is more precise
    in number of rows for each Dataframe.


    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    table : str
        AWS Glue Catalog table name.
    database : str
        AWS Glue Catalog database name.
    filename_suffix: Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]).
        If None, will try to read all files. (default)
    filename_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.(e.g. [".csv", "_SUCCESS"]).
        If None, will try to read all files. (default)
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    partition_filter: Optional[Callable[[Dict[str, str]], bool]]
        Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        This function MUST receive a single argument (Dict[str, str]) where keys are partitions
        names and values are partitions values. Partitions values will be always strings extracted from S3.
        This function MUST return a bool, True to read the partition or False to ignore it.
        Ignored if `dataset=False`.
        E.g ``lambda x: True if x["year"] == "2020" and x["month"] == "1" else False``
        https://aws-data-wrangler.readthedocs.io/en/2.11.0/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    columns : List[str], optional
        Names of columns to read from the file(s).
    validate_schema:
        Check that individual file schemas are all the same / compatible. Schemas within a
        folder prefix should all be the same. Disable if you have schemas that are different
        and want to disable this check.
    categories: Optional[List[str]], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    safe : bool, default True
        For certain data types, a cast is needed in order to store the
        data in a pandas DataFrame or Series (e.g. timestamps are always
        stored as nanoseconds in pandas). This option controls whether it
        is a safe cast or not.
    map_types : bool, default True
        True to convert pyarrow DataTypes to pandas ExtensionDtypes. It is
        used to override the default pandas type for conversion of built-in
        pyarrow types or in absence of pandas_metadata in the Table schema.
    chunked : bool
        If True will break the data in smaller DataFrames (Non deterministic number of lines).
        Otherwise return a single DataFrame with the whole data.
    use_threads : Union[bool, int]
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If given an int will use the given amount of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.

    Returns
    -------
    Union[pandas.DataFrame, Generator[pandas.DataFrame, None, None]]
        Pandas DataFrame or a Generator in case of `chunked=True`.

    Examples
    --------
    Reading Parquet Table

    >>> import awswrangler as wr
    >>> df = wr.s3.read_parquet_table(database='...', table='...')

    Reading Parquet Table encrypted

    >>> import awswrangler as wr
    >>> df = wr.s3.read_parquet_table(
    ...     database='...',
    ...     table='...'
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )

    Reading Parquet Table in chunks (Chunk by file)

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_parquet_table(database='...', table='...', chunked=True)
    >>> for df in dfs:
    >>>     print(df)  # Smaller Pandas DataFrame

    Reading Parquet Dataset with PUSH-DOWN filter over partitions

    >>> import awswrangler as wr
    >>> my_filter = lambda x: True if x["city"].startswith("new") else False
    >>> df = wr.s3.read_parquet_table(path, dataset=True, partition_filter=my_filter)

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    args: Dict[str, Any] = {"DatabaseName": database, "Name": table}
    if catalog_id is not None:
        args["CatalogId"] = catalog_id
    res: Dict[str, Any] = client_glue.get_table(**args)
    try:
        location: str = res["Table"]["StorageDescriptor"]["Location"]
        path: str = location if location.endswith("/") else f"{location}/"
    except KeyError as ex:
        raise exceptions.InvalidTable(f"Missing s3 location for {database}.{table}.") from ex
    df = read_parquet(
        path=path,
        path_suffix=filename_suffix,
        path_ignore_suffix=filename_ignore_suffix,
        partition_filter=partition_filter,
        columns=columns,
        validate_schema=validate_schema,
        categories=categories,
        safe=safe,
        map_types=map_types,
        chunked=chunked,
        dataset=True,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    partial_cast_function = functools.partial(
        _data_types.cast_pandas_with_athena_types, dtype=_extract_partitions_dtypes_from_table_details(response=res)
    )

    if isinstance(df, pd.DataFrame):
        return partial_cast_function(df)

    # df is a generator, so map is needed for casting dtypes
    return map(partial_cast_function, df)


@apply_configs
def read_parquet_metadata(
    path: Union[str, List[str]],
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    path_suffix: Optional[str] = None,
    path_ignore_suffix: Optional[str] = None,
    ignore_empty: bool = True,
    dtype: Optional[Dict[str, str]] = None,
    sampling: float = 1.0,
    dataset: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]]]:
    """Read Apache Parquet file(s) metadata from from a received S3 prefix or list of S3 objects paths.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]).
        If None, will try to read all files. (default)
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.(e.g. [".csv", "_SUCCESS"]).
        If None, will try to read all files. (default)
    ignore_empty: bool
        Ignore files with 0 bytes.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined data types as partitions columns.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    sampling : float
        Random sample ratio of files that will have the metadata inspected.
        Must be `0.0 < sampling <= 1.0`.
        The higher, the more accurate.
        The lower, the faster.
    dataset: bool
        If True read a parquet dataset instead of simple file(s) loading all the related partitions as columns.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.

    Returns
    -------
    Tuple[Dict[str, str], Optional[Dict[str, str]]]
        columns_types: Dictionary with keys as column names and values as
        data types (e.g. {'col0': 'bigint', 'col1': 'double'}). /
        partitions_types: Dictionary with keys as partition names
        and values as data types (e.g. {'col2': 'date'}).

    Examples
    --------
    Reading all Parquet files (with partitions) metadata under a prefix

    >>> import awswrangler as wr
    >>> columns_types, partitions_types = wr.s3.read_parquet_metadata(path='s3://bucket/prefix/', dataset=True)

    Reading all Parquet files metadata from a list

    >>> import awswrangler as wr
    >>> columns_types, partitions_types = wr.s3.read_parquet_metadata(path=[
    ...     's3://bucket/filename0.parquet',
    ...     's3://bucket/filename1.parquet'
    ... ])

    """
    return _read_parquet_metadata(
        path=path,
        version_id=version_id,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        ignore_empty=ignore_empty,
        dtype=dtype,
        sampling=sampling,
        dataset=dataset,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=_utils.ensure_session(session=boto3_session),
    )[:2]
