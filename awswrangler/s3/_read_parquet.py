"""Amazon S3 Read PARQUET Module (PRIVATE)."""

import datetime
import functools
import itertools
import logging
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet

from awswrangler import _data_types, _utils, exceptions
from awswrangler._arrow import _add_table_partitions, _table_to_df
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler._threading import _get_executor
from awswrangler.catalog._get import _get_partitions
from awswrangler.catalog._utils import _catalog_id
from awswrangler.distributed.ray import RayLogger, ray_get
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._list import _path2list
from awswrangler.s3._read import (
    _apply_partition_filter,
    _extract_partitions_dtypes_from_table_details,
    _extract_partitions_metadata_from_paths,
    _get_path_ignore_suffix,
    _get_path_root,
)

BATCH_READ_BLOCK_SIZE = 65_536
CHUNKED_READ_S3_BLOCK_SIZE = 10_485_760  # 10 MB (20 * 2**20)
FULL_READ_S3_BLOCK_SIZE = 20_971_520  # 20 MB (20 * 2**20)
METADATA_READ_S3_BLOCK_SIZE = 131_072  # 128 KB (128 * 2**10)

_logger: logging.Logger = logging.getLogger(__name__)


def _ensure_locations_are_valid(paths: Iterable[str]) -> Iterator[str]:
    for path in paths:
        suffix: str = path.rpartition("/")[2]
        # If the suffix looks like a partition,
        if (suffix != "") and (suffix.count("=") == 1):
            # the path should end in a '/' character.
            path = f"{path}/"
        yield path


def _pyarrow_parquet_file_wrapper(
    source: Any, coerce_int96_timestamp_unit: Optional[str] = None
) -> pyarrow.parquet.ParquetFile:
    try:
        return pyarrow.parquet.ParquetFile(source=source, coerce_int96_timestamp_unit=coerce_int96_timestamp_unit)
    except pyarrow.ArrowInvalid as ex:
        if str(ex) == "Parquet file size is 0 bytes":
            _logger.warning("Ignoring empty file...")
            return None
        raise


@engine.dispatch_on_engine
def _read_parquet_metadata_file(
    boto3_session: boto3.Session,
    path: str,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_id: Optional[str] = None,
    coerce_int96_timestamp_unit: Optional[str] = None,
) -> pa.schema:
    RayLogger().get_logger(name=_read_parquet_metadata_file.__name__)
    with open_s3_object(
        path=path,
        mode="rb",
        version_id=version_id,
        use_threads=use_threads,
        s3_block_size=METADATA_READ_S3_BLOCK_SIZE,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
    ) as f:
        pq_file: Optional[pyarrow.parquet.ParquetFile] = _pyarrow_parquet_file_wrapper(
            source=f, coerce_int96_timestamp_unit=coerce_int96_timestamp_unit
        )
        if pq_file:
            return pq_file.schema.to_arrow_schema()
        return None


def _read_schemas_from_files(
    paths: List[str],
    sampling: float,
    use_threads: Union[bool, int],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_ids: Optional[Dict[str, str]] = None,
    coerce_int96_timestamp_unit: Optional[str] = None,
) -> List[pa.schema]:
    paths = _utils.list_sampling(lst=paths, sampling=sampling)

    executor = _get_executor(use_threads=use_threads)
    schemas = ray_get(
        executor.map(
            _read_parquet_metadata_file,
            boto3_session,
            paths,
            itertools.repeat(s3_additional_kwargs),
            itertools.repeat(use_threads),
            [version_ids.get(p) if isinstance(version_ids, dict) else None for p in paths],
            itertools.repeat(coerce_int96_timestamp_unit),
        )
    )
    return [schema for schema in schemas if schema is not None]


def _validate_schemas(schemas: List[pa.schema], validate_schema: bool) -> pa.schema:
    first: pa.schema = schemas[0]
    if len(schemas) == 1:
        return first
    first_dict = {s.name: s.type for s in first}
    if validate_schema:
        for schema in schemas[1:]:
            if first_dict != {s.name: s.type for s in schema}:
                raise exceptions.InvalidSchemaConvergence(
                    f"At least 2 different schemas were detected:\n    1 - {first}\n    2 - {schema}."
                )
    return pa.unify_schemas(schemas)


def _validate_schemas_from_files(
    validate_schema: bool,
    paths: List[str],
    sampling: float,
    use_threads: Union[bool, int],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_ids: Optional[Dict[str, str]] = None,
    coerce_int96_timestamp_unit: Optional[str] = None,
) -> pa.schema:
    schemas: List[pa.schema] = _read_schemas_from_files(
        paths=paths,
        sampling=sampling,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        version_ids=version_ids,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )
    return _validate_schemas(schemas, validate_schema)


def _read_parquet_metadata(
    path: Union[str, List[str]],
    path_suffix: Optional[str],
    path_ignore_suffix: Optional[str],
    ignore_empty: bool,
    ignore_null: bool,
    dtype: Optional[Dict[str, str]],
    sampling: float,
    dataset: bool,
    use_threads: Union[bool, int],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    coerce_int96_timestamp_unit: Optional[str] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Handle wr.s3.read_parquet_metadata internally."""
    boto3_session = _utils.ensure_session(session=boto3_session)
    path_root: Optional[str] = _get_path_root(path=path, dataset=dataset)
    paths: List[str] = _path2list(
        path=path,
        boto3_session=boto3_session,
        suffix=path_suffix,
        ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
        ignore_empty=ignore_empty,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    version_ids = (
        version_id if isinstance(version_id, dict) else {paths[0]: version_id} if isinstance(version_id, str) else None
    )

    # Files
    schemas: List[pa.schema] = _read_schemas_from_files(
        paths=paths,
        sampling=sampling,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        version_ids=version_ids,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )
    merged_schemas = _validate_schemas(schemas=schemas, validate_schema=False)

    columns_types: Dict[str, str] = _data_types.athena_types_from_pyarrow_schema(
        schema=merged_schemas, partitions=None, ignore_null=ignore_null
    )[0]

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


def _read_parquet_file(
    boto3_session: boto3.Session,
    path: str,
    path_root: Optional[str],
    columns: Optional[List[str]],
    coerce_int96_timestamp_unit: Optional[str],
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_id: Optional[str] = None,
) -> pa.Table:
    s3_block_size: int = FULL_READ_S3_BLOCK_SIZE if columns else -1  # One shot for a full read or see constant
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
            source=f,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        )
        if pq_file is None:
            raise exceptions.InvalidFile(f"Invalid Parquet file: {path}")
        return _add_table_partitions(
            table=pq_file.read(columns=columns, use_threads=False, use_pandas_metadata=False),
            path=path,
            path_root=path_root,
        )


def _read_parquet_chunked(
    boto3_session: boto3.Session,
    paths: List[str],
    path_root: Optional[str],
    columns: Optional[List[str]],
    coerce_int96_timestamp_unit: Optional[str],
    chunked: Union[int, bool],
    use_threads: Union[bool, int],
    s3_additional_kwargs: Optional[Dict[str, str]],
    arrow_kwargs: Dict[str, Any],
    version_ids: Optional[Dict[str, str]] = None,
) -> Iterator[pd.DataFrame]:
    next_slice: Optional[pd.DataFrame] = None
    batch_size = BATCH_READ_BLOCK_SIZE if chunked is True else chunked

    for path in paths:
        with open_s3_object(
            path=path,
            version_id=version_ids.get(path) if version_ids else None,
            mode="rb",
            use_threads=use_threads,
            s3_block_size=CHUNKED_READ_S3_BLOCK_SIZE,
            s3_additional_kwargs=s3_additional_kwargs,
            boto3_session=boto3_session,
        ) as f:
            pq_file: Optional[pyarrow.parquet.ParquetFile] = _pyarrow_parquet_file_wrapper(
                source=f,
                coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
            )
            if pq_file is None:
                continue

            use_threads_flag: bool = use_threads if isinstance(use_threads, bool) else bool(use_threads > 1)
            chunks = pq_file.iter_batches(
                batch_size=batch_size, columns=columns, use_threads=use_threads_flag, use_pandas_metadata=False
            )
            table = _add_table_partitions(
                table=pa.Table.from_batches(chunks),
                path=path,
                path_root=path_root,
            )
            df = _table_to_df(table=table, kwargs=arrow_kwargs)
            if chunked is True:
                yield df
            else:
                if next_slice is not None:
                    df = pd.concat(objs=[next_slice, df], sort=False, copy=False)
                while len(df.index) >= chunked:
                    yield df.iloc[:chunked, :].copy()
                    df = df.iloc[chunked:, :]
                if df.empty:
                    next_slice = None
                else:
                    next_slice = df
    if next_slice is not None:
        yield next_slice


@engine.dispatch_on_engine
def _read_parquet(  # pylint: disable=W0613
    paths: List[str],
    path_root: Optional[str],
    columns: Optional[List[str]],
    coerce_int96_timestamp_unit: Optional[str],
    boto3_session: Optional[boto3.Session],
    use_threads: Union[bool, int],
    parallelism: int,
    version_ids: Optional[Dict[str, str]],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    arrow_kwargs: Dict[str, Any],
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    executor = _get_executor(use_threads=use_threads)
    tables = executor.map(
        _read_parquet_file,
        boto3_session,
        paths,
        itertools.repeat(path_root),
        itertools.repeat(columns),
        itertools.repeat(coerce_int96_timestamp_unit),
        itertools.repeat(s3_additional_kwargs),
        itertools.repeat(use_threads),
        [version_ids.get(p) if isinstance(version_ids, dict) else None for p in paths],
    )
    return _utils.table_refs_to_df(tables, kwargs=arrow_kwargs)


def read_parquet(
    path: Union[str, List[str]],
    path_root: Optional[str] = None,
    dataset: bool = False,
    path_suffix: Union[str, List[str], None] = None,
    path_ignore_suffix: Union[str, List[str], None] = None,
    ignore_empty: bool = True,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    columns: Optional[List[str]] = None,
    validate_schema: bool = False,
    coerce_int96_timestamp_unit: Optional[str] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    chunked: Union[bool, int] = False,
    use_threads: Union[bool, int] = True,
    parallelism: int = -1,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read Parquet file(s) from an S3 prefix or list of S3 objects paths.

    The concept of `dataset` enables more complex features like partitioning
    and catalog integration (AWS Glue Catalog).

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the argument to this function.

    Note
    ----
    ``Batching`` (`chunked` argument) (Memory Friendly):

    Used to return an Iterable of DataFrames instead of a regular DataFrame.

    Two batching strategies are available:

    - **chunked=True**, a DataFrame is returned for each file in the dataset.

    - **chunked=INTEGER**, a DataFrame is returned with maximum rows equal to the received INTEGER.

    `P.S.` `chunked=True` if faster and uses less memory while `chunked=INTEGER` is more precise
    in the number of rows.

    Note
    ----
    If `use_threads=True`, the number of threads is obtained from os.cpu_count().

    Note
    ----
    Filtering by `last_modified begin` and `last_modified end` is applied after listing all S3 files

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    path_root : str, optional
        Root path of the dataset. If dataset=`True`, it is used as a starting point to load partition columns.
    dataset : bool, default False
        If `True`, read a parquet dataset instead of individual file(s), loading all related partitions as columns.
    path_suffix : Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]).
        If None, reads all files. (default)
    path_ignore_suffix : Union[str, List[str], None]
        Suffix or List of suffixes to be ignored.(e.g. [".csv", "_SUCCESS"]).
        If None, reads all files. (default)
    ignore_empty : bool, default True
        Ignore files with 0 bytes.
    partition_filter : Callable[[Dict[str, str]], bool], optional
        Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        This function must receive a single argument (Dict[str, str]) where keys are partitions
        names and values are partitions values. Partitions values must be strings and the function
        must return a bool, True to read the partition or False to ignore it.
        Ignored if `dataset=False`.
        E.g ``lambda x: True if x["year"] == "2020" and x["month"] == "1" else False``
        https://aws-data-wrangler.readthedocs.io/en/3.0.0b3/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    columns : List[str], optional
        List of columns to read from the file(s).
    validate_schema : bool, default False
        Check that the schema is consistent across individual files.
    coerce_int96_timestamp_unit : str, optional
        Cast timestamps that are stored in INT96 format to a particular resolution (e.g. "ms").
        Setting to None is equivalent to "ns" and therefore INT96 timestamps are inferred as in nanoseconds.
    last_modified_begin : datetime, optional
        Filter S3 objects by Last modified date.
        Filter is only applied after listing all objects.
    last_modified_end : datetime, optional
        Filter S3 objects by Last modified date.
        Filter is only applied after listing all objects.
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    chunked : Union[int, bool]
        If passed, the data is split into an iterable of DataFrames (Memory friendly).
        If `True` an iterable of DataFrames is returned without guarantee of chunksize.
        If an `INTEGER` is passed, an iterable of DataFrames is returned with maximum rows
        equal to the received INTEGER.
    use_threads : Union[bool, int], default True
        True to enable concurrent requests, False to disable multiple threads.
        If enabled, os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.
    parallelism : int, optional
        The requested parallelism of the read. Only used when `distributed` add-on is installed.
        Parallelism may be limited by the number of files of the dataset. -1 (autodetect) by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if None is received.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to S3 botocore requests.
    pyarrow_additional_kwargs : Dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

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
    paths: List[str] = _path2list(
        path=path,
        boto3_session=boto3_session,
        suffix=path_suffix,
        ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_empty=ignore_empty,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if not path_root:
        path_root = _get_path_root(path=path, dataset=dataset)
    if path_root and partition_filter:
        paths = _apply_partition_filter(path_root=path_root, paths=paths, filter_func=partition_filter)
    if len(paths) < 1:
        raise exceptions.NoFilesFound(f"No files Found on: {path}.")
    _logger.debug("paths:\n%s", paths)

    version_ids: Optional[Dict[str, str]] = (
        version_id if isinstance(version_id, dict) else {paths[0]: version_id} if isinstance(version_id, str) else None
    )

    # Create PyArrow schema based on file metadata, columns filter, and partitions
    schema = _validate_schemas_from_files(
        validate_schema=validate_schema,
        paths=paths,
        sampling=1.0,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        version_ids=version_ids,
    )
    if path_root:
        partition_types, _ = _extract_partitions_metadata_from_paths(path=path_root, paths=paths)
        if partition_types:
            partition_schema = pa.schema(
                fields={k: _data_types.athena2pyarrow(dtype=v) for k, v in partition_types.items()}
            )
            schema = pa.unify_schemas([schema, partition_schema])
    if columns:
        schema = pa.schema([schema.field(column) for column in columns], schema.metadata)
    _logger.debug("schema:\n%s", schema)

    arrow_kwargs = _data_types.pyarrow2pandas_defaults(use_threads=use_threads, kwargs=pyarrow_additional_kwargs)

    if chunked:
        return _read_parquet_chunked(
            boto3_session=boto3_session,
            paths=paths,
            path_root=path_root,
            columns=columns,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
            chunked=chunked,
            use_threads=use_threads,
            s3_additional_kwargs=s3_additional_kwargs,
            arrow_kwargs=arrow_kwargs,
            version_ids=version_ids,
        )

    return _read_parquet(
        paths,
        path_root=path_root,
        columns=columns,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        use_threads=use_threads,
        parallelism=parallelism,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        arrow_kwargs=arrow_kwargs,
        version_ids=version_ids,
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
    coerce_int96_timestamp_unit: Optional[str] = None,
    chunked: Union[bool, int] = False,
    use_threads: Union[bool, int] = True,
    parallelism: int = 200,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read Apache Parquet table registered in the AWS Glue Catalog.

    Note
    ----
    ``Batching`` (`chunked` argument) (Memory Friendly):

    Used to return an Iterable of DataFrames instead of a regular DataFrame.

    Two batching strategies are available:

    - **chunked=True**, a DataFrame is returned for each file in the dataset.

    - **chunked=INTEGER**, a DataFrame is returned with maximum rows equal to the received INTEGER.

    `P.S.` `chunked=True` if faster and uses less memory while `chunked=INTEGER` is more precise
    in the number of rows.

    Note
    ----
    If `use_threads=True`, the number of threads is obtained from os.cpu_count().

    Parameters
    ----------
    table : str
        AWS Glue Catalog table name.
    database : str
        AWS Glue Catalog database name.
    filename_suffix : Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]).
        If None, read all files. (default)
    filename_ignore_suffix : Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.(e.g. [".csv", "_SUCCESS"]).
        If None, read all files. (default)
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    partition_filter: Optional[Callable[[Dict[str, str]], bool]]
        Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        This function must receive a single argument (Dict[str, str]) where keys are partitions
        names and values are partitions values. Partitions values must be strings and the function
        must return a bool, True to read the partition or False to ignore it.
        Ignored if `dataset=False`.
        E.g ``lambda x: True if x["year"] == "2020" and x["month"] == "1" else False``
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0b3/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    columns : List[str], optional
        List of columns to read from the file(s).
    validate_schema : bool, default False
        Check that the schema is consistent across individual files.
    coerce_int96_timestamp_unit : str, optional
        Cast timestamps that are stored in INT96 format to a particular resolution (e.g. "ms").
        Setting to None is equivalent to "ns" and therefore INT96 timestamps are inferred as in nanoseconds.
    chunked : Union[int, bool]
        If passed, the data is split into an iterable of DataFrames (Memory friendly).
        If `True` an iterable of DataFrames is returned without guarantee of chunksize.
        If an `INTEGER` is passed, an iterable of DataFrames is returned with maximum rows
        equal to the received INTEGER.
    use_threads : Union[bool, int], default True
        True to enable concurrent requests, False to disable multiple threads.
        If enabled, os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.
    parallelism : int, optional
        The requested parallelism of the read. Only used when `distributed` add-on is installed.
        Parallelism may be limited by the number of files of the dataset. 200 by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if None is received.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to S3 botocore requests.
    pyarrow_additional_kwargs : Dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

    Returns
    -------
    Union[pandas.DataFrame, Generator[pandas.DataFrame, None, None]]
        Pandas DataFrame or a Generator in case of `chunked=True`.

    Examples
    --------
    Reading Parquet Table

    >>> import awswrangler as wr
    >>> df = wr.s3.read_parquet_table(database='...', table='...')

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
    res: Dict[str, Any] = client_glue.get_table(**_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table))
    try:
        location: str = res["Table"]["StorageDescriptor"]["Location"]
        path: str = location if location.endswith("/") else f"{location}/"
    except KeyError as ex:
        raise exceptions.InvalidTable(f"Missing s3 location for {database}.{table}.") from ex
    path_root: Optional[str] = None
    paths: Union[str, List[str]] = path
    # If filter is available, fetch & filter out partitions
    # Then list objects & process individual object keys under path_root
    if partition_filter:
        available_partitions_dict = _get_partitions(
            database=database,
            table=table,
            catalog_id=catalog_id,
            boto3_session=boto3_session,
        )
        available_partitions = list(_ensure_locations_are_valid(available_partitions_dict.keys()))
        if available_partitions:
            paths = []
            path_root = path
            partitions: Union[str, List[str]] = _apply_partition_filter(
                path_root=path_root, paths=available_partitions, filter_func=partition_filter
            )
            for partition in partitions:
                paths += _path2list(
                    path=partition,
                    boto3_session=boto3_session,
                    suffix=filename_suffix,
                    ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=filename_ignore_suffix),
                    s3_additional_kwargs=s3_additional_kwargs,
                )
    df = read_parquet(
        path=paths,
        path_root=path_root,
        dataset=True,
        path_suffix=filename_suffix if path_root is None else None,
        path_ignore_suffix=filename_ignore_suffix if path_root is None else None,
        columns=columns,
        validate_schema=validate_schema,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        chunked=chunked,
        use_threads=use_threads,
        parallelism=parallelism,
        boto3_session=boto3_session,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
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
    dataset: bool = False,
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    path_suffix: Optional[str] = None,
    path_ignore_suffix: Optional[str] = None,
    ignore_empty: bool = True,
    ignore_null: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    sampling: float = 1.0,
    coerce_int96_timestamp_unit: Optional[str] = None,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]]]:
    """Read Apache Parquet file(s) metadata from an S3 prefix or list of S3 objects paths.

    The concept of `dataset` enables more complex features like partitioning
    and catalog integration (AWS Glue Catalog).

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the argument to this function.

    Note
    ----
    If `use_threads=True`, the number of threads is obtained from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    dataset : bool, default False
        If `True`, read a parquet dataset instead of individual file(s), loading all related partitions as columns.
    version_id : Union[str, Dict[str, str]], optional
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    path_suffix : Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]).
        If None, reads all files. (default)
    path_ignore_suffix : Union[str, List[str], None]
        Suffix or List of suffixes to be ignored.(e.g. [".csv", "_SUCCESS"]).
        If None, reads all files. (default)
    ignore_empty : bool, default True
        Ignore files with 0 bytes.
    ignore_null : bool, default False
        Ignore columns with null type.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to cast.
        Use when you have columns with undetermined data types as partitions columns.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    sampling : float
        Ratio of files metadata to inspect.
        Must be `0.0 < sampling <= 1.0`.
        The higher, the more accurate.
        The lower, the faster.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to S3 botocore requests.

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
        ignore_null=ignore_null,
        dtype=dtype,
        sampling=sampling,
        dataset=dataset,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
    )[:2]
