"""Amazon S3 Read Module (PRIVATE)."""

import concurrent.futures
import itertools
import logging
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import boto3  # type: ignore
import pandas as pd  # type: ignore
import pandas.io.parsers  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.lib  # type: ignore
import pyarrow.parquet  # type: ignore
import s3fs  # type: ignore
from pandas.io.common import infer_compression  # type: ignore

from awswrangler import _data_types, _utils, catalog, exceptions
from awswrangler.s3._list import path2list

_logger: logging.Logger = logging.getLogger(__name__)


def read_parquet_metadata_internal(
    path: Union[str, List[str]],
    dtype: Optional[Dict[str, str]],
    sampling: float,
    dataset: bool,
    use_threads: bool,
    boto3_session: Optional[boto3.Session],
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Handle wr.s3.read_parquet_metadata internally."""
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if dataset is True:
        if isinstance(path, str):
            _path: Optional[str] = path if path.endswith("/") else f"{path}/"
            paths: List[str] = path2list(path=_path, boto3_session=session)
        else:  # pragma: no cover
            raise exceptions.InvalidArgumentType("Argument <path> must be str if dataset=True.")
    else:
        if isinstance(path, str):
            _path = None
            paths = path2list(path=path, boto3_session=session)
        elif isinstance(path, list):
            _path = None
            paths = path
        else:  # pragma: no cover
            raise exceptions.InvalidArgumentType(f"Argument path must be str or List[str] instead of {type(path)}.")
    schemas: List[Dict[str, str]] = [
        _read_parquet_metadata_file(path=x, use_threads=use_threads, boto3_session=session)
        for x in _utils.list_sampling(lst=paths, sampling=sampling)
    ]
    _logger.debug("schemas: %s", schemas)
    columns_types: Dict[str, str] = {}
    for schema in schemas:
        for column, _dtype in schema.items():
            if (column in columns_types) and (columns_types[column] != _dtype):  # pragma: no cover
                raise exceptions.InvalidSchemaConvergence(
                    f"Was detect at least 2 different types in column {column} ({columns_types[column]} and {dtype})."
                )
            columns_types[column] = _dtype
    partitions_types: Optional[Dict[str, str]] = None
    partitions_values: Optional[Dict[str, List[str]]] = None
    if (dataset is True) and (_path is not None):
        partitions_types, partitions_values = _utils.extract_partitions_metadata_from_paths(path=_path, paths=paths)
    if dtype:
        for k, v in dtype.items():
            if columns_types and k in columns_types:
                columns_types[k] = v
            if partitions_types and k in partitions_types:
                partitions_types[k] = v
    _logger.debug("columns_types: %s", columns_types)
    return columns_types, partitions_types, partitions_values


def _read_text(
    parser_func: Callable,
    path: Union[str, List[str]],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    lastModified_begin: Optional[datetime] = None,
    lastModified_end: Optional[datetime] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    if "iterator" in pandas_kwargs:
        raise exceptions.InvalidArgument("Please, use chunksize instead of iterator.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if (dataset is True) and (not isinstance(path, str)):  # pragma: no cover
        raise exceptions.InvalidArgument("The path argument must be a string Amazon S3 prefix if dataset=True.")
    if dataset is True:
        path_root: str = str(path)
    else:
        path_root = ""
    paths: List[str] = path2list(
        path=path, boto3_session=session, lastModified_begin=lastModified_begin, lastModified_end=lastModified_end
    )
    if len(paths) < 1:
        raise exceptions.InvalidArgument("No files Found.")

    _logger.debug("paths:\n%s", paths)
    if chunksize is not None:
        dfs: Iterator[pd.DataFrame] = _read_text_chunksize(
            parser_func=parser_func,
            paths=paths,
            boto3_session=session,
            chunksize=chunksize,
            pandas_args=pandas_kwargs,
            s3_additional_kwargs=s3_additional_kwargs,
            dataset=dataset,
            path_root=path_root,
        )
        return dfs
    if use_threads is False:
        df: pd.DataFrame = pd.concat(
            objs=[
                _read_text_full(
                    parser_func=parser_func,
                    path=p,
                    boto3_session=session,
                    pandas_args=pandas_kwargs,
                    s3_additional_kwargs=s3_additional_kwargs,
                    dataset=dataset,
                    path_root=path_root,
                )
                for p in paths
            ],
            ignore_index=True,
            sort=False,
        )
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            df = pd.concat(
                objs=executor.map(
                    _read_text_full,
                    itertools.repeat(parser_func),
                    itertools.repeat(path_root),
                    paths,
                    itertools.repeat(_utils.boto3_to_primitives(boto3_session=session)),  # Boto3.Session
                    itertools.repeat(pandas_kwargs),
                    itertools.repeat(s3_additional_kwargs),
                    itertools.repeat(dataset),
                ),
                ignore_index=True,
                sort=False,
            )
    return df


def _read_text_chunksize(
    parser_func: Callable,
    path_root: str,
    paths: List[str],
    boto3_session: boto3.Session,
    chunksize: int,
    pandas_args: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    dataset: bool = False,
) -> Iterator[pd.DataFrame]:
    fs: s3fs.S3FileSystem = _utils.get_fs(session=boto3_session, s3_additional_kwargs=s3_additional_kwargs)
    for path in paths:
        _logger.debug("path: %s", path)
        partitions: Dict[str, Any] = {}
        if dataset is True:
            partitions = _utils.extract_partitions_from_path(path_root=path_root, path=path)
        if pandas_args.get("compression", "infer") == "infer":
            pandas_args["compression"] = infer_compression(path, compression="infer")
        mode: str = "r" if pandas_args.get("compression") is None else "rb"
        with fs.open(path, mode) as f:
            reader: pandas.io.parsers.TextFileReader = parser_func(f, chunksize=chunksize, **pandas_args)
            for df in reader:
                if dataset is True:
                    for column_name, value in partitions.items():
                        df[column_name] = value
                yield df


def _read_text_full(
    parser_func: Callable,
    path_root: str,
    path: str,
    boto3_session: Union[boto3.Session, Dict[str, Optional[str]]],
    pandas_args: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    dataset: bool = False,
) -> pd.DataFrame:
    fs: s3fs.S3FileSystem = _utils.get_fs(session=boto3_session, s3_additional_kwargs=s3_additional_kwargs)
    if pandas_args.get("compression", "infer") == "infer":
        pandas_args["compression"] = infer_compression(path, compression="infer")
    mode: str = "r" if pandas_args.get("compression") is None else "rb"
    encoding: Optional[str] = pandas_args.get("encoding", None)
    newline: Optional[str] = pandas_args.get("lineterminator", None)
    with fs.open(path=path, mode=mode, encoding=encoding, newline=newline) as f:
        df: pd.DataFrame = parser_func(f, **pandas_args)
    if dataset is True:
        partitions: Dict[str, Any] = _utils.extract_partitions_from_path(path_root=path_root, path=path)
        for column_name, value in partitions.items():
            df[column_name] = value
    return df


def _read_parquet_init(
    path: Union[str, List[str]],
    filters: Optional[Union[List[Tuple], List[List[Tuple]]]] = None,
    categories: List[str] = None,
    validate_schema: bool = True,
    dataset: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    lastModified_begin: Optional[datetime] = None,
    lastModified_end: Optional[datetime] = None,
) -> pyarrow.parquet.ParquetDataset:
    """Encapsulate all initialization before the use of the pyarrow.parquet.ParquetDataset."""
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if dataset is False:
        path_or_paths: Union[str, List[str]] = path2list(
            path=path, boto3_session=session, lastModified_begin=lastModified_begin, lastModified_end=lastModified_end
        )
    elif isinstance(path, str):
        path_or_paths = path[:-1] if path.endswith("/") else path
    else:
        path_or_paths = path
    _logger.debug("path_or_paths: %s", path_or_paths)
    if len(path_or_paths) < 1:
        raise exceptions.InvalidArgumentType("No Files Found")

    fs: s3fs.S3FileSystem = _utils.get_fs(session=session, s3_additional_kwargs=s3_additional_kwargs)
    cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
    data: pyarrow.parquet.ParquetDataset = pyarrow.parquet.ParquetDataset(
        path_or_paths=path_or_paths,
        filesystem=fs,
        metadata_nthreads=cpus,
        filters=filters,
        read_dictionary=categories,
        validate_schema=validate_schema,
        split_row_groups=False,
        use_legacy_dataset=True,
    )
    return data


def _read_parquet(
    data: pyarrow.parquet.ParquetDataset,
    columns: Optional[List[str]] = None,
    categories: List[str] = None,
    use_threads: bool = True,
    validate_schema: bool = True,
) -> pd.DataFrame:
    tables: List[pa.Table] = []
    _logger.debug("Reading pieces...")
    for piece in data.pieces:
        table: pa.Table = piece.read(
            columns=columns, use_threads=use_threads, partitions=data.partitions, use_pandas_metadata=False
        )
        _logger.debug("Appending piece in the list...")
        tables.append(table)
    promote: bool = not validate_schema
    _logger.debug("Concating pieces...")
    table = pa.lib.concat_tables(tables, promote=promote)
    _logger.debug("Converting PyArrow table to Pandas DataFrame...")
    return table.to_pandas(
        use_threads=use_threads,
        split_blocks=True,
        self_destruct=True,
        integer_object_nulls=False,
        date_as_object=True,
        ignore_metadata=True,
        categories=categories,
        types_mapper=_data_types.pyarrow2pandas_extension,
    )


def _read_parquet_chunked(
    data: pyarrow.parquet.ParquetDataset,
    columns: Optional[List[str]] = None,
    categories: List[str] = None,
    chunked: Union[bool, int] = True,
    use_threads: bool = True,
) -> Iterator[pd.DataFrame]:
    next_slice: Optional[pd.DataFrame] = None
    for piece in data.pieces:
        df: pd.DataFrame = _table2df(
            table=piece.read(
                columns=columns, use_threads=use_threads, partitions=data.partitions, use_pandas_metadata=False
            ),
            categories=categories,
            use_threads=use_threads,
        )
        if chunked is True:
            yield df
        else:
            if next_slice is not None:
                df = pd.concat(objs=[next_slice, df], ignore_index=True, sort=False)
            while len(df.index) >= chunked:
                yield df.iloc[:chunked]
                df = df.iloc[chunked:]
            if df.empty:
                next_slice = None
            else:
                next_slice = df
    if next_slice is not None:
        yield next_slice


def _table2df(table: pa.Table, categories: List[str] = None, use_threads: bool = True) -> pd.DataFrame:
    return table.to_pandas(
        use_threads=use_threads,
        split_blocks=True,
        self_destruct=True,
        integer_object_nulls=False,
        date_as_object=True,
        ignore_metadata=True,
        categories=categories,
        types_mapper=_data_types.pyarrow2pandas_extension,
    )


def _read_parquet_metadata_file(path: str, use_threads: bool, boto3_session: boto3.Session) -> Dict[str, str]:
    data: pyarrow.parquet.ParquetDataset = _read_parquet_init(
        path=path, filters=None, dataset=False, use_threads=use_threads, boto3_session=boto3_session
    )
    return _data_types.athena_types_from_pyarrow_schema(schema=data.schema.to_arrow_schema(), partitions=None)[0]


def read_csv(
    path: Union[str, List[str]],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    lastModified_begin: Optional[datetime] = None,
    lastModified_end: Optional[datetime] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read CSV file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Note
    ----
    The filter by lastModified begin lastModified end is applied after list all S3 files

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    chunksize: int, optional
        If specified, return an generator where chunksize is the number of rows to include in each chunk.
    dataset: bool
        If `True` read a CSV dataset instead of simple file(s) loading all the related partitions as columns.
    pandas_kwargs:
        keyword arguments forwarded to pandas.read_csv().
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
    lastModified_begin lastModified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object

    Returns
    -------
    Union[pandas.DataFrame, Generator[pandas.DataFrame, None, None]]
        Pandas DataFrame or a Generator in case of `chunksize != None`.

    Examples
    --------
    Reading all CSV files under a prefix

    >>> import awswrangler as wr
    >>> df = wr.s3.read_csv(path='s3://bucket/prefix/')

    Reading all CSV files under a prefix encrypted with a KMS key

    >>> import awswrangler as wr
    >>> df = wr.s3.read_csv(
    ...     path='s3://bucket/prefix/',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
    ...     }
    ... )

    Reading all CSV files from a list

    >>> import awswrangler as wr
    >>> df = wr.s3.read_csv(path=['s3://bucket/filename0.csv', 's3://bucket/filename1.csv'])

    Reading in chunks of 100 lines

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_csv(path=['s3://bucket/filename0.csv', 's3://bucket/filename1.csv'], chunksize=100)
    >>> for df in dfs:
    >>>     print(df)  # 100 lines Pandas DataFrame

    """
    return _read_text(
        parser_func=pd.read_csv,
        path=path,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        dataset=dataset,
        lastModified_begin=lastModified_begin,
        lastModified_end=lastModified_end,
        **pandas_kwargs,
    )


def read_fwf(
    path: Union[str, List[str]],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    lastModified_begin: Optional[datetime] = None,
    lastModified_end: Optional[datetime] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read fixed-width formatted file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Note
    ----
    The filter by lastModified begin lastModified end is applied after list all S3 files

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    chunksize: int, optional
        If specified, return an generator where chunksize is the number of rows to include in each chunk.
    dataset: bool
        If `True` read a FWF dataset instead of simple file(s) loading all the related partitions as columns.
    lastModified_begin lastModified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object
    pandas_kwargs:
        keyword arguments forwarded to pandas.read_fwf().
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_fwf.html

    Returns
    -------
    Union[pandas.DataFrame, Generator[pandas.DataFrame, None, None]]
        Pandas DataFrame or a Generator in case of `chunksize != None`.

    Examples
    --------
    Reading all fixed-width formatted (FWF) files under a prefix

    >>> import awswrangler as wr
    >>> df = wr.s3.read_fwf(path='s3://bucket/prefix/')

    Reading all fixed-width formatted (FWF) files under a prefix encrypted with a KMS key

    >>> import awswrangler as wr
    >>> df = wr.s3.read_fwf(
    ...     path='s3://bucket/prefix/',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
    ...     }
    ... )

    Reading all fixed-width formatted (FWF) files from a list

    >>> import awswrangler as wr
    >>> df = wr.s3.read_fwf(path=['s3://bucket/filename0.txt', 's3://bucket/filename1.txt'])

    Reading in chunks of 100 lines

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_fwf(path=['s3://bucket/filename0.txt', 's3://bucket/filename1.txt'], chunksize=100)
    >>> for df in dfs:
    >>>     print(df)  # 100 lines Pandas DataFrame

    """
    return _read_text(
        parser_func=pd.read_fwf,
        path=path,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        dataset=dataset,
        lastModified_begin=lastModified_begin,
        lastModified_end=lastModified_end,
        **pandas_kwargs,
    )


def read_json(
    path: Union[str, List[str]],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    lastModified_begin: Optional[datetime] = None,
    lastModified_end: Optional[datetime] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read JSON file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Note
    ----
    The filter by lastModified begin lastModified end is applied after list all S3 files

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    chunksize: int, optional
        If specified, return an generator where chunksize is the number of rows to include in each chunk.
    dataset: bool
        If `True` read a JSON dataset instead of simple file(s) loading all the related partitions as columns.
        If `True`, the `lines=True` will be assumed by default.
    lastModified_begin lastModified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object
    pandas_kwargs:
        keyword arguments forwarded to pandas.read_json().
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html

    Returns
    -------
    Union[pandas.DataFrame, Generator[pandas.DataFrame, None, None]]
        Pandas DataFrame or a Generator in case of `chunksize != None`.

    Examples
    --------
    Reading all JSON files under a prefix

    >>> import awswrangler as wr
    >>> df = wr.s3.read_json(path='s3://bucket/prefix/')

    Reading all JSON files under a prefix encrypted with a KMS key

    >>> import awswrangler as wr
    >>> df = wr.s3.read_json(
    ...     path='s3://bucket/prefix/',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
    ...     }
    ... )

    Reading all JSON files from a list

    >>> import awswrangler as wr
    >>> df = wr.s3.read_json(path=['s3://bucket/filename0.json', 's3://bucket/filename1.json'])

    Reading in chunks of 100 lines

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_json(path=['s3://bucket/filename0.json', 's3://bucket/filename1.json'], chunksize=100)
    >>> for df in dfs:
    >>>     print(df)  # 100 lines Pandas DataFrame

    """
    if (dataset is True) and ("lines" not in pandas_kwargs):
        pandas_kwargs["lines"] = True
    return _read_text(
        parser_func=pd.read_json,
        path=path,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        dataset=dataset,
        lastModified_begin=lastModified_begin,
        lastModified_end=lastModified_end,
        **pandas_kwargs,
    )


def read_parquet(
    path: Union[str, List[str]],
    filters: Optional[Union[List[Tuple], List[List[Tuple]]]] = None,
    columns: Optional[List[str]] = None,
    validate_schema: bool = True,
    chunked: Union[bool, int] = False,
    dataset: bool = False,
    categories: List[str] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    lastModified_begin: Optional[datetime] = None,
    lastModified_end: Optional[datetime] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read Apache Parquet file(s) from from a received S3 prefix or list of S3 objects paths.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

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
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Note
    ----
    The filter by lastModified begin lastModified end is applied after list all S3 files

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    filters: Union[List[Tuple], List[List[Tuple]]], optional
        List of filters to apply on PARTITION columns (PUSH-DOWN filter), like ``[[('x', '=', 0), ...], ...]``.
        Ignored if `dataset=False`.
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
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    lastModified_begin lastModified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object

    Returns
    -------
    Union[pandas.DataFrame, Generator[pandas.DataFrame, None, None]]
        Pandas DataFrame or a Generator in case of `chunked=True`.

    Examples
    --------
    Reading all Parquet files under a prefix

    >>> import awswrangler as wr
    >>> df = wr.s3.read_parquet(path='s3://bucket/prefix/')

    Reading all Parquet files under a prefix encrypted with a KMS key

    >>> import awswrangler as wr
    >>> df = wr.s3.read_parquet(
    ...     path='s3://bucket/prefix/',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
    ...     }
    ... )

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

    """
    data: pyarrow.parquet.ParquetDataset = _read_parquet_init(
        path=path,
        filters=filters,
        dataset=dataset,
        categories=categories,
        validate_schema=validate_schema,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        lastModified_begin=lastModified_begin,
        lastModified_end=lastModified_end,
    )
    _logger.debug("pyarrow.parquet.ParquetDataset initialized.")
    if chunked is False:
        return _read_parquet(
            data=data, columns=columns, categories=categories, use_threads=use_threads, validate_schema=validate_schema
        )
    return _read_parquet_chunked(
        data=data, columns=columns, categories=categories, chunked=chunked, use_threads=use_threads
    )


def read_parquet_metadata(
    path: Union[str, List[str]],
    dtype: Optional[Dict[str, str]] = None,
    sampling: float = 1.0,
    dataset: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]]]:
    """Read Apache Parquet file(s) metadata from from a received S3 prefix or list of S3 objects paths.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
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

    Returns
    -------
    Tuple[Dict[str, str], Optional[Dict[str, str]]]
        columns_types: Dictionary with keys as column names and vales as
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
    return read_parquet_metadata_internal(
        path=path, dtype=dtype, sampling=sampling, dataset=dataset, use_threads=use_threads, boto3_session=boto3_session
    )[:2]


def read_parquet_table(
    table: str,
    database: str,
    filters: Optional[Union[List[Tuple], List[List[Tuple]]]] = None,
    columns: Optional[List[str]] = None,
    categories: List[str] = None,
    chunked: Union[bool, int] = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
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
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    table : str
        AWS Glue Catalog table name.
    database : str
        AWS Glue Catalog database name.
    filters: Union[List[Tuple], List[List[Tuple]]], optional
        List of filters to apply, like ``[[('x', '=', 0), ...], ...]``.
    columns : List[str], optional
        Names of columns to read from the file(s).
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    chunked : bool
        If True will break the data in smaller DataFrames (Non deterministic number of lines).
        Otherwise return a single DataFrame with the whole data.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption

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
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
    ...     }
    ... )

    Reading Parquet Table in chunks (Chunk by file)

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_parquet_table(database='...', table='...', chunked=True)
    >>> for df in dfs:
    >>>     print(df)  # Smaller Pandas DataFrame

    Reading in chunks (Chunk by 1MM rows)

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_parquet(path=['s3://bucket/filename0.csv', 's3://bucket/filename1.csv'], chunked=1_000_000)
    >>> for df in dfs:
    >>>     print(df)  # 1MM Pandas DataFrame

    """
    path: str = catalog.get_table_location(database=database, table=table, boto3_session=boto3_session)
    return read_parquet(
        path=path,
        filters=filters,
        columns=columns,
        categories=categories,
        chunked=chunked,
        dataset=True,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
