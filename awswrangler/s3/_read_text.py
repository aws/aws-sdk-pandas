"""Amazon S3 Read Module (PRIVATE)."""

import datetime
import logging
import pprint
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import boto3  # type: ignore
import pandas as pd  # type: ignore
import pandas.io.parsers  # type: ignore
import s3fs  # type: ignore
from pandas.io.common import infer_compression  # type: ignore

from awswrangler import _utils, exceptions
from awswrangler.s3._list import _path2list
from awswrangler.s3._read import (
    _apply_partition_filter,
    _apply_partitions,
    _get_path_ignore_suffix,
    _get_path_root,
    _union,
)
from awswrangler.s3._read_concurrent import _read_concurrent

_logger: logging.Logger = logging.getLogger(__name__)


def _get_read_details(path: str, pandas_kwargs: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    if pandas_kwargs.get("compression", "infer") == "infer":
        pandas_kwargs["compression"] = infer_compression(path, compression="infer")
    mode: str = "r" if pandas_kwargs.get("compression") is None else "rb"
    encoding: Optional[str] = pandas_kwargs.get("encoding", None)
    newline: Optional[str] = pandas_kwargs.get("lineterminator", None)
    return mode, encoding, newline


def _read_text_chunked(
    paths: List[str],
    chunksize: int,
    parser_func: Callable,
    path_root: Optional[str],
    boto3_session: boto3.Session,
    pandas_kwargs: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
) -> Iterator[pd.DataFrame]:
    for path in paths:
        _logger.debug("path: %s", path)
        fs: s3fs.S3FileSystem = _utils.get_fs(
            s3fs_block_size=8_388_608,
            session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,  # 8 MB (8 * 2**20)
        )
        mode, encoding, newline = _get_read_details(path=path, pandas_kwargs=pandas_kwargs)
        with _utils.open_file(fs=fs, path=path, mode=mode, encoding=encoding, newline=newline) as f:
            reader: pandas.io.parsers.TextFileReader = parser_func(f, chunksize=chunksize, **pandas_kwargs)
            for df in reader:
                yield _apply_partitions(df=df, dataset=dataset, path=path, path_root=path_root)


def _read_text_file(
    path: str,
    parser_func: Callable,
    path_root: Optional[str],
    boto3_session: Union[boto3.Session, Dict[str, Optional[str]]],
    pandas_kwargs: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
) -> pd.DataFrame:
    fs: s3fs.S3FileSystem = _utils.get_fs(
        s3fs_block_size=134_217_728,
        session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,  # 128 MB (128 * 2**20)
    )
    mode, encoding, newline = _get_read_details(path=path, pandas_kwargs=pandas_kwargs)
    with _utils.open_file(fs=fs, path=path, mode=mode, encoding=encoding, newline=newline) as f:
        df: pd.DataFrame = parser_func(f, **pandas_kwargs)
    return _apply_partitions(df=df, dataset=dataset, path=path, path_root=path_root)


def _read_text(
    parser_func: Callable,
    path: Union[str, List[str]],
    path_suffix: Union[str, List[str], None],
    path_ignore_suffix: Union[str, List[str], None],
    use_threads: bool,
    last_modified_begin: Optional[datetime.datetime],
    last_modified_end: Optional[datetime.datetime],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    chunksize: Optional[int],
    dataset: bool,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]],
    ignore_index: bool,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    if "iterator" in pandas_kwargs:
        raise exceptions.InvalidArgument("Please, use the chunksize argument instead of iterator.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    paths: List[str] = _path2list(
        path=path,
        boto3_session=session,
        suffix=path_suffix,
        ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
    )
    path_root: Optional[str] = _get_path_root(path=path, dataset=dataset)
    if path_root is not None:
        paths = _apply_partition_filter(path_root=path_root, paths=paths, filter_func=partition_filter)
    if len(paths) < 1:
        raise exceptions.NoFilesFound(f"No files Found on: {path}.")
    _logger.debug("paths:\n%s", paths)

    args: Dict[str, Any] = {
        "parser_func": parser_func,
        "boto3_session": session,
        "dataset": dataset,
        "path_root": path_root,
        "pandas_kwargs": pandas_kwargs,
        "s3_additional_kwargs": s3_additional_kwargs,
    }
    _logger.debug("args:\n%s", pprint.pformat(args))
    ret: Union[pd.DataFrame, Iterator[pd.DataFrame]]
    if chunksize is not None:
        ret = _read_text_chunked(paths=paths, chunksize=chunksize, **args)
    elif len(paths) == 1:
        ret = _read_text_file(path=paths[0], **args)
    elif use_threads is True:
        ret = _read_concurrent(func=_read_text_file, paths=paths, ignore_index=ignore_index, **args)
    else:
        ret = _union(dfs=[_read_text_file(path=p, **args) for p in paths], ignore_index=ignore_index)
    return ret


def read_csv(
    path: Union[str, List[str]],
    path_suffix: Union[str, List[str], None] = None,
    path_ignore_suffix: Union[str, List[str], None] = None,
    use_threads: bool = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read CSV file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

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
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for filtering S3 keys.
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    chunksize: int, optional
        If specified, return an generator where chunksize is the number of rows to include in each chunk.
    dataset: bool
        If `True` read a CSV dataset instead of simple file(s) loading all the related partitions as columns.
    partition_filter: Optional[Callable[[Dict[str, str]], bool]]
        Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        This function MUST receive a single argument (Dict[str, str]) where keys are partitions
        names and values are partitions values. Partitions values will be always strings extracted from S3.
        This function MUST return a bool, True to read the partition or False to ignore it.
        Ignored if `dataset=False`.
        E.g ``lambda x: True if x["year"] == "2020" and x["month"] == "1" else False``
        https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/023%20-%20Flexible%20Partitions%20Filter.ipynb
    pandas_kwargs:
        keyword arguments forwarded to pandas.read_csv().
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

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

    Reading CSV Dataset with PUSH-DOWN filter over partitions

    >>> import awswrangler as wr
    >>> my_filter = lambda x: True if x["city"].startswith("new") else False
    >>> df = wr.s3.read_csv(path, dataset=True, partition_filter=my_filter)

    """
    ignore_index: bool = "index_col" not in pandas_kwargs
    return _read_text(
        parser_func=pd.read_csv,
        path=path,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        dataset=dataset,
        partition_filter=partition_filter,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_index=ignore_index,
        **pandas_kwargs,
    )


def read_fwf(
    path: Union[str, List[str]],
    path_suffix: Union[str, List[str], None] = None,
    path_ignore_suffix: Union[str, List[str], None] = None,
    use_threads: bool = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read fixed-width formatted file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

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
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for filtering S3 keys.
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    chunksize: int, optional
        If specified, return an generator where chunksize is the number of rows to include in each chunk.
    dataset: bool
        If `True` read a FWF dataset instead of simple file(s) loading all the related partitions as columns.
    partition_filter: Optional[Callable[[Dict[str, str]], bool]]
        Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        This function MUST receive a single argument (Dict[str, str]) where keys are partitions
        names and values are partitions values. Partitions values will be always strings extracted from S3.
        This function MUST return a bool, True to read the partition or False to ignore it.
        Ignored if `dataset=False`.
        E.g ``lambda x: True if x["year"] == "2020" and x["month"] == "1" else False``
        https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/023%20-%20Flexible%20Partitions%20Filter.ipynb
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

    Reading FWF Dataset with PUSH-DOWN filter over partitions

    >>> import awswrangler as wr
    >>> my_filter = lambda x: True if x["city"].startswith("new") else False
    >>> df = wr.s3.read_fwf(path, dataset=True, partition_filter=my_filter)

    """
    return _read_text(
        parser_func=pd.read_fwf,
        path=path,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        dataset=dataset,
        partition_filter=partition_filter,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_index=True,
        sort_index=False,
        **pandas_kwargs,
    )


def read_json(
    path: Union[str, List[str]],
    path_suffix: Union[str, List[str], None] = None,
    path_ignore_suffix: Union[str, List[str], None] = None,
    orient: str = "columns",
    use_threads: bool = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read JSON file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

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
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for filtering S3 keys.
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.
    orient : str
        Same as Pandas: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
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
    partition_filter: Optional[Callable[[Dict[str, str]], bool]]
        Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        This function MUST receive a single argument (Dict[str, str]) where keys are partitions
        names and values are partitions values. Partitions values will be always strings extracted from S3.
        This function MUST return a bool, True to read the partition or False to ignore it.
        Ignored if `dataset=False`.
        E.g ``lambda x: True if x["year"] == "2020" and x["month"] == "1" else False``
        https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/023%20-%20Flexible%20Partitions%20Filter.ipynb
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

    Reading JSON Dataset with PUSH-DOWN filter over partitions

    >>> import awswrangler as wr
    >>> my_filter = lambda x: True if x["city"].startswith("new") else False
    >>> df = wr.s3.read_json(path, dataset=True, partition_filter=my_filter)

    """
    if (dataset is True) and ("lines" not in pandas_kwargs):
        pandas_kwargs["lines"] = True
    pandas_kwargs["orient"] = orient
    ignore_index: bool = orient not in ("split", "index", "columns")
    return _read_text(
        parser_func=pd.read_json,
        path=path,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        dataset=dataset,
        partition_filter=partition_filter,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_index=ignore_index,
        **pandas_kwargs,
    )
