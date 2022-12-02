"""Amazon S3 Read Module (PRIVATE)."""
import datetime
import logging
import pprint
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import botocore.exceptions
import pandas as pd
import pandas.io.parsers
from pandas.io.common import infer_compression

from awswrangler import _utils, exceptions
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._list import _path2list
from awswrangler.s3._read import (
    _apply_partition_filter,
    _apply_partitions,
    _get_path_ignore_suffix,
    _get_path_root,
    _read_dfs_from_multiple_paths,
    _union,
)

_logger: logging.Logger = logging.getLogger(__name__)


def _get_read_details(path: str, pandas_kwargs: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    if pandas_kwargs.get("compression", "infer") == "infer":
        pandas_kwargs["compression"] = infer_compression(path, compression="infer")
    mode: str = (
        "r" if pandas_kwargs.get("compression") is None and pandas_kwargs.get("encoding_errors") != "ignore" else "rb"
    )
    encoding: Optional[str] = pandas_kwargs.get("encoding", "utf-8")
    newline: Optional[str] = pandas_kwargs.get("lineterminator", None)
    return mode, encoding, newline


def _read_text_chunked(
    paths: List[str],
    chunksize: int,
    parser_func: Callable[..., pd.DataFrame],
    path_root: Optional[str],
    boto3_session: boto3.Session,
    pandas_kwargs: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
    use_threads: Union[bool, int],
    version_ids: Optional[Dict[str, str]] = None,
) -> Iterator[pd.DataFrame]:
    for path in paths:
        _logger.debug("path: %s", path)
        mode, encoding, newline = _get_read_details(path=path, pandas_kwargs=pandas_kwargs)
        with open_s3_object(
            path=path,
            version_id=version_ids.get(path) if version_ids else None,
            mode=mode,
            s3_block_size=10_485_760,  # 10 MB (10 * 2**20)
            encoding=encoding,
            use_threads=use_threads,
            s3_additional_kwargs=s3_additional_kwargs,
            newline=newline,
            boto3_session=boto3_session,
        ) as f:
            reader: pandas.io.parsers.TextFileReader = parser_func(f, chunksize=chunksize, **pandas_kwargs)
            for df in reader:
                yield _apply_partitions(df=df, dataset=dataset, path=path, path_root=path_root)


def _read_text_file(
    path: str,
    version_id: Optional[str],
    parser_func: Callable[..., pd.DataFrame],
    path_root: Optional[str],
    boto3_session: Union[boto3.Session, _utils.Boto3PrimitivesType],
    pandas_kwargs: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
    use_threads: Union[bool, int],
) -> pd.DataFrame:
    boto3_session = _utils.ensure_session(boto3_session)
    mode, encoding, newline = _get_read_details(path=path, pandas_kwargs=pandas_kwargs)
    try:
        with open_s3_object(
            path=path,
            version_id=version_id,
            mode=mode,
            use_threads=use_threads,
            s3_block_size=-1,  # One shot download
            encoding=encoding,
            s3_additional_kwargs=s3_additional_kwargs,
            newline=newline,
            boto3_session=boto3_session,
        ) as f:
            df: pd.DataFrame = parser_func(f, **pandas_kwargs)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise exceptions.NoFilesFound(f"No files Found on: {path}.")
        raise e
    return _apply_partitions(df=df, dataset=dataset, path=path, path_root=path_root)


def _read_text(
    parser_func: Callable[..., pd.DataFrame],
    path: Union[str, List[str]],
    path_suffix: Union[str, List[str], None],
    path_ignore_suffix: Union[str, List[str], None],
    ignore_empty: bool,
    use_threads: Union[bool, int],
    last_modified_begin: Optional[datetime.datetime],
    last_modified_end: Optional[datetime.datetime],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    chunksize: Optional[int],
    dataset: bool,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]],
    ignore_index: bool,
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    if "iterator" in pandas_kwargs:
        raise exceptions.InvalidArgument("Please, use the chunksize argument instead of iterator.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    paths: List[str] = _path2list(
        path=path,
        boto3_session=session,
        suffix=path_suffix,
        ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
        ignore_empty=ignore_empty,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        s3_additional_kwargs=s3_additional_kwargs,
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
        "use_threads": use_threads,
    }
    _logger.debug("args:\n%s", pprint.pformat(args))
    ret: Union[pd.DataFrame, Iterator[pd.DataFrame]]
    if chunksize is not None:
        ret = _read_text_chunked(
            paths=paths, version_ids=version_id if isinstance(version_id, dict) else None, chunksize=chunksize, **args
        )
    elif len(paths) == 1:
        ret = _read_text_file(
            path=paths[0], version_id=version_id[paths[0]] if isinstance(version_id, dict) else version_id, **args
        )
    else:
        ret = _union(
            dfs=_read_dfs_from_multiple_paths(
                read_func=_read_text_file,
                paths=paths,
                version_ids=version_id if isinstance(version_id, dict) else None,
                use_threads=use_threads,
                kwargs=args,
            ),
            ignore_index=ignore_index,
        )
    return ret


def read_csv(
    path: Union[str, List[str]],
    path_suffix: Union[str, List[str], None] = None,
    path_ignore_suffix: Union[str, List[str], None] = None,
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    ignore_empty: bool = True,
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read CSV file(s) from a received S3 prefix or list of S3 objects paths.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

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
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".csv"]).
        If None, will try to read all files. (default)
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.(e.g. ["_SUCCESS"]).
        If None, will try to read all files. (default)
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    ignore_empty: bool
        Ignore files with 0 bytes.
    use_threads : Union[bool, int]
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
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
    chunksize: int, optional
        If specified, return an generator where chunksize is the number of rows to include in each chunk.
    dataset : bool
        If `True` read a CSV dataset instead of simple file(s) loading all the related partitions as columns.
    partition_filter : Optional[Callable[[Dict[str, str]], bool]]
        Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter).
        This function MUST receive a single argument (Dict[str, str]) where keys are partitions
        names and values are partitions values. Partitions values will be always strings extracted from S3.
        This function MUST return a bool, True to read the partition or False to ignore it.
        Ignored if `dataset=False`.
        E.g ``lambda x: True if x["year"] == "2020" and x["month"] == "1" else False``
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    pandas_kwargs :
        KEYWORD arguments forwarded to pandas.read_csv(). You can NOT pass `pandas_kwargs` explicitly, just add valid
        Pandas arguments in the function call and awswrangler will accept it.
        e.g. wr.s3.read_csv('s3://bucket/prefix/', sep='|', na_values=['null', 'none'], skip_blank_lines=True)
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

    Reading all CSV files under a prefix and using pandas_kwargs

    >>> import awswrangler as wr
    >>> df = wr.s3.read_csv('s3://bucket/prefix/', sep='|', na_values=['null', 'none'], skip_blank_lines=True)

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
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicitly, just add valid "
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.read_csv('s3://bucket/prefix/', sep='|', skip_blank_lines=True)"
        )
    ignore_index: bool = "index_col" not in pandas_kwargs
    return _read_text(
        parser_func=pd.read_csv,
        path=path,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        version_id=version_id,
        ignore_empty=ignore_empty,
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
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    ignore_empty: bool = True,
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read fixed-width formatted file(s) from a received S3 prefix or list of S3 objects paths.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

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
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".txt"]).
        If None, will try to read all files. (default)
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.(e.g. ["_SUCCESS"]).
        If None, will try to read all files. (default)
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    ignore_empty: bool
        Ignore files with 0 bytes.
    use_threads : Union[bool, int]
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
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
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    pandas_kwargs:
        KEYWORD arguments forwarded to pandas.read_fwf(). You can NOT pass `pandas_kwargs` explicit, just add valid
        Pandas arguments in the function call and awswrangler will accept it.
        e.g. wr.s3.read_fwf(path='s3://bucket/prefix/', widths=[1, 3], names=["c0", "c1"])
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_fwf.html

    Returns
    -------
    Union[pandas.DataFrame, Generator[pandas.DataFrame, None, None]]
        Pandas DataFrame or a Generator in case of `chunksize != None`.

    Examples
    --------
    Reading all fixed-width formatted (FWF) files under a prefix

    >>> import awswrangler as wr
    >>> df = wr.s3.read_fwf(path='s3://bucket/prefix/', widths=[1, 3], names=['c0', 'c1'])

    Reading all fixed-width formatted (FWF) files from a list

    >>> import awswrangler as wr
    >>> df = wr.s3.read_fwf(path=['s3://bucket/0.txt', 's3://bucket/1.txt'], widths=[1, 3], names=['c0', 'c1'])

    Reading in chunks of 100 lines

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_fwf(
    ...     path=['s3://bucket/0.txt', 's3://bucket/1.txt'],
    ...     chunksize=100,
    ...     widths=[1, 3],
    ...     names=["c0", "c1"]
    ... )
    >>> for df in dfs:
    >>>     print(df)  # 100 lines Pandas DataFrame

    Reading FWF Dataset with PUSH-DOWN filter over partitions

    >>> import awswrangler as wr
    >>> my_filter = lambda x: True if x["city"].startswith("new") else False
    >>> df = wr.s3.read_fwf(path, dataset=True, partition_filter=my_filter, widths=[1, 3], names=["c0", "c1"])

    """
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.read_fwf(path, widths=[1, 3], names=['c0', 'c1'])"
        )
    return _read_text(
        parser_func=pd.read_fwf,
        path=path,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        version_id=version_id,
        ignore_empty=ignore_empty,
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
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    ignore_empty: bool = True,
    orient: str = "columns",
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    chunksize: Optional[int] = None,
    dataset: bool = False,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read JSON file(s) from a received S3 prefix or list of S3 objects paths.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

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
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".json"]).
        If None, will try to read all files. (default)
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.(e.g. ["_SUCCESS"]).
        If None, will try to read all files. (default)
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    ignore_empty: bool
        Ignore files with 0 bytes.
    orient : str
        Same as Pandas: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html
    use_threads : Union[bool, int]
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
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
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    pandas_kwargs:
        KEYWORD arguments forwarded to pandas.read_json(). You can NOT pass `pandas_kwargs` explicit, just add valid
        Pandas arguments in the function call and awswrangler will accept it.
        e.g. wr.s3.read_json('s3://bucket/prefix/', lines=True, keep_default_dates=True)
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

    Reading all CSV files under a prefix and using pandas_kwargs

    >>> import awswrangler as wr
    >>> df = wr.s3.read_json('s3://bucket/prefix/', lines=True, keep_default_dates=True)

    Reading all JSON files from a list

    >>> import awswrangler as wr
    >>> df = wr.s3.read_json(path=['s3://bucket/filename0.json', 's3://bucket/filename1.json'])

    Reading in chunks of 100 lines

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_json(path=['s3://bucket/0.json', 's3://bucket/1.json'], chunksize=100, lines=True)
    >>> for df in dfs:
    >>>     print(df)  # 100 lines Pandas DataFrame

    Reading JSON Dataset with PUSH-DOWN filter over partitions

    >>> import awswrangler as wr
    >>> my_filter = lambda x: True if x["city"].startswith("new") else False
    >>> df = wr.s3.read_json(path, dataset=True, partition_filter=my_filter)

    """
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.read_json(path, lines=True, keep_default_dates=True)"
        )
    if (dataset is True) and ("lines" not in pandas_kwargs):
        pandas_kwargs["lines"] = True
    pandas_kwargs["orient"] = orient
    ignore_index: bool = orient not in ("split", "index", "columns")
    return _read_text(
        parser_func=pd.read_json,
        path=path,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        version_id=version_id,
        ignore_empty=ignore_empty,
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
