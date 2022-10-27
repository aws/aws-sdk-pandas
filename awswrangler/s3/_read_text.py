"""Amazon S3 Read Module (PRIVATE)."""
import datetime
import itertools
import logging
import pprint
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import boto3
import pandas as pd

from awswrangler import _utils, exceptions
from awswrangler._distributed import engine
from awswrangler._threading import _get_executor
from awswrangler.s3._list import _path2list
from awswrangler.s3._read import _apply_partition_filter, _get_path_ignore_suffix, _get_path_root, _union
from awswrangler.s3._read_text_core import _read_text_file, _read_text_files_chunked

_logger: logging.Logger = logging.getLogger(__name__)


def _resolve_format(read_format: str) -> Any:
    if read_format == "csv":
        return pd.read_csv
    if read_format == "fwf":
        return pd.read_fwf
    if read_format == "json":
        return pd.read_json
    raise exceptions.UnsupportedType("Unsupported read format")


def _get_version_id_for(version_id: Optional[Union[str, Dict[str, str]]], path: str) -> Optional[str]:
    if isinstance(version_id, dict):
        return version_id.get(path, None)

    return version_id


@engine.dispatch_on_engine
def _read_text(  # pylint: disable=W0613
    read_format: str,
    paths: List[str],
    path_root: Optional[str],
    use_threads: Union[bool, int],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
    ignore_index: bool,
    parallelism: int,
    version_id_dict: Dict[str, Optional[str]],
    pandas_kwargs: Dict[str, Any],
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    parser_func = _resolve_format(read_format)
    executor = _get_executor(use_threads=use_threads)
    tables = executor.map(
        _read_text_file,
        boto3_session,
        paths,
        [version_id_dict[path] for path in paths],
        itertools.repeat(parser_func),
        itertools.repeat(path_root),
        itertools.repeat(pandas_kwargs),
        itertools.repeat(s3_additional_kwargs),
        itertools.repeat(dataset),
    )
    return _union(dfs=tables, ignore_index=ignore_index)


def _read_text_format(
    read_format: str,
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
    parallelism: int,
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

    if len(paths) > 1 and version_id is not None and not isinstance(version_id, dict):
        raise exceptions.InvalidArgumentCombination(
            "If multiple paths are provided along with a file version ID, the version ID parameter must be a dict."
        )
    version_id_dict = {path: _get_version_id_for(version_id, path) for path in paths}

    args: Dict[str, Any] = {
        "parser_func": _resolve_format(read_format),
        "boto3_session": boto3_session,
        "dataset": dataset,
        "path_root": path_root,
        "pandas_kwargs": pandas_kwargs,
        "s3_additional_kwargs": s3_additional_kwargs,
        "use_threads": use_threads,
    }
    _logger.debug("args:\n%s", pprint.pformat(args))

    if chunksize is not None:
        return _read_text_files_chunked(
            paths=paths,
            version_ids=version_id_dict,
            chunksize=chunksize,
            **args,
        )

    return _read_text(
        read_format,
        paths=paths,
        path_root=path_root,
        use_threads=use_threads,
        boto3_session=session,
        s3_additional_kwargs=s3_additional_kwargs,
        dataset=dataset,
        ignore_index=ignore_index,
        parallelism=parallelism,
        version_id_dict=version_id_dict,
        pandas_kwargs=pandas_kwargs,
    )


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
    parallelism: int = 200,
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
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0rc1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    parallelism : int, optional
        The requested parallelism of the read. Only used when `distributed` add-on is installed.
        Parallelism may be limited by the number of files of the dataset. 200 by default.
    pandas_kwargs :
        KEYWORD arguments forwarded to pandas.read_csv(). You can NOT pass `pandas_kwargs` explicit, just add valid
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
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.read_csv('s3://bucket/prefix/', sep='|', skip_blank_lines=True)"
        )
    ignore_index: bool = "index_col" not in pandas_kwargs
    return _read_text_format(
        read_format="csv",
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
        parallelism=parallelism,
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
    parallelism: int = 200,
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
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0rc1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    parallelism : int, optional
        The requested parallelism of the read. Only used when `distributed` add-on is installed.
        Parallelism may be limited by the number of files of the dataset. 200 by default.
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
    return _read_text_format(
        read_format="fwf",
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
        parallelism=parallelism,
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
    parallelism: int = 200,
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
        https://aws-sdk-pandas.readthedocs.io/en/3.0.0rc1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
    parallelism : int, optional
        The requested parallelism of the read. Only used when `distributed` add-on is installed.
        Parallelism may be limited by the number of files of the dataset. 200 by default.
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
    return _read_text_format(
        read_format="json",
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
        parallelism=parallelism,
        **pandas_kwargs,
    )
