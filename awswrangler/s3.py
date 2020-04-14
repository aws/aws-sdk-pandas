"""Amazon S3 Module."""

import concurrent.futures
import csv
import logging
import time
import uuid
from itertools import repeat
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import boto3  # type: ignore
import botocore.exceptions  # type: ignore
import pandas as pd  # type: ignore
import pandas.io.parsers  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.lib  # type: ignore
import pyarrow.parquet  # type: ignore
import s3fs  # type: ignore
from pandas.io.common import infer_compression  # type: ignore

from awswrangler import _data_types, _utils, catalog, exceptions

_COMPRESSION_2_EXT: Dict[Optional[str], str] = {None: "", "gzip": ".gz", "snappy": ".snappy"}

_logger: logging.Logger = logging.getLogger(__name__)


def get_bucket_region(bucket: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get bucket region name.

    Parameters
    ----------
    bucket : str
        Bucket name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Region code (e.g. 'us-east-1').

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> region = wr.s3.get_bucket_region('bucket-name')

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> region = wr.s3.get_bucket_region('bucket-name', boto3_session=boto3.Session())

    """
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    _logger.debug(f"bucket: {bucket}")
    region: str = client_s3.get_bucket_location(Bucket=bucket)["LocationConstraint"]
    region = "us-east-1" if region is None else region
    _logger.debug(f"region: {region}")
    return region


def does_object_exist(path: str, boto3_session: Optional[boto3.Session] = None) -> bool:
    """Check if object exists on S3.

    Parameters
    ----------
    path: str
        S3 path (e.g. s3://bucket/key).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    bool
        True if exists, False otherwise.

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> wr.s3.does_object_exist('s3://bucket/key_real')
    True
    >>> wr.s3.does_object_exist('s3://bucket/key_unreal')
    False

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> wr.s3.does_object_exist('s3://bucket/key_real', boto3_session=boto3.Session())
    True
    >>> wr.s3.does_object_exist('s3://bucket/key_unreal', boto3_session=boto3.Session())
    False

    """
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    bucket: str
    key: str
    bucket, key = path.replace("s3://", "").split("/", 1)
    try:
        client_s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as ex:
        if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise ex  # pragma: no cover


def list_objects(path: str, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    """List Amazon S3 objects from a prefix.

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/prefix).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        List of objects paths.

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> wr.s3.list_objects('s3://bucket/prefix')
    ['s3://bucket/prefix0', 's3://bucket/prefix1', 's3://bucket/prefix2']

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> wr.s3.list_objects('s3://bucket/prefix', boto3_session=boto3.Session())
    ['s3://bucket/prefix0', 's3://bucket/prefix1', 's3://bucket/prefix2']

    """
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    paginator = client_s3.get_paginator("list_objects_v2")
    bucket: str
    prefix: str
    bucket, prefix = _utils.parse_path(path=path)
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000})
    paths: List[str] = []
    for page in response_iterator:
        contents: Optional[List] = page.get("Contents")
        if contents is not None:
            for content in contents:
                if (content is not None) and ("Key" in content):
                    key: str = content["Key"]
                    paths.append(f"s3://{bucket}/{key}")
    return paths


def _path2list(path: Union[str, List[str]], boto3_session: Optional[boto3.Session]) -> List[str]:
    if isinstance(path, str):  # prefix
        paths: List[str] = list_objects(path=path, boto3_session=boto3_session)
    elif isinstance(path, list):
        paths = path
    else:
        raise exceptions.InvalidArgumentType(f"{type(path)} is not a valid path type. Please, use str or List[str].")
    return paths


def delete_objects(
    path: Union[str, List[str]], use_threads: bool = True, boto3_session: Optional[boto3.Session] = None
) -> None:
    """Delete Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.s3.delete_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Delete both objects
    >>> wr.s3.delete_objects('s3://bucket/prefix')  # Delete all objects under the received prefix

    """
    paths: List[str] = _path2list(path=path, boto3_session=boto3_session)
    if len(paths) < 1:
        return
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    buckets: Dict[str, List[str]] = _split_paths_by_bucket(paths=paths)
    for bucket, keys in buckets.items():
        chunks: List[List[str]] = _utils.chunkify(lst=keys, max_length=1_000)
        if use_threads is False:
            for chunk in chunks:
                _delete_objects(bucket=bucket, keys=chunk, client_s3=client_s3)
        else:
            cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
            with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
                executor.map(_delete_objects, repeat(bucket), chunks, repeat(client_s3))


def _split_paths_by_bucket(paths: List[str]) -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}
    bucket: str
    key: str
    for path in paths:
        bucket, key = _utils.parse_path(path=path)
        if bucket not in buckets:
            buckets[bucket] = []
        buckets[bucket].append(key)
    return buckets


def _delete_objects(bucket: str, keys: List[str], client_s3: boto3.client) -> None:
    _logger.debug(f"len(keys): {len(keys)}")
    batch: List[Dict[str, str]] = [{"Key": key} for key in keys]
    client_s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})


def describe_objects(
    path: Union[str, List[str]],
    wait_time: Optional[Union[int, float]] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Dict[str, Any]]:
    """Describe Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    Fetch attributes like ContentLength, DeleteMarker, LastModified, ContentType, etc
    The full list of attributes can be explored under the boto3 head_object documentation:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    wait_time : Union[int,float], optional
        How much time (seconds) should Wrangler try to reach this objects.
        Very useful to overcome eventual consistence issues.
        `None` means only a single try will be done.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Return a dictionary of objects returned from head_objects where the key is the object path.
        The response object can be explored here:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    Examples
    --------
    >>> import awswrangler as wr
    >>> descs0 = wr.s3.describe_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Describe both objects
    >>> descs1 = wr.s3.describe_objects('s3://bucket/prefix')  # Describe all objects under the prefix
    >>> descs2 = wr.s3.describe_objects('s3://bucket/prefix', wait_time=30)  # Overcoming eventual consistence issues

    """
    paths: List[str] = _path2list(path=path, boto3_session=boto3_session)
    if len(paths) < 1:
        return {}
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    resp_list: List[Tuple[str, Dict[str, Any]]]
    if use_threads is False:
        resp_list = [_describe_object(path=p, wait_time=wait_time, client_s3=client_s3) for p in paths]
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            resp_list = list(executor.map(_describe_object, paths, repeat(wait_time), repeat(client_s3)))
    desc_list: Dict[str, Dict[str, Any]] = dict(resp_list)
    return desc_list


def _describe_object(
    path: str, wait_time: Optional[Union[int, float]], client_s3: boto3.client
) -> Tuple[str, Dict[str, Any]]:
    wait_time = int(wait_time) if isinstance(wait_time, float) else wait_time
    tries: int = wait_time if (wait_time is not None) and (wait_time > 0) else 1
    bucket: str
    key: str
    bucket, key = _utils.parse_path(path=path)
    desc: Dict[str, Any] = {}
    for i in range(tries, 0, -1):
        try:
            desc = client_s3.head_object(Bucket=bucket, Key=key)
            break
        except botocore.exceptions.ClientError as e:  # pragma: no cover
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:  # Not Found
                _logger.debug(f"Object not found. {i} seconds remaining to wait.")
                if i == 1:  # Last try, there is no more need to sleep
                    break
                time.sleep(1)
            else:
                raise e
    return path, desc


def size_objects(
    path: Union[str, List[str]],
    wait_time: Optional[Union[int, float]] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Optional[int]]:
    """Get the size (ContentLength) in bytes of Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    wait_time : Union[int,float], optional
        How much time (seconds) should Wrangler try to reach this objects.
        Very useful to overcome eventual consistence issues.
        `None` means only a single try will be done.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Optional[int]]
        Dictionary where the key is the object path and the value is the object size.

    Examples
    --------
    >>> import awswrangler as wr
    >>> sizes0 = wr.s3.size_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Get the sizes of both objects
    >>> sizes1 = wr.s3.size_objects('s3://bucket/prefix')  # Get the sizes of all objects under the received prefix
    >>> sizes2 = wr.s3.size_objects('s3://bucket/prefix', wait_time=30)  # Overcoming eventual consistence issues

    """
    desc_list: Dict[str, Dict[str, Any]] = describe_objects(
        path=path, wait_time=wait_time, use_threads=use_threads, boto3_session=boto3_session
    )
    size_list: Dict[str, Optional[int]] = {k: d.get("ContentLength", None) for k, d in desc_list.items()}
    return size_list


def to_csv(  # pylint: disable=too-many-arguments
    df: pd.DataFrame,
    path: str,
    sep: str = ",",
    index: bool = True,
    columns: Optional[List[str]] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    dataset: bool = False,
    partition_cols: Optional[List[str]] = None,
    mode: Optional[str] = None,
    database: Optional[str] = None,
    table: Optional[str] = None,
    dtype: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    **pandas_kwargs,
) -> Dict[str, Union[List[str], Dict[str, List[str]]]]:
    """Write CSV file or dataset on Amazon S3.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning, casting and catalog integration (Amazon Athena/AWS Glue Catalog).

    Note
    ----
    The table name and all column names will be automatically sanitize using
    `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path : str
        Amazon S3 path (e.g. s3://bucket/filename.csv).
    sep : str
        String of length 1. Field delimiter for the output file.
    index : bool
        Write row names (index).
    columns : List[str], optional
        Columns to write.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    dataset: bool
        If True store a parquet dataset instead of a single file.
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, .
    partition_cols: List[str], optional
        List of column names that will be used to create partitions. Only takes effect if dataset=True.
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
    database : str
        Glue/Athena catalog: Database name.
    table : str
        Glue/Athena catalog: Table name.
    dtype: Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        Only takes effect if dataset=True.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    description: str, optional
        Glue/Athena catalog: Table description
    parameters: Dict[str, str], optional
        Glue/Athena catalog: Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Glue/Athena catalog:
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    pandas_kwargs:
        keyword arguments forwarded to pandas.DataFrame.to_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing single file

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.csv',
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.csv'],
        'partitions_values': {}
    }

    Writing single file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.csv',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
    ...     }
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.csv'],
        'partitions_values': {}
    }

    Writing partitioned dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2']
    ... )
    {
        'paths': ['s3://.../col2=A/x.csv', 's3://.../col2=B/y.csv'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing dataset to S3 with metadata on Athena/Glue Catalog.

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2'],
    ...     database='default',  # Athena/Glue database
    ...     table='my_table'  # Athena/Glue table
    ... )
    {
        'paths': ['s3://.../col2=A/x.csv', 's3://.../col2=B/y.csv'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing dataset casting empty column data type

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B'],
    ...         'col3': [None, None, None]
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     database='default',  # Athena/Glue database
    ...     table='my_table'  # Athena/Glue table
    ...     dtype={'col3': 'date'}
    ... )
    {
        'paths': ['s3://.../x.csv'],
        'partitions_values: {}
    }

    """
    if (database is None) ^ (table is None):
        raise exceptions.InvalidArgumentCombination(
            "Please pass database and table arguments to be able to store the metadata into the Athena/Glue Catalog."
        )
    if df.empty is True:
        raise exceptions.EmptyDataFrame()
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    partition_cols = partition_cols if partition_cols else []
    dtype = dtype if dtype else {}
    columns_comments = columns_comments if columns_comments else {}
    partitions_values: Dict[str, List[str]] = {}
    fs: s3fs.S3FileSystem = _utils.get_fs(session=session, s3_additional_kwargs=s3_additional_kwargs)
    if dataset is False:
        if partition_cols:
            raise exceptions.InvalidArgumentCombination("Please, pass dataset=True to be able to use partition_cols.")
        if mode is not None:
            raise exceptions.InvalidArgumentCombination("Please pass dataset=True to be able to use mode.")
        if any(arg is not None for arg in (database, table, description, parameters)):
            raise exceptions.InvalidArgumentCombination(
                "Please pass dataset=True to be able to use any one of these "
                "arguments: database, table, description, parameters, "
                "columns_comments."
            )
        pandas_kwargs["sep"] = sep
        pandas_kwargs["index"] = index
        pandas_kwargs["columns"] = columns
        _to_text(file_format="csv", df=df, path=path, fs=fs, **pandas_kwargs)
        paths = [path]
    else:
        mode = "append" if mode is None else mode
        exist: bool = False
        if columns:
            df = df[columns]
        if (database is not None) and (table is not None):  # Normalize table to respect Athena's standards
            df = catalog.sanitize_dataframe_columns_names(df=df)
            partition_cols = [catalog.sanitize_column_name(p) for p in partition_cols]
            dtype = {catalog.sanitize_column_name(k): v.lower() for k, v in dtype.items()}
            columns_comments = {catalog.sanitize_column_name(k): v for k, v in columns_comments.items()}
            exist = catalog.does_table_exist(database=database, table=table, boto3_session=session)
            if (exist is True) and (mode in ("append", "overwrite_partitions")):
                for k, v in catalog.get_table_types(database=database, table=table, boto3_session=session).items():
                    dtype[k] = v
        df = catalog.drop_duplicated_columns(df=df)
        paths, partitions_values = _to_csv_dataset(
            df=df,
            path=path,
            index=index,
            sep=sep,
            fs=fs,
            use_threads=use_threads,
            partition_cols=partition_cols,
            dtype=dtype,
            mode=mode,
            boto3_session=session,
        )
        if (database is not None) and (table is not None):
            columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
                df=df, index=index, partition_cols=partition_cols, dtype=dtype, index_left=True
            )
            if (exist is False) or (mode == "overwrite"):
                catalog.create_csv_table(
                    database=database,
                    table=table,
                    path=path,
                    columns_types=columns_types,
                    partitions_types=partitions_types,
                    description=description,
                    parameters=parameters,
                    columns_comments=columns_comments,
                    boto3_session=session,
                    mode="overwrite",
                    sep=sep,
                )
            if partitions_values:
                _logger.debug(f"partitions_values:\n{partitions_values}")
                catalog.add_csv_partitions(
                    database=database, table=table, partitions_values=partitions_values, boto3_session=session, sep=sep
                )
    return {"paths": paths, "partitions_values": partitions_values}


def _to_csv_dataset(
    df: pd.DataFrame,
    path: str,
    index: bool,
    sep: str,
    fs: s3fs.S3FileSystem,
    use_threads: bool,
    mode: str,
    dtype: Dict[str, str],
    partition_cols: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Tuple[List[str], Dict[str, List[str]]]:
    paths: List[str] = []
    partitions_values: Dict[str, List[str]] = {}
    path = path if path[-1] == "/" else f"{path}/"
    if mode not in ["append", "overwrite", "overwrite_partitions"]:
        raise exceptions.InvalidArgumentValue(
            f"{mode} is a invalid mode, please use append, overwrite or overwrite_partitions."
        )
    if (mode == "overwrite") or ((mode == "overwrite_partitions") and (not partition_cols)):
        delete_objects(path=path, use_threads=use_threads, boto3_session=boto3_session)
    df = _data_types.cast_pandas_with_athena_types(df=df, dtype=dtype)
    _logger.debug(f"dtypes: {df.dtypes}")
    if not partition_cols:
        file_path: str = f"{path}{uuid.uuid4().hex}.csv"
        _to_text(
            file_format="csv",
            df=df,
            path=file_path,
            fs=fs,
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            header=False,
            date_format="%Y-%m-%d %H:%M:%S.%f",
            index=index,
            sep=sep,
        )
        paths.append(file_path)
    else:
        for keys, subgroup in df.groupby(by=partition_cols, observed=True):
            subgroup = subgroup.drop(partition_cols, axis="columns")
            keys = (keys,) if not isinstance(keys, tuple) else keys
            subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
            prefix: str = f"{path}{subdir}/"
            if mode == "overwrite_partitions":
                delete_objects(path=prefix, use_threads=use_threads, boto3_session=boto3_session)
            file_path = f"{prefix}{uuid.uuid4().hex}.csv"
            _to_text(
                file_format="csv",
                df=subgroup,
                path=file_path,
                fs=fs,
                quoting=csv.QUOTE_NONE,
                escapechar="\\",
                header=False,
                date_format="%Y-%m-%d %H:%M:%S.%f",
                index=index,
                sep=sep,
            )
            paths.append(file_path)
            partitions_values[prefix] = [str(k) for k in keys]
    return paths, partitions_values


def to_json(
    df: pd.DataFrame,
    path: str,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    **pandas_kwargs,
) -> None:
    """Write JSON file on Amazon S3.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path : str
        Amazon S3 path (e.g. s3://bucket/filename.csv).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    pandas_kwargs:
        keyword arguments forwarded to pandas.DataFrame.to_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing JSON file

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/filename.json',
    ... )

    Writing CSV file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/filename.json',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
    ...     }
    ... )

    """
    return _to_text(
        file_format="json",
        df=df,
        path=path,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        **pandas_kwargs,
    )


def _to_text(
    file_format: str,
    df: pd.DataFrame,
    path: str,
    fs: Optional[s3fs.S3FileSystem] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    **pandas_kwargs,
) -> None:
    if df.empty is True:  # pragma: no cover
        raise exceptions.EmptyDataFrame()
    if fs is None:
        fs = _utils.get_fs(session=boto3_session, s3_additional_kwargs=s3_additional_kwargs)
    with fs.open(path, "w") as f:
        if file_format == "csv":
            df.to_csv(f, **pandas_kwargs)
        elif file_format == "json":
            df.to_json(f, **pandas_kwargs)


def to_parquet(  # pylint: disable=too-many-arguments
    df: pd.DataFrame,
    path: str,
    index: bool = False,
    compression: Optional[str] = "snappy",
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    dataset: bool = False,
    partition_cols: Optional[List[str]] = None,
    mode: Optional[str] = None,
    database: Optional[str] = None,
    table: Optional[str] = None,
    dtype: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
) -> Dict[str, Union[List[str], Dict[str, List[str]]]]:
    """Write Parquet file or dataset on Amazon S3.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning, casting and catalog integration (Amazon Athena/AWS Glue Catalog).

    Note
    ----
    The table name and all column names will be automatically sanitize using
    `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path : str
        S3 path (for file e.g. ``s3://bucket/prefix/filename.parquet``) (for dataset e.g. ``s3://bucket/prefix``).
    index : bool
        True to store the DataFrame index in file, otherwise False to ignore it.
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    dataset: bool
        If True store a parquet dataset instead of a single file.
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, .
    partition_cols: List[str], optional
        List of column names that will be used to create partitions. Only takes effect if dataset=True.
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
    database : str
        Glue/Athena catalog: Database name.
    table : str
        Glue/Athena catalog: Table name.
    dtype: Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        Only takes effect if dataset=True.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    description: str, optional
        Glue/Athena catalog: Table description
    parameters: Dict[str, str], optional
        Glue/Athena catalog: Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Glue/Athena catalog:
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).

    Returns
    -------
    Dict[str, Union[List[str], Dict[str, List[str]]]]
        Dictionary with:
        'paths': List of all stored files paths on S3.
        'partitions_values': Dictionary of partitions added with keys as S3 path locations
        and values as a list of partitions values as str.

    Examples
    --------
    Writing single file

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.parquet',
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.parquet'],
        'partitions_values': {}
    }

    Writing single file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.parquet',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
    ...     }
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.parquet'],
        'partitions_values': {}
    }

    Writing partitioned dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2']
    ... )
    {
        'paths': ['s3://.../col2=A/x.parquet', 's3://.../col2=B/y.parquet'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing dataset to S3 with metadata on Athena/Glue Catalog.

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2'],
    ...     database='default',  # Athena/Glue database
    ...     table='my_table'  # Athena/Glue table
    ... )
    {
        'paths': ['s3://.../col2=A/x.parquet', 's3://.../col2=B/y.parquet'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing dataset casting empty column data type

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B'],
    ...         'col3': [None, None, None]
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     database='default',  # Athena/Glue database
    ...     table='my_table'  # Athena/Glue table
    ...     dtype={'col3': 'date'}
    ... )
    {
        'paths': ['s3://.../x.parquet'],
        'partitions_values: {}
    }

    """
    if (database is None) ^ (table is None):
        raise exceptions.InvalidArgumentCombination(
            "Please pass database and table arguments to be able to store the metadata into the Athena/Glue Catalog."
        )
    if df.empty is True:
        raise exceptions.EmptyDataFrame()
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    partition_cols = partition_cols if partition_cols else []
    dtype = dtype if dtype else {}
    columns_comments = columns_comments if columns_comments else {}
    partitions_values: Dict[str, List[str]] = {}
    cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
    fs: s3fs.S3FileSystem = _utils.get_fs(session=session, s3_additional_kwargs=s3_additional_kwargs)
    compression_ext: Optional[str] = _COMPRESSION_2_EXT.get(compression, None)
    if compression_ext is None:
        raise exceptions.InvalidCompression(f"{compression} is invalid, please use None, snappy or gzip.")
    if dataset is False:
        if partition_cols:
            raise exceptions.InvalidArgumentCombination("Please, pass dataset=True to be able to use partition_cols.")
        if mode is not None:
            raise exceptions.InvalidArgumentCombination("Please pass dataset=True to be able to use mode.")
        if any(arg is not None for arg in (database, table, description, parameters)):
            raise exceptions.InvalidArgumentCombination(
                "Please pass dataset=True to be able to use any one of these "
                "arguments: database, table, description, parameters, "
                "columns_comments."
            )
        paths = [
            _to_parquet_file(
                df=df, path=path, schema=None, index=index, compression=compression, cpus=cpus, fs=fs, dtype={}
            )
        ]
    else:
        mode = "append" if mode is None else mode
        exist: bool = False
        if (database is not None) and (table is not None):  # Normalize table to respect Athena's standards
            df = catalog.sanitize_dataframe_columns_names(df=df)
            partition_cols = [catalog.sanitize_column_name(p) for p in partition_cols]
            dtype = {catalog.sanitize_column_name(k): v.lower() for k, v in dtype.items()}
            columns_comments = {catalog.sanitize_column_name(k): v for k, v in columns_comments.items()}
            exist = catalog.does_table_exist(database=database, table=table, boto3_session=session)
            if (exist is True) and (mode in ("append", "overwrite_partitions")):
                for k, v in catalog.get_table_types(database=database, table=table, boto3_session=session).items():
                    dtype[k] = v
        df = catalog.drop_duplicated_columns(df=df)
        paths, partitions_values = _to_parquet_dataset(
            df=df,
            path=path,
            index=index,
            compression=compression,
            compression_ext=compression_ext,
            cpus=cpus,
            fs=fs,
            use_threads=use_threads,
            partition_cols=partition_cols,
            dtype=dtype,
            mode=mode,
            boto3_session=session,
        )
        if (database is not None) and (table is not None):
            columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
                df=df, index=index, partition_cols=partition_cols, dtype=dtype
            )
            if (exist is False) or (mode == "overwrite"):
                catalog.create_parquet_table(
                    database=database,
                    table=table,
                    path=path,
                    columns_types=columns_types,
                    partitions_types=partitions_types,
                    compression=compression,
                    description=description,
                    parameters=parameters,
                    columns_comments=columns_comments,
                    boto3_session=session,
                    mode="overwrite",
                )
            if partitions_values:
                _logger.debug(f"partitions_values:\n{partitions_values}")
                catalog.add_parquet_partitions(
                    database=database,
                    table=table,
                    partitions_values=partitions_values,
                    compression=compression,
                    boto3_session=session,
                )
    return {"paths": paths, "partitions_values": partitions_values}


def _to_parquet_dataset(
    df: pd.DataFrame,
    path: str,
    index: bool,
    compression: Optional[str],
    compression_ext: str,
    cpus: int,
    fs: s3fs.S3FileSystem,
    use_threads: bool,
    mode: str,
    dtype: Dict[str, str],
    partition_cols: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Tuple[List[str], Dict[str, List[str]]]:
    paths: List[str] = []
    partitions_values: Dict[str, List[str]] = {}
    path = path if path[-1] == "/" else f"{path}/"
    if mode not in ["append", "overwrite", "overwrite_partitions"]:
        raise exceptions.InvalidArgumentValue(
            f"{mode} is a invalid mode, please use append, overwrite or overwrite_partitions."
        )
    if (mode == "overwrite") or ((mode == "overwrite_partitions") and (not partition_cols)):
        delete_objects(path=path, use_threads=use_threads, boto3_session=boto3_session)
    df = _data_types.cast_pandas_with_athena_types(df=df, dtype=dtype)
    schema: pa.Schema = _data_types.pyarrow_schema_from_pandas(
        df=df, index=index, ignore_cols=partition_cols, dtype=dtype
    )
    _logger.debug(f"schema: {schema}")
    if not partition_cols:
        file_path: str = f"{path}{uuid.uuid4().hex}{compression_ext}.parquet"
        _to_parquet_file(
            df=df, schema=schema, path=file_path, index=index, compression=compression, cpus=cpus, fs=fs, dtype=dtype
        )
        paths.append(file_path)
    else:
        for keys, subgroup in df.groupby(by=partition_cols, observed=True):
            subgroup = subgroup.drop(partition_cols, axis="columns")
            keys = (keys,) if not isinstance(keys, tuple) else keys
            subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
            prefix: str = f"{path}{subdir}/"
            if mode == "overwrite_partitions":
                delete_objects(path=prefix, use_threads=use_threads, boto3_session=boto3_session)
            file_path = f"{prefix}{uuid.uuid4().hex}{compression_ext}.parquet"
            _to_parquet_file(
                df=subgroup,
                schema=schema,
                path=file_path,
                index=index,
                compression=compression,
                cpus=cpus,
                fs=fs,
                dtype=dtype,
            )
            paths.append(file_path)
            partitions_values[prefix] = [str(k) for k in keys]
    return paths, partitions_values


def _to_parquet_file(
    df: pd.DataFrame,
    path: str,
    schema: pa.Schema,
    index: bool,
    compression: Optional[str],
    cpus: int,
    fs: s3fs.S3FileSystem,
    dtype: Dict[str, str],
) -> str:
    table: pa.Table = pyarrow.Table.from_pandas(df=df, schema=schema, nthreads=cpus, preserve_index=index, safe=True)
    for col_name, col_type in dtype.items():
        if col_name in table.column_names:
            col_index = table.column_names.index(col_name)
            pyarrow_dtype = _data_types.athena2pyarrow(col_type)
            field = pa.field(name=col_name, type=pyarrow_dtype)
            table = table.set_column(col_index, field, table.column(col_name).cast(pyarrow_dtype))
            _logger.debug(f"Casting column {col_name} ({col_index}) to {col_type} ({pyarrow_dtype})")
    pyarrow.parquet.write_table(
        table=table,
        where=path,
        write_statistics=True,
        use_dictionary=True,
        filesystem=fs,
        coerce_timestamps="ms",
        compression=compression,
        flavor="spark",
    )
    return path


def read_csv(
    path: Union[str, List[str]],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read CSV file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

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

    """
    return _read_text(
        parser_func=pd.read_csv,
        path=path,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        **pandas_kwargs,
    )


def read_fwf(
    path: Union[str, List[str]],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read fixed-width formatted file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

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
        **pandas_kwargs,
    )


def read_json(
    path: Union[str, List[str]],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read JSON file(s) from from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    For partial and gradual reading use the argument ``chunksize`` instead of ``iterator``.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

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
    return _read_text(
        parser_func=pd.read_json,
        path=path,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        chunksize=chunksize,
        **pandas_kwargs,
    )


def _read_text(
    parser_func: Callable,
    path: Union[str, List[str]],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    chunksize: Optional[int] = None,
    **pandas_kwargs,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    if "iterator" in pandas_kwargs:
        raise exceptions.InvalidArgument("Please, use chunksize instead of iterator.")
    paths: List[str] = _path2list(path=path, boto3_session=boto3_session)
    if chunksize is not None:
        dfs: Iterator[pd.DataFrame] = _read_text_chunksize(
            parser_func=parser_func,
            paths=paths,
            boto3_session=boto3_session,
            chunksize=chunksize,
            pandas_args=pandas_kwargs,
            s3_additional_kwargs=s3_additional_kwargs,
        )
        return dfs
    if use_threads is False:
        df: pd.DataFrame = pd.concat(
            objs=[
                _read_text_full(
                    parser_func=parser_func,
                    path=p,
                    boto3_session=boto3_session,
                    pandas_args=pandas_kwargs,
                    s3_additional_kwargs=s3_additional_kwargs,
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
                    repeat(parser_func),
                    paths,
                    repeat(boto3_session),
                    repeat(pandas_kwargs),
                    repeat(s3_additional_kwargs),
                ),
                ignore_index=True,
                sort=False,
            )
    return df


def _read_text_chunksize(
    parser_func: Callable,
    paths: List[str],
    boto3_session: boto3.Session,
    chunksize: int,
    pandas_args: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> Iterator[pd.DataFrame]:
    fs: s3fs.S3FileSystem = _utils.get_fs(session=boto3_session, s3_additional_kwargs=s3_additional_kwargs)
    for path in paths:
        _logger.debug(f"path: {path}")
        if pandas_args.get("compression", "infer") == "infer":
            pandas_args["compression"] = infer_compression(path, compression="infer")
        with fs.open(path, "rb") as f:
            reader: pandas.io.parsers.TextFileReader = parser_func(f, chunksize=chunksize, **pandas_args)
            for df in reader:
                yield df


def _read_text_full(
    parser_func: Callable,
    path: str,
    boto3_session: boto3.Session,
    pandas_args: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    fs: s3fs.S3FileSystem = _utils.get_fs(session=boto3_session, s3_additional_kwargs=s3_additional_kwargs)
    if pandas_args.get("compression", "infer") == "infer":
        pandas_args["compression"] = infer_compression(path, compression="infer")
    with fs.open(path, "rb") as f:
        return parser_func(f, **pandas_args)


def _read_parquet_init(
    path: Union[str, List[str]],
    filters: Optional[Union[List[Tuple], List[List[Tuple]]]] = None,
    categories: List[str] = None,
    validate_schema: bool = True,
    dataset: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> pyarrow.parquet.ParquetDataset:
    """Encapsulate all initialization before the use of the pyarrow.parquet.ParquetDataset."""
    if dataset is False:
        path_or_paths: Union[str, List[str]] = _path2list(path=path, boto3_session=boto3_session)
    elif isinstance(path, str):
        path_or_paths = path[:-1] if path.endswith("/") else path
    else:
        path_or_paths = path
    _logger.debug(f"path_or_paths: {path_or_paths}")
    fs: s3fs.S3FileSystem = _utils.get_fs(session=boto3_session, s3_additional_kwargs=s3_additional_kwargs)
    cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
    data: pyarrow.parquet.ParquetDataset = pyarrow.parquet.ParquetDataset(
        path_or_paths=path_or_paths,
        filesystem=fs,
        metadata_nthreads=cpus,
        filters=filters,
        read_dictionary=categories,
        validate_schema=validate_schema,
    )
    return data


def read_parquet(
    path: Union[str, List[str]],
    filters: Optional[Union[List[Tuple], List[List[Tuple]]]] = None,
    columns: Optional[List[str]] = None,
    validate_schema: bool = True,
    chunked: bool = False,
    dataset: bool = False,
    categories: List[str] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read Apache Parquet file(s) from from a received S3 prefix or list of S3 objects paths.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    filters: Union[List[Tuple], List[List[Tuple]]], optional
        List of filters to apply, like ``[[('x', '=', 0), ...], ...]``.
    columns : List[str], optional
        Names of columns to read from the file(s).
    validate_schema:
        Check that individual file schemas are all the same / compatible. Schemas within a
        folder prefix should all be the same. Disable if you have schemas that are different
        and want to disable this check.
    chunked : bool
        If True will break the data in smaller DataFrames (Non deterministic number of lines).
        Otherwise return a single DataFrame with the whole data.
    dataset: bool
        If True read a parquet dataset instead of simple file(s) loading all the related partitions as columns.
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

    Reading in chunks

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_parquet(path=['s3://bucket/filename0.csv', 's3://bucket/filename1.csv'], chunked=True)
    >>> for df in dfs:
    >>>     print(df)  # Smaller Pandas DataFrame

    """
    data: pyarrow.parquet.ParquetDataset = _read_parquet_init(
        path=path,
        filters=filters,
        dataset=dataset,
        categories=categories,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        validate_schema=validate_schema,
    )
    if chunked is False:
        return _read_parquet(
            data=data, columns=columns, categories=categories, use_threads=use_threads, validate_schema=validate_schema
        )
    return _read_parquet_chunked(data=data, columns=columns, categories=categories, use_threads=use_threads)


def _read_parquet(
    data: pyarrow.parquet.ParquetDataset,
    columns: Optional[List[str]] = None,
    categories: List[str] = None,
    use_threads: bool = True,
    validate_schema: bool = True,
) -> pd.DataFrame:
    tables: List[pa.Table] = []
    for piece in data.pieces:
        table: pa.Table = piece.read(
            columns=columns, use_threads=use_threads, partitions=data.partitions, use_pandas_metadata=False
        )
        tables.append(table)
    promote: bool = not validate_schema
    table = pa.lib.concat_tables(tables, promote=promote)
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
    use_threads: bool = True,
) -> Iterator[pd.DataFrame]:
    for piece in data.pieces:
        table: pa.Table = piece.read(
            columns=columns, use_threads=use_threads, partitions=data.partitions, use_pandas_metadata=False
        )
        yield table.to_pandas(
            use_threads=use_threads,
            split_blocks=True,
            self_destruct=True,
            integer_object_nulls=False,
            date_as_object=True,
            ignore_metadata=True,
            categories=categories,
            types_mapper=_data_types.pyarrow2pandas_extension,
        )


def read_parquet_metadata(
    path: Union[str, List[str]],
    filters: Optional[Union[List[Tuple], List[List[Tuple]]]] = None,
    dataset: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]]]:
    """Read Apache Parquet file(s) metadata from from a received S3 prefix or list of S3 objects paths.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    filters: Union[List[Tuple], List[List[Tuple]]], optional
        List of filters to apply, like ``[[('x', '=', 0), ...], ...]``.
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
    data: pyarrow.parquet.ParquetDataset = _read_parquet_init(
        path=path, filters=filters, dataset=dataset, use_threads=use_threads, boto3_session=boto3_session
    )
    return _data_types.athena_types_from_pyarrow_schema(
        schema=data.schema.to_arrow_schema(), partitions=data.partitions
    )


def store_parquet_metadata(
    path: str,
    database: str,
    table: str,
    filters: Optional[Union[List[Tuple], List[List[Tuple]]]] = None,
    dataset: bool = False,
    use_threads: bool = True,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    compression: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Infer and store parquet metadata on AWS Glue Catalog.

    Infer Apache Parquet file(s) metadata from from a received S3 prefix or list of S3 objects paths
    And then stores it on AWS Glue Catalog including all inferred partitions
    (No need of 'MCSK REPAIR TABLE')

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    database : str
        Glue/Athena catalog: Database name.
    table : str
        Glue/Athena catalog: Table name.
    filters: Union[List[Tuple], List[List[Tuple]]], optional
        List of filters to apply, like ``[[('x', '=', 0), ...], ...]``.
    dataset: bool
        If True read a parquet dataset instead of simple file(s) loading all the related partitions as columns.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    description: str, optional
        Glue/Athena catalog: Table description
    parameters: Dict[str, str], optional
        Glue/Athena catalog: Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Glue/Athena catalog:
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]
        The metadata used to create the Glue Table.
        columns_types: Dictionary with keys as column names and vales as
        data types (e.g. {'col0': 'bigint', 'col1': 'double'}). /
        partitions_types: Dictionary with keys as partition names
        and values as data types (e.g. {'col2': 'date'}). /
        partitions_values: Dictionary with keys as S3 path locations and values as a
        list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

    Examples
    --------
    Reading all Parquet files metadata under a prefix

    >>> import awswrangler as wr
    >>> columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
    ...     path='s3://bucket/prefix/',
    ...     database='...',
    ...     table='...',
    ...     dataset=True
    ... )

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    data: pyarrow.parquet.ParquetDataset = _read_parquet_init(
        path=path, filters=filters, dataset=dataset, use_threads=use_threads, boto3_session=session
    )
    partitions: Optional[pyarrow.parquet.ParquetPartitions] = data.partitions
    columns_types, partitions_types = _data_types.athena_types_from_pyarrow_schema(
        schema=data.schema.to_arrow_schema(), partitions=partitions
    )
    catalog.create_parquet_table(
        database=database,
        table=table,
        path=path,
        columns_types=columns_types,
        partitions_types=partitions_types,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        boto3_session=session,
    )
    partitions_values: Dict[str, List[str]] = _data_types.athena_partitions_from_pyarrow_partitions(
        path=path, partitions=partitions
    )
    catalog.add_parquet_partitions(
        database=database,
        table=table,
        partitions_values=partitions_values,
        compression=compression,
        boto3_session=session,
    )
    return columns_types, partitions_types, partitions_values


def wait_objects_exist(
    paths: List[str],
    delay: Optional[Union[int, float]] = None,
    max_attempts: Optional[int] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Wait Amazon S3 objects exist.

    Polls S3.Client.head_object() every 5 seconds (default) until a successful
    state is reached. An error is returned after 20 (default) failed checks.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Waiter.ObjectExists

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    paths : List[str]
        List of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    delay : Union[int,float], optional
        The amount of time in seconds to wait between attempts. Default: 5
    max_attempts : int, optional
        The maximum number of attempts to be made. Default: 20
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.s3.wait_objects_exist(['s3://bucket/key0', 's3://bucket/key1'])  # wait both objects

    """
    return _wait_objects(
        waiter_name="object_exists",
        paths=paths,
        delay=delay,
        max_attempts=max_attempts,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )


def wait_objects_not_exist(
    paths: List[str],
    delay: Optional[Union[int, float]] = None,
    max_attempts: Optional[int] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Wait Amazon S3 objects not exist.

    Polls S3.Client.head_object() every 5 seconds (default) until a successful
    state is reached. An error is returned after 20 (default) failed checks.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Waiter.ObjectNotExists

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    paths : List[str]
        List of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    delay : Union[int,float], optional
        The amount of time in seconds to wait between attempts. Default: 5
    max_attempts : int, optional
        The maximum number of attempts to be made. Default: 20
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.s3.wait_objects_not_exist(['s3://bucket/key0', 's3://bucket/key1'])  # wait both objects not exist

    """
    return _wait_objects(
        waiter_name="object_not_exists",
        paths=paths,
        delay=delay,
        max_attempts=max_attempts,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )


def _wait_objects(
    waiter_name: str,
    paths: List[str],
    delay: Optional[Union[int, float]] = None,
    max_attempts: Optional[int] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    delay = 5 if delay is None else delay
    max_attempts = 20 if max_attempts is None else max_attempts
    _delay: int = int(delay) if isinstance(delay, float) else delay

    if len(paths) < 1:
        return None
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    waiter = client_s3.get_waiter(waiter_name)
    _paths: List[Tuple[str, str]] = [_utils.parse_path(path=p) for p in paths]
    if use_threads is False:
        for bucket, key in _paths:
            waiter.wait(Bucket=bucket, Key=key, WaiterConfig={"Delay": _delay, "MaxAttempts": max_attempts})
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            futures: List[concurrent.futures.Future] = []
            for bucket, key in _paths:
                future: concurrent.futures.Future = executor.submit(
                    fn=waiter.wait, Bucket=bucket, Key=key, WaiterConfig={"Delay": _delay, "MaxAttempts": max_attempts}
                )
                futures.append(future)
            for future in futures:
                future.result()
    return None


def read_parquet_table(
    table: str,
    database: str,
    filters: Optional[Union[List[Tuple], List[List[Tuple]]]] = None,
    columns: Optional[List[str]] = None,
    categories: List[str] = None,
    chunked: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read Apache Parquet table registered on AWS Glue Catalog.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

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

    Reading Parquet Table in chunks

    >>> import awswrangler as wr
    >>> dfs = wr.s3.read_parquet_table(database='...', table='...', chunked=True)
    >>> for df in dfs:
    >>>     print(df)  # Smaller Pandas DataFrame

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
