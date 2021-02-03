"""Amazon S3 Write Dataset (PRIVATE)."""

import logging
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import boto3
import numpy as np
import pandas as pd

from awswrangler import exceptions
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._write_concurrent import _WriteProxy

_logger: logging.Logger = logging.getLogger(__name__)


def _to_partitions(
    func: Callable[..., List[str]],
    concurrent_partitioning: bool,
    df: pd.DataFrame,
    path_root: str,
    use_threads: bool,
    mode: str,
    partition_cols: List[str],
    bucketing_info: Optional[Tuple[List[str], int]],
    boto3_session: boto3.Session,
    **func_kwargs: Any,
) -> Tuple[List[str], Dict[str, List[str]]]:
    partitions_values: Dict[str, List[str]] = {}
    proxy: _WriteProxy = _WriteProxy(use_threads=concurrent_partitioning)
    filename_prefix = uuid.uuid4().hex

    for keys, subgroup in df.groupby(by=partition_cols, observed=True):
        subgroup = subgroup.drop(partition_cols, axis="columns")
        keys = (keys,) if not isinstance(keys, tuple) else keys
        subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
        prefix: str = f"{path_root}{subdir}/"
        if mode == "overwrite_partitions":
            delete_objects(
                path=prefix,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=func_kwargs.get("s3_additional_kwargs"),
            )
        if bucketing_info:
            _to_buckets(
                func=func,
                df=subgroup,
                path_root=prefix,
                bucketing_info=bucketing_info,
                boto3_session=boto3_session,
                use_threads=use_threads,
                proxy=proxy,
                filename_prefix=filename_prefix,
                **func_kwargs,
            )
        else:
            proxy.write(
                func=func,
                df=subgroup,
                path_root=prefix,
                boto3_session=boto3_session,
                use_threads=use_threads,
                **func_kwargs,
            )
        partitions_values[prefix] = [str(k) for k in keys]
    paths: List[str] = proxy.close()  # blocking
    return paths, partitions_values


def _to_buckets(
    func: Callable[..., List[str]],
    df: pd.DataFrame,
    path_root: str,
    bucketing_info: Tuple[List[str], int],
    boto3_session: boto3.Session,
    use_threads: bool,
    proxy: Optional[_WriteProxy] = None,
    filename_prefix: Optional[str] = None,
    **func_kwargs: Any,
) -> List[str]:
    _proxy: _WriteProxy = proxy if proxy else _WriteProxy(use_threads=False)
    bucket_number_series = df.apply(
        lambda row: _get_bucket_number(bucketing_info[1], [row[col_name] for col_name in bucketing_info[0]]),
        axis="columns",
    )
    if filename_prefix is None:
        filename_prefix = uuid.uuid4().hex
    for bucket_number, subgroup in df.groupby(by=bucket_number_series, observed=True):
        _proxy.write(
            func=func,
            df=subgroup,
            path_root=path_root,
            filename=f"{filename_prefix}_bucket-{bucket_number:05d}",
            boto3_session=boto3_session,
            use_threads=use_threads,
            **func_kwargs,
        )
    if proxy:
        return []

    paths: List[str] = _proxy.close()  # blocking
    return paths


def _get_bucket_number(number_of_buckets: int, values: List[Union[str, int, bool]]) -> int:
    hash_code = 0
    for value in values:
        hash_code = 31 * hash_code + _get_value_hash(value)

    return hash_code % number_of_buckets


def _get_value_hash(value: Union[str, int, bool]) -> int:
    if isinstance(value, (int, np.int_)):
        return int(value)
    if isinstance(value, (str, np.str_)):
        value_hash = 0
        for byte in value.encode():
            value_hash = value_hash * 31 + byte
        return value_hash
    if isinstance(value, (bool, np.bool_)):
        return int(value)

    raise exceptions.InvalidDataFrame(
        "Column specified for bucketing contains invalid data type. Only string, int and bool are supported."
    )


def _to_dataset(
    func: Callable[..., List[str]],
    concurrent_partitioning: bool,
    df: pd.DataFrame,
    path_root: str,
    index: bool,
    use_threads: bool,
    mode: str,
    partition_cols: Optional[List[str]],
    bucketing_info: Optional[Tuple[List[str], int]],
    boto3_session: boto3.Session,
    **func_kwargs: Any,
) -> Tuple[List[str], Dict[str, List[str]]]:
    path_root = path_root if path_root.endswith("/") else f"{path_root}/"

    # Evaluate mode
    if mode not in ["append", "overwrite", "overwrite_partitions"]:
        raise exceptions.InvalidArgumentValue(
            f"{mode} is a invalid mode, please use append, overwrite or overwrite_partitions."
        )
    if (mode == "overwrite") or ((mode == "overwrite_partitions") and (not partition_cols)):
        delete_objects(
            path=path_root,
            use_threads=use_threads,
            boto3_session=boto3_session,
            s3_additional_kwargs=func_kwargs.get("s3_additional_kwargs"),
        )

    # Writing
    partitions_values: Dict[str, List[str]] = {}
    paths: List[str]
    if partition_cols:
        paths, partitions_values = _to_partitions(
            func=func,
            concurrent_partitioning=concurrent_partitioning,
            df=df,
            path_root=path_root,
            use_threads=use_threads,
            mode=mode,
            bucketing_info=bucketing_info,
            partition_cols=partition_cols,
            boto3_session=boto3_session,
            index=index,
            **func_kwargs,
        )
    elif bucketing_info:
        paths = _to_buckets(
            func=func,
            df=df,
            path_root=path_root,
            use_threads=use_threads,
            bucketing_info=bucketing_info,
            boto3_session=boto3_session,
            index=index,
            **func_kwargs,
        )
    else:
        paths = func(
            df=df, path_root=path_root, use_threads=use_threads, boto3_session=boto3_session, index=index, **func_kwargs
        )
    _logger.debug("paths: %s", paths)
    _logger.debug("partitions_values: %s", partitions_values)
    return paths, partitions_values
