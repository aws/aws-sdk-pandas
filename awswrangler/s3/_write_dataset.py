"""Amazon S3 Write Dataset (PRIVATE)."""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3
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
    boto3_session: boto3.Session,
    **func_kwargs: Any,
) -> Tuple[List[str], Dict[str, List[str]]]:
    partitions_values: Dict[str, List[str]] = {}
    proxy: _WriteProxy = _WriteProxy(use_threads=concurrent_partitioning)
    for keys, subgroup in df.groupby(by=partition_cols, observed=True):
        subgroup = subgroup.drop(partition_cols, axis="columns")
        keys = (keys,) if not isinstance(keys, tuple) else keys
        subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
        prefix: str = f"{path_root}{subdir}/"
        if mode == "overwrite_partitions":
            delete_objects(path=prefix, use_threads=use_threads, boto3_session=boto3_session)
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


def _to_dataset(
    func: Callable[..., List[str]],
    concurrent_partitioning: bool,
    df: pd.DataFrame,
    path_root: str,
    index: bool,
    use_threads: bool,
    mode: str,
    partition_cols: Optional[List[str]],
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
        delete_objects(path=path_root, use_threads=use_threads, boto3_session=boto3_session)

    # Writing
    partitions_values: Dict[str, List[str]] = {}
    if not partition_cols:
        paths: List[str] = func(
            df=df, path_root=path_root, use_threads=use_threads, boto3_session=boto3_session, index=index, **func_kwargs
        )
    else:
        paths, partitions_values = _to_partitions(
            func=func,
            concurrent_partitioning=concurrent_partitioning,
            df=df,
            path_root=path_root,
            use_threads=use_threads,
            mode=mode,
            partition_cols=partition_cols,
            boto3_session=boto3_session,
            index=index,
            **func_kwargs,
        )
    _logger.debug("paths: %s", paths)
    _logger.debug("partitions_values: %s", partitions_values)
    return paths, partitions_values
