"""Modin on Ray S3 write dataset module (PRIVATE)."""

from __future__ import annotations

from typing import Any, Callable, Iterator

import boto3
import modin.pandas as pd
import numpy as np

from awswrangler import _utils, typing
from awswrangler._distributed import engine
from awswrangler.distributed.ray import ray_get, ray_remote
from awswrangler.distributed.ray.modin import modin_repartition
from awswrangler.distributed.ray.modin._utils import _ray_dataset_from_df
from awswrangler.s3._write_concurrent import _WriteProxy
from awswrangler.s3._write_dataset import _delete_objects, _get_bucketing_series, _to_partitions


def _retrieve_paths(values: str | list[Any]) -> Iterator[str]:
    if isinstance(values, (list, np.ndarray)):
        for v in values:
            yield from _retrieve_paths(v)
        return

    yield values


@modin_repartition
def _to_buckets_distributed(
    df: pd.DataFrame,
    func: Callable[..., list[str]],
    path_root: str,
    bucketing_info: typing.BucketingInfoTuple,
    filename_prefix: str,
    boto3_session: "boto3.Session" | None,
    use_threads: bool | int,
    proxy: _WriteProxy | None = None,
    **func_kwargs: Any,
) -> list[str]:
    df_groups = df.groupby(by=_get_bucketing_series(df=df, bucketing_info=bucketing_info))
    paths: list[str] = []

    df_paths = df_groups.apply(
        engine.dispatch_func(func),
        path_root=path_root,
        filename_prefix=filename_prefix,
        s3_client=None,
        use_threads=False,
        bucketing=True,
        **func_kwargs,
    )
    for df_path in df_paths.values:
        # The value in df_path can be a string, a list of string, or a list of lists of strings
        row_paths = list(_retrieve_paths(df_path))
        paths.extend(row_paths)

    return paths


def _write_partitions_distributed(
    df_group: pd.DataFrame,
    write_func: Callable[..., list[str]],
    path_root: str,
    use_threads: bool | int,
    mode: str,
    partition_cols: list[str],
    filename_prefix: str,
    bucketing_info: typing.BucketingInfoTuple | None,
    boto3_session: "boto3.Session" | None = None,
    **func_kwargs: Any,
) -> pd.DataFrame:
    prefix = _delete_objects(
        keys=df_group.name,
        path_root=path_root,
        use_threads=use_threads,
        mode=mode,
        partition_cols=partition_cols,
        boto3_session=boto3_session,
        **func_kwargs,
    )
    if bucketing_info:
        paths = _to_buckets_distributed(
            func=write_func,
            df=df_group.drop(partition_cols, axis="columns"),
            path_root=prefix,
            bucketing_info=bucketing_info,
            boto3_session=boto3_session,
            use_threads=use_threads,
            filename_prefix=filename_prefix,
            **func_kwargs,
        )
    else:
        s3_client = _utils.client(service_name="s3", session=boto3_session)
        paths = write_func(
            df_group.drop(partition_cols, axis="columns"),
            path_root=prefix,
            filename_prefix=filename_prefix,
            s3_client=s3_client,
            use_threads=use_threads,
            **func_kwargs,
        )
    return prefix, df_group.name, paths


@modin_repartition
def _to_partitions_distributed(
    df: pd.DataFrame,
    func: Callable[..., list[str]],
    concurrent_partitioning: bool,
    path_root: str,
    use_threads: bool | int,
    mode: str,
    partition_cols: list[str],
    bucketing_info: typing.BucketingInfoTuple | None,
    filename_prefix: str,
    boto3_session: "boto3.Session" | None,
    **func_kwargs: Any,
) -> tuple[list[str], dict[str, list[str]]]:
    paths: list[str]
    partitions_values: dict[str, list[str]]

    if not bucketing_info:
        # If only partitioning (without bucketing), avoid expensive modin groupby
        # by partitioning and writing each block as an ordinary Pandas DataFrame
        _to_partitions_func = engine.dispatch_func(_to_partitions, "python")
        func = engine.dispatch_func(func, "python")

        @ray_remote()
        def write_partitions(df: pd.DataFrame, block_index: int) -> tuple[list[str], dict[str, list[str]]]:
            paths, partitions_values = _to_partitions_func(
                # Passing a copy of the data frame because data in ray object store is immutable
                # and that leads to "ValueError: buffer source array is read-only" during df.groupby()
                df.copy(),
                func=func,
                concurrent_partitioning=concurrent_partitioning,
                path_root=path_root,
                use_threads=use_threads,
                mode=mode,
                bucketing_info=None,
                filename_prefix=f"{filename_prefix}_{block_index:05d}",
                partition_cols=partition_cols,
                boto3_session=None,
                **func_kwargs,
            )
            return paths, partitions_values

        block_object_refs = _ray_dataset_from_df(df).get_internal_block_refs()
        result = ray_get(
            [write_partitions(object_ref, block_index) for block_index, object_ref in enumerate(block_object_refs)]
        )
        paths = [path for row in result for path in row[0]]
        partitions_values = {
            partition_key: partition_value for row in result for partition_key, partition_value in row[1].items()
        }
        return paths, partitions_values

    df_write_metadata = df.groupby(by=partition_cols, observed=True, sort=False).apply(
        _write_partitions_distributed,
        write_func=func,
        path_root=path_root,
        use_threads=use_threads,
        mode=mode,
        partition_cols=partition_cols,
        filename_prefix=filename_prefix,
        bucketing_info=bucketing_info,
        boto3_session=None,
        **func_kwargs,
    )
    paths = [path for _, _, paths in df_write_metadata.values for path in paths]
    partitions_values = {
        prefix: list(str(p) for p in partitions) if isinstance(partitions, tuple) else [str(partitions)]
        for prefix, partitions, _ in df_write_metadata.values
    }
    return paths, partitions_values
