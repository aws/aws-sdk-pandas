"""Modin on Ray S3 write dataset module (PRIVATE)."""
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

import modin.pandas as pd
import ray

from awswrangler._distributed import engine
from awswrangler.distributed.ray import ray_get, ray_remote
from awswrangler.s3._write_concurrent import _WriteProxy
from awswrangler.s3._write_dataset import _delete_objects, _get_bucketing_series, _retrieve_paths, _to_partitions

if TYPE_CHECKING:
    import boto3


def _to_buckets_distributed(  # pylint: disable=unused-argument
    df: pd.DataFrame,
    func: Callable[..., List[str]],
    path_root: str,
    bucketing_info: Tuple[List[str], int],
    filename_prefix: str,
    boto3_session: "boto3.Session",
    use_threads: Union[bool, int],
    proxy: Optional[_WriteProxy] = None,
    **func_kwargs: Any,
) -> List[str]:
    df_groups = df.groupby(by=_get_bucketing_series(df=df, bucketing_info=bucketing_info))
    paths: List[str] = []

    df_paths = df_groups.apply(
        engine.dispatch_func(func),  # type: ignore
        path_root=path_root,
        filename_prefix=filename_prefix,
        boto3_session=None,
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
    write_func: Callable[..., List[str]],
    path_root: str,
    use_threads: Union[bool, int],
    mode: str,
    partition_cols: List[str],
    partitions_types: Optional[Dict[str, str]],
    catalog_id: Optional[str],
    database: Optional[str],
    table: Optional[str],
    table_type: Optional[str],
    transaction_id: Optional[str],
    filename_prefix: str,
    bucketing_info: Optional[Tuple[List[str], int]],
    boto3_session: Optional["boto3.Session"] = None,
    **func_kwargs: Any,
) -> pd.DataFrame:
    prefix = _delete_objects(
        keys=df_group.name,
        path_root=path_root,
        use_threads=use_threads,
        mode=mode,
        partition_cols=partition_cols,
        partitions_types=partitions_types,
        catalog_id=catalog_id,
        database=database,
        table=table,
        table_type=table_type,
        transaction_id=transaction_id,
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
        paths = write_func(  # type: ignore
            df_group.drop(partition_cols, axis="columns"),
            path_root=prefix,
            filename_prefix=filename_prefix,
            boto3_session=boto3_session,
            use_threads=use_threads,
            **func_kwargs,
        )
    return prefix, df_group.name, paths


def _to_partitions_distributed(  # pylint: disable=unused-argument
    df: pd.DataFrame,
    func: Callable[..., List[str]],
    concurrent_partitioning: bool,
    path_root: str,
    use_threads: Union[bool, int],
    mode: str,
    partition_cols: List[str],
    partitions_types: Optional[Dict[str, str]],
    catalog_id: Optional[str],
    database: Optional[str],
    table: Optional[str],
    table_type: Optional[str],
    transaction_id: Optional[str],
    bucketing_info: Optional[Tuple[List[str], int]],
    filename_prefix: str,
    boto3_session: "boto3.Session",
    **func_kwargs: Any,
) -> Tuple[List[str], Dict[str, List[str]]]:
    paths: List[str]
    partitions_values: Dict[str, List[str]]

    if not bucketing_info:
        # If only partitioning (without bucketing), avoid expensive modin groupby
        # by partitioning and writing each block as an ordinary Pandas DataFrame
        _to_partitions_func = _to_partitions._source_func  # type: ignore
        func = func._source_func  # type: ignore

        @ray_remote
        def write_partitions(df: pd.DataFrame) -> Tuple[List[str], Dict[str, List[str]]]:
            paths, partitions_values = _to_partitions_func(
                # Passing a copy of the data frame because data in ray object store is immutable
                # and that leads to "ValueError: buffer source array is read-only" during df.groupby()
                df.copy(),
                func=func,
                concurrent_partitioning=concurrent_partitioning,
                path_root=path_root,
                use_threads=use_threads,
                mode=mode,
                catalog_id=catalog_id,
                database=database,
                table=table,
                table_type=table_type,
                transaction_id=transaction_id,
                bucketing_info=None,
                filename_prefix=filename_prefix,
                partition_cols=partition_cols,
                partitions_types=partitions_types,
                boto3_session=None,
                **func_kwargs,
            )
            return paths, partitions_values

        block_object_refs = ray.data.from_modin(df).get_internal_block_refs()
        result = ray_get([write_partitions(object_ref) for object_ref in block_object_refs])
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
        partitions_types=partitions_types,
        catalog_id=catalog_id,
        database=database,
        table=table,
        table_type=table_type,
        transaction_id=transaction_id,
        filename_prefix=filename_prefix,
        bucketing_info=bucketing_info,
        boto3_session=None,
        **func_kwargs,
    )
    paths = [path for metadata in df_write_metadata.values for _, _, paths in metadata for path in paths]
    partitions_values = {
        prefix: list(str(p) for p in partitions) if isinstance(partitions, tuple) else [str(partitions)]
        for metadata in df_write_metadata.values
        for prefix, partitions, _ in metadata
    }
    return paths, partitions_values
