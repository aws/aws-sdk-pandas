"""Amazon S3 Write Dataset (PRIVATE)."""

import logging
from functools import singledispatch
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import boto3
import numpy as np
from pandas import DataFrame as PandasDataFrame

from awswrangler import exceptions, lakeformation
from awswrangler._config import config
from awswrangler.distributed import ray_get, ray_remote
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._write_concurrent import _WriteProxy

if config.distributed:
    import modin.pandas as pd
    import ray
    from modin.pandas import DataFrame as ModinDataFrame
else:
    import pandas as pd

_logger: logging.Logger = logging.getLogger(__name__)


def _get_bucketing_series(df: pd.DataFrame, bucketing_info: Tuple[List[str], int]) -> pd.Series:
    bucket_number_series = df.astype("O").apply(
        lambda row: _get_bucket_number(bucketing_info[1], [row[col_name] for col_name in bucketing_info[0]]),
        axis="columns",
    )
    return bucket_number_series.astype(pd.CategoricalDtype(range(bucketing_info[1])))


def _simulate_overflow(value: int, bits: int = 31, signed: bool = False) -> int:
    base = 1 << bits
    value %= base
    return value - base if signed and value.bit_length() == bits else value


def _get_bucket_number(number_of_buckets: int, values: List[Union[str, int, bool]]) -> int:
    hash_code = 0
    for value in values:
        hash_code = 31 * hash_code + _get_value_hash(value)
        hash_code = _simulate_overflow(hash_code)

    return hash_code % number_of_buckets


def _get_value_hash(value: Union[str, int, bool]) -> int:
    if isinstance(value, (int, np.int_)):
        value = int(value)
        bigint_min, bigint_max = -(2**63), 2**63 - 1
        int_min, int_max = -(2**31), 2**31 - 1
        if not bigint_min <= value <= bigint_max:
            raise ValueError(f"{value} exceeds the range that Athena cannot handle as bigint.")
        if not int_min <= value <= int_max:
            value = (value >> 32) ^ value
        if value < 0:
            return -value - 1
        return int(value)
    if isinstance(value, (str, np.str_)):
        value_hash = 0
        for byte in value.encode():
            value_hash = value_hash * 31 + byte
            value_hash = _simulate_overflow(value_hash)
        return value_hash
    if isinstance(value, (bool, np.bool_)):
        return int(value)

    raise exceptions.InvalidDataFrame(
        "Column specified for bucketing contains invalid data type. Only string, int and bool are supported."
    )


def _get_subgroup_prefix(keys: Tuple[str, None], partition_cols: List[str], path_root: str) -> str:
    subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
    return f"{path_root}{subdir}/"


def _delete_objects(
    keys: Tuple[str, None],
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
    boto3_session: Optional[boto3.Session] = None,
    **func_kwargs: Any,
) -> str:
    # Keys are either a primitive type or a tuple if partitioning by multiple cols
    keys = (keys,) if not isinstance(keys, tuple) else keys
    prefix = _get_subgroup_prefix(keys, partition_cols, path_root)
    if mode == "overwrite_partitions":
        if (table_type == "GOVERNED") and (table is not None) and (database is not None):
            del_objects: List[Dict[str, Any]] = lakeformation._get_table_objects(  # pylint: disable=protected-access
                catalog_id=catalog_id,
                database=database,
                table=table,
                transaction_id=transaction_id,  # type: ignore
                partition_cols=partition_cols,
                partitions_values=keys,  # type: ignore
                partitions_types=partitions_types,
                boto3_session=boto3_session,
            )
            if del_objects:
                lakeformation._update_table_objects(  # pylint: disable=protected-access
                    catalog_id=catalog_id,
                    database=database,
                    table=table,
                    transaction_id=transaction_id,  # type: ignore
                    del_objects=del_objects,
                    boto3_session=boto3_session,
                )
        else:
            delete_objects(
                path=prefix,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=func_kwargs.get("s3_additional_kwargs"),
            )
    return prefix


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
    boto3_session: Optional[boto3.Session] = None,
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


@singledispatch
def _to_partitions(
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
    boto3_session: boto3.Session,
    **func_kwargs: Any,
) -> Tuple[List[str], Dict[str, List[str]]]:
    partitions_values: Dict[str, List[str]] = {}
    proxy: _WriteProxy = _WriteProxy(use_threads=concurrent_partitioning)

    for keys, subgroup in df.groupby(by=partition_cols, observed=True):
        # Keys are either a primitive type or a tuple if partitioning by multiple cols
        keys = (keys,) if not isinstance(keys, tuple) else keys
        subgroup = subgroup.drop(partition_cols, axis="columns")
        prefix = _delete_objects(
            keys=keys,
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
            _to_buckets(
                subgroup,
                func=func,
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
                func,
                boto3_session,
                subgroup,
                path_root=prefix,
                filename_prefix=filename_prefix,
                use_threads=use_threads,
                **func_kwargs,
            )
        partitions_values[prefix] = [str(k) for k in keys]
    paths: List[str] = proxy.close()  # blocking
    return paths, partitions_values


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
    boto3_session: boto3.Session,
    **func_kwargs: Any,
) -> Tuple[List[str], Dict[str, List[str]]]:
    paths: List[str]
    partitions_values: Dict[str, List[str]]

    if not bucketing_info:
        # If only partitioning (without bucketing), avoid expensive modin groupby
        # by partitioning and writing each block as an ordinary Pandas DataFrame
        _to_partitions_func = _to_partitions.dispatch(PandasDataFrame)

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


@singledispatch
def _to_buckets(
    df: pd.DataFrame,
    func: Callable[..., List[str]],
    path_root: str,
    bucketing_info: Tuple[List[str], int],
    filename_prefix: str,
    boto3_session: boto3.Session,
    use_threads: Union[bool, int],
    proxy: Optional[_WriteProxy] = None,
    **func_kwargs: Any,
) -> List[str]:
    _proxy: _WriteProxy = proxy if proxy else _WriteProxy(use_threads=False)
    df_groups = df.groupby(by=_get_bucketing_series(df=df, bucketing_info=bucketing_info))
    for bucket_number, subgroup in df_groups:
        _proxy.write(
            func,
            boto3_session,
            subgroup,
            path_root=path_root,
            filename_prefix=f"{filename_prefix}_bucket-{bucket_number:05d}",
            use_threads=use_threads,
            **func_kwargs,
        )
    if proxy:
        return []
    paths: List[str] = _proxy.close()  # blocking
    return paths


def _to_buckets_distributed(  # pylint: disable=unused-argument
    df: pd.DataFrame,
    func: Callable[..., List[str]],
    path_root: str,
    bucketing_info: Tuple[List[str], int],
    filename_prefix: str,
    boto3_session: boto3.Session,
    use_threads: Union[bool, int],
    proxy: Optional[_WriteProxy] = None,
    **func_kwargs: Any,
) -> List[str]:
    df_groups = df.groupby(by=_get_bucketing_series(df=df, bucketing_info=bucketing_info))
    paths: List[str] = []

    df_paths = df_groups.apply(
        func.dispatch(ModinDataFrame),  # type: ignore
        path_root=path_root,
        filename_prefix=filename_prefix,
        boto3_session=None,
        use_threads=False,
        bucketing=True,
        **func_kwargs,
    )
    for df_path in df_paths.values:
        # The value in df_path can be a string, a list of string, or a list of lists of strings
        try:
            try:
                row_paths = [p for arr in df_path for p in arr]
                paths.extend(row_paths)
            except TypeError:
                paths.extend(df_path)
        except TypeError:
            paths.append(df_path)
    return paths


def _to_dataset(
    func: Callable[..., List[str]],
    concurrent_partitioning: bool,
    df: pd.DataFrame,
    path_root: str,
    filename_prefix: str,
    index: bool,
    use_threads: Union[bool, int],
    mode: str,
    partition_cols: Optional[List[str]],
    partitions_types: Optional[Dict[str, str]],
    catalog_id: Optional[str],
    database: Optional[str],
    table: Optional[str],
    table_type: Optional[str],
    transaction_id: Optional[str],
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
        if (table_type == "GOVERNED") and (table is not None) and (database is not None):
            del_objects: List[Dict[str, Any]] = lakeformation._get_table_objects(  # pylint: disable=protected-access
                catalog_id=catalog_id,
                database=database,
                table=table,
                transaction_id=transaction_id,  # type: ignore
                boto3_session=boto3_session,
            )
            if del_objects:
                lakeformation._update_table_objects(  # pylint: disable=protected-access
                    catalog_id=catalog_id,
                    database=database,
                    table=table,
                    transaction_id=transaction_id,  # type: ignore
                    del_objects=del_objects,
                    boto3_session=boto3_session,
                )
        else:
            delete_objects(path=path_root, use_threads=use_threads, boto3_session=boto3_session)

    # Writing
    partitions_values: Dict[str, List[str]] = {}
    paths: List[str]
    if partition_cols:
        paths, partitions_values = _to_partitions(
            df,
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
            bucketing_info=bucketing_info,
            filename_prefix=filename_prefix,
            partition_cols=partition_cols,
            partitions_types=partitions_types,
            boto3_session=boto3_session,
            index=index,
            **func_kwargs,
        )
    elif bucketing_info:
        paths = _to_buckets(
            df,
            func=func,
            path_root=path_root,
            use_threads=use_threads,
            bucketing_info=bucketing_info,
            filename_prefix=filename_prefix,
            boto3_session=boto3_session,
            index=index,
            **func_kwargs,
        )
    else:
        paths = func(
            df,
            path_root=path_root,
            filename_prefix=filename_prefix,
            use_threads=use_threads,
            boto3_session=boto3_session,
            index=index,
            **func_kwargs,
        )
    _logger.debug("paths: %s", paths)
    _logger.debug("partitions_values: %s", partitions_values)
    if (table_type == "GOVERNED") and (table is not None) and (database is not None):
        add_objects: List[Dict[str, Any]] = lakeformation._build_table_objects(  # pylint: disable=protected-access
            paths, partitions_values, use_threads=use_threads, boto3_session=boto3_session
        )
        try:
            if add_objects:
                lakeformation._update_table_objects(  # pylint: disable=protected-access
                    catalog_id=catalog_id,
                    database=database,
                    table=table,
                    transaction_id=transaction_id,  # type: ignore
                    add_objects=add_objects,
                    boto3_session=boto3_session,
                )
        except Exception as ex:
            _logger.error(ex)
            raise

    return paths, partitions_values
