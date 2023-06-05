"""Amazon CSV S3 Write Module (PRIVATE)."""

import logging
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional

import pandas as pd

from awswrangler import _data_types, _utils, catalog, exceptions, typing
from awswrangler._distributed import EngineEnum

_logger: logging.Logger = logging.getLogger(__name__)

_COMPRESSION_2_EXT: Dict[Optional[str], str] = {
    None: "",
    "gzip": ".gz",
    "snappy": ".snappy",
    "bz2": ".bz2",
    "xz": ".xz",
    "zip": ".zip",
    "zstd": ".zstd",
}


def _extract_dtypes_from_table_input(table_input: Dict[str, Any]) -> Dict[str, str]:
    dtypes: Dict[str, str] = {}
    for col in table_input["StorageDescriptor"]["Columns"]:
        dtypes[col["Name"]] = col["Type"]
    if "PartitionKeys" in table_input:
        for par in table_input["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
    return dtypes


def _apply_dtype(
    df: pd.DataFrame, dtype: Dict[str, str], catalog_table_input: Optional[Dict[str, Any]], mode: str
) -> pd.DataFrame:
    if mode in ("append", "overwrite_partitions"):
        if catalog_table_input is not None:
            catalog_types: Optional[Dict[str, str]] = _extract_dtypes_from_table_input(table_input=catalog_table_input)
            if catalog_types is not None:
                for k, v in catalog_types.items():
                    dtype[k] = v
    df = _data_types.cast_pandas_with_athena_types(df=df, dtype=dtype)
    return df


def _validate_args(
    df: pd.DataFrame,
    table: Optional[str],
    database: Optional[str],
    dataset: bool,
    path: Optional[str],
    partition_cols: Optional[List[str]],
    bucketing_info: Optional[typing.BucketingInfoTuple],
    mode: Optional[str],
    description: Optional[str],
    parameters: Optional[Dict[str, str]],
    columns_comments: Optional[Dict[str, str]],
    execution_engine: Enum,
) -> None:
    if df.empty is True:
        _logger.warning("Empty DataFrame will be written.")
    if dataset is False:
        if path is None:
            raise exceptions.InvalidArgumentValue("If dataset is False, the `path` argument must be passed.")
        if execution_engine == EngineEnum.PYTHON and path.endswith("/"):
            raise exceptions.InvalidArgumentValue(
                "If <dataset=False>, the argument <path> should be a key, not a prefix."
            )
        if partition_cols:
            raise exceptions.InvalidArgumentCombination("Please, pass dataset=True to be able to use partition_cols.")
        if bucketing_info:
            raise exceptions.InvalidArgumentCombination("Please, pass dataset=True to be able to use bucketing_info.")
        if mode is not None:
            raise exceptions.InvalidArgumentCombination("Please pass dataset=True to be able to use mode.")
        if any(arg is not None for arg in (table, description, parameters, columns_comments)):
            raise exceptions.InvalidArgumentCombination(
                "Please pass dataset=True to be able to use any one of these "
                "arguments: database, table, description, parameters, "
                "columns_comments."
            )
    elif (database is None) != (table is None):
        raise exceptions.InvalidArgumentCombination(
            "Arguments database and table must be passed together. If you want to store your dataset metadata in "
            "the Glue Catalog, please ensure you are passing both."
        )
    elif all(x is None for x in [path, database, table]):
        raise exceptions.InvalidArgumentCombination(
            "You must specify a `path` if dataset is True and database/table are not enabled."
        )
    elif bucketing_info and bucketing_info[1] <= 0:
        raise exceptions.InvalidArgumentValue(
            "Please pass a value greater than 1 for the number of buckets for bucketing."
        )


class _SanitizeResult(NamedTuple):
    frame: pd.DataFrame
    dtype: Dict[str, str]
    partition_cols: List[str]
    bucketing_info: Optional[typing.BucketingInfoTuple]


def _sanitize(
    df: pd.DataFrame,
    dtype: Dict[str, str],
    partition_cols: List[str],
    bucketing_info: Optional[typing.BucketingInfoTuple] = None,
) -> _SanitizeResult:
    df = catalog.sanitize_dataframe_columns_names(df=df)
    partition_cols = [catalog.sanitize_column_name(p) for p in partition_cols]
    if bucketing_info:
        bucketing_info = [
            catalog.sanitize_column_name(bucketing_col) for bucketing_col in bucketing_info[0]
        ], bucketing_info[1]
    dtype = {catalog.sanitize_column_name(k): v.lower() for k, v in dtype.items()}
    _utils.check_duplicated_columns(df=df)
    return _SanitizeResult(df, dtype, partition_cols, bucketing_info)


def _get_chunk_file_path(file_counter: int, file_path: str) -> str:
    slash_index: int = file_path.rfind("/")
    dot_index: int = file_path.find(".", slash_index)
    file_index: str = "_" + str(file_counter)
    if dot_index == -1:
        file_path = file_path + file_index
    else:
        file_path = file_path[:dot_index] + file_index + file_path[dot_index:]
    return file_path


def _get_write_table_args(pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    write_table_args: Dict[str, Any] = {}
    if pyarrow_additional_kwargs and "write_table_args" in pyarrow_additional_kwargs:
        write_table_args = pyarrow_additional_kwargs.pop("write_table_args")
    return write_table_args


def _get_file_path(
    path_root: Optional[str] = None,
    path: Optional[str] = None,
    filename_prefix: Optional[str] = None,
    compression_ext: str = "",
    bucket_id: Optional[int] = None,
    extension: str = ".parquet",
) -> str:
    if bucket_id is not None:
        filename_prefix = f"{filename_prefix}_bucket-{bucket_id:05d}"
    if path is None and path_root is not None:
        file_path: str = f"{path_root}{filename_prefix}{compression_ext}{extension}"
    elif path is not None and path_root is None:
        file_path = path
    else:
        raise RuntimeError("path and path_root received at the same time.")
    return file_path
