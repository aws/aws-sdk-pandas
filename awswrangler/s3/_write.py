"""Amazon CSV S3 Write Module (PRIVATE)."""

import importlib.util
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa

from awswrangler import _data_types, _utils, catalog, exceptions
from pandas import DataFrame as PandasDataFrame

_ray_found = importlib.util.find_spec("ray")
if _ray_found:
    import ray
    from ray.data.block import Block, BlockAccessor, BlockExecStats
    from ray.data.dataset import Dataset
    from ray.data.impl.arrow_block import ArrowRow
    from ray.data.impl.block_list import BlockList
    from ray.data.impl.plan import ExecutionPlan
    from ray.data.impl.remote_fn import cached_remote_fn
    from ray.data.impl.stats import DatasetStats
    from ray.types import ObjectRef

_modin_found = importlib.util.find_spec("modin")
if _modin_found:
    import modin.pandas as pd
    from modin.distributed.dataframe.pandas.partitions import unwrap_partitions
else:
    import pandas as pd

_logger: logging.Logger = logging.getLogger(__name__)

_COMPRESSION_2_EXT: Dict[Optional[str], str] = {
    None: "",
    "gzip": ".gz",
    "snappy": ".snappy",
    "bz2": ".bz2",
    "xz": ".xz",
    "zip": ".zip",
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
    bucketing_info: Optional[Tuple[List[str], int]],
    mode: Optional[str],
    description: Optional[str],
    parameters: Optional[Dict[str, str]],
    columns_comments: Optional[Dict[str, str]],
) -> None:
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")
    if dataset is False:
        if path is None:
            raise exceptions.InvalidArgumentValue("If dataset is False, the `path` argument must be passed.")
        if path.endswith("/"):
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


def _sanitize(
    df: pd.DataFrame, dtype: Dict[str, str], partition_cols: List[str]
) -> Tuple[pd.DataFrame, Dict[str, str], List[str]]:
    df = catalog.sanitize_dataframe_columns_names(df=df)
    partition_cols = [catalog.sanitize_column_name(p) for p in partition_cols]
    dtype = {catalog.sanitize_column_name(k): v.lower() for k, v in dtype.items()}
    _utils.check_duplicated_columns(df=df)
    return df, dtype, partition_cols


def _df_to_block(
    df: pd.DataFrame,
    schema: pa.Schema,
    index: bool,
    dtype: Dict[str, str],
    nthreads: Optional[int] = None,
) -> Union[pa.Table, "Block[ArrowRow]"]:
    block = pa.Table.from_pandas(df=df, schema=schema, nthreads=nthreads, preserve_index=index, safe=True)
    for col_name, col_type in dtype.items():
        if col_name in block.column_names:
            col_index = block.column_names.index(col_name)
            pyarrow_dtype = _data_types.athena2pyarrow(col_type)
            field = pa.field(name=col_name, type=pyarrow_dtype)
            block = block.set_column(col_index, field, block.column(col_name).cast(pyarrow_dtype))
            _logger.debug("Casting column %s (%s) to %s (%s)", col_name, col_index, col_type, pyarrow_dtype)
    if _ray_found:
        return (
            block,
            BlockAccessor.for_block(block).get_metadata(
                input_files=None, exec_stats=BlockExecStats.builder().build()  # type: ignore
            ),
        )
    return block


def _from_pandas_refs(
    dfs: Union["ObjectRef[pd.DataFrame]", List["ObjectRef[pd.DataFrame]"]],
    schema: pa.Schema,
    index: bool,
    dtype: Dict[str, str],
) -> "Dataset[ArrowRow]":
    if isinstance(dfs, ray.ObjectRef):
        dfs = [dfs]  # type: ignore
    elif isinstance(dfs, list):
        for df in dfs:
            if not isinstance(df, ray.ObjectRef):
                raise ValueError("Expected list of Ray object refs, " f"got list containing {type(df)}")
    else:
        raise ValueError("Expected Ray object ref or list of Ray object refs, " f"got {type(df)}")

    df_to_block = cached_remote_fn(_df_to_block, num_returns=2)

    res = [df_to_block.remote(df, schema, index, dtype) for df in dfs]
    blocks, metadata = zip(*res)
    return Dataset(
        ExecutionPlan(
            BlockList(blocks, ray.get(list(metadata))),
            DatasetStats(stages={"from_pandas_refs": metadata}, parent=None),
        ),
        0,
        False,
    )


def _df_to_dataset(
    df: pd.DataFrame,
    schema: pa.Schema,
    index: bool,
    dtype: Dict[str, str],
    nthreads: Optional[int] = None,
) -> Union[pa.Table, "Dataset[ArrowRow]"]:
    if _ray_found:
        return _from_pandas_refs(
            dfs=[ray.put(df)] if isinstance(df, PandasDataFrame) else unwrap_partitions(df, axis=0),
            schema=schema,
            index=index,
            dtype=dtype,
        )
    return _df_to_block(df=df, schema=schema, index=index, dtype=dtype, nthreads=nthreads)


def _get_file_path(file_counter: int, file_path: str) -> str:
    slash_index: int = file_path.rfind("/")
    dot_index: int = file_path.find(".", slash_index)
    file_index: str = "_" + str(file_counter)
    if dot_index == -1:
        file_path = file_path + file_index
    else:
        file_path = file_path[:dot_index] + file_index + file_path[dot_index:]
    return file_path
