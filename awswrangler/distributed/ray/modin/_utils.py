"""Modin on Ray utilities (PRIVATE)."""
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Union

import modin.pandas as modin_pd
import pandas as pd
import pyarrow as pa
import ray
from modin.distributed.dataframe.pandas import from_partitions
from ray.data import Dataset, from_modin, from_pandas
from ray.data.block import BlockAccessor
from ray.types import ObjectRef

from awswrangler import engine, exceptions
from awswrangler._arrow import _table_to_df
from awswrangler._utils import copy_df_shallow
from awswrangler.distributed.ray import ray_get, ray_remote


@ray_remote()
def _block_to_df(
    block: Any,
    to_pandas_kwargs: Dict[str, Any],
) -> pd.DataFrame:
    if isinstance(block, pd.DataFrame):
        return block

    block = BlockAccessor.for_block(block)
    return _table_to_df(table=block._table, kwargs=to_pandas_kwargs)  # pylint: disable=protected-access


def _ray_dataset_from_df(df: Union[pd.DataFrame, modin_pd.DataFrame]) -> Dataset[Any]:
    """Create Ray dataset from supported types of data frames."""
    if isinstance(df, modin_pd.DataFrame):
        return from_modin(df)  # type: ignore[no-any-return]
    if isinstance(df, pd.DataFrame):
        return from_pandas(df)  # type: ignore[no-any-return]
    raise ValueError(f"Unknown DataFrame type: {type(df)}")


def _to_modin(
    dataset: Union[ray.data.Dataset[Any], ray.data.Dataset[pd.DataFrame]],
    to_pandas_kwargs: Optional[Dict[str, Any]] = None,
    ignore_index: Optional[bool] = True,
) -> modin_pd.DataFrame:
    index = modin_pd.RangeIndex(start=0, stop=dataset.count()) if ignore_index else None
    _to_pandas_kwargs = {} if to_pandas_kwargs is None else to_pandas_kwargs

    return from_partitions(
        partitions=[
            _block_to_df(block=block, to_pandas_kwargs=_to_pandas_kwargs) for block in dataset.get_internal_block_refs()
        ],
        axis=0,
        index=index,
    )


def _split_modin_frame(df: modin_pd.DataFrame, splits: int) -> List[ObjectRef[Any]]:  # pylint: disable=unused-argument
    object_refs: List[ObjectRef[Any]] = _ray_dataset_from_df(df).get_internal_block_refs()
    return object_refs


def _arrow_refs_to_df(arrow_refs: List[Callable[..., Any]], kwargs: Optional[Dict[str, Any]]) -> modin_pd.DataFrame:
    @ray_remote()
    def _is_not_empty(table: pa.Table) -> Any:
        return table.num_rows > 0 or table.num_columns > 0

    ref_rows: List[bool] = ray_get([_is_not_empty(arrow_ref) for arrow_ref in arrow_refs])
    refs: List[Callable[..., Any]] = [ref for ref_rows, ref in zip(ref_rows, arrow_refs) if ref_rows]
    return _to_modin(
        dataset=ray.data.from_arrow_refs(refs if len(refs) > 0 else [pa.Table.from_arrays([])]), to_pandas_kwargs=kwargs
    )


def _is_pandas_or_modin_frame(obj: Any) -> bool:
    return isinstance(obj, (pd.DataFrame, modin_pd.DataFrame))


def _copy_modin_df_shallow(frame: Union[pd.DataFrame, modin_pd.DataFrame]) -> Union[pd.DataFrame, modin_pd.DataFrame]:
    if isinstance(frame, pd.DataFrame):
        engine.dispatch_func(copy_df_shallow, "python")(frame)

    return modin_pd.DataFrame(frame, copy=False)


@dataclass
class ParamConfig:
    """
    Configuration for a Pandas argument that is supported in PyArrow.

    Contains a default value and, optionally, a list of supports values.
    """

    default: Any
    supported_values: Optional[Set[Any]] = None


def _check_parameters(pandas_kwargs: Dict[str, Any], supported_params: Dict[str, ParamConfig]) -> None:
    for pandas_arg_key, pandas_args_value in pandas_kwargs.items():
        if pandas_arg_key not in supported_params:
            raise exceptions.InvalidArgument(f"Unsupported Pandas parameter for PyArrow loader: {pandas_arg_key}")

        param_config = supported_params[pandas_arg_key]
        if param_config.supported_values is None:
            continue

        if pandas_args_value not in param_config.supported_values:
            raise exceptions.InvalidArgument(
                f"Unsupported Pandas parameter value for PyArrow loader: {pandas_arg_key}={pandas_args_value}",
            )
