"""Modin on Ray utilities (PRIVATE)."""
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Union

import modin.pandas as modin_pd
import pandas as pd
import ray
from modin.distributed.dataframe.pandas import from_partitions
from ray.data._internal.arrow_block import ArrowBlockAccessor, ArrowRow
from ray.data._internal.remote_fn import cached_remote_fn

from awswrangler import exceptions
from awswrangler._arrow import _table_to_df


def _block_to_df(
    block: Any,
    to_pandas_kwargs: Dict[str, Any],
) -> pd.DataFrame:
    if isinstance(block, pd.DataFrame):
        return block

    block = ArrowBlockAccessor.for_block(block)
    return _table_to_df(table=block._table, kwargs=to_pandas_kwargs)  # pylint: disable=protected-access


def _to_modin(
    dataset: Union[ray.data.Dataset[ArrowRow], ray.data.Dataset[pd.DataFrame]],
    to_pandas_kwargs: Optional[Dict[str, Any]] = None,
    ignore_index: bool = True,
) -> modin_pd.DataFrame:
    index = modin_pd.RangeIndex(start=0, stop=dataset.count()) if ignore_index else None
    _to_pandas_kwargs = {} if to_pandas_kwargs is None else to_pandas_kwargs

    block_to_df = cached_remote_fn(_block_to_df)
    return from_partitions(
        partitions=[
            block_to_df.remote(block=block, to_pandas_kwargs=_to_pandas_kwargs)
            for block in dataset.get_internal_block_refs()
        ],
        axis=0,
        index=index,
    )


def _arrow_refs_to_df(arrow_refs: List[Callable[..., Any]], kwargs: Optional[Dict[str, Any]]) -> modin_pd.DataFrame:
    return _to_modin(dataset=ray.data.from_arrow_refs(arrow_refs), to_pandas_kwargs=kwargs)


def _is_pandas_or_modin_frame(obj: Any) -> bool:
    return isinstance(obj, (pd.DataFrame, modin_pd.DataFrame))


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
