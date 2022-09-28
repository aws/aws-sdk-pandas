"""Utilities Module for Distributed methods."""

from typing import Any, Callable, Dict, List, Optional, Union

import modin.pandas as modin_pd
import pandas as pd
import ray
from modin.distributed.dataframe.pandas.partitions import from_partitions
from ray.data._internal.arrow_block import ArrowBlockAccessor, ArrowRow
from ray.data._internal.remote_fn import cached_remote_fn

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
