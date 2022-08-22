"""Utilities Module for Distributed methods."""

from typing import Any, Callable, Dict, List

import modin.pandas as pd
import pyarrow as pa
import ray
from modin.distributed.dataframe.pandas.partitions import from_partitions
from ray.data.impl.arrow_block import ArrowBlockAccessor, ArrowRow
from ray.data.impl.remote_fn import cached_remote_fn

from awswrangler._arrow import _table_to_df


def _block_to_df(
    block: Any,
    kwargs: Dict[str, Any],
) -> pa.Table:
    block = ArrowBlockAccessor.for_block(block)
    return _table_to_df(table=block._table, kwargs=kwargs)  # pylint: disable=protected-access


def _to_modin(dataset: ray.data.Dataset[ArrowRow], kwargs: Dict[str, Any]) -> pd.DataFrame:
    block_to_df = cached_remote_fn(_block_to_df)
    return from_partitions(
        partitions=[block_to_df.remote(block=block, kwargs=kwargs) for block in dataset.get_internal_block_refs()],
        axis=0,
        index=pd.RangeIndex(start=0, stop=dataset.count()),
    )


def _arrow_refs_to_df(arrow_refs: List[Callable[..., Any]], kwargs: Dict[str, Any]) -> pd.DataFrame:
    return _to_modin(dataset=ray.data.from_arrow_refs(arrow_refs), kwargs=kwargs)
