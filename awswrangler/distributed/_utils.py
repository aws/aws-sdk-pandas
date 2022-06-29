"""Utilities Module for Distributed methods."""

from typing import Any, Callable, Dict, List, Optional

import modin.pandas as pd
import pyarrow as pa
import ray
from modin.distributed.dataframe.pandas.partitions import from_partitions
from ray.data.impl.arrow_block import ArrowBlockAccessor
from ray.data.impl.remote_fn import cached_remote_fn


def _block_to_df(
    block: Any,
    kwargs: Dict[str, Any],
    dtype: Optional[Dict[str, str]] = None,
) -> pa.Table:
    block = ArrowBlockAccessor.for_block(block)
    df = block._table.to_pandas(**kwargs)  # pylint: disable=protected-access
    return df.astype(dtype=dtype) if dtype else df


def _arrow_refs_to_df(arrow_refs: List[Callable[..., Any]], kwargs: Dict[str, Any]) -> pd.DataFrame:
    ds = ray.data.from_arrow_refs(arrow_refs)
    block_to_df = cached_remote_fn(_block_to_df)
    return from_partitions(
        [block_to_df.remote(block=block, kwargs=kwargs) for block in ds.get_internal_block_refs()], axis=0
    )
