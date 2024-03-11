"""Internal (private) Data Types Module."""

from __future__ import annotations

import modin.pandas as pd
import pyarrow as pa

from awswrangler._data_types import pyarrow_types_from_pandas
from awswrangler.distributed.ray import ray_get, ray_remote
from awswrangler.distributed.ray.modin._utils import _ray_dataset_from_df


def pyarrow_types_from_pandas_distributed(
    df: pd.DataFrame, index: bool, ignore_cols: list[str] | None = None, index_left: bool = False
) -> dict[str, pa.DataType]:
    """Extract the related Pyarrow data types from a pandas DataFrame."""
    func = ray_remote()(pyarrow_types_from_pandas)
    first_block_object_ref = _ray_dataset_from_df(df).get_internal_block_refs()[0]
    return ray_get(  # type: ignore[no-any-return]
        func(
            df=first_block_object_ref,
            index=index,
            ignore_cols=ignore_cols,
            index_left=index_left,
        )
    )
