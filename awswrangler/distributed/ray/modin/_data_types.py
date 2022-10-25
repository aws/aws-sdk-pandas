"""Internal (private) Data Types Module."""
from typing import Dict, List, Optional

import modin.pandas as pd
import pyarrow as pa
import ray

from awswrangler._data_types import pyarrow_types_from_pandas
from awswrangler.distributed.ray import ray_get, ray_remote


def pyarrow_types_from_pandas_distributed(
    df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None, index_left: bool = False
) -> Dict[str, pa.DataType]:
    """Extract the related Pyarrow data types from a pandas DataFrame."""
    func = ray_remote(pyarrow_types_from_pandas)
    first_block_object_ref = ray.data.from_modin(df).get_internal_block_refs()[0]
    return ray_get(  # type: ignore
        func(
            df=first_block_object_ref,
            index=index,
            ignore_cols=ignore_cols,
            index_left=index_left,
        )
    )
