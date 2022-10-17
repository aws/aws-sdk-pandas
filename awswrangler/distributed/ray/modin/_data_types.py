"""Internal (private) Data Types Module."""
from collections import Counter
from typing import Dict, List, Optional

import modin.pandas as pd
import pyarrow as pa
import ray

from awswrangler._data_types import pyarrow_types_from_pandas
from awswrangler.distributed.ray._core import ray_get, ray_remote


def pyarrow_types_from_pandas_distributed(
    df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None, index_left: bool = False
) -> Dict[str, pa.DataType]:
    """Extract the related Pyarrow data types from a pandas DataFrame."""
    func = ray_remote(pyarrow_types_from_pandas)
    block_object_refs = ray.data.from_modin(df).get_internal_block_refs()
    list_col_types = ray_get(
        [
            func(
                df=object_ref,
                index=index,
                ignore_cols=ignore_cols,
                index_left=index_left,
            )
            for object_ref in block_object_refs
        ]
    )

    # Dictionaries in list_col_types might not be equal (i.e. different col types in different blocks)
    # In which case we return the most frequent value for each key
    # More details here: https://github.com/aws/aws-sdk-pandas/pull/1692
    keys = set().union(*(d.keys() for d in list_col_types))
    col_types = {}
    for key in keys:
        c = Counter()  # type: ignore
        for d in list_col_types:
            if key in d.keys():
                c[d[key]] += 1
        col_types[key] = c.most_common(1)[0][0]
    return col_types
