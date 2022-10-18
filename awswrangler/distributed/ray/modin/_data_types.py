"""Internal (private) Data Types Module."""
from collections import Counter
from typing import Dict, List, Optional
from warnings import warn

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
    list_column_types: List[Dict[str, pa.DataType]] = ray_get(
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

    if len(list_column_types) > 1:
        first: Dict[str, pa.DataType] = list_column_types[0]
        for column_types_dict in list_column_types[1:]:
            if first != column_types_dict:
                warn(
                    "At least 2 different column types were detected:"
                    f"\n    1 - {first}\n    2 - {column_types_dict}."
                    "Defaulting to the most common data type for each column instead.",
                    UserWarning,
                )

    # Dictionaries in list_column_types might not be equal (i.e. different column types in different blocks)
    # In which case we return the most frequent value for each key
    # More details here: https://github.com/aws/aws-sdk-pandas/pull/1692
    list_keys = [k for d in list_column_types for k in d.keys()]
    set_keys = sorted(set(list_keys), key=list_keys.index)
    column_types: Dict[str, pa.DataType] = {}
    for key in set_keys:
        c = Counter()  # type: ignore
        for d in list_column_types:
            if key in d.keys():
                c[d[key]] += 1
        column_types[key] = c.most_common(1)[0][0]
    return column_types
