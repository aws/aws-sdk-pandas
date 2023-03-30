"""Pandas "proxy" package."""

from typing import Union

import pandas as pd

from awswrangler._distributed import MemoryFormatEnum, memory_format

if memory_format.get_installed() == MemoryFormatEnum.MODIN:
    from modin.pandas import DataFrame as ModinDataFrame


if memory_format.get() == MemoryFormatEnum.PANDAS:
    from pandas import *  # noqa: F403

    # Explicit import because mypy doesn't support forward references to a star import
    from pandas import (  # noqa: F401
        DataFrame,
        Series,
        concat,
        isna,
        isnull,
        json_normalize,
        notna,
        read_csv,
        to_datetime,
    )
elif memory_format.get() == MemoryFormatEnum.MODIN:
    from modin.pandas import *  # noqa: F403

    # Explicit import because mypy doesn't support forward references to a star import
    from modin.pandas import (  # noqa: F401
        DataFrame,
        Series,
        concat,
        isna,
        isnull,
        json_normalize,
        notna,
        read_csv,
        to_datetime,
    )
else:
    raise ImportError(f"Unknown memory format {memory_format}")

DataFrameType = Union[pd.DataFrame, "ModinDataFrame"]


__all__ = [
    "DataFrame",
    "DataFrameType",
    "Series",
    "concat",
    "isna",
    "isnull",
    "json_normalize",
    "notna",
    "read_csv",
    "to_datetime",
]
