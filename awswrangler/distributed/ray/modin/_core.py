"""Modin on Ray Core module (PRIVATE)."""
# pylint: disable=import-outside-toplevel
from functools import wraps
from typing import Any, Callable, Optional

import pandas as pd
from modin.distributed.dataframe.pandas import from_partitions, unwrap_partitions
from modin.pandas import DataFrame as ModinDataFrame


def modin_repartition(function: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorate callable to repartition Modin data frame.

    By default, repartition along row (axis=0) axis.
    This avoids a situation where columns are split along multiple blocks.

    Parameters
    ----------
    function : Callable[..., Any]
        Callable as input to ray.remote

    Returns
    -------
    Callable[..., Any]
    """
    # Access the source function if it exists
    function = getattr(function, "_source_func", function)

    @wraps(function)
    def wrapper(
        df: pd.DataFrame,
        *args: Any,
        axis: int = 0,
        row_lengths: Optional[int] = None,
        **kwargs: Any,
    ) -> Any:
        # Repartition Modin data frame along row (axis=0) axis
        # to avoid a situation where columns are split along multiple blocks
        if isinstance(df, ModinDataFrame) and axis is not None:
            df = from_partitions(unwrap_partitions(df, axis=axis), axis=axis, row_lengths=row_lengths)
        return function(df, *args, **kwargs)

    return wrapper
