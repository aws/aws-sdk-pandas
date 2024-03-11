"""Modin on Ray Core module (PRIVATE)."""

from __future__ import annotations

import logging
from functools import wraps
from typing import Any, Callable, TypeVar

import numpy as np
import pandas as pd
from modin.distributed.dataframe.pandas import from_partitions, unwrap_partitions
from modin.pandas import DataFrame as ModinDataFrame

_logger: logging.Logger = logging.getLogger(__name__)


def _validate_partition_shape(df: pd.DataFrame) -> bool:
    """
    Validate if partitions of the data frame are partitioned along row axis.

    Parameters
    ----------
    df : pd.DataFrame
        Modin data frame

    Returns
    -------
    bool
    """
    # Unwrap partitions as they are currently stored (axis=None)
    partitions_shape = np.array(unwrap_partitions(df)).shape
    return partitions_shape[1] == 1  # type: ignore[no-any-return,unused-ignore]


FunctionType = TypeVar("FunctionType", bound=Callable[..., Any])


def modin_repartition(function: FunctionType) -> FunctionType:
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
        axis: int | None = None,
        row_lengths: int | None = None,
        validate_partitions: bool = True,
        **kwargs: Any,
    ) -> Any:
        # Validate partitions and repartition Modin data frame along row (axis=0) axis
        # to avoid a situation where columns are split along multiple blocks
        if isinstance(df, ModinDataFrame):
            if validate_partitions and not _validate_partition_shape(df):
                _logger.warning(
                    "Partitions of this data frame are detected to be split along column axis. "
                    "The DataFrame will be automatically repartitioned along row axis to ensure "
                    "each partition can be processed independently."
                )
                axis = 0
            if axis is not None:
                df = from_partitions(unwrap_partitions(df, axis=axis), axis=axis, row_lengths=row_lengths)
        return function(df, *args, **kwargs)

    return wrapper  # type: ignore[return-value]
