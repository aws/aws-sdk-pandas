"""Modin on Ray Core module (PRIVATE)."""
# pylint: disable=import-outside-toplevel
import logging
from functools import wraps
from typing import Any, Callable, List, Optional

import pandas as pd
import ray
from modin.distributed.dataframe.pandas import from_partitions, unwrap_partitions
from modin.pandas import DataFrame as ModinDataFrame

_logger: logging.Logger = logging.getLogger(__name__)


def _validate_partition_cols(df: pd.DataFrame) -> bool:
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

    @ray.remote
    def _validate_partition(df: pd.DataFrame, n_columns: int) -> bool:
        return len(df.columns) == n_columns

    # Unwrap partitions as they are currently stored (axis=None)
    # Partitions are a 2D array because the data frame is split along both row and column axis
    partitions: List[List[ray.types.ObjectRef[pd.DataFrame]]] = unwrap_partitions(df, axis=None)
    return all(
        ray.get(
            [
                _validate_partition.remote(partition, len(df.columns))
                for partitions_row in partitions
                for partition in partitions_row
            ]
        )
    )


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
        axis: Optional[int] = None,
        row_lengths: Optional[int] = None,
        validate_partitions: bool = True,
        **kwargs: Any,
    ) -> Any:
        # Validate partitions and repartition Modin data frame along row (axis=0) axis
        # to avoid a situation where columns are split along multiple blocks
        if isinstance(df, ModinDataFrame):
            if validate_partitions and not _validate_partition_cols(df):
                _logger.warning(
                    "Partitions of this data frame are detected to be split along column axis. "
                    "The dataframe will be automatically repartitioned along row axis to ensure "
                    "each partition can be processed independently."
                )
                axis = 0
            if axis is not None:
                df = from_partitions(unwrap_partitions(df, axis=axis), axis=axis, row_lengths=row_lengths)
        return function(df, *args, **kwargs)

    return wrapper
