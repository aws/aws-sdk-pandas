"""Utilities Module."""

from typing import Any, List

import numpy as np  # type: ignore


def chunkify(lst: List[Any], n: int) -> List[List[Any]]:
    """Split a list in a List of List (chunks) with even sizes.

    Parameters
    ----------
    lst: List
        List of anything to be splitted.
    n: int
        Maximum number of chunks

    Returns
    -------
    List[List[Any]]
        List of List (chunks) with even sizes.

    Examples
    --------
    >>> from awswrangler.utils import chunkify
    >>> chunkify(list(range(13)), 3)
    [[0, 1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]

    """
    np_chunks = np.array_split(lst, n)
    return [arr.tolist() for arr in np_chunks]
