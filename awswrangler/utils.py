"""Utilities Module."""

from math import ceil
from os import cpu_count
from typing import Any, List, Optional

import numpy as np  # type: ignore


def chunkify(lst: List[Any],
             num_chunks: int = 1,
             max_length: Optional[int] = None) -> List[List[Any]]:
    """Split a list in a List of List (chunks) with even sizes.

    Parameters
    ----------
    lst: List
        List of anything to be splitted.
    num_chunks: int, optional
        Maximum number of chunks.
    max_length: int, optional
        Max length of each chunk. Has priority over num_chunks.

    Returns
    -------
    List[List[Any]]
        List of List (chunks) with even sizes.

    Examples
    --------
    >>> from awswrangler.utils import chunkify
    >>> chunkify(list(range(13)), num_chunks=3)
    [[0, 1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
    >>> chunkify(list(range(13)), max_length=4)
    [[0, 1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]

    """
    n: int = num_chunks if max_length is None else int(
        ceil((float(len(lst)) / float(max_length))))
    np_chunks = np.array_split(lst, n)
    return [arr.tolist() for arr in np_chunks]


def get_cpu_count(parallel: bool = True) -> int:
    """Get the number of cpu cores to be used.

    Note
    ----
    In case of `parallel=True` the number of process that could be spawned will be get from os.cpu_count().

    Parameters
    ----------
    parallel : bool
            True to enable parallelism, False to disable.

    Returns
    -------
    int
        Number of cpu cores to be used.

    Examples
    --------
    >>> from awswrangler.utils import get_cpu_count
    >>> get_cpu_count(parallel=True)
    4
    >>> get_cpu_count(parallel=False)
    1

    """
    cpus: int = 1
    if parallel is True:
        cpu_cnt: Optional[int] = cpu_count()
        if cpu_cnt is not None:
            cpus = cpu_cnt if cpu_cnt > cpus else cpus
    return cpus
