"""Utilities Module."""

import multiprocessing as mp
from io import BytesIO
from logging import Logger, getLogger
from math import ceil
from os import cpu_count
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple

import numpy as np  # type: ignore
from boto3.s3.transfer import TransferConfig  # type: ignore

if TYPE_CHECKING:  # pragma: no cover
    from awswrangler.session import Session, _SessionPrimitives
    import boto3  # type: ignore

logger: Logger = getLogger(__name__)


def chunkify(lst: List[Any], num_chunks: int = 1, max_length: Optional[int] = None) -> List[List[Any]]:
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
    >>> from awswrangler._utils import chunkify
    >>> chunkify(list(range(13)), num_chunks=3)
    [[0, 1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
    >>> chunkify(list(range(13)), max_length=4)
    [[0, 1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]

    """
    n: int = num_chunks if max_length is None else int(ceil((float(len(lst)) / float(max_length))))
    np_chunks = np.array_split(lst, n)
    return [arr.tolist() for arr in np_chunks if len(arr) > 0]


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
    >>> from awswrangler._utils import get_cpu_count
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


def parallelize(func: Callable,
                session: "Session",
                lst: List[Any],
                extra_args: Tuple = None,
                has_return: bool = False,
                **kwargs) -> List[Any]:
    """Do a generic parallelization boilerplate abstracted."""
    extra_args = tuple() if extra_args is None else extra_args
    primitives: "_SessionPrimitives" = session.primitives
    cpus: int = get_cpu_count(parallel=True)
    chunks: List[List[str]] = chunkify(lst=lst, num_chunks=cpus)
    procs: List[mp.Process] = []
    receive_pipes: List[mp.connection.Connection] = []
    return_list: list = []
    for chunk in chunks:
        args: Tuple = (primitives, chunk) + extra_args
        if has_return is True:
            receive_pipe: mp.connection.Connection
            send_pipe: mp.connection.Connection
            receive_pipe, send_pipe = mp.Pipe()
            receive_pipes.append(receive_pipe)
            args = (send_pipe, ) + args
        proc: mp.Process = mp.Process(target=func, args=args, kwargs=kwargs)
        proc.daemon = False
        proc.start()
        procs.append(proc)
    for i in range(len(procs)):
        if has_return is True:
            logger.debug(f"Waiting pipe number: {i}")
            return_list.append(receive_pipes[i].recv())
            logger.debug(f"Closing pipe number: {i}")
            receive_pipes[i].close()
        logger.debug(f"Waiting proc number: {i}")
        procs[i].join()
    return return_list


def parse_path(path: str) -> Tuple[str, str]:
    """Split a full S3 path in bucket and key strings.

    "s3://bucket/key" -> ("bucket", "key")

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/key).

    Returns
    -------
    Tuple[str, str]
        Tuple of bucket and key strings

    Examples
    --------
    >>> from awswrangler._utils import parse_path
    >>> bucket, key = parse_path("s3://bucket/key")

    """
    parts = path.replace("s3://", "").split("/", 1)
    bucket: str = parts[0]
    key: str = ""
    if len(parts) == 2:
        key = key if parts[1] is None else parts[1]
    return bucket, key


def upload_fileobj(s3_client: "boto3.session.Session.client", file_obj: BytesIO, path: str, cpus: int) -> None:
    """Upload object from memory to Amazon S3."""
    bucket: str
    key: str
    bucket, key = parse_path(path=path)
    if cpus > 1:
        config: TransferConfig = TransferConfig(max_concurrency=cpus, use_threads=True)
    else:
        config = TransferConfig(max_concurrency=1, use_threads=False)
    s3_client.upload_fileobj(Fileobj=file_obj, Bucket=bucket, Key=key, Config=config)


def download_fileobj(s3_client: "boto3.session.Session.client", path: str, cpus: int) -> BytesIO:
    """Download object from Amazon S3 to memory."""
    bucket: str
    key: str
    bucket, key = parse_path(path=path)
    if cpus > 1:
        config: TransferConfig = TransferConfig(max_concurrency=cpus, use_threads=True)
    else:
        config = TransferConfig(max_concurrency=1, use_threads=False)
    file_obj: BytesIO = BytesIO()
    s3_client.download_fileobj(Fileobj=file_obj, Bucket=bucket, Key=key, Config=config)
    file_obj.seek(0)
    return file_obj
