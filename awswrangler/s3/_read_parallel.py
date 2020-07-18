"""Amazon S3 PARALLEL Read Module (PRIVATE)."""

import io
import logging
import math
import multiprocessing as mp
from typing import Any, Callable, Dict, Iterator, List, cast

import boto3  # type: ignore
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def _calculate_chunks_sizes(total_size: int, max_size: int) -> Iterator[int]:
    num: int = int(math.ceil(float(total_size) / float(max_size)))
    min_size: int = int(total_size / num)
    rest: int = total_size % num
    for _ in range(num):
        if rest > 0:
            rest -= 1
            yield min_size + 1
        else:
            yield min_size


def _calculate_nthreads(paths: List[str], cpus: int) -> int:
    nthreads: int = int(len(paths) / cpus)
    return nthreads if nthreads > 0 else 1


def _remote_parser(
    send_end, func: Callable, nthreads: int, boto3_primitives: _utils.Boto3PrimitivesType, func_kwargs: Dict[str, Any]
) -> None:
    boto3_session: boto3.Session = _utils.boto3_from_primitives(primitives=boto3_primitives)
    func_kwargs["boto3_session"] = boto3_session
    df: pd.DataFrame = pa.serialize(func(**func_kwargs)).to_buffer(nthreads=nthreads)
    offset: int = 0
    for size in _calculate_chunks_sizes(total_size=df.size, max_size=100_000):
        send_end.send_bytes(df, offset=offset, size=size)
        offset += size
    send_end.close()


def _launcher(
    func: Callable, paths: List[str], cpus: int, boto3_session: boto3.Session, **func_kwargs,
) -> Iterator[mp.connection.Connection]:
    nthreads: int = _calculate_nthreads(paths=paths, cpus=cpus)
    for path in paths:
        recv_end, send_end = mp.Pipe(duplex=False)
        func_kwargs["path"] = path
        process = mp.Process(
            target=_remote_parser,
            args=(send_end, func, nthreads, _utils.boto3_to_primitives(boto3_session=boto3_session), func_kwargs),
        )
        process.start()
        send_end.close()
        yield recv_end


def _read_pipes(pipes: List[mp.connection.Connection], buffers: Dict[int, io.BytesIO], dfs: List[pd.DataFrame]):
    for pipe in mp.connection.wait(pipes):
        pipe = cast(mp.connection.Connection, pipe)
        buffer: io.BytesIO = buffers[pipe.fileno()]
        try:
            buffer.write(pipe.recv_bytes())
        except EOFError:
            pipes.remove(pipe)
            dfs.append(cast(pd.DataFrame, pa.deserialize(buffer.getbuffer())))


def _process_manager(
    func: Callable, paths: List[str], boto3_session: boto3.Session, **func_kwargs,
) -> List[pd.DataFrame]:
    cpus: int = _utils.ensure_cpu_count(use_threads=True)
    dfs: List[pd.DataFrame] = []
    pipes: List[mp.connection.Connection] = []
    buffers: Dict[int, io.BytesIO] = {}
    for pipe in _launcher(func=func, paths=paths, cpus=cpus, boto3_session=boto3_session, **func_kwargs):
        pipes.append(pipe)
        buffers[pipe.fileno()] = io.BytesIO()
        while len(pipes) >= cpus:
            _read_pipes(pipes=pipes, buffers=buffers, dfs=dfs)
    while pipes:
        _read_pipes(pipes=pipes, buffers=buffers, dfs=dfs)
    return dfs


def _read_parallel(
    func: Callable, paths: List[str], ignore_index: bool, boto3_session: boto3.Session, **func_kwargs,
) -> pd.DataFrame:
    return pd.concat(
        objs=_process_manager(func=func, paths=paths, boto3_session=boto3_session, **func_kwargs),
        ignore_index=ignore_index,
        sort=False,
        copy=False,
    )
