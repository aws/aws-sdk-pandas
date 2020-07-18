"""Amazon S3 PARALLEL Read Module (PRIVATE)."""

import concurrent.futures
import datetime
import itertools
import multiprocessing as mp
import io
import math
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import boto3  # type: ignore
import pandas as pd  # type: ignore
import pandas.io.parsers  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.lib  # type: ignore
import pyarrow.parquet  # type: ignore
import s3fs  # type: ignore
from pandas.io.common import infer_compression  # type: ignore

from awswrangler import _data_types, _utils, catalog, exceptions
from awswrangler.s3._list import path2list

_logger: logging.Logger = logging.getLogger(__name__)


def _calculate_sizes(total_size: int, max_size: int) -> Iterator[int]:
    num: int = int(math.ceil(float(total_size) / float(max_size)))
    min_size: int = int(total_size / num)
    rest: int = total_size % num
    for _ in range(num):
        if rest > 0:
            rest -= 1
            yield min_size + 1
        else:
            yield min_size


def _remote_parser(send_end, parser_func: Callable, nthreads: int, kwargs) -> None:
    df: pd.DataFrame = pa.serialize(parser_func(**kwargs)).to_buffer(nthreads=nthreads)
    offset: int = 0
    for size in _calculate_sizes(total_size=df.size, max_size=100_000):
        send_end.send_bytes(df, offset=offset, size=size)
        offset += size
    send_end.close()


def _launcher(
    parser_func: Callable,
    boto3_session: boto3.Session,
    **parser_kwargs,
) -> List[pd.DataFrame]:
    cpus: int = _utils.ensure_cpu_count(use_threads=True)
    readers = []
    buffers = {}
    for _ in range(cpus):
        recv_end, send_end = mp.Pipe(duplex=False)
        process = mp.Process(target=_remote_parser, args=(
            send_end,
            parser_func,
            parser_kwargs,

        ))
        process.start()
        send_end.close()
        readers.append(recv_end)
        descriptor = str(recv_end.fileno())
        buffers[descriptor] = io.BytesIO()

    dfs = []
    while readers:
        for reader in mp.connection.wait(readers):
            buffer = buffers[str(reader.fileno())]
            try:
                buffer.write(reader.recv_bytes())
            except EOFError:
                readers.remove(reader)
                dfs.append(pa.deserialize(buffer.getbuffer()))

    return dfs


def _read_parallel(
    parser_func: Callable,
    ignore_index: bool,
    boto3_session: boto3.Session,
    **parser_kwargs,
) -> pd.DataFrame:
    return pd.concat(
        objs=_launcher(
            parser_func=parser_func,
            boto3_session=boto3_session,
            **parser_kwargs
        ),
        ignore_index=ignore_index,
        sort=False,
        copy=False,
    )
