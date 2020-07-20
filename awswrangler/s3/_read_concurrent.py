"""Amazon S3 CONCURRENT Read Module (PRIVATE)."""

import concurrent.futures
import itertools
import logging
from typing import Any, Callable, Dict, List

import boto3  # type: ignore
import pandas as pd  # type: ignore

from awswrangler import _utils
from awswrangler.s3._read import _union

_logger: logging.Logger = logging.getLogger(__name__)


def _caller(
    path: str, func: Callable, boto3_primitives: _utils.Boto3PrimitivesType, func_kwargs: Dict[str, Any]
) -> pd.DataFrame:
    boto3_session: boto3.Session = _utils.boto3_from_primitives(primitives=boto3_primitives)
    func_kwargs["path"] = path
    func_kwargs["boto3_session"] = boto3_session
    return func(**func_kwargs)


def _read_concurrent(
    func: Callable, paths: List[str], ignore_index: bool, boto3_session: boto3.Session, **func_kwargs,
) -> pd.DataFrame:
    cpus: int = _utils.ensure_cpu_count(use_threads=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
        return _union(
            dfs=list(
                executor.map(
                    _caller,
                    paths,
                    itertools.repeat(func),
                    itertools.repeat(_utils.boto3_to_primitives(boto3_session=boto3_session)),
                    itertools.repeat(func_kwargs),
                )
            ),
            ignore_index=ignore_index,
        )
