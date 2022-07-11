"""Threading Module (PRIVATE)."""

import concurrent.futures
import itertools
import logging
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

import boto3

from awswrangler import _utils
from awswrangler._config import config

if TYPE_CHECKING or config.distributed:
    from awswrangler.distributed._pool import _RayPoolExecutor

_logger: logging.Logger = logging.getLogger(__name__)


def _get_executor(use_threads: Union[bool, int]) -> Union["_ThreadPoolExecutor", "_RayPoolExecutor"]:
    executor = _RayPoolExecutor if config.distributed else _ThreadPoolExecutor
    return executor(use_threads)  # type: ignore


class _ThreadPoolExecutor:
    def __init__(self, use_threads: Union[bool, int]):
        super().__init__()
        self._exec: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        if self._cpus > 1:
            self._exec = concurrent.futures.ThreadPoolExecutor(max_workers=self._cpus)  # pylint: disable=R1732

    def map(self, func: Callable[..., List[str]], boto3_session: boto3.Session, *iterables: Any) -> List[Any]:
        _logger.debug("Map: %s", func)
        if self._exec is not None:
            # Deserialize boto3 session into pickable object
            boto3_primitives = _utils.boto3_to_primitives(boto3_session=boto3_session)
            args = (itertools.repeat(boto3_primitives), *iterables)
            return list(self._exec.map(func, *args))
        # Single-threaded
        return list(map(func, *(itertools.repeat(boto3_session), *iterables)))  # type: ignore
