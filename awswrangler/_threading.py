"""Threading Module (PRIVATE)."""

import concurrent.futures
import itertools
import logging
from typing import TYPE_CHECKING, Any, Callable, List, Optional, TypeVar, Union

from awswrangler import _utils
from awswrangler._distributed import EngineEnum, engine

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger: logging.Logger = logging.getLogger(__name__)


MapOutputType = TypeVar("MapOutputType")


class _ThreadPoolExecutor:
    def __init__(self, use_threads: Union[bool, int]):
        super().__init__()
        self._exec: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        if self._cpus > 1:
            self._exec = concurrent.futures.ThreadPoolExecutor(max_workers=self._cpus)  # pylint: disable=R1732

    def map(
        self, func: Callable[..., MapOutputType], boto3_client: Optional["BaseClient"], *iterables: Any
    ) -> List[MapOutputType]:
        """Map iterables to multi-threaded function."""
        _logger.debug("Map: %s", func)
        if self._exec is not None:
            args = (itertools.repeat(boto3_client), *iterables)
            return list(self._exec.map(func, *args))
        # Single-threaded
        return list(map(func, *(itertools.repeat(boto3_client), *iterables)))


def _get_executor(use_threads: Union[bool, int]) -> _ThreadPoolExecutor:
    if engine.get() == EngineEnum.RAY:
        from awswrangler.distributed.ray._pool import _RayPoolExecutor  # pylint: disable=import-outside-toplevel

        return _RayPoolExecutor()  # type: ignore
    return _ThreadPoolExecutor(use_threads)
