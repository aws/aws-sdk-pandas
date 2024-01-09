"""Executor Module (PRIVATE)."""

from __future__ import annotations

import concurrent.futures
import itertools
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from awswrangler import _utils
from awswrangler._distributed import engine

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger: logging.Logger = logging.getLogger(__name__)


MapOutputType = TypeVar("MapOutputType")


class _BaseExecutor(ABC):
    def __init__(self) -> None:
        _logger.debug("Creating an %s executor: ", self.__class__)

    @abstractmethod
    def map(
        self,
        func: Callable[..., MapOutputType],
        boto3_client: "BaseClient" | None,
        *args: Any,
    ) -> list[MapOutputType]:
        pass


class _ThreadPoolExecutor(_BaseExecutor):
    def __init__(self, use_threads: bool | int):
        super().__init__()
        self._exec: concurrent.futures.ThreadPoolExecutor | None = None
        self._cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        if self._cpus > 1:
            _logger.debug("Initializing ThreadPoolExecutor with %d workers", self._cpus)
            self._exec = concurrent.futures.ThreadPoolExecutor(max_workers=self._cpus)

    def map(
        self, func: Callable[..., MapOutputType], boto3_client: "BaseClient" | None, *args: Any
    ) -> list[MapOutputType]:
        """Map iterables to multi-threaded function."""
        _logger.debug("Map: %s", func)
        if self._exec is not None:
            iterables = (itertools.repeat(boto3_client), *args)
            return list(self._exec.map(func, *iterables))
        # Single-threaded
        return list(map(func, *(itertools.repeat(boto3_client), *args)))


@engine.dispatch_on_engine
def _get_executor(use_threads: bool | int, **kwargs: Any) -> _BaseExecutor:
    # kwargs allows for parameter that will be used by other variants of this function,
    # such as `parallelism` for _get_ray_executor
    return _ThreadPoolExecutor(use_threads)
