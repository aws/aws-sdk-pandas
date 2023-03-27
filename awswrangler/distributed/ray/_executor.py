"""Ray Executor Module (PRIVATE)."""

import itertools
import logging
from typing import TYPE_CHECKING, Any, Callable, List, Optional, TypeVar, Union

from ray.util.multiprocessing import Pool

from awswrangler._executor import _BaseExecutor

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger: logging.Logger = logging.getLogger(__name__)

MapOutputType = TypeVar("MapOutputType")


class _RayExecutor(_BaseExecutor):
    def map(self, func: Callable[..., MapOutputType], _: Optional["BaseClient"], *args: Any) -> List[MapOutputType]:
        """Map func and return ray futures."""
        _logger.debug("Ray map: %s", func)
        # Discard boto3 client
        return list(func(*arg) for arg in zip(itertools.repeat(None), *args))


class _RayMultiprocessingPoolExecutor(_BaseExecutor):
    def __init__(self, max_concurrency: int) -> None:
        super().__init__()

        self._exec: Pool = Pool(processes=max_concurrency)

    def map(self, func: Callable[..., MapOutputType], _: Optional["BaseClient"], *args: Any) -> List[MapOutputType]:
        """Map func and return ray futures."""
        _logger.debug("Ray map: %s", func)

        # Discard boto3 client
        iterables = (itertools.repeat(None), *args)
        return [self._exec.apply(func, arg) for arg in zip(*iterables)]


def _get_ray_executor(use_threads: Union[bool, int], **kwargs: Any) -> _BaseExecutor:  # pylint: disable=unused-argument
    parallelism: Optional[int] = kwargs.get("parallelism")
    return _RayMultiprocessingPoolExecutor(parallelism) if parallelism else _RayExecutor()
