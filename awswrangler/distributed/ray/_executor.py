"""Ray Executor Module (PRIVATE)."""

import itertools
import logging
from typing import TYPE_CHECKING, Any, Callable, List, Optional, TypeVar, Union

import ray
import ray.actor

from awswrangler import engine
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


@ray.remote
class AsyncActor:
    async def run_concurrent(self, func: Callable[..., MapOutputType], *args: Any) -> MapOutputType:
        return func(*args)


class _RayMaxConcurrencyExecutor(_BaseExecutor):
    def __init__(self, max_concurrency: int) -> None:
        super().__init__()

        _logger.debug("Initializing Ray Actor with maximum concurrency %d", max_concurrency)
        self._actor: ray.actor.ActorHandle = AsyncActor.options(max_concurrency=max_concurrency).remote()  # type: ignore[attr-defined]

    def map(self, func: Callable[..., MapOutputType], _: Optional["BaseClient"], *args: Any) -> List[MapOutputType]:
        """Map func and return ray futures."""
        _logger.debug("Ray map: %s", func)

        # Discard boto3 client
        iterables = (itertools.repeat(None), *args)
        func_python = engine.dispatch_func(func, "python")

        return [self._actor.run_concurrent.remote(func_python, *arg) for arg in zip(*iterables)]


def _get_ray_executor(use_threads: Union[bool, int], **kwargs: Any) -> _BaseExecutor:  # pylint: disable=unused-argument
    # We want the _RayMaxConcurrencyExecutor only to be used when the `parallelism` parameter is specified
    parallelism: Optional[int] = kwargs.get("ray_parallelism")
    return _RayMaxConcurrencyExecutor(parallelism) if parallelism else _RayExecutor()
