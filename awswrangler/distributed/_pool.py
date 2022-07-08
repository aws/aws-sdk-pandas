"""Threading Module (PRIVATE)."""

import itertools
import logging
from typing import Any, Callable, List, Optional, Union

import boto3
from ray.util.multiprocessing import Pool

_logger: logging.Logger = logging.getLogger(__name__)


class _RayPoolExecutor:
    def __init__(self, processes: Optional[Union[bool, int]] = None):
        self._exec: Pool = Pool(processes=None if isinstance(processes, bool) else processes)

    def map(self, func: Callable[..., List[str]], _: boto3.Session, *args: Any) -> List[Any]:
        """Map function and its args to Ray pool."""
        futures = []
        _logger.debug("Ray map: %s", func)
        # Discard boto3.Session object & call the fn asynchronously
        for arg in zip(itertools.repeat(None), *args):
            futures.append(self._exec.apply_async(func, arg))
        return [f.get() for f in futures]
