"""Threading Module (PRIVATE)."""

import itertools
import logging
from typing import Any, Callable, List

import boto3

_logger: logging.Logger = logging.getLogger(__name__)


class _RayPoolExecutor:
    def __init__(self) -> None:
        pass

    def map(self, func: Callable[..., Any], _: boto3.Session, *args: Any) -> List[Any]:
        """Map func and return ray futures."""
        _logger.debug("Ray map: %s", func)
        # Discard boto3.Session object & return futures
        return list(func(*arg) for arg in zip(itertools.repeat(None), *args))
