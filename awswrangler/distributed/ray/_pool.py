"""Ray Threading Module (PRIVATE)."""

import itertools
import logging
from typing import TYPE_CHECKING, Any, Callable, List, Optional

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger: logging.Logger = logging.getLogger(__name__)


class _RayPoolExecutor:
    def __init__(self) -> None:
        pass

    def map(self, func: Callable[..., Any], _: Optional["BaseClient"], *args: Any) -> List[Any]:
        """Map func and return ray futures."""
        _logger.debug("Ray map: %s", func)
        # Discard boto3 client
        return list(func(*arg) for arg in zip(itertools.repeat(None), *args))
