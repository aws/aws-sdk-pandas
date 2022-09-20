"""Distributed Module (PRIVATE)."""

import logging
import multiprocessing
import os
import sys
import warnings
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

from awswrangler._config import apply_configs, config

class RayLogger:
    def __init__(
        self,
        log_level: int = logging.INFO,
        format: str = "%(asctime)s::%(levelname)-2s::%(name)s::%(message)s",
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ): ...
    def get_logger(self, name: Union[str, Any] = None) -> logging.Logger: ...

def ray_get(futures: List[Any]) -> List[Any]: ...
def ray_remote(function: Callable[..., Any]) -> Callable[..., Any]: ...
def initialize_ray(
    address: Optional[str] = None,
    redis_password: Optional[str] = None,
    ignore_reinit_error: Optional[bool] = True,
    include_dashboard: Optional[bool] = False,
    log_to_driver: Optional[bool] = True,
    object_store_memory: Optional[int] = None,
    cpu_count: Optional[int] = None,
    gpu_count: Optional[int] = 0,
) -> None: ...
def _get_ray_object_store_memory() -> Optional[int]: ...
