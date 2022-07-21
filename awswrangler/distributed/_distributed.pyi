"""Distributed Module (PRIVATE)."""

import multiprocessing
import os
import sys
import warnings
from typing import TYPE_CHECKING, Any, Callable, List, Optional

from awswrangler._config import apply_configs, config

def ray_get(futures: List[Any]) -> List[Any]:...
def ray_remote(function: Callable[..., Any]) -> Callable[..., Any]:...
def initialize_ray(
    address: Optional[str] = None,
    redis_password: Optional[str] = None,
    ignore_reinit_error: Optional[bool] = True,
    include_dashboard: Optional[bool] = False,
    object_store_memory: Optional[int] = None,
    cpu_count: Optional[int] = None,
    gpu_count: Optional[int] = 0,
) -> None:...
def _get_ray_object_store_memory() -> Optional[int]:...