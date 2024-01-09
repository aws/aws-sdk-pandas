"""Ray Module."""

import logging
from typing import Any, Callable

class RayLogger:
    def __init__(
        self,
        log_level: int = logging.INFO,
        format: str = "%(asctime)s::%(levelname)-2s::%(name)s::%(message)s",
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ): ...
    def get_logger(self, name: str | Any = None) -> logging.Logger: ...

def ray_logger(function: Callable[..., Any]) -> Callable[..., Any]: ...
def ray_remote(**options: Any) -> Callable[..., Any]: ...
def ray_get(futures: list[Any]) -> Any: ...
def initialize_ray(
    address: str | None = None,
    redis_password: str | None = None,
    ignore_reinit_error: bool | None = True,
    include_dashboard: bool | None = False,
    log_to_driver: bool | None = True,
    object_store_memory: int | None = None,
    cpu_count: int | None = None,
    gpu_count: int | None = 0,
) -> None: ...
