"""Distributed Module (PRIVATE)."""

import multiprocessing
import os
import sys
import warnings
from typing import TYPE_CHECKING, Any, Callable, List, Optional

from awswrangler._config import apply_configs, config

if config.distributed or TYPE_CHECKING:
    import psutil
    import ray  # pylint: disable=import-error


def ray_get(futures: List[Any]) -> List[Any]:
    """
    Run ray.get on futures if distributed.

    Parameters
    ----------
    futures : List[Any]
        List of Ray futures

    Returns
    -------
    List[Any]
    """
    if config.distributed:
        return ray.get(futures)
    return futures


def ray_remote(function: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorate callable to wrap within ray.remote.

    Parameters
    ----------
    function : Callable[..., Any]
        Callable as input to ray.remote

    Returns
    -------
    Callable[..., Any]
    """
    if config.distributed:

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return ray.remote(function).remote(*args, **kwargs)

        return wrapper
    return function


@apply_configs
def initialize_ray(
    address: Optional[str] = None,
    redis_password: Optional[str] = None,
    ignore_reinit_error: Optional[bool] = True,
    include_dashboard: Optional[bool] = False,
    object_store_memory: Optional[int] = None,
    cpu_count: Optional[int] = None,
    gpu_count: Optional[int] = 0,
) -> None:
    """
    Connect to an existing Ray cluster or start one and connect to it.

    Parameters
    ----------
    address : Optional[str]
        Address of the Ray cluster to connect to, by default None
    redis_password : Optional[str]
        Password to the Redis cluster, by default None
    ignore_reinit_error : Optional[bool]
        If true, Ray suppress errors from calling ray.init() twice, by default True
    include_dashboard : Optional[bool]
        Boolean flag indicating whether or not to start the Ray dashboard, by default False
    object_store_memory : Optional[int]
        The amount of memory (in bytes) to start the object store with, by default None
    cpu_count : Optional[int]
        Number of CPUs to assign to each raylet, by default None
    gpu_count : Optional[int]
        Number of GPUs to assign to each raylet, by default 0
    """
    if not ray.is_initialized():
        if address:
            ray.init(
                address=address,
                include_dashboard=include_dashboard,
                ignore_reinit_error=ignore_reinit_error,
            )
        else:
            if not object_store_memory:
                object_store_memory = _get_ray_object_store_memory()

            mac_size_limit = getattr(ray.ray_constants, "MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT", None)
            if sys.platform == "darwin" and mac_size_limit is not None and object_store_memory > mac_size_limit:
                warnings.warn(
                    "On Macs, Ray's performance is known to degrade with "
                    + "object store size greater than "
                    + f"{mac_size_limit / 2 ** 30:.4} GiB. Ray by default does "
                    + "not allow setting an object store size greater than "
                    + "that. This default is overridden to avoid "
                    + "spilling to disk more often. To override this "
                    + "behavior, you can initialize Ray yourself."
                )
                os.environ["RAY_ENABLE_MAC_LARGE_OBJECT_STORE"] = "1"

            ray_init_kwargs = {
                "num_cpus": cpu_count or multiprocessing.cpu_count(),
                "num_gpus": gpu_count,
                "include_dashboard": include_dashboard,
                "ignore_reinit_error": ignore_reinit_error,
                "object_store_memory": object_store_memory,
                "_redis_password": redis_password,
                "_memory": object_store_memory,
                "runtime_env": {"env_vars": {"__MODIN_AUTOIMPORT_PANDAS__": "1"}},
            }
            ray.init(**ray_init_kwargs)


def _get_ray_object_store_memory() -> Optional[int]:
    virtual_memory = psutil.virtual_memory().total
    if sys.platform.startswith("linux"):
        shm_fd = os.open("/dev/shm", os.O_RDONLY)
        try:
            shm_stats = os.fstatvfs(shm_fd)
            system_memory = shm_stats.f_bsize * shm_stats.f_bavail
            if system_memory / (virtual_memory / 2) < 0.99:
                warnings.warn(
                    f"The size of /dev/shm is too small ({system_memory} bytes). The required size "
                    + f"is at least half of RAM ({virtual_memory // 2} bytes). Please, delete files "
                    + "in /dev/shm or increase the size with --shm-size in Docker. Alternatively, set the "
                    + "memory size for each Ray worker in bytes with the RAY_OBJECT_STORE_MEMORY env var."
                )
        finally:
            os.close(shm_fd)
    else:
        system_memory = virtual_memory
    object_store_memory: Optional[int] = int(0.6 * system_memory // 1e9 * 1e9)  # type: ignore
    # If the memory pool is smaller than 2GB, just use the default in ray.
    if object_store_memory == 0:
        object_store_memory = None
    return object_store_memory
