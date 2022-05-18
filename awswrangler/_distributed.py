"""Distributed Module (PRIVATE)."""

import importlib.util
import multiprocessing
import os
import sys
import warnings
from typing import Any, Callable

from awswrangler.s3._read import _block_to_df

_ray_found = importlib.util.find_spec("ray")
if _ray_found:
    import ray


def _ray_remote(function: Callable[..., Any]) -> Any:
    if _ray_found:

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return ray.remote(function).remote(*args, **kwargs)

        return wrapper
    return function


def _initialize_ray() -> None:
    redis_address = os.getenv("RAY_ADDRESS")
    redis_password = os.getenv("REDIS_PASSWORD", ray.ray_constants.REDIS_DEFAULT_PASSWORD)

    if not ray.is_initialized() or redis_address:  # pylint: disable=too-many-nested-blocks
        if redis_address:
            ray.init(
                address=redis_address,
                include_dashboard=False,
                ignore_reinit_error=True,
                _redis_password=redis_password,
            )
        else:
            object_store_memory = int(os.getenv("RAY_OBJECT_STORE_MEMORY", 0))
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
                "num_cpus": int(os.getenv("RAY_CPU_COUNT", multiprocessing.cpu_count())),
                "num_gpus": int(os.getenv("RAY_GPU_COUNT", "0")),
                "include_dashboard": False,
                "ignore_reinit_error": True,
                "object_store_memory": object_store_memory,
                "_redis_password": redis_password,
                "_memory": object_store_memory,
            }
            ray.init(**ray_init_kwargs)


def to_modin(ds: ray.data.Dataset[Any], **pandas_kwargs: Any) -> Any:
    from modin.distributed.dataframe.pandas.partitions import from_partitions

    block_to_df = ray.data.impl.remote_fn.cached_remote_fn(_block_to_df)
    pd_objs = [block_to_df.remote(block, **pandas_kwargs) for block in ds.get_internal_block_refs()]
    return from_partitions(pd_objs, axis=0)
