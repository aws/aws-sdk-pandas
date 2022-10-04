"""Ray Module."""
import logging
import os
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

from awswrangler._config import apply_configs, config

if TYPE_CHECKING or config.execution_engine == "ray":
    import ray  # pylint: disable=import-error

    if config.memory_format == "modin":
        from modin.distributed.dataframe.pandas import from_partitions, unwrap_partitions
        from modin.pandas import DataFrame as ModinDataFrame

_logger: logging.Logger = logging.getLogger(__name__)


class RayLogger:
    """Create discrete Logger instance for Ray Tasks."""

    def __init__(
        self,
        log_level: int = logging.INFO,
        format: str = "%(asctime)s::%(levelname)-2s::%(name)s::%(message)s",  # pylint: disable=redefined-builtin
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ):
        logging.basicConfig(level=log_level, format=format, datefmt=datefmt)

    def get_logger(self, name: Union[str, Any] = None) -> Union[logging.Logger, Any]:
        """Return logger object."""
        return logging.getLogger(name) if config.execution_engine == "ray" else None


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
    if config.execution_engine == "ray":
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
    if config.execution_engine == "ray":

        @wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return ray.remote(function).remote(*args, **kwargs)

        return wrapper
    return function


def modin_repartition(function: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorate callable to repartition Modin data frame.

    By default, repartition along row (axis=0) axis.
    This avoids a situation where columns are split along multiple blocks.

    Parameters
    ----------
    function : Callable[..., Any]
        Callable as input to ray.remote

    Returns
    -------
    Callable[..., Any]
    """

    @wraps(function)
    def wrapper(df, *args: Any, axis=0, row_lengths=None, **kwargs: Any) -> Any:
        if isinstance(df, ModinDataFrame) and axis is not None:
            # Repartition Modin data frame along row (axis=0) axis
            # to avoid a situation where columns are split along multiple blocks
            df = from_partitions(unwrap_partitions(df, axis=axis), axis=axis, row_lengths=row_lengths)
        return function(df, *args, **kwargs)

    return wrapper


@apply_configs
def initialize_ray(
    address: Optional[str] = None,
    redis_password: Optional[str] = None,
    ignore_reinit_error: Optional[bool] = True,
    include_dashboard: Optional[bool] = False,
    log_to_driver: Optional[bool] = True,
    object_store_memory: Optional[int] = None,
    cpu_count: Optional[int] = None,
    gpu_count: Optional[int] = None,
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
    log_to_driver : Optional[bool]
        Boolean flag to enable routing of all worker logs to the driver, by default True
    object_store_memory : Optional[int]
        The amount of memory (in bytes) to start the object store with, by default None
    cpu_count : Optional[int]
        Number of CPUs to assign to each raylet, by default None
    gpu_count : Optional[int]
        Number of GPUs to assign to each raylet, by default None
    """
    if not ray.is_initialized():
        # Detect an existing cluster
        ray_address = os.environ.get("RAY_ADDRESS")
        if not address and ray_address:
            _logger.info("Using address %s set in the environment variable RAY_ADDRESS", ray_address)
            address = ray_address

        if address:
            ray.init(
                address=address,
                include_dashboard=include_dashboard,
                ignore_reinit_error=ignore_reinit_error,
                log_to_driver=log_to_driver,
            )
        else:
            ray_runtime_env_vars = [
                "__MODIN_AUTOIMPORT_PANDAS__",
            ]

            ray_init_kwargs = {
                "address": "local",
                "num_cpus": cpu_count,
                "num_gpus": gpu_count,
                "include_dashboard": include_dashboard,
                "ignore_reinit_error": ignore_reinit_error,
                "log_to_driver": log_to_driver,
                "object_store_memory": object_store_memory,
                "_redis_password": redis_password,
                "_memory": object_store_memory,
                "runtime_env": {
                    "env_vars": {var: os.environ.get(var) for var in ray_runtime_env_vars if os.environ.get(var)}
                },
            }
            _logger.info("Starting a local Ray cluster")
            ray.init(**ray_init_kwargs)
