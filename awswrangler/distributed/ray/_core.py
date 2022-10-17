"""Ray Module."""
import logging
import os
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

from awswrangler._config import apply_configs
from awswrangler._distributed import EngineEnum, engine

if engine.get() == EngineEnum.RAY or TYPE_CHECKING:
    import ray

_logger: logging.Logger = logging.getLogger(__name__)


class RayLogger:
    """Create discrete Logger instance for Ray Tasks."""

    def __init__(
        self,
        log_level: int = logging.DEBUG,
        format: str = "%(asctime)s::%(levelname)-2s::%(name)s::%(message)s",  # pylint: disable=redefined-builtin
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ):
        logging.basicConfig(level=log_level, format=format, datefmt=datefmt)

    def get_logger(self, name: Union[str, Any] = None) -> Optional[logging.Logger]:
        """Return logger object."""
        return logging.getLogger(name)


def ray_logger(function: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorate callable to add RayLogger.

    Parameters
    ----------
    function : Callable[..., Any]
        Callable as input to decorator.

    Returns
    -------
    Callable[..., Any]
    """

    @wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        RayLogger().get_logger(name=function.__name__)
        return function(*args, **kwargs)

    return wrapper


def ray_remote(function: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorate callable to wrap within ray.remote.

    Parameters
    ----------
    function : Callable[..., Any]
        Callable as input to ray.remote.

    Returns
    -------
    Callable[..., Any]
    """
    # Access the source function if it exists
    function = getattr(function, "_source_func", function)

    @wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return ray.remote(ray_logger(function)).remote(*args, **kwargs)  # type: ignore

    return wrapper


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
    if engine.get() == EngineEnum.RAY:
        return ray.get(futures)
    return futures


@apply_configs
def initialize_ray(
    address: Optional[str] = None,
    redis_password: Optional[str] = None,
    ignore_reinit_error: bool = True,
    include_dashboard: Optional[bool] = False,
    log_to_driver: bool = False,
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
    ignore_reinit_error : bool
        If true, Ray suppress errors from calling ray.init() twice, by default True
    include_dashboard : Optional[bool]
        Boolean flag indicating whether or not to start the Ray dashboard, by default False
    log_to_driver : bool
        Boolean flag to enable routing of all worker logs to the driver, by default False
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
            _logger.info("Connecting to a Ray cluster at: %s", address)
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
