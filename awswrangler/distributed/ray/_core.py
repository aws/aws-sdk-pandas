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
        logging_level: int = logging.INFO,
        format: str = "%(asctime)s::%(levelname)-2s::%(name)s::%(message)s",  # pylint: disable=redefined-builtin
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ):
        logging.basicConfig(level=logging_level, format=format, datefmt=datefmt)

    def get_logger(self, name: Union[str, Any] = None) -> Optional[logging.Logger]:
        """Return logger object."""
        return logging.getLogger(name)


@apply_configs
def ray_logger(
    function: Callable[..., Any],
    configure_logging: bool = True,
    logging_level: int = logging.INFO,
) -> Callable[..., Any]:
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
        if configure_logging:
            RayLogger(logging_level=logging_level).get_logger(name=function.__name__)
        return function(*args, **kwargs)

    return wrapper


def ray_remote(**options: Any) -> Callable[..., Any]:
    """
    Decorate with @ray.remote providing .options().

    Parameters
    ----------
    options : Any
        Ray remote options

    Returns
    -------
    Callable[..., Any]
    """

    def remote_decorator(function: Callable[..., Any]) -> Callable[..., Any]:
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
            remote_fn = ray.remote(ray_logger(function))
            if options:
                remote_fn = remote_fn.options(**options)
            return remote_fn.remote(*args, **kwargs)

        return wrapper

    return remote_decorator


def ray_get(futures: Union["ray.ObjectRef[Any]", List["ray.ObjectRef[Any]"]]) -> Any:
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
        return ray.get(futures)  # type: ignore[attr-defined]
    return futures


@apply_configs
def initialize_ray(
    address: Optional[str] = None,
    redis_password: Optional[str] = None,
    ignore_reinit_error: bool = True,
    include_dashboard: Optional[bool] = False,
    configure_logging: bool = True,
    log_to_driver: bool = False,
    logging_level: int = logging.INFO,
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
    configure_logging : Optional[bool]
        Boolean flag indicating whether or not to enable logging, by default True
    log_to_driver : bool
        Boolean flag to enable routing of all worker logs to the driver, by default False
    logging_level : int
        Logging level, defaults to logging.INFO. Ignored unless "configure_logging" is True
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
                configure_logging=configure_logging,
                log_to_driver=log_to_driver,
                logging_level=logging_level,
            )
        else:
            ray_runtime_env_vars = [
                "__MODIN_AUTOIMPORT_PANDAS__",
            ]

            ray_init_kwargs = {
                "num_cpus": cpu_count,
                "num_gpus": gpu_count,
                "include_dashboard": include_dashboard,
                "ignore_reinit_error": ignore_reinit_error,
                "configure_logging": configure_logging,
                "log_to_driver": log_to_driver,
                "logging_level": logging_level,
                "object_store_memory": object_store_memory,
                "_redis_password": redis_password,
                "_memory": object_store_memory,
                "runtime_env": {
                    "env_vars": {var: os.environ.get(var) for var in ray_runtime_env_vars if os.environ.get(var)}
                },
            }
            _logger.info("Starting a Ray cluster")
            ray.init(**ray_init_kwargs)
