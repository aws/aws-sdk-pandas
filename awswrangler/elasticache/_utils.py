import logging
from typing import Optional


import boto3

from awswrangler import _utils

redis = _utils.import_optional_dependency("redis")

_logger: logging.Logger = logging.getLogger(__name__)


@_utils.check_optional_dependency(redis, "redis")
def connect(
    host: str,
    port: Optional[int] = 11211,
    timeout: int = 30,
    username: Optional[str] = None,
    password: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> "redis.Redis":
    """Create a connection to ElastiCache running Cluster

    Note
    -------

    In order to connect to the elasti-cache cluster you need to be in the same VPC.
    Since the elasti-cache cluster cannot be accessed outside of the VPC.

    We are using redis-py client to connect to the cluster

    Parameters
    ----------

    """
    try:
        ec = redis.Redis(
            host=host,
            port=port,
            username=username,
            password=password,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
        )
        ec.ping()
        _logger.info("Connected to Redis Cluster")
    except Exception as e:
        _logger.error("Error connecting to Redis cluster.")
        raise e

    return ec
