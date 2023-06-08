import json
import logging
import re
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union, cast
from awswrangler import _utils, exceptions

import boto3
import botocore

redis = _utils.import_optional_dependency("redis")

from awswrangler import _utils, exceptions
from awswrangler.annotations import Experimental


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
    """
    In order to connect to the elasti-cache cluster you need to be in the same VPC.
    Since the elasti-cache cluster cannot be accessed outside of the VPC.

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
    except Exception as e:
        _logger.error("Error connecting to Redis cluster.")
        raise e

    return ec
