import itertools
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union


import boto3
import pyarrow as pa

import awswrangler.pandas as pd

from awswrangler import _utils
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler._executor import _get_executor
from awswrangler.distributed.ray import ray_get

_logger: logging.Logger = logging.getLogger(__name__)

redis = _utils.import_optional_dependency("redis")


@_utils.check_optional_dependency(redis, "redis")
def bulk_write_df(client: "redis.Redis", key_name: str, df: pd.DataFrame) -> None:
    """Bulk writes the dataframe data as bytes in ElastiCache cluster with the given key

    Parameters
    ----------



    """
    compressed_df = pa.serialize_pandas(df).to_pybytes()
    res = client.set(key_name, compressed_df)
    if res:
        _logger.info("%s cached to redis cluster", key_name)
    else:
        _logger.error("%s was not cached to redis cluster", key_name)
