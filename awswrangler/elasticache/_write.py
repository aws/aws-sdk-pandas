import logging

import pyarrow as pa
import awswrangler.pandas as pd

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)

redis = _utils.import_optional_dependency("redis")


@_utils.check_optional_dependency(redis, "redis")
def cache_df(
    client: "redis.Redis",
    key_name: str,
    df: pd.DataFrame,
) -> None:
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
