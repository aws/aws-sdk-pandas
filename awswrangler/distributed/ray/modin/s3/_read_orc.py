"""Modin on Ray S3 read text module (PRIVATE)."""
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import modin.pandas as pd
import pyarrow as pa
from ray.data import read_datasource
from ray.data.datasource import FastFileMetadataProvider

from awswrangler import _data_types
from awswrangler.distributed.ray.datasources import ArrowORCDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _read_orc_distributed(  # pylint: disable=unused-argument
    paths: List[str],
    path_root: Optional[str],
    schema: Optional[pa.schema],
    columns: Optional[List[str]],
    use_threads: Union[bool, int],
    parallelism: int,
    version_ids: Optional[Dict[str, str]],
    s3_client: Optional["S3Client"],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    arrow_kwargs: Dict[str, Any],
) -> pd.DataFrame:
    ray_dataset = read_datasource(
        datasource=ArrowORCDatasource(),
        meta_provider=FastFileMetadataProvider(),
        parallelism=parallelism,
        use_threads=use_threads,
        paths=paths,
        schema=schema,
        columns=columns,
        path_root=path_root,
    )
    to_pandas_kwargs = _data_types.pyarrow2pandas_defaults(
        use_threads=use_threads,
        kwargs=arrow_kwargs,
    )
    return _to_modin(dataset=ray_dataset, to_pandas_kwargs=to_pandas_kwargs)
