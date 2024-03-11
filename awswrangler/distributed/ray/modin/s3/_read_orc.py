"""Modin on Ray S3 read text module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

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


def _read_orc_distributed(
    paths: list[str],
    path_root: str | None,
    schema: pa.schema | None,
    columns: list[str] | None,
    use_threads: bool | int,
    parallelism: int,
    version_ids: dict[str, str] | None,
    s3_client: "S3Client" | None,
    s3_additional_kwargs: dict[str, Any] | None,
    arrow_kwargs: dict[str, Any],
) -> pd.DataFrame:
    datasource = ArrowORCDatasource(
        paths=paths,
        dataset=True,
        path_root=path_root,
        use_threads=use_threads,
        schema=schema,
        arrow_orc_args={"columns": columns},
        meta_provider=FastFileMetadataProvider(),
    )
    ray_dataset = read_datasource(
        datasource,
        parallelism=parallelism,
    )
    to_pandas_kwargs = _data_types.pyarrow2pandas_defaults(
        use_threads=use_threads,
        kwargs=arrow_kwargs,
    )
    return _to_modin(dataset=ray_dataset, to_pandas_kwargs=to_pandas_kwargs)
