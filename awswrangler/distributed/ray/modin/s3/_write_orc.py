"""Modin on Ray S3 write parquet module (PRIVATE)."""
from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any, cast

import modin.pandas as pd
import pyarrow as pa
from ray.data.datasource.block_path_provider import DefaultBlockWritePathProvider

from awswrangler import exceptions
from awswrangler.distributed.ray.datasources import ArrowORCDatasink, UserProvidedKeyBlockWritePathProvider
from awswrangler.distributed.ray.modin._utils import _ray_dataset_from_df

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _to_orc_distributed(
    df: pd.DataFrame,
    schema: pa.Schema,
    index: bool,
    compression: str | None,
    compression_ext: str,
    pyarrow_additional_kwargs: dict[str, Any],
    cpus: int,
    dtype: dict[str, str],
    s3_client: "S3Client" | None,
    s3_additional_kwargs: dict[str, str] | None,
    use_threads: bool | int,
    path: str | None = None,
    path_root: str | None = None,
    filename_prefix: str | None = None,
    max_rows_by_file: int | None = 0,
    bucketing: bool = False,
) -> list[str]:
    if bucketing:
        # Add bucket id to the prefix
        path = f"{path_root}{filename_prefix}_bucket-{df.name:05d}{compression_ext}.orc"

    # Create Ray Dataset
    ds = _ray_dataset_from_df(df)

    if df.index.name is not None:
        raise exceptions.InvalidArgumentCombination("Orc does not serialize index metadata on a default index.")

    # Repartition into a single block if or writing into a single key or if bucketing is enabled
    if ds.count() > 0 and (path or bucketing) and not max_rows_by_file:
        _logger.warning(
            "Repartitioning frame to single partition as a strict path was defined: %s. "
            "This operation is inefficient for large datasets.",
            path,
        )
        ds = ds.repartition(1)

    # Repartition by max_rows_by_file
    elif max_rows_by_file and (max_rows_by_file > 0):
        ds = ds.repartition(math.ceil(ds.count() / max_rows_by_file))

    datasink = ArrowORCDatasink(
        path=cast(str, path or path_root),
        dataset_uuid=filename_prefix,
        # If user has provided a single key, use that instead of generating a path per block
        # The dataset will be repartitioned into a single block
        block_path_provider=UserProvidedKeyBlockWritePathProvider()
        if path and not path.endswith("/") and not max_rows_by_file
        else DefaultBlockWritePathProvider(),
        open_s3_object_args={
            "s3_additional_kwargs": s3_additional_kwargs,
        },
        index=index,
        dtype=dtype,
        compression=compression,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        schema=schema,
    )
    ds.write_datasink(datasink)
    return datasink.get_write_paths()
