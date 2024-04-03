"""Modin on Ray S3 write parquet module (PRIVATE)."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any, cast

import modin.pandas as pd
import pyarrow as pa

from awswrangler import exceptions
from awswrangler.distributed.ray.datasources import ArrowParquetDatasink
from awswrangler.distributed.ray.modin._utils import _ray_dataset_from_df
from awswrangler.typing import ArrowEncryptionConfiguration

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _to_parquet_distributed(
    df: pd.DataFrame,
    schema: "pa.Schema",
    index: bool,
    compression: str | None,
    compression_ext: str,
    pyarrow_additional_kwargs: dict[str, Any] | None,
    cpus: int,
    dtype: dict[str, str],
    s3_client: "S3Client" | None,
    s3_additional_kwargs: dict[str, str] | None,
    use_threads: bool | int,
    path: str | None = None,
    path_root: str | None = None,
    filename_prefix: str | None = "",
    max_rows_by_file: int | None = 0,
    bucketing: bool = False,
    encryption_configuration: ArrowEncryptionConfiguration | None = None,
) -> list[str]:
    # Create Ray Dataset
    ds = _ray_dataset_from_df(df)
    # Repartition into a single block if or writing into a single key or if bucketing is enabled
    if ds.count() > 0 and (path or bucketing) and not max_rows_by_file:
        _logger.warning(
            "Repartitioning frame to single partition as a strict path was defined: %s. "
            "This operation is inefficient for large datasets.",
            path,
        )

        if index and df.index.name:
            raise exceptions.InvalidArgumentCombination(
                "Cannot write a named index when repartitioning to a single file"
            )

        ds = ds.repartition(1)
    # Repartition by max_rows_by_file
    elif max_rows_by_file and (max_rows_by_file > 0):
        if index:
            raise exceptions.InvalidArgumentCombination(
                "Cannot write indexed file when `max_rows_by_file` is specified"
            )
        ds = ds.repartition(math.ceil(ds.count() / max_rows_by_file))

        if path and not path.endswith("/"):
            path = f"{path}/"

    datasink = ArrowParquetDatasink(
        path=cast(str, path or path_root),
        dataset_uuid=filename_prefix,
        index=index,
        dtype=dtype,
        compression=compression,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        schema=schema,
        bucket_id=df.name if bucketing else None,
    )
    ds.write_datasink(datasink)
    return datasink.get_write_paths()
