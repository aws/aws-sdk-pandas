"""Modin on Ray S3 read parquet module (PRIVATE)."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import modin.pandas as pd
import pyarrow as pa
from ray.data import read_datasource
from ray.data.datasource import FastFileMetadataProvider

from awswrangler.distributed.ray.datasources import ArrowParquetBaseDatasource, ArrowParquetDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def _resolve_datasource_parameters(bulk_read: bool) -> dict[str, Any]:
    if bulk_read:
        return {
            "datasource": ArrowParquetBaseDatasource(),
            "meta_provider": FastFileMetadataProvider(),
        }
    return {
        "datasource": ArrowParquetDatasource(),
    }


def _read_parquet_distributed(  # pylint: disable=unused-argument
    paths: list[str],
    path_root: str | None,
    schema: pa.schema | None,
    columns: list[str] | None,
    coerce_int96_timestamp_unit: str | None,
    use_threads: bool | int,
    parallelism: int,
    version_ids: dict[str, str] | None,
    s3_client: "S3Client" | None,
    s3_additional_kwargs: dict[str, Any] | None,
    arrow_kwargs: dict[str, Any],
    bulk_read: bool,
) -> pd.DataFrame:
    dataset_kwargs = {}
    if coerce_int96_timestamp_unit:
        dataset_kwargs["coerce_int96_timestamp_unit"] = coerce_int96_timestamp_unit

    dataset = read_datasource(
        **_resolve_datasource_parameters(bulk_read),
        parallelism=parallelism,
        use_threads=use_threads,
        paths=paths,
        schema=schema,
        columns=columns,
        path_root=path_root,
        dataset_kwargs=dataset_kwargs,
    )
    return _to_modin(
        dataset=dataset,
        to_pandas_kwargs=arrow_kwargs,
        ignore_index=arrow_kwargs.get("ignore_metadata"),
    )
