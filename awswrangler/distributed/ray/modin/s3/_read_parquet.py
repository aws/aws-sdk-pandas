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


def _resolve_datasource_parameters(bulk_read: bool, *args: Any, **kwargs: Any) -> dict[str, Any]:
    if bulk_read:
        return {
            "datasource": ArrowParquetBaseDatasource(*args, **kwargs),
            "meta_provider": FastFileMetadataProvider(),
        }
    return {
        "datasource": ArrowParquetDatasource(*args, **kwargs),
    }


def _read_parquet_distributed(
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
    decryption_properties: pa.parquet.encryption.DecryptionConfiguration | None = None,
) -> pd.DataFrame:
    dataset_kwargs = {}
    if coerce_int96_timestamp_unit:
        dataset_kwargs["coerce_int96_timestamp_unit"] = coerce_int96_timestamp_unit
    if decryption_properties:
        dataset_kwargs["decryption_properties"] = decryption_properties

    dataset = read_datasource(
        **_resolve_datasource_parameters(
            bulk_read,
            paths=paths,
            path_root=path_root,
            arrow_parquet_args={
                "use_threads": use_threads,
                "schema": schema,
                "columns": columns,
                "dataset_kwargs": dataset_kwargs,
            },
        ),
        parallelism=parallelism,
    )
    return _to_modin(
        dataset=dataset,
        to_pandas_kwargs=arrow_kwargs,
        ignore_index=arrow_kwargs.get("ignore_metadata"),
    )
