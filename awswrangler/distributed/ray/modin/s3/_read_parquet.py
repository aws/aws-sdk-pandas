"""Modin on Ray S3 read parquet module (PRIVATE)."""
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import modin.pandas as pd
import pyarrow as pa
from ray.data import read_datasource
from ray.data.datasource import FastFileMetadataProvider

from awswrangler.distributed.ray.datasources import ArrowParquetBaseDatasource, ArrowParquetDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def _resolve_datasource_parameters(bulk_read: bool) -> Dict[str, Any]:
    if bulk_read:
        return {
            "datasource": ArrowParquetBaseDatasource(),
            "meta_provider": FastFileMetadataProvider(),
        }
    return {
        "datasource": ArrowParquetDatasource(),
    }


def _read_parquet_distributed(  # pylint: disable=unused-argument
    paths: List[str],
    path_root: Optional[str],
    schema: Optional[pa.schema],
    columns: Optional[List[str]],
    coerce_int96_timestamp_unit: Optional[str],
    use_threads: Union[bool, int],
    parallelism: int,
    version_ids: Optional[Dict[str, str]],
    s3_client: Optional["S3Client"],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    arrow_kwargs: Dict[str, Any],
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
