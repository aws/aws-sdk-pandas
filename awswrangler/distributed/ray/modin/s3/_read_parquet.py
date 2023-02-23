"""Modin on Ray S3 read parquet module (PRIVATE)."""
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import modin.pandas as pd
import pyarrow as pa
from ray.data import read_datasource
from ray.data.datasource import FastFileMetadataProvider
from ray.exceptions import RayTaskError

from awswrangler.distributed.ray.datasources import ArrowParquetBaseDatasource, ArrowParquetDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def _resolve_datasource_parameters(bulk_read_parquet: bool) -> Dict[str, Any]:
    if bulk_read_parquet:
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
    bulk_read_parquet: bool,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    try:
        dataset = read_datasource(
            **_resolve_datasource_parameters(bulk_read_parquet),
            parallelism=parallelism,
            use_threads=use_threads,
            paths=paths,
            schema=schema,
            columns=columns,
            path_root=path_root,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        )
        dataset.fully_executed()
    except RayTaskError as e:
        raise e.cause

    return _to_modin(dataset=dataset, to_pandas_kwargs=arrow_kwargs, ignore_index=bool(path_root))
