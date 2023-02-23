"""Modin on Ray S3 read parquet module (PRIVATE)."""
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import modin.pandas as pd
import pyarrow as pa
import pyarrow.parquet
from ray.data import read_datasource

from awswrangler.distributed.ray import ray_remote
from awswrangler.distributed.ray.datasources import ArrowParquetDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin
from awswrangler.s3._read_parquet import _pyarrow_parquet_file_wrapper

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


_logger: logging.Logger = logging.getLogger(__name__)


@ray_remote()
def _read_parquet_metadata_file_distributed(
    s3_client: Optional["S3Client"],
    path: str,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_id: Optional[str] = None,
    coerce_int96_timestamp_unit: Optional[str] = None,
) -> Optional[pa.schema]:
    fs = pyarrow.fs.S3FileSystem()
    path_without_s3_prefix = path[len("s3://") :]

    with fs.open_input_file(path_without_s3_prefix) as f:
        pq_file = _pyarrow_parquet_file_wrapper(
            source=f,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        )

        if pq_file:
            return pq_file.schema.to_arrow_schema()

    return None


def _read_parquet_distributed(  # pylint: disable=unused-argument
    paths: List[str],
    path_root: Optional[str],
    schema: "pa.schema",
    columns: Optional[List[str]],
    coerce_int96_timestamp_unit: Optional[str],
    use_threads: Union[bool, int],
    parallelism: int,
    version_ids: Optional[Dict[str, str]],
    s3_client: Optional["S3Client"],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    arrow_kwargs: Dict[str, Any],
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    dataset_kwargs = {}
    if coerce_int96_timestamp_unit:
        dataset_kwargs["coerce_int96_timestamp_unit"] = coerce_int96_timestamp_unit
    dataset = read_datasource(
        datasource=ArrowParquetDatasource(),
        parallelism=parallelism,
        use_threads=use_threads,
        paths=paths,
        schema=schema,
        columns=columns,
        dataset_kwargs=dataset_kwargs,
        path_root=path_root,
    )
    return _to_modin(dataset=dataset, to_pandas_kwargs=arrow_kwargs, ignore_index=bool(path_root))
