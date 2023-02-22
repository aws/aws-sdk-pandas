"""Modin on Ray S3 read parquet module (PRIVATE)."""
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import modin.pandas as pd
import pyarrow as pa
import pyarrow.parquet
from ray.data import read_datasource

from awswrangler.distributed.ray.datasources import ArrowParquetDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


_logger: logging.Logger = logging.getLogger(__name__)


def _read_parquet_metadata_file_distributed(
    s3_client: Optional["S3Client"],
    path: str,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_id: Optional[str] = None,
    coerce_int96_timestamp_unit: Optional[str] = None,
) -> Optional[pa.schema]:
    fs = pyarrow.fs.S3FileSystem()
    schema: pa.schema

    try:
        with fs.open_input_file(path[len("s3://") :]) as f:
            pq_file = pyarrow.parquet.read_metadata(f)
            schema = pq_file.schema.to_arrow_schema()

    except pyarrow.ArrowInvalid as ex:
        if str(ex) == "Parquet file size is 0 bytes":
            _logger.warning("Ignoring empty file...")
            return None
        raise ex

    return schema


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
