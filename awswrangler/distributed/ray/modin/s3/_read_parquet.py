"""Modin on Ray S3 read parquet module (PRIVATE)."""
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import modin.pandas as pd
from ray.data import read_datasource

from awswrangler.distributed.ray.datasources import ParquetDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin

if TYPE_CHECKING:
    import boto3
    import pyarrow as pa


def _read_parquet_distributed(  # pylint: disable=unused-argument
    paths: List[str],
    path_root: Optional[str],
    schema: "pa.schema",
    columns: Optional[List[str]],
    coerce_int96_timestamp_unit: Optional[str],
    boto3_session: Optional["boto3.Session"],
    use_threads: Union[bool, int],
    parallelism: int,
    version_ids: Optional[Dict[str, str]],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    arrow_kwargs: Dict[str, Any],
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    dataset_kwargs = {}
    if coerce_int96_timestamp_unit:
        dataset_kwargs["coerce_int96_timestamp_unit"] = coerce_int96_timestamp_unit
    dataset = read_datasource(
        datasource=ParquetDatasource(),  # type: ignore
        parallelism=parallelism,
        use_threads=use_threads,
        paths=paths,
        schema=schema,
        columns=columns,
        dataset_kwargs=dataset_kwargs,
        path_root=path_root,
    )
    return _to_modin(dataset=dataset, to_pandas_kwargs=arrow_kwargs)
