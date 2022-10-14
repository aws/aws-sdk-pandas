"""Modin on Ray S3 read parquet module (PRIVATE)."""
from typing import Any, Dict, Iterator, List, Optional, Union

import boto3
import modin.pandas as pd
from ray.data import read_datasource

from awswrangler.distributed.ray.datasources import PandasParquetDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin


def _read_parquet_distributed(  # pylint: disable=unused-argument
    paths: List[str],
    path_root: Optional[str],
    columns: Optional[List[str]],
    coerce_int96_timestamp_unit: Optional[str],
    use_threads: Union[bool, int],
    parallelism: int,
    arrow_kwargs: Dict[str, Any],
    boto3_session: Optional["boto3.Session"],
    version_ids: Optional[Dict[str, str]],
    s3_additional_kwargs: Optional[Dict[str, Any]],
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    dataset = read_datasource(
        datasource=PandasParquetDatasource(),  # type: ignore
        parallelism=parallelism,
        paths=paths,
        path_root=path_root,
        columns=columns,
        coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        arrow_kwargs=arrow_kwargs,
        version_ids=version_ids,
    )
    return _to_modin(dataset=dataset, to_pandas_kwargs=arrow_kwargs)
