"""Modin on Ray S3 read text module (PRIVATE)."""
from typing import Any, Dict, List, Optional, Union

import boto3
import modin.pandas as pd
from ray.data import read_datasource

from awswrangler import exceptions
from awswrangler.distributed.ray.datasources import PandasCSVDataSource, PandasFWFDataSource, PandasJSONDatasource
from awswrangler.distributed.ray.modin._utils import _to_modin


def _resolve_format(read_format: str) -> Any:
    if read_format == "csv":
        return PandasCSVDataSource()
    if read_format == "fwf":
        return PandasFWFDataSource()
    if read_format == "json":
        return PandasJSONDatasource()
    raise exceptions.UnsupportedType("Unsupported read format")


def _read_text_distributed(  # pylint: disable=unused-argument
    read_format: str,
    paths: List[str],
    path_root: Optional[str],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
    ignore_index: bool,
    parallelism: int,
    version_id_dict: Dict[str, Optional[str]],
    pandas_kwargs: Dict[str, Any],
    use_threads: Union[bool, int],
    boto3_session: Optional["boto3.Session"],
) -> pd.DataFrame:
    ds = read_datasource(
        datasource=_resolve_format(read_format),
        parallelism=parallelism,
        paths=paths,
        path_root=path_root,
        dataset=dataset,
        version_id_dict=version_id_dict,
        s3_additional_kwargs=s3_additional_kwargs,
        pandas_kwargs=pandas_kwargs,
    )
    return _to_modin(dataset=ds, ignore_index=ignore_index)
