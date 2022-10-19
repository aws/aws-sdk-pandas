"""Modin on Ray S3 read text module (PRIVATE)."""
from typing import Any, Dict, List, Optional, Union

import boto3
import logging
import modin.pandas as pd
from ray.data import read_datasource

from awswrangler import exceptions
from awswrangler.distributed.ray.datasources import (
    ArrowCSVDatasource,
    PandasCSVDataSource,
    PandasFWFDataSource,
    PandasJSONDatasource,
)
from awswrangler.distributed.ray.modin._utils import _to_modin


_logger: logging.Logger = logging.getLogger(__name__)


def _can_use_pyarrow_csv(
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_id_dict: Dict[str, Optional[str]],
    pandas_kwargs: Dict[str, Any],
) -> bool:
    if s3_additional_kwargs:
        return False

    if {key: value for key, value in version_id_dict.items() if value is not None}:
        return False

    if pandas_kwargs:
        return False

    return True


def _create_datasource(
    read_format: str,
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_id_dict: Dict[str, Optional[str]],
    pandas_kwargs: Dict[str, Any],
) -> Any:
    if read_format == "csv":

        if _can_use_pyarrow_csv(s3_additional_kwargs, version_id_dict, pandas_kwargs):
            return ArrowCSVDatasource()
        else:
            _logger.warn("Cannot use PyArrow")
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
    boto3_session: Optional[boto3.Session],
) -> pd.DataFrame:
    ray_dataset = read_datasource(
        datasource=_create_datasource(
            read_format,
            s3_additional_kwargs,
            version_id_dict,
            pandas_kwargs,
        ),
        parallelism=parallelism,
        paths=paths,
        path_root=path_root,
        dataset=dataset,
        version_id_dict=version_id_dict,
        s3_additional_kwargs=s3_additional_kwargs,
        pandas_kwargs=pandas_kwargs,
    )
    modin_frame = _to_modin(dataset=ray_dataset, ignore_index=ignore_index)
    return modin_frame
