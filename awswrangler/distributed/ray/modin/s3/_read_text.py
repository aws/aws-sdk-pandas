"""Modin on Ray S3 read text module (PRIVATE)."""
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import modin.pandas as pd
from pyarrow import csv
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

_CSV_SUPPORTED_PARAMS_WITH_DEFAULTS = {
    "delimiter": ",",
    "quotechar": '"',
    "doublequote": True,
}


def _parse_csv_configuration(
    pandas_kwargs: Dict[str, Any],
) -> Tuple[csv.ReadOptions, csv.ParseOptions, csv.ConvertOptions]:
    for pandas_arg_key in pandas_kwargs:
        if pandas_arg_key not in _CSV_SUPPORTED_PARAMS_WITH_DEFAULTS:
            raise exceptions.InvalidArgument(f"Unsupported Pandas parameter for PyArrow loaded: {pandas_arg_key}")

    read_options = csv.ReadOptions(
        use_threads=False,
    )
    parse_options = csv.ParseOptions(
        delimiter=pandas_kwargs.get("delimiter", _CSV_SUPPORTED_PARAMS_WITH_DEFAULTS["delimiter"]),
        quote_char=pandas_kwargs.get("quotechar", _CSV_SUPPORTED_PARAMS_WITH_DEFAULTS["quotechar"]),
        double_quote=pandas_kwargs.get("doublequote", _CSV_SUPPORTED_PARAMS_WITH_DEFAULTS["doublequote"]),
    )
    convert_options = csv.ConvertOptions()

    return read_options, parse_options, convert_options


def _parse_configuration(
    file_format: str,
    version_ids: Dict[str, Optional[str]],
    s3_additional_kwargs: Optional[Dict[str, str]],
    pandas_kwargs: Dict[str, Any],
) -> Tuple[csv.ReadOptions, csv.ParseOptions, csv.ConvertOptions]:
    if {key: value for key, value in version_ids.items() if value is not None}:
        raise exceptions.InvalidArgument("Specific version ID found for object")

    if s3_additional_kwargs:
        raise exceptions.InvalidArgument(f"Additional S3 args specified: {s3_additional_kwargs}")

    if file_format == "csv":
        return _parse_csv_configuration(pandas_kwargs)

    raise exceptions.InvalidArgument()


def _resolve_format(read_format: str, can_use_arrow: bool) -> Any:
    if read_format == "csv":
        return ArrowCSVDatasource() if can_use_arrow else PandasCSVDataSource()
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
    try:
        read_options, parse_options, convert_options = _parse_configuration(
            read_format,
            version_id_dict,
            s3_additional_kwargs,
            pandas_kwargs,
        )
        can_use_arrow = True
    except exceptions.InvalidArgument as e:
        _logger.warning("PyArrow method unavailable, defaulting to Pandas I/O functions: %s", e)
        read_options, parse_options, convert_options = None, None, None
        can_use_arrow = False

    ray_dataset = read_datasource(
        datasource=_resolve_format(read_format, can_use_arrow),
        parallelism=parallelism,
        paths=paths,
        path_root=path_root,
        dataset=dataset,
        version_ids=version_id_dict,
        s3_additional_kwargs=s3_additional_kwargs,
        pandas_kwargs=pandas_kwargs,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )
    return _to_modin(dataset=ray_dataset, ignore_index=ignore_index)
