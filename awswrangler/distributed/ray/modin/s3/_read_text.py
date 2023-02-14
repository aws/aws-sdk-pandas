"""Modin on Ray S3 read text module (PRIVATE)."""
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypedDict, Union

import modin.pandas as pd
from pyarrow import csv, json
from ray.data import read_datasource
from ray.data.datasource import FastFileMetadataProvider

from awswrangler import exceptions
from awswrangler.distributed.ray.datasources import (
    ArrowCSVDatasource,
    ArrowJSONDatasource,
    PandasCSVDataSource,
    PandasFWFDataSource,
    PandasJSONDatasource,
)
from awswrangler.distributed.ray.modin._utils import ParamConfig, _check_parameters, _to_modin

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)

_CSV_SUPPORTED_PARAMS = {
    "sep": ParamConfig(default=","),
    "delimiter": ParamConfig(default=","),
    "quotechar": ParamConfig(default='"'),
    "doublequote": ParamConfig(default=True),
    "names": ParamConfig(default=None),
}

_JSON_SUPPORTED_PARAMS = {
    "orient": ParamConfig(default="columns", supported_values={"records"}),
    "lines": ParamConfig(default=False, supported_values={True}),
}


class CSVReadConfiguration(TypedDict):
    read_options: csv.ReadOptions
    parse_options: csv.ParseOptions
    convert_options: csv.ConvertOptions


class JSONReadConfiguration(TypedDict):
    read_options: json.ReadOptions
    parse_options: json.ParseOptions


def _parse_csv_configuration(
    pandas_kwargs: Dict[str, Any],
) -> CSVReadConfiguration:
    _check_parameters(pandas_kwargs, _CSV_SUPPORTED_PARAMS)

    read_options = csv.ReadOptions(
        use_threads=False,
        column_names=pandas_kwargs.get("names", _CSV_SUPPORTED_PARAMS["names"].default),
    )
    parse_options = csv.ParseOptions(
        delimiter=pandas_kwargs.get("sep", _CSV_SUPPORTED_PARAMS["sep"].default),
        quote_char=pandas_kwargs.get("quotechar", _CSV_SUPPORTED_PARAMS["quotechar"].default),
        double_quote=pandas_kwargs.get("doublequote", _CSV_SUPPORTED_PARAMS["doublequote"].default),
    )
    convert_options = csv.ConvertOptions()

    return CSVReadConfiguration(
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )


def _parse_json_configuration(
    pandas_kwargs: Dict[str, Any],
) -> JSONReadConfiguration:
    _check_parameters(pandas_kwargs, _JSON_SUPPORTED_PARAMS)

    return JSONReadConfiguration(
        read_options=json.ReadOptions(use_threads=False),
        parse_options=json.ParseOptions(),
    )


def _parse_configuration(
    file_format: str,
    version_ids: Dict[str, Optional[str]],
    s3_additional_kwargs: Optional[Dict[str, str]],
    pandas_kwargs: Dict[str, Any],
) -> Union[CSVReadConfiguration, JSONReadConfiguration]:
    if {key: value for key, value in version_ids.items() if value is not None}:
        raise exceptions.InvalidArgument("Specific version ID found for object")

    if s3_additional_kwargs:
        raise exceptions.InvalidArgument(f"Additional S3 args specified: {s3_additional_kwargs}")

    if file_format == "csv":
        return _parse_csv_configuration(pandas_kwargs)

    if file_format == "json":
        return _parse_json_configuration(pandas_kwargs)

    raise exceptions.InvalidArgument(f"File is in the {format} format")


def _resolve_format(read_format: str, can_use_arrow: bool) -> Any:
    if read_format == "csv":
        return ArrowCSVDatasource() if can_use_arrow else PandasCSVDataSource()
    if read_format == "fwf":
        return PandasFWFDataSource()
    if read_format == "json":
        return ArrowJSONDatasource() if can_use_arrow else PandasJSONDatasource()
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
    s3_client: Optional["S3Client"],
) -> pd.DataFrame:
    try:
        configuration: Dict[str, Any] = _parse_configuration(  # type: ignore[assignment]
            read_format,
            version_id_dict,
            s3_additional_kwargs,
            pandas_kwargs,
        )
        can_use_arrow = True
    except exceptions.InvalidArgument as e:
        _logger.warning(
            "PyArrow method unavailable, defaulting to Pandas I/O functions: %s. "
            "This will result in slower performance of the read operations",
            e,
        )
        configuration = {}
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
        meta_provider=FastFileMetadataProvider(),
        **configuration,
    )
    return _to_modin(dataset=ray_dataset, ignore_index=ignore_index)
