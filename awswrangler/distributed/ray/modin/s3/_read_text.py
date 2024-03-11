"""Modin on Ray S3 read text module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypedDict

import modin.pandas as pd
from pyarrow import csv
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
    "skip_blank_lines": ParamConfig(default=True),
    "escapechar": ParamConfig(default=False),
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
    read_options: dict[str, Any]
    parse_options: dict[str, Any]


def _parse_csv_configuration(
    pandas_kwargs: dict[str, Any],
) -> dict[str, CSVReadConfiguration]:
    _check_parameters(pandas_kwargs, _CSV_SUPPORTED_PARAMS)

    read_options = csv.ReadOptions(
        use_threads=False,
        column_names=pandas_kwargs.get("names", _CSV_SUPPORTED_PARAMS["names"].default),
    )
    parse_options = csv.ParseOptions(
        delimiter=pandas_kwargs.get("sep", _CSV_SUPPORTED_PARAMS["sep"].default),
        quote_char=pandas_kwargs.get("quotechar", _CSV_SUPPORTED_PARAMS["quotechar"].default),
        double_quote=pandas_kwargs.get("doublequote", _CSV_SUPPORTED_PARAMS["doublequote"].default),
        escape_char=pandas_kwargs.get("escapechar", _CSV_SUPPORTED_PARAMS["escapechar"].default),
        ignore_empty_lines=pandas_kwargs.get("skip_blank_lines", _CSV_SUPPORTED_PARAMS["skip_blank_lines"].default),
    )
    convert_options = csv.ConvertOptions()

    return {
        "arrow_csv_args": CSVReadConfiguration(
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )
    }


def _parse_json_configuration(
    pandas_kwargs: dict[str, Any],
) -> dict[str, JSONReadConfiguration]:
    _check_parameters(pandas_kwargs, _JSON_SUPPORTED_PARAMS)

    # json.ReadOptions and json.ParseOptions cannot be pickled for some reason so we're building a Python dict
    return {
        "arrow_json_args": JSONReadConfiguration(
            read_options=dict(use_threads=False),
            parse_options={},
        )
    }


def _parse_configuration(
    file_format: str,
    version_ids: dict[str, str] | None,
    s3_additional_kwargs: dict[str, str] | None,
    pandas_kwargs: dict[str, Any],
) -> dict[str, CSVReadConfiguration] | dict[str, JSONReadConfiguration]:
    if version_ids:
        raise exceptions.InvalidArgument("Specific version ID found for object")

    if s3_additional_kwargs:
        raise exceptions.InvalidArgument(f"Additional S3 args specified: {s3_additional_kwargs}")

    if file_format == "csv":
        return _parse_csv_configuration(pandas_kwargs)

    if file_format == "json":
        return _parse_json_configuration(pandas_kwargs)

    raise exceptions.InvalidArgument(f"File is in the {file_format} format")


def _resolve_datasource(
    read_format: str,
    can_use_arrow: bool,
    *args: Any,
    **kwargs: Any,
) -> Any:
    if read_format == "csv":
        return ArrowCSVDatasource(*args, **kwargs) if can_use_arrow else PandasCSVDataSource(*args, **kwargs)
    if read_format == "fwf":
        return PandasFWFDataSource(*args, **kwargs)
    if read_format == "json":
        return ArrowJSONDatasource(*args, **kwargs) if can_use_arrow else PandasJSONDatasource(*args, **kwargs)
    raise exceptions.UnsupportedType("Unsupported read format")


def _read_text_distributed(
    read_format: str,
    paths: list[str],
    path_root: str | None,
    use_threads: bool | int,
    s3_client: "S3Client" | None,
    s3_additional_kwargs: dict[str, str] | None,
    dataset: bool,
    ignore_index: bool,
    parallelism: int,
    version_ids: dict[str, str] | None,
    pandas_kwargs: dict[str, Any],
) -> pd.DataFrame:
    try:
        configuration: dict[str, Any] = _parse_configuration(
            read_format,
            version_ids,
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
        datasource=_resolve_datasource(
            read_format,
            can_use_arrow,
            paths,
            dataset,
            path_root,
            version_ids=version_ids,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            meta_provider=FastFileMetadataProvider(),
            **configuration,
        ),
        parallelism=parallelism,
    )
    return _to_modin(dataset=ray_dataset, ignore_index=ignore_index)
