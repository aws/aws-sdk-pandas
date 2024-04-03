"""Modin on Ray S3 write text module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import modin.pandas as pd

from awswrangler import exceptions
from awswrangler.distributed.ray.datasources import (
    ArrowCSVDatasink,
    PandasCSVDatasink,
    PandasJSONDatasink,
    _BlockFileDatasink,
)
from awswrangler.distributed.ray.modin._utils import ParamConfig, _check_parameters, _ray_dataset_from_df
from awswrangler.s3._write_text import _get_write_details

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


_CSV_SUPPORTED_PARAMS: dict[str, ParamConfig] = {
    "header": ParamConfig(default=True),
    "sep": ParamConfig(default=",", supported_values={","}),
    "index": ParamConfig(default=True, supported_values={True}),
    "compression": ParamConfig(default=None, supported_values={None}),
    "quoting": ParamConfig(default=None, supported_values={None}),
    "escapechar": ParamConfig(default=None, supported_values={None}),
    "date_format": ParamConfig(default=None, supported_values={None}),
    "columns": ParamConfig(default=None, supported_values={None}),
}


def _parse_csv_configuration(
    pandas_kwargs: dict[str, Any],
) -> dict[str, Any]:
    _check_parameters(pandas_kwargs, _CSV_SUPPORTED_PARAMS)

    # csv.WriteOptions cannot be pickled for some reason so we're building a Python dict
    return {
        "include_header": pandas_kwargs.get("header", _CSV_SUPPORTED_PARAMS["header"].default),
    }


def _parse_configuration(
    file_format: str,
    s3_additional_kwargs: dict[str, str] | None,
    pandas_kwargs: dict[str, Any],
) -> dict[str, Any]:
    if s3_additional_kwargs:
        raise exceptions.InvalidArgument(f"Additional S3 args specified: {s3_additional_kwargs}")

    if file_format == "csv":
        return _parse_csv_configuration(pandas_kwargs)

    raise exceptions.InvalidArgument(f"File is in the {file_format} format")


def _datasink_for_format(
    write_format: str,
    can_use_arrow: bool,
    *args: Any,
    **kwargs: Any,
) -> _BlockFileDatasink:
    if write_format == "csv":
        return ArrowCSVDatasink(*args, **kwargs) if can_use_arrow else PandasCSVDatasink(*args, **kwargs)
    if write_format == "json":
        return PandasJSONDatasink(*args, **kwargs)
    raise exceptions.UnsupportedType(f"Unsupported write format {write_format}")


def _to_text_distributed(
    df: pd.DataFrame,
    file_format: str,
    use_threads: bool | int,
    s3_client: "S3Client" | None,
    s3_additional_kwargs: dict[str, str] | None,
    path: str | None = None,
    path_root: str | None = None,
    filename_prefix: str | None = None,
    bucketing: bool = False,
    **pandas_kwargs: Any,
) -> list[str]:
    if df.empty is True:
        _logger.warning("Empty DataFrame will be written.")

    # Create Ray Dataset
    ds = _ray_dataset_from_df(df)

    # Repartition into a single block if writing into a single key or if bucketing is enabled
    if ds.count() > 0 and path:
        ds = ds.repartition(1)
        _logger.warning(
            "Repartitioning frame to single partition as a strict path was defined: %s. "
            "This operation is inefficient for large datasets.",
            path,
        )

    # Figure out which data source to use, and convert PyArrow parameters if needed
    try:
        write_options = _parse_configuration(
            file_format,
            s3_additional_kwargs,
            pandas_kwargs,
        )
        can_use_arrow = True
    except exceptions.InvalidArgument as e:
        _logger.warning(
            "PyArrow method unavailable, defaulting to Pandas I/O functions: %s. "
            "This will result in slower performance of the write operations.",
            e,
        )
        write_options = None
        can_use_arrow = False

    mode, encoding, newline = _get_write_details(path=path or path_root, pandas_kwargs=pandas_kwargs)  # type: ignore[arg-type]

    datasink: _BlockFileDatasink = _datasink_for_format(
        file_format,
        can_use_arrow,
        path or path_root,
        dataset_uuid=filename_prefix,
        open_s3_object_args={
            "mode": "wb" if can_use_arrow else mode,
            "encoding": encoding,
            "newline": newline,
            "s3_additional_kwargs": s3_additional_kwargs,
        },
        pandas_kwargs=pandas_kwargs,
        write_options=write_options,
        bucket_id=df.name if bucketing else None,
    )

    ds.write_datasink(datasink)

    return datasink.get_write_paths()
