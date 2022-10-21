"""Modin on Ray S3 write text module (PRIVATE)."""
import logging
from typing import Any, Dict, List, Optional, Union

import boto3
import modin.pandas as pd
from modin.pandas import DataFrame as ModinDataFrame
from ray.data import from_modin, from_pandas
from ray.data.datasource.file_based_datasource import DefaultBlockWritePathProvider

from awswrangler import exceptions
from awswrangler.distributed.ray.datasources import (  # pylint: disable=ungrouped-imports
    ArrowCSVDatasource,
    PandasCSVDataSource,
    PandasJSONDatasource,
    UserProvidedKeyBlockWritePathProvider,
)
from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource
from awswrangler.distributed.ray.modin._utils import ParamConfig, _check_parameters
from awswrangler.s3._write import _COMPRESSION_2_EXT
from awswrangler.s3._write_text import _get_write_details

_logger: logging.Logger = logging.getLogger(__name__)


_CSV_SUPPORTED_PARAMS: Dict[str, ParamConfig] = {
    "header": ParamConfig(default=True),
    "sep": ParamConfig(default=",", supported_values={","}),
    "index": ParamConfig(default=True, supported_values={True}),
    "compression": ParamConfig(default=None, supported_values={None}),
    "quoting": ParamConfig(default=None, supported_values={None}),
    "escapechar": ParamConfig(default=None, supported_values={None}),
    "date_format": ParamConfig(default=None, supported_values={None}),
}


def _parse_csv_configuration(
    pandas_kwargs: Dict[str, Any],
) -> Dict[str, Any]:
    _check_parameters(pandas_kwargs, _CSV_SUPPORTED_PARAMS)

    # csv.WriteOptions cannot be pickled for some reason so we're building a Python dict
    return {
        "include_header": pandas_kwargs.get("header", _CSV_SUPPORTED_PARAMS["header"].default),
    }


def _parse_configuration(
    file_format: str,
    s3_additional_kwargs: Optional[Dict[str, str]],
    pandas_kwargs: Dict[str, Any],
) -> Dict[str, Any]:
    if s3_additional_kwargs:
        raise exceptions.InvalidArgument(f"Additional S3 args specified: {s3_additional_kwargs}")

    if file_format == "csv":
        return _parse_csv_configuration(pandas_kwargs)

    raise exceptions.InvalidArgument()


def _datasource_for_format(read_format: str, can_use_arrow: bool) -> PandasFileBasedDatasource:
    if read_format == "csv":
        return ArrowCSVDatasource() if can_use_arrow else PandasCSVDataSource()
    if read_format == "json":
        return PandasJSONDatasource()
    raise exceptions.UnsupportedType("Unsupported read format")


def _to_text_distributed(  # pylint: disable=unused-argument
    df: pd.DataFrame,
    file_format: str,
    use_threads: Union[bool, int],
    boto3_session: Optional["boto3.Session"],
    s3_additional_kwargs: Optional[Dict[str, str]],
    path: Optional[str] = None,
    path_root: Optional[str] = None,
    filename_prefix: Optional[str] = None,
    bucketing: bool = False,
    **pandas_kwargs: Any,
) -> List[str]:
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")

    if bucketing:
        # Add bucket id to the prefix
        prefix = f"{filename_prefix}_bucket-{df.name:05d}"
        extension = f"{file_format}{_COMPRESSION_2_EXT.get(pandas_kwargs.get('compression'))}"

        file_path = f"{path_root}{prefix}.{extension}"
        path = file_path

    elif path is None and path_root is not None:
        file_path = path_root
    elif path is not None and path_root is None:
        file_path = path
    else:
        raise RuntimeError("path and path_root received at the same time.")

    # Create Ray Dataset
    ds = from_modin(df) if isinstance(df, ModinDataFrame) else from_pandas(df)

    # Repartition into a single block if or writing into a single key or if bucketing is enabled
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

    datasource = _datasource_for_format(file_format, can_use_arrow)

    mode, encoding, newline = _get_write_details(path=file_path, pandas_kwargs=pandas_kwargs)
    ds.write_datasource(
        datasource=datasource,
        path=file_path,
        block_path_provider=(
            UserProvidedKeyBlockWritePathProvider()
            if path and not path.endswith("/")
            else DefaultBlockWritePathProvider()
        ),
        file_path=file_path,
        dataset_uuid=filename_prefix,
        boto3_session=None,
        s3_additional_kwargs=s3_additional_kwargs,
        mode="wb" if can_use_arrow else mode,
        encoding=encoding,
        newline=newline,
        pandas_kwargs=pandas_kwargs,
        write_options=write_options,
    )

    return datasource.get_write_paths()
