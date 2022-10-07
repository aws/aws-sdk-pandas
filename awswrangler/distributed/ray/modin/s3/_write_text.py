"""Modin on Ray S3 write text module (PRIVATE)."""
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import modin.pandas as pd
from modin.pandas import DataFrame as ModinDataFrame
from ray.data import from_modin, from_pandas
from ray.data.datasource.file_based_datasource import DefaultBlockWritePathProvider

from awswrangler import exceptions
from awswrangler.distributed.ray.datasources import (  # pylint: disable=ungrouped-imports
    PandasCSVDataSource,
    PandasJSONDatasource,
    PandasTextDatasource,
    UserProvidedKeyBlockWritePathProvider,
)
from awswrangler.s3._write import _COMPRESSION_2_EXT
from awswrangler.s3._write_text import _get_write_details

if TYPE_CHECKING:
    import boto3

_logger: logging.Logger = logging.getLogger(__name__)


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

    def _datasource_for_format(file_format: str) -> PandasTextDatasource:
        if file_format == "csv":
            return PandasCSVDataSource()

        if file_format == "json":
            return PandasJSONDatasource()

        raise RuntimeError(f"Unknown file format: {file_format}")

    datasource = _datasource_for_format(file_format)

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
        mode=mode,
        encoding=encoding,
        newline=newline,
        pandas_kwargs=pandas_kwargs,
    )

    return datasource.get_write_paths()
