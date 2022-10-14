"""Modin on Ray S3 write parquet module (PRIVATE)."""
import logging
import math
from typing import Any, Dict, List, Optional, Union

import boto3
import modin.pandas as pd
import pyarrow as pa
from modin.pandas import DataFrame as ModinDataFrame
from ray.data import from_modin, from_pandas
from ray.data.datasource.file_based_datasource import DefaultBlockWritePathProvider

from awswrangler.distributed.ray.datasources import PandasParquetDatasource, UserProvidedKeyBlockWritePathProvider

_logger: logging.Logger = logging.getLogger(__name__)


def _to_parquet_distributed(  # pylint: disable=unused-argument
    df: pd.DataFrame,
    schema: "pa.Schema",
    index: bool,
    compression: Optional[str],
    compression_ext: str,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]],
    cpus: int,
    dtype: Dict[str, str],
    boto3_session: Optional["boto3.Session"],
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    path: Optional[str] = None,
    path_root: Optional[str] = None,
    filename_prefix: Optional[str] = "",
    max_rows_by_file: Optional[int] = 0,
    bucketing: bool = False,
) -> List[str]:
    if bucketing:
        # Add bucket id to the prefix
        path = f"{path_root}{filename_prefix}_bucket-{df.name:05d}{compression_ext}.parquet"
    # Create Ray Dataset
    ds = from_modin(df) if isinstance(df, ModinDataFrame) else from_pandas(df)
    # Repartition into a single block if or writing into a single key or if bucketing is enabled
    if ds.count() > 0 and (path or bucketing):
        _logger.warning(
            "Repartitioning frame to single partition as a strict path was defined: %s. "
            "This operation is inefficient for large datasets.",
            path,
        )
        ds = ds.repartition(1)
    # Repartition by max_rows_by_file
    elif max_rows_by_file and (max_rows_by_file > 0):
        ds = ds.repartition(math.ceil(ds.count() / max_rows_by_file))
    datasource = PandasParquetDatasource()
    ds.write_datasource(
        datasource=datasource,
        path=path or path_root,
        path_root=path_root,
        # If user provided a key, the dataset will be repartitioned into a single block and written
        # into that specific key instead of generating a key pre block under a prefix
        block_path_provider=(
            UserProvidedKeyBlockWritePathProvider()
            if path and not path.endswith("/")
            else DefaultBlockWritePathProvider()
        ),
        dataset_uuid=filename_prefix,
        boto3_session=None,
        s3_additional_kwargs=s3_additional_kwargs,
        pandas_kwargs={},
        schema=schema,
        index=index,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
    )
    return datasource.get_write_paths()
