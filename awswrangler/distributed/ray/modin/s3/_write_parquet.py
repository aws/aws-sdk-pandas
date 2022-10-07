"""Modin on Ray S3 write parquet module (PRIVATE)."""
import math
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import modin.pandas as pd
from modin.pandas import DataFrame as ModinDataFrame
from ray.data import from_modin, from_pandas
from ray.data.datasource.file_based_datasource import DefaultBlockWritePathProvider

from awswrangler.distributed.ray.datasources import ParquetDatasource, UserProvidedKeyBlockWritePathProvider

if TYPE_CHECKING:
    import boto3
    import pyarrow as pa


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
        path = f"{path_root}{filename_prefix}_bucket-{df.name:05d}.parquet"
    # Create Ray Dataset
    ds = from_modin(df) if isinstance(df, ModinDataFrame) else from_pandas(df)
    # Repartition into a single block if or writing into a single key or if bucketing is enabled
    if ds.count() > 0 and (path or bucketing):
        ds = ds.repartition(1)
    # Repartition by max_rows_by_file
    elif max_rows_by_file and (max_rows_by_file > 0):
        ds = ds.repartition(math.ceil(ds.count() / max_rows_by_file))
    datasource = ParquetDatasource()
    ds.write_datasource(
        datasource,  # type: ignore
        path=path or path_root,
        dataset_uuid=filename_prefix,
        # If user has provided a single key, use that instead of generating a path per block
        # The dataset will be repartitioned into a single block
        block_path_provider=UserProvidedKeyBlockWritePathProvider()
        if path and not path.endswith("/")
        else DefaultBlockWritePathProvider(),
    )
    return datasource.get_write_paths()
