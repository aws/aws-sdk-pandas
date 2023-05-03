import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Literal, Optional, Tuple, Union, overload

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler.typing import RayReadParquetSettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

def _pyarrow_parquet_file_wrapper(
    source: Any, coerce_int96_timestamp_unit: Optional[str] = ...
) -> pa.parquet.ParquetFile: ...
def _read_parquet_metadata_file(
    s3_client: Optional["S3Client"],
    path: str,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_id: Optional[str] = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
) -> pa.schema: ...
def _read_parquet(  # pylint: disable=W0613
    paths: List[str],
    path_root: Optional[str],
    schema: Optional[pa.schema],
    columns: Optional[List[str]],
    coerce_int96_timestamp_unit: Optional[str],
    use_threads: Union[bool, int],
    parallelism: int,
    version_ids: Optional[Dict[str, str]],
    s3_client: Optional["S3Client"],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    arrow_kwargs: Dict[str, Any],
    bulk_read: bool,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
def _read_parquet_metadata(
    path: Union[str, List[str]],
    path_suffix: Optional[str],
    path_ignore_suffix: Union[str, List[str], None],
    ignore_empty: bool,
    ignore_null: bool,
    dtype: Optional[Dict[str, str]],
    sampling: float,
    dataset: bool,
    use_threads: Union[bool, int],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]: ...
def read_parquet_metadata(
    path: Union[str, List[str]],
    dataset: bool = ...,
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    path_suffix: Optional[str] = ...,
    path_ignore_suffix: Union[str, List[str], None] = ...,
    ignore_empty: bool = ...,
    ignore_null: bool = ...,
    dtype: Optional[Dict[str, str]] = ...,
    sampling: float = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]]]: ...
@overload
def read_parquet(
    path: Union[str, List[str]],
    path_root: Optional[str] = ...,
    dataset: bool = ...,
    path_suffix: Union[str, List[str], None] = ...,
    path_ignore_suffix: Union[str, List[str], None] = ...,
    ignore_empty: bool = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    columns: Optional[List[str]] = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    chunked: Literal[False] = ...,
    use_threads: Union[bool, int] = ...,
    ray_args: Optional[RayReadParquetSettings] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> pd.DataFrame: ...
@overload
def read_parquet(
    path: Union[str, List[str]],
    *,
    path_root: Optional[str] = ...,
    dataset: bool = ...,
    path_suffix: Union[str, List[str], None] = ...,
    path_ignore_suffix: Union[str, List[str], None] = ...,
    ignore_empty: bool = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    columns: Optional[List[str]] = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    chunked: Literal[True],
    use_threads: Union[bool, int] = ...,
    ray_args: Optional[RayReadParquetSettings] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_parquet(
    path: Union[str, List[str]],
    *,
    path_root: Optional[str] = ...,
    dataset: bool = ...,
    path_suffix: Union[str, List[str], None] = ...,
    path_ignore_suffix: Union[str, List[str], None] = ...,
    ignore_empty: bool = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    columns: Optional[List[str]] = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    chunked: bool,
    use_threads: Union[bool, int] = ...,
    ray_args: Optional[RayReadParquetSettings] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_parquet(
    path: Union[str, List[str]],
    *,
    path_root: Optional[str] = ...,
    dataset: bool = ...,
    path_suffix: Union[str, List[str], None] = ...,
    path_ignore_suffix: Union[str, List[str], None] = ...,
    ignore_empty: bool = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    columns: Optional[List[str]] = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    chunked: int,
    use_threads: Union[bool, int] = ...,
    ray_args: Optional[RayReadParquetSettings] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_parquet_table(
    table: str,
    database: str,
    *,
    filename_suffix: Union[str, List[str], None] = ...,
    filename_ignore_suffix: Union[str, List[str], None] = ...,
    catalog_id: Optional[str] = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    columns: Optional[List[str]] = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    chunked: Literal[False] = ...,
    use_threads: Union[bool, int] = ...,
    ray_args: Optional[RayReadParquetSettings] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> pd.DataFrame: ...
@overload
def read_parquet_table(
    table: str,
    database: str,
    *,
    filename_suffix: Union[str, List[str], None] = ...,
    filename_ignore_suffix: Union[str, List[str], None] = ...,
    catalog_id: Optional[str] = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    columns: Optional[List[str]] = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    chunked: Literal[True],
    use_threads: Union[bool, int] = ...,
    ray_args: Optional[RayReadParquetSettings] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_parquet_table(
    table: str,
    database: str,
    *,
    filename_suffix: Union[str, List[str], None] = ...,
    filename_ignore_suffix: Union[str, List[str], None] = ...,
    catalog_id: Optional[str] = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    columns: Optional[List[str]] = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    chunked: bool,
    use_threads: Union[bool, int] = ...,
    ray_args: Optional[RayReadParquetSettings] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_parquet_table(
    table: str,
    database: str,
    *,
    filename_suffix: Union[str, List[str], None] = ...,
    filename_ignore_suffix: Union[str, List[str], None] = ...,
    catalog_id: Optional[str] = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    columns: Optional[List[str]] = ...,
    validate_schema: bool = ...,
    coerce_int96_timestamp_unit: Optional[str] = ...,
    chunked: int,
    use_threads: Union[bool, int] = ...,
    ray_args: Optional[RayReadParquetSettings] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Iterator[pd.DataFrame]: ...
