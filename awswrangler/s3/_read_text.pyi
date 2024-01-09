import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterator, overload

import boto3
import pandas as pd
from typing_extensions import Literal

from awswrangler.typing import RaySettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

def _read_text(
    read_format: str,
    paths: list[str],
    path_root: str | None,
    use_threads: bool | int,
    s3_client: "S3Client",
    s3_additional_kwargs: dict[str, str] | None,
    dataset: bool,
    ignore_index: bool,
    parallelism: int,
    version_ids: dict[str, str] | None,
    pandas_kwargs: dict[str, Any],
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_csv(
    path: str | list[str],
    *,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame: ...
@overload
def read_csv(
    path: str | list[str],
    *,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_csv(
    path: str | list[str],
    *,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int | None,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_json(
    path: str | list[str],
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    orient: str = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame: ...
@overload
def read_json(
    path: str | list[str],
    *,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    orient: str = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_json(
    path: str | list[str],
    *,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    orient: str = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int | None,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_fwf(
    path: str | list[str],
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    chunksize: None = ...,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame: ...
@overload
def read_fwf(
    path: str | list[str],
    *,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    chunksize: int,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_fwf(
    path: str | list[str],
    *,
    path_suffix: str | list[str] | None = ...,
    path_ignore_suffix: str | list[str] | None = ...,
    version_id: str | dict[str, str] | None = ...,
    ignore_empty: bool = ...,
    use_threads: bool | int = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    chunksize: int | None,
    dataset: bool = ...,
    partition_filter: Callable[[dict[str, str]], bool] | None = ...,
    ray_args: RaySettings | None = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
