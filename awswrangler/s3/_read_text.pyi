import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional, Union, overload

import boto3
import pandas as pd
from typing_extensions import Literal

from awswrangler.typing import RaySettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

def _read_text(  # pylint: disable=W0613
    read_format: str,
    paths: list[str],
    path_root: Optional[str],
    use_threads: Union[bool, int],
    s3_client: "S3Client",
    s3_additional_kwargs: Optional[dict[str, str]],
    dataset: bool,
    ignore_index: bool,
    parallelism: int,
    version_ids: Optional[dict[str, str]],
    pandas_kwargs: dict[str, Any],
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_csv(
    path: Union[str, list[str]],
    *,
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame: ...
@overload
def read_csv(
    path: Union[str, list[str]],
    *,
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_csv(
    path: Union[str, list[str]],
    *,
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: Optional[int],
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_json(
    path: Union[str, list[str]],
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    orient: str = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame: ...
@overload
def read_json(
    path: Union[str, list[str]],
    *,
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    orient: str = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_json(
    path: Union[str, list[str]],
    *,
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    orient: str = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: Optional[int],
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_fwf(
    path: Union[str, list[str]],
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    chunksize: None = ...,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame: ...
@overload
def read_fwf(
    path: Union[str, list[str]],
    *,
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    chunksize: int,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_fwf(
    path: Union[str, list[str]],
    *,
    path_suffix: Union[str, list[str], None] = ...,
    path_ignore_suffix: Union[str, list[str], None] = ...,
    version_id: Optional[Union[str, dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    chunksize: Optional[int],
    dataset: bool = ...,
    partition_filter: Optional[Callable[[dict[str, str]], bool]] = ...,
    ray_args: Optional[RaySettings] = ...,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
