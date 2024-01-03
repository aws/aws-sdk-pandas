from typing import TYPE_CHECKING, Any, Iterator, Literal, Optional, Union, overload

import boto3
import pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    try:
        import redshift_connector
    except ImportError:
        pass

@overload
def read_sql_query(
    sql: str,
    con: "redshift_connector.Connection",
    index_col: Optional[Union[str, list[str]]] = ...,
    params: Optional[Union[list[Any], tuple[Any, ...], dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dtype: Optional[dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> pd.DataFrame: ...
@overload
def read_sql_query(
    sql: str,
    con: "redshift_connector.Connection",
    *,
    index_col: Optional[Union[str, list[str]]] = ...,
    params: Optional[Union[list[Any], tuple[Any, ...], dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dtype: Optional[dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_query(
    sql: str,
    con: "redshift_connector.Connection",
    *,
    index_col: Optional[Union[str, list[str]]] = ...,
    params: Optional[Union[list[Any], tuple[Any, ...], dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: Optional[int],
    dtype: Optional[dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, list[str]]] = ...,
    params: Optional[Union[list[Any], tuple[Any, ...], dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dtype: Optional[dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> pd.DataFrame: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, list[str]]] = ...,
    params: Optional[Union[list[Any], tuple[Any, ...], dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dtype: Optional[dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, list[str]]] = ...,
    params: Optional[Union[list[Any], tuple[Any, ...], dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: Optional[int],
    dtype: Optional[dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
def unload_to_files(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    iam_role: Optional[str] = ...,
    aws_access_key_id: Optional[str] = ...,
    aws_secret_access_key: Optional[str] = ...,
    aws_session_token: Optional[str] = ...,
    region: Optional[str] = ...,
    unload_format: Optional[Literal["CSV", "PARQUET"]] = ...,
    parallel: bool = ...,
    max_file_size: Optional[float] = ...,
    kms_key_id: Optional[str] = ...,
    manifest: bool = ...,
    partition_cols: Optional[list[str]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> None: ...
@overload
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    iam_role: Optional[str] = ...,
    aws_access_key_id: Optional[str] = ...,
    aws_secret_access_key: Optional[str] = ...,
    aws_session_token: Optional[str] = ...,
    region: Optional[str] = ...,
    max_file_size: Optional[float] = ...,
    kms_key_id: Optional[str] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Literal[False] = ...,
    keep_files: bool = ...,
    parallel: bool = ...,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[dict[str, Any]] = ...,
) -> pd.DataFrame: ...
@overload
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    *,
    iam_role: Optional[str] = ...,
    aws_access_key_id: Optional[str] = ...,
    aws_secret_access_key: Optional[str] = ...,
    aws_session_token: Optional[str] = ...,
    region: Optional[str] = ...,
    max_file_size: Optional[float] = ...,
    kms_key_id: Optional[str] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Literal[True],
    keep_files: bool = ...,
    parallel: bool = ...,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[dict[str, Any]] = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    *,
    iam_role: Optional[str] = ...,
    aws_access_key_id: Optional[str] = ...,
    aws_secret_access_key: Optional[str] = ...,
    aws_session_token: Optional[str] = ...,
    region: Optional[str] = ...,
    max_file_size: Optional[float] = ...,
    kms_key_id: Optional[str] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: bool,
    keep_files: bool = ...,
    parallel: bool = ...,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[dict[str, Any]] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    *,
    iam_role: Optional[str] = ...,
    aws_access_key_id: Optional[str] = ...,
    aws_secret_access_key: Optional[str] = ...,
    aws_session_token: Optional[str] = ...,
    region: Optional[str] = ...,
    max_file_size: Optional[float] = ...,
    kms_key_id: Optional[str] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Union[bool, int],
    keep_files: bool = ...,
    parallel: bool = ...,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[dict[str, Any]] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
