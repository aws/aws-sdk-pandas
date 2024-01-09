from typing import TYPE_CHECKING, Any, Iterator, Literal, overload

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
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> pd.DataFrame: ...
@overload
def read_sql_query(
    sql: str,
    con: "redshift_connector.Connection",
    *,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_query(
    sql: str,
    con: "redshift_connector.Connection",
    *,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int | None,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: str | None = None,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> pd.DataFrame: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: str | None = None,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: str | None = None,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int | None,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
def unload_to_files(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    iam_role: str | None = ...,
    aws_access_key_id: str | None = ...,
    aws_secret_access_key: str | None = ...,
    aws_session_token: str | None = ...,
    region: str | None = ...,
    unload_format: Literal["CSV", "PARQUET"] | None = ...,
    parallel: bool = ...,
    max_file_size: float | None = ...,
    kms_key_id: str | None = ...,
    manifest: bool = ...,
    partition_cols: list[str] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> None: ...
@overload
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    iam_role: str | None = ...,
    aws_access_key_id: str | None = ...,
    aws_secret_access_key: str | None = ...,
    aws_session_token: str | None = ...,
    region: str | None = ...,
    max_file_size: float | None = ...,
    kms_key_id: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Literal[False] = ...,
    keep_files: bool = ...,
    parallel: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, str] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame: ...
@overload
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    *,
    iam_role: str | None = ...,
    aws_access_key_id: str | None = ...,
    aws_secret_access_key: str | None = ...,
    aws_session_token: str | None = ...,
    region: str | None = ...,
    max_file_size: float | None = ...,
    kms_key_id: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: Literal[True],
    keep_files: bool = ...,
    parallel: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, str] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    *,
    iam_role: str | None = ...,
    aws_access_key_id: str | None = ...,
    aws_secret_access_key: str | None = ...,
    aws_session_token: str | None = ...,
    region: str | None = ...,
    max_file_size: float | None = ...,
    kms_key_id: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: bool,
    keep_files: bool = ...,
    parallel: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, str] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    *,
    iam_role: str | None = ...,
    aws_access_key_id: str | None = ...,
    aws_secret_access_key: str | None = ...,
    aws_session_token: str | None = ...,
    region: str | None = ...,
    max_file_size: float | None = ...,
    kms_key_id: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunked: bool | int,
    keep_files: bool = ...,
    parallel: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, str] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
