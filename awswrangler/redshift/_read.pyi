from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Literal, Optional, Tuple, Union, overload

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
    index_col: Optional[Union[str, List[str]]] = ...,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dtype: Optional[Dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> pd.DataFrame: ...
@overload
def read_sql_query(
    sql: str,
    con: "redshift_connector.Connection",
    *,
    index_col: Optional[Union[str, List[str]]] = ...,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dtype: Optional[Dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_query(
    sql: str,
    con: "redshift_connector.Connection",
    *,
    index_col: Optional[Union[str, List[str]]] = ...,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: Optional[int],
    dtype: Optional[Dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = ...,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None = ...,
    dtype: Optional[Dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> pd.DataFrame: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = ...,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: int,
    dtype: Optional[Dict[str, pa.DataType]] = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    *,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = ...,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: Optional[int],
    dtype: Optional[Dict[str, pa.DataType]] = ...,
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
    max_file_size: Optional[float] = ...,
    kms_key_id: Optional[str] = ...,
    manifest: bool = ...,
    partition_cols: Optional[List[str]] = ...,
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
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
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
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
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
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
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
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
