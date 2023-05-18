from typing import Any, Dict, Iterator, List, Literal, Optional, Union, overload

import boto3
import pandas as pd

@overload
def query(
    sql: str,
    chunked: Literal[False] = ...,
    pagination_config: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> pd.DataFrame: ...
@overload
def query(
    sql: str,
    chunked: Literal[True],
    pagination_config: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def query(
    sql: str,
    chunked: bool,
    pagination_config: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
def unload(
    sql: str,
    path: str,
    unload_format: Optional[Literal["CSV", "PARQUET"]] = ...,
    compression: Optional[Literal["GZIP", "..."]] = ...,
    partition_cols: Optional[List[str]] = ...,
    encryption: Optional[Literal["SSE_KMS", "SSE_S3"]] = ...,
    kms_key_id: Optional[str] = ...,
    field_delimiter: Optional[str] = ",",
    escaped_by: Optional[str] = "\\",
    chunked: Union[bool, int] = False,
    keep_files: bool = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, str]] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
def unload_to_files(
    sql: str,
    path: str,
    unload_format: Optional[Literal["CSV", "PARQUET"]] = ...,
    compression: Optional[Literal["GZIP", "NONE"]] = ...,
    partition_cols: Optional[List[str]] = ...,
    encryption: Optional[Literal["SSE_KMS", "SSE_S3"]] = ...,
    kms_key_id: Optional[str] = ...,
    field_delimiter: Optional[str] = ...,
    escaped_by: Optional[str] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> None: ...
