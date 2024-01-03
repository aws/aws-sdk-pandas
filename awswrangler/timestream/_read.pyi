from typing import Any, Iterator, Literal, overload

import boto3
import pandas as pd

@overload
def query(
    sql: str,
    chunked: Literal[False] = ...,
    pagination_config: dict[str, Any] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> pd.DataFrame: ...
@overload
def query(
    sql: str,
    chunked: Literal[True],
    pagination_config: dict[str, Any] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def query(
    sql: str,
    chunked: bool,
    pagination_config: dict[str, Any] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
def unload(
    sql: str,
    path: str,
    unload_format: Literal["CSV", "PARQUET"] | None = ...,
    compression: Literal["GZIP", "..."] | None = ...,
    partition_cols: list[str] | None = ...,
    encryption: Literal["SSE_KMS", "SSE_S3"] | None = ...,
    kms_key_id: str | None = ...,
    field_delimiter: str | None = ",",
    escaped_by: str | None = "\\",
    chunked: bool | int = False,
    keep_files: bool = False,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, str] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
def unload_to_files(
    sql: str,
    path: str,
    unload_format: Literal["CSV", "PARQUET"] | None = ...,
    compression: Literal["GZIP", "NONE"] | None = ...,
    partition_cols: list[str] | None = ...,
    encryption: Literal["SSE_KMS", "SSE_S3"] | None = ...,
    kms_key_id: str | None = ...,
    field_delimiter: str | None = ...,
    escaped_by: str | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> None: ...
