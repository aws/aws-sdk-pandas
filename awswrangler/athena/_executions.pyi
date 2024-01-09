from typing import (
    Any,
    Literal,
    overload,
)

import boto3

from awswrangler import typing

@overload
def start_query_execution(
    sql: str,
    database: str | None = ...,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: str | None = ...,
    wait: Literal[False] = ...,
) -> str: ...
@overload
def start_query_execution(
    sql: str,
    *,
    database: str | None = ...,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: str | None = ...,
    wait: Literal[True],
) -> dict[str, Any]: ...
@overload
def start_query_execution(
    sql: str,
    *,
    database: str | None = ...,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: str | None = ...,
    wait: bool,
) -> str | dict[str, Any]: ...
def stop_query_execution(query_execution_id: str, boto3_session: boto3.Session | None = ...) -> None: ...
def wait_query(
    query_execution_id: str,
    boto3_session: boto3.Session | None = None,
    athena_query_wait_polling_delay: float = ...,
) -> dict[str, Any]: ...
def get_query_execution(query_execution_id: str, boto3_session: boto3.Session | None = ...) -> dict[str, Any]: ...
