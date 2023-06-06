from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Union,
    overload,
)

import boto3

from awswrangler import typing

@overload
def start_query_execution(
    sql: str,
    database: Optional[str] = ...,
    s3_output: Optional[str] = ...,
    workgroup: Optional[str] = ...,
    encryption: Optional[str] = ...,
    kms_key: Optional[str] = ...,
    params: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    athena_cache_settings: Optional[typing.AthenaCacheSettings] = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: Optional[str] = ...,
    wait: Literal[False] = ...,
    execution_params: Optional[List[str]] = ...,
) -> str: ...
@overload
def start_query_execution(
    sql: str,
    *,
    database: Optional[str] = ...,
    s3_output: Optional[str] = ...,
    workgroup: Optional[str] = ...,
    encryption: Optional[str] = ...,
    kms_key: Optional[str] = ...,
    params: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    athena_cache_settings: Optional[typing.AthenaCacheSettings] = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: Optional[str] = ...,
    wait: Literal[True],
    execution_params: Optional[List[str]] = ...,
) -> Dict[str, Any]: ...
@overload
def start_query_execution(
    sql: str,
    *,
    database: Optional[str] = ...,
    s3_output: Optional[str] = ...,
    workgroup: Optional[str] = ...,
    encryption: Optional[str] = ...,
    kms_key: Optional[str] = ...,
    params: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    athena_cache_settings: Optional[typing.AthenaCacheSettings] = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: Optional[str] = ...,
    wait: bool,
    execution_params: Optional[List[str]] = ...,
) -> Union[str, Dict[str, Any]]: ...
def stop_query_execution(query_execution_id: str, boto3_session: Optional[boto3.Session] = ...) -> None: ...
def wait_query(
    query_execution_id: str,
    boto3_session: Optional[boto3.Session] = None,
    athena_query_wait_polling_delay: float = ...,
) -> Dict[str, Any]: ...
def get_query_execution(query_execution_id: str, boto3_session: Optional[boto3.Session] = ...) -> Dict[str, Any]: ...
