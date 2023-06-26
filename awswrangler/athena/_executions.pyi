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
    params: Union[Dict[str, Any], List[str], None] = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    athena_cache_settings: Optional[typing.AthenaCacheSettings] = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: Optional[str] = ...,
    wait: Literal[False] = ...,
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
    params: Union[Dict[str, Any], List[str], None] = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    athena_cache_settings: Optional[typing.AthenaCacheSettings] = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: Optional[str] = ...,
    wait: Literal[True],
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
    params: Union[Dict[str, Any], List[str], None] = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    athena_cache_settings: Optional[typing.AthenaCacheSettings] = ...,
    athena_query_wait_polling_delay: float = ...,
    data_source: Optional[str] = ...,
    wait: bool,
) -> Union[str, Dict[str, Any]]: ...
def stop_query_execution(query_execution_id: str, boto3_session: Optional[boto3.Session] = ...) -> None: ...
def wait_query(
    query_execution_id: str,
    boto3_session: Optional[boto3.Session] = None,
    athena_query_wait_polling_delay: float = ...,
) -> Dict[str, Any]: ...
def get_query_execution(query_execution_id: str, boto3_session: Optional[boto3.Session] = ...) -> Dict[str, Any]: ...
