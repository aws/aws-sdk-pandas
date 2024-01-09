import datetime
from typing import TYPE_CHECKING, Any, Iterator, Literal, Sequence, overload

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

def _path2list(
    path: str | Sequence[str],
    s3_client: "S3Client",
    s3_additional_kwargs: dict[str, Any] | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    suffix: str | list[str] | None = ...,
    ignore_suffix: str | list[str] | None = ...,
    ignore_empty: bool = ...,
) -> list[str]: ...
def _prefix_cleanup(prefix: str) -> str: ...
def _list_objects_paginate(
    bucket: str,
    pattern: str,
    prefix: str,
    s3_client: "S3Client",
    delimiter: str | None,
    s3_additional_kwargs: dict[str, Any] | None,
    suffix: list[str] | None,
    ignore_suffix: list[str] | None,
    last_modified_begin: datetime.datetime | None,
    last_modified_end: datetime.datetime | None,
    ignore_empty: bool,
) -> Iterator[list[str]]: ...
def does_object_exist(
    path: str,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    boto3_session: boto3.Session | None = ...,
    version_id: str | None = ...,
) -> bool: ...
def list_buckets(boto3_session: boto3.Session | None = ...) -> list[str]: ...
@overload
def list_directories(
    path: str,
    chunked: Literal[False],
    s3_additional_kwargs: dict[str, Any] | dict[str, str] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> list[str]: ...
@overload
def list_directories(
    path: str,
    *,
    boto3_session: boto3.Session | None = ...,
    s3_additional_kwargs: dict[str, Any] | dict[str, str] | None = ...,
) -> list[str]: ...
@overload
def list_directories(
    path: str,
    chunked: Literal[True],
    s3_additional_kwargs: dict[str, Any] | dict[str, str] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> Iterator[list[str]]: ...
@overload
def list_directories(
    path: str,
    chunked: bool,
    s3_additional_kwargs: dict[str, Any] | dict[str, str] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> list[str] | Iterator[list[str]]: ...
@overload
def list_objects(
    path: str,
    chunked: Literal[False],
    suffix: str | list[str] | None = ...,
    ignore_suffix: str | list[str] | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: dict[str, Any] | dict[str, str] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> list[str]: ...
@overload
def list_objects(
    path: str,
    suffix: str | list[str] | None = ...,
    ignore_suffix: str | list[str] | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: dict[str, Any] | dict[str, str] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> list[str]: ...
@overload
def list_objects(
    path: str,
    chunked: Literal[True],
    suffix: str | list[str] | None = ...,
    ignore_suffix: str | list[str] | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: dict[str, Any] | dict[str, str] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> Iterator[list[str]]: ...
@overload
def list_objects(
    path: str,
    chunked: bool,
    suffix: str | list[str] | None = ...,
    ignore_suffix: str | list[str] | None = ...,
    last_modified_begin: datetime.datetime | None = ...,
    last_modified_end: datetime.datetime | None = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: dict[str, Any] | dict[str, str] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> list[str] | Iterator[list[str]]: ...
