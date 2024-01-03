import datetime
from typing import TYPE_CHECKING, Any, Iterator, Literal, Optional, Sequence, Union, overload

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

def _path2list(
    path: Union[str, Sequence[str]],
    s3_client: "S3Client",
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    suffix: Union[str, list[str], None] = ...,
    ignore_suffix: Union[str, list[str], None] = ...,
    ignore_empty: bool = ...,
) -> list[str]: ...
def _prefix_cleanup(prefix: str) -> str: ...
def _list_objects_paginate(  # pylint: disable=too-many-branches
    bucket: str,
    pattern: str,
    prefix: str,
    s3_client: "S3Client",
    delimiter: Optional[str],
    s3_additional_kwargs: Optional[dict[str, Any]],
    suffix: Union[list[str], None],
    ignore_suffix: Union[list[str], None],
    last_modified_begin: Optional[datetime.datetime],
    last_modified_end: Optional[datetime.datetime],
    ignore_empty: bool,
) -> Iterator[list[str]]: ...
def does_object_exist(
    path: str,
    s3_additional_kwargs: Optional[dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    version_id: Optional[str] = ...,
) -> bool: ...
def list_buckets(boto3_session: Optional[boto3.Session] = ...) -> list[str]: ...
@overload
def list_directories(
    path: str,
    chunked: Literal[False],
    s3_additional_kwargs: Union[dict[str, Any], dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> list[str]: ...
@overload
def list_directories(
    path: str,
    *,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Union[dict[str, Any], dict[str, str], None] = ...,
) -> list[str]: ...
@overload
def list_directories(
    path: str,
    chunked: Literal[True],
    s3_additional_kwargs: Union[dict[str, Any], dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Iterator[list[str]]: ...
@overload
def list_directories(
    path: str,
    chunked: bool,
    s3_additional_kwargs: Union[dict[str, Any], dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Union[list[str], Iterator[list[str]]]: ...
@overload
def list_objects(
    path: str,
    chunked: Literal[False],
    suffix: Union[str, list[str], None] = ...,
    ignore_suffix: Union[str, list[str], None] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: Union[dict[str, Any], dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> list[str]: ...
@overload
def list_objects(
    path: str,
    suffix: Union[str, list[str], None] = ...,
    ignore_suffix: Union[str, list[str], None] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: Union[dict[str, Any], dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> list[str]: ...
@overload
def list_objects(
    path: str,
    chunked: Literal[True],
    suffix: Union[str, list[str], None] = ...,
    ignore_suffix: Union[str, list[str], None] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: Union[dict[str, Any], dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Iterator[list[str]]: ...
@overload
def list_objects(
    path: str,
    chunked: bool,
    suffix: Union[str, list[str], None] = ...,
    ignore_suffix: Union[str, list[str], None] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: Union[dict[str, Any], dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Union[list[str], Iterator[list[str]]]: ...
