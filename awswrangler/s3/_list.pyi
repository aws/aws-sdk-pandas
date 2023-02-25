import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Literal, Optional, Sequence, Union, overload

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

def _path2list(
    path: Union[str, Sequence[str]],
    s3_client: "S3Client",
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    suffix: Union[str, List[str], None] = ...,
    ignore_suffix: Union[str, List[str], None] = ...,
    ignore_empty: bool = ...,
) -> List[str]: ...
def _prefix_cleanup(prefix: str) -> str: ...
def _list_objects_paginate(  # pylint: disable=too-many-branches
    bucket: str,
    pattern: str,
    prefix: str,
    s3_client: "S3Client",
    delimiter: Optional[str],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    suffix: Union[List[str], None],
    ignore_suffix: Union[List[str], None],
    last_modified_begin: Optional[datetime.datetime],
    last_modified_end: Optional[datetime.datetime],
    ignore_empty: bool,
) -> Iterator[List[str]]: ...
def does_object_exist(
    path: str,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    version_id: Optional[str] = ...,
) -> bool: ...
def list_buckets(boto3_session: Optional[boto3.Session] = ...) -> List[str]: ...
@overload
def list_directories(
    path: str,
    chunked: Literal[False],
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> List[str]: ...
@overload
def list_directories(
    path: str,
    *,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
) -> List[str]: ...
@overload
def list_directories(
    path: str,
    chunked: Literal[True],
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Iterator[List[str]]: ...
@overload
def list_directories(
    path: str,
    chunked: bool,
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Union[List[str], Iterator[List[str]]]: ...
@overload
def list_objects(
    path: str,
    chunked: Literal[False],
    ignore_suffix: Union[str, List[str], None] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> List[str]: ...
@overload
def list_objects(
    path: str,
    ignore_suffix: Union[str, List[str], None] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> List[str]: ...
@overload
def list_objects(
    path: str,
    chunked: Literal[True],
    ignore_suffix: Union[str, List[str], None] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Iterator[List[str]]: ...
@overload
def list_objects(
    path: str,
    chunked: bool,
    ignore_suffix: Union[str, List[str], None] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    ignore_empty: bool = ...,
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Union[List[str], Iterator[List[str]]]: ...
