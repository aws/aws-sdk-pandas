import datetime
from typing import Any, Dict, Iterator, List, Literal, Optional, Sequence, Union, overload

import boto3

def _path2list(
    path: Union[str, Sequence[str]],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    last_modified_begin: Optional[datetime.datetime] = ...,
    last_modified_end: Optional[datetime.datetime] = ...,
    suffix: Union[str, List[str], None] = ...,
    ignore_suffix: Union[str, List[str], None] = ...,
    ignore_empty: bool = ...,
) -> List[str]: ...
def _prefix_cleanup(prefix: str) -> str: ...
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
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> List[str]: ...
@overload
def list_objects(
    path: str,
    *,
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> List[str]: ...
@overload
def list_objects(
    path: str,
    chunked: Literal[True],
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Iterator[List[str]]: ...
@overload
def list_objects(
    path: str,
    chunked: bool,
    s3_additional_kwargs: Union[Dict[str, Any], Dict[str, str], None] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Union[List[str], Iterator[List[str]]]: ...
