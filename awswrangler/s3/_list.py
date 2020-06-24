"""Amazon S3 List Module (PRIVATE)."""

import datetime
import logging
from typing import Any, Dict, List, Optional

import boto3  # type: ignore
import botocore.exceptions  # type: ignore

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def path2list(
    path: object,
    boto3_session: boto3.Session,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    suffix: str = None,
) -> List[str]:
    """Convert Amazon S3 path to list of objects."""
    if isinstance(path, str):  # prefix
        paths: List[str] = list_objects(
            path=path,
            suffix=suffix,
            boto3_session=boto3_session,
            last_modified_begin=last_modified_begin,
            last_modified_end=last_modified_end,
        )
    elif isinstance(path, list):
        if last_modified_begin or last_modified_end:
            raise exceptions.InvalidArgumentCombination(
                "Specify a list of files or (last_modified_begin and last_modified_end)"
            )  # pragma: no cover
        paths = path if suffix is None else [x for x in path if x.endswith(suffix)]
    else:
        raise exceptions.InvalidArgumentType(f"{type(path)} is not a valid path type. Please, use str or List[str].")
    return paths

def _validate_datetimes(
    last_modified_begin: Optional[datetime.datetime] = None, last_modified_end: Optional[datetime.datetime] = None
) -> None:
    if last_modified_begin is not None:
        if last_modified_begin.tzinfo is None  is None:
            raise exceptions.InvalidArgumentValue(
                "Timezone is not defined for last_modified_begin."
            )  # pragma: no cover
    if last_modified_end is not None:
        if last_modified_begin.tzinfo is None:
            raise exceptions.InvalidArgumentValue("Timezone is not defined for last_modified_end.")  # pragma: no cover
    if (last_modified_begin is not None) and (last_modified_end is not None):
        if last_modified_begin > last_modified_end:
            raise exceptions.InvalidArgumentValue(
                "last_modified_begin is bigger than last_modified_end."
            )  # pragma: no cover


def _list_objects(
    path: str,
    delimiter: Optional[str] = None,
    suffix: Optional[str] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[str]:
    bucket: str
    prefix: str
    bucket, prefix = _utils.parse_path(path=path)
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    paginator = client_s3.get_paginator("list_objects_v2")
    args: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "PaginationConfig": {"PageSize": 1000}}
    if delimiter is not None:
        args["Delimiter"] = delimiter
    response_iterator = paginator.paginate(**args)
    paths: List[str] = []

    _validate_datetimes(last_modified_begin=last_modified_begin, last_modified_end=last_modified_end)

    for page in response_iterator:  # pylint: disable=too-many-nested-blocks
        if delimiter is None:
            contents: Optional[List] = page.get("Contents")
            if contents is not None:
                for content in contents:
                    key: str = content["Key"]
                    if (content is not None) and ("Key" in content):
                        if (suffix is None) or key.endswith(suffix):
                            if last_modified_begin is not None:
                                if content["LastModified"] < last_modified_begin:
                                    continue
                            if last_modified_end is not None:
                                if content["LastModified"] > last_modified_end:
                                    continue
                            paths.append(f"s3://{bucket}/{key}")
        else:
            prefixes: Optional[List[Optional[Dict[str, str]]]] = page.get("CommonPrefixes")
            if prefixes is not None:
                for pfx in prefixes:
                    if (pfx is not None) and ("Prefix" in pfx):
                        key = pfx["Prefix"]
                        paths.append(f"s3://{bucket}/{key}")
    return paths


def does_object_exist(path: str, boto3_session: Optional[boto3.Session] = None) -> bool:
    """Check if object exists on S3.

    Parameters
    ----------
    path: str
        S3 path (e.g. s3://bucket/key).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    bool
        True if exists, False otherwise.

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> wr.s3.does_object_exist('s3://bucket/key_real')
    True
    >>> wr.s3.does_object_exist('s3://bucket/key_unreal')
    False

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> wr.s3.does_object_exist('s3://bucket/key_real', boto3_session=boto3.Session())
    True
    >>> wr.s3.does_object_exist('s3://bucket/key_unreal', boto3_session=boto3.Session())
    False

    """
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    bucket: str
    key: str
    bucket, key = path.replace("s3://", "").split("/", 1)
    try:
        client_s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as ex:
        if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise ex  # pragma: no cover


def list_directories(path: str, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    """List Amazon S3 objects from a prefix.

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/prefix).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        List of objects paths.

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> wr.s3.list_objects('s3://bucket/prefix/')
    ['s3://bucket/prefix/dir0', 's3://bucket/prefix/dir1', 's3://bucket/prefix/dir2']

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> wr.s3.list_objects('s3://bucket/prefix/', boto3_session=boto3.Session())
    ['s3://bucket/prefix/dir0', 's3://bucket/prefix/dir1', 's3://bucket/prefix/dir2']

    """
    return _list_objects(path=path, delimiter="/", boto3_session=boto3_session)


def list_objects(
    path: str,
    suffix: Optional[str] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[str]:
    """List Amazon S3 objects from a prefix.

    Note
    ----
    The filter by last_modified begin last_modified end is applied after list all S3 files

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/prefix).
    suffix: str, optional
        Suffix for filtering S3 keys.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        List of objects paths.

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> wr.s3.list_objects('s3://bucket/prefix')
    ['s3://bucket/prefix0', 's3://bucket/prefix1', 's3://bucket/prefix2']

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> wr.s3.list_objects('s3://bucket/prefix', boto3_session=boto3.Session())
    ['s3://bucket/prefix0', 's3://bucket/prefix1', 's3://bucket/prefix2']

    """
    paths: List[str] = _list_objects(
        path=path,
        delimiter=None,
        suffix=suffix,
        boto3_session=boto3_session,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
    )
    return [p for p in paths if not p.endswith("/")]
