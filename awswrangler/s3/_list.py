"""Amazon S3 List Module (PRIVATE)."""

import datetime
import fnmatch
import logging
from typing import Any, Dict, List, Optional, Sequence, Union

import boto3
import botocore.exceptions

from awswrangler import _utils, exceptions
from awswrangler.s3 import _fs

_logger: logging.Logger = logging.getLogger(__name__)


def _path2list(
    path: Union[str, Sequence[str]],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, Any]],
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    suffix: Union[str, List[str], None] = None,
    ignore_suffix: Union[str, List[str], None] = None,
    ignore_empty: bool = False,
) -> List[str]:
    """Convert Amazon S3 path to list of objects."""
    _suffix: Optional[List[str]] = [suffix] if isinstance(suffix, str) else suffix
    _ignore_suffix: Optional[List[str]] = [ignore_suffix] if isinstance(ignore_suffix, str) else ignore_suffix
    if isinstance(path, str):  # prefix
        paths: List[str] = list_objects(
            path=path,
            suffix=_suffix,
            ignore_suffix=_ignore_suffix,
            boto3_session=boto3_session,
            last_modified_begin=last_modified_begin,
            last_modified_end=last_modified_end,
            ignore_empty=ignore_empty,
            s3_additional_kwargs=s3_additional_kwargs,
        )
    elif isinstance(path, list):
        if last_modified_begin or last_modified_end:
            raise exceptions.InvalidArgumentCombination(
                "Specify a list of files or (last_modified_begin and last_modified_end)"
            )
        paths = path if _suffix is None else [x for x in path if x.endswith(tuple(_suffix))]
        paths = path if _ignore_suffix is None else [x for x in paths if x.endswith(tuple(_ignore_suffix)) is False]
    else:
        raise exceptions.InvalidArgumentType(f"{type(path)} is not a valid path type. Please, use str or List[str].")
    return paths


def _validate_datetimes(
    last_modified_begin: Optional[datetime.datetime] = None, last_modified_end: Optional[datetime.datetime] = None
) -> None:
    if (last_modified_begin is not None) and (last_modified_begin.tzinfo is None):
        raise exceptions.InvalidArgumentValue("Timezone is not defined for last_modified_begin.")
    if (last_modified_end is not None) and (last_modified_end.tzinfo is None):
        raise exceptions.InvalidArgumentValue("Timezone is not defined for last_modified_end.")
    if (last_modified_begin is not None) and (last_modified_end is not None):
        if last_modified_begin > last_modified_end:
            raise exceptions.InvalidArgumentValue("last_modified_begin is bigger than last_modified_end.")


def _prefix_cleanup(prefix: str) -> str:
    for n, c in enumerate(prefix):
        if c in ["*", "?", "["]:
            return prefix[:n]
    return prefix


def _list_objects(  # pylint: disable=too-many-branches
    path: str,
    s3_additional_kwargs: Optional[Dict[str, Any]],
    delimiter: Optional[str] = None,
    suffix: Union[str, List[str], None] = None,
    ignore_suffix: Union[str, List[str], None] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    ignore_empty: bool = False,
) -> List[str]:
    bucket: str
    prefix_original: str
    bucket, prefix_original = _utils.parse_path(path=path)
    prefix: str = _prefix_cleanup(prefix=prefix_original)
    _suffix: Union[List[str], None] = [suffix] if isinstance(suffix, str) else suffix
    _ignore_suffix: Union[List[str], None] = [ignore_suffix] if isinstance(ignore_suffix, str) else ignore_suffix
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    if s3_additional_kwargs:
        extra_kwargs: Dict[str, Any] = _fs.get_botocore_valid_kwargs(
            function_name="list_objects_v2", s3_additional_kwargs=s3_additional_kwargs
        )
    else:
        extra_kwargs = {}
    paginator = client_s3.get_paginator("list_objects_v2")
    args: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "PaginationConfig": {"PageSize": 1000}, **extra_kwargs}
    if delimiter is not None:
        args["Delimiter"] = delimiter
    _logger.debug("args: %s", args)
    response_iterator = paginator.paginate(**args)
    paths: List[str] = []
    _validate_datetimes(last_modified_begin=last_modified_begin, last_modified_end=last_modified_end)

    for page in response_iterator:  # pylint: disable=too-many-nested-blocks
        if delimiter is None:
            contents: Optional[List[Dict[str, Any]]] = page.get("Contents")
            if contents is not None:
                for content in contents:
                    key: str = content["Key"]
                    if ignore_empty and content.get("Size", 0) == 0:
                        _logger.debug("Skipping empty file: %s", f"s3://{bucket}/{key}")
                    elif (content is not None) and ("Key" in content):
                        if (_suffix is None) or key.endswith(tuple(_suffix)):
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

    if prefix != prefix_original:
        paths = fnmatch.filter(paths, path)

    if _ignore_suffix is not None:
        paths = [p for p in paths if p.endswith(tuple(_ignore_suffix)) is False]

    return paths


def does_object_exist(
    path: str, s3_additional_kwargs: Optional[Dict[str, Any]] = None, boto3_session: Optional[boto3.Session] = None
) -> bool:
    """Check if object exists on S3.

    Parameters
    ----------
    path: str
        S3 path (e.g. s3://bucket/key).
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "RequestPayer".
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
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
    bucket, key = _utils.parse_path(path=path)
    if s3_additional_kwargs:
        extra_kwargs: Dict[str, Any] = _fs.get_botocore_valid_kwargs(
            function_name="head_object", s3_additional_kwargs=s3_additional_kwargs
        )
    else:
        extra_kwargs = {}
    try:
        client_s3.head_object(Bucket=bucket, Key=key, **extra_kwargs)
        return True
    except botocore.exceptions.ClientError as ex:
        if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise ex


def list_directories(
    path: str, s3_additional_kwargs: Optional[Dict[str, Any]] = None, boto3_session: Optional[boto3.Session] = None
) -> List[str]:
    """List Amazon S3 objects from a prefix.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/prefix).
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "RequestPayer".
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
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
    >>> wr.s3.list_directories('s3://bucket/prefix/')
    ['s3://bucket/prefix/dir0/', 's3://bucket/prefix/dir1/', 's3://bucket/prefix/dir2/']

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> wr.s3.list_directories('s3://bucket/prefix/', boto3_session=boto3.Session())
    ['s3://bucket/prefix/dir0/', 's3://bucket/prefix/dir1/', 's3://bucket/prefix/dir2/']

    """
    return _list_objects(
        path=path, delimiter="/", boto3_session=boto3_session, s3_additional_kwargs=s3_additional_kwargs
    )


def list_objects(
    path: str,
    suffix: Union[str, List[str], None] = None,
    ignore_suffix: Union[str, List[str], None] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    ignore_empty: bool = False,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[str]:
    """List Amazon S3 objects from a prefix.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).

    Note
    ----
    The filter by last_modified begin last_modified end is applied after list all S3 files

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/prefix).
    suffix: Union[str, List[str], None]
        Suffix or List of suffixes for filtering S3 keys.
    ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    ignore_empty: bool
        Ignore files with 0 bytes.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "RequestPayer".
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
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
        ignore_suffix=ignore_suffix,
        boto3_session=boto3_session,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_empty=ignore_empty,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    return [p for p in paths if not p.endswith("/")]
