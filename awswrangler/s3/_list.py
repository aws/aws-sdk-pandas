"""Amazon S3 List Module (PRIVATE)."""

from __future__ import annotations

import datetime
import fnmatch
import logging
from typing import TYPE_CHECKING, Any, Iterator, Sequence

import boto3
import botocore.exceptions

from awswrangler import _utils, exceptions
from awswrangler._distributed import engine
from awswrangler.s3 import _fs

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _path2list(
    path: str | Sequence[str],
    s3_client: "S3Client",
    s3_additional_kwargs: dict[str, Any] | None,
    last_modified_begin: datetime.datetime | None = None,
    last_modified_end: datetime.datetime | None = None,
    suffix: str | list[str] | None = None,
    ignore_suffix: str | list[str] | None = None,
    ignore_empty: bool = False,
) -> list[str]:
    """Convert Amazon S3 path to list of objects."""
    _suffix: list[str] | None = [suffix] if isinstance(suffix, str) else suffix
    _ignore_suffix: list[str] | None = [ignore_suffix] if isinstance(ignore_suffix, str) else ignore_suffix
    if isinstance(path, str):  # prefix
        paths: list[str] = [
            path
            for paths in _list_objects(
                path=path,
                s3_client=s3_client,
                suffix=_suffix,
                ignore_suffix=_ignore_suffix,
                last_modified_begin=last_modified_begin,
                last_modified_end=last_modified_end,
                ignore_empty=ignore_empty,
                s3_additional_kwargs=s3_additional_kwargs,
            )
            for path in paths
        ]
        _logger.debug("Listed %s paths", len(paths))
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
    last_modified_begin: datetime.datetime | None = None, last_modified_end: datetime.datetime | None = None
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


def _list_objects(
    path: str,
    s3_client: "S3Client",
    delimiter: str | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    suffix: str | list[str] | None = None,
    ignore_suffix: str | list[str] | None = None,
    last_modified_begin: datetime.datetime | None = None,
    last_modified_end: datetime.datetime | None = None,
    ignore_empty: bool = False,
) -> Iterator[list[str]]:
    suffix: list[str] | None = [suffix] if isinstance(suffix, str) else suffix
    ignore_suffix: list[str] | None = [ignore_suffix] if isinstance(ignore_suffix, str) else ignore_suffix
    _validate_datetimes(last_modified_begin=last_modified_begin, last_modified_end=last_modified_end)
    bucket, pattern = _utils.parse_path(path=path)
    prefix: str = _prefix_cleanup(prefix=pattern)

    return _list_objects_paginate(
        bucket=bucket,
        pattern=pattern,
        prefix=prefix,
        s3_client=s3_client,
        delimiter=delimiter,
        suffix=suffix,
        ignore_suffix=ignore_suffix,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_empty=ignore_empty,
        s3_additional_kwargs=s3_additional_kwargs,
    )


@engine.dispatch_on_engine
def _list_objects_paginate(  # noqa: PLR0912
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
) -> Iterator[list[str]]:
    default_pagination: dict[str, int] = {"PageSize": 1000}
    extra_kwargs: dict[str, Any] = {"PaginationConfig": default_pagination}
    if s3_additional_kwargs:
        extra_kwargs = _fs.get_botocore_valid_kwargs(
            function_name="list_objects_v2", s3_additional_kwargs=s3_additional_kwargs
        )
        extra_kwargs["PaginationConfig"] = (
            s3_additional_kwargs["PaginationConfig"]
            if "PaginationConfig" in s3_additional_kwargs
            else default_pagination
        )
    paginator = s3_client.get_paginator("list_objects_v2")
    args: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, **extra_kwargs}
    if delimiter is not None:
        args["Delimiter"] = delimiter
    _logger.debug("args: %s", args)
    response_iterator = paginator.paginate(**args)
    paths: list[str] = []

    for page in response_iterator:
        if delimiter is None:
            contents = page.get("Contents")
            if contents is not None:
                for content in contents:
                    key: str = content["Key"]
                    if ignore_empty and content.get("Size", 0) == 0:
                        _logger.debug("Skipping empty file: %s", f"s3://{bucket}/{key}")
                    elif (content is not None) and ("Key" in content):
                        if (suffix is None) or key.endswith(tuple(suffix)):
                            if last_modified_begin is not None:
                                if content["LastModified"] < last_modified_begin:
                                    continue
                            if last_modified_end is not None:
                                if content["LastModified"] > last_modified_end:
                                    continue
                            paths.append(f"s3://{bucket}/{key}")
        else:
            prefixes = page.get("CommonPrefixes")
            if prefixes is not None:
                for pfx in prefixes:
                    if (pfx is not None) and ("Prefix" in pfx):
                        key = pfx["Prefix"]
                        paths.append(f"s3://{bucket}/{key}")

        if prefix != pattern:
            paths = fnmatch.filter(paths, f"s3://{bucket}/{pattern}")

        if ignore_suffix is not None:
            paths = [p for p in paths if p.endswith(tuple(ignore_suffix)) is False]

        if paths:
            yield paths
        paths = []


def does_object_exist(
    path: str,
    s3_additional_kwargs: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
    version_id: str | None = None,
) -> bool:
    """Check if object exists on S3.

    Parameters
    ----------
    path: str
        S3 path (e.g. s3://bucket/key).
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    version_id: str, optional
        Specific version of the object that should exist.

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
    s3_client = _utils.client(service_name="s3", session=boto3_session)
    bucket: str
    key: str
    bucket, key = _utils.parse_path(path=path)
    if s3_additional_kwargs:
        extra_kwargs: dict[str, Any] = _fs.get_botocore_valid_kwargs(
            function_name="head_object", s3_additional_kwargs=s3_additional_kwargs
        )
    else:
        extra_kwargs = {}
    try:
        if version_id:
            extra_kwargs["VersionId"] = version_id
        s3_client.head_object(Bucket=bucket, Key=key, **extra_kwargs)
        return True
    except botocore.exceptions.ClientError as ex:
        if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise ex


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def list_directories(
    path: str,
    chunked: bool = False,
    s3_additional_kwargs: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> list[str] | Iterator[list[str]]:
    """List Amazon S3 objects from a prefix.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/prefix).
    chunked: bool
        If True returns iterator, and a single list otherwise. False by default.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Union[List[str], Iterator[List[str]]]
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
    s3_client = _utils.client(service_name="s3", session=boto3_session)
    result_iterator = _list_objects(
        path=path,
        delimiter="/",
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if chunked:
        return result_iterator
    return [path for paths in result_iterator for path in paths]


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def list_objects(
    path: str,
    suffix: str | list[str] | None = None,
    ignore_suffix: str | list[str] | None = None,
    last_modified_begin: datetime.datetime | None = None,
    last_modified_end: datetime.datetime | None = None,
    ignore_empty: bool = False,
    chunked: bool = False,
    s3_additional_kwargs: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> list[str] | Iterator[list[str]]:
    """List Amazon S3 objects from a prefix.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

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
    chunked: bool
        If True returns iterator, and a single list otherwise. False by default.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Union[List[str], Iterator[List[str]]]
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
    s3_client = _utils.client(service_name="s3", session=boto3_session)
    # On top of user provided ignore_suffix input, add "/"
    ignore_suffix_acc = set("/")
    if isinstance(ignore_suffix, str):
        ignore_suffix_acc.add(ignore_suffix)
    elif isinstance(ignore_suffix, list):
        ignore_suffix_acc.update(ignore_suffix)

    result_iterator = _list_objects(
        path=path,
        suffix=suffix,
        ignore_suffix=list(ignore_suffix_acc),
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_empty=ignore_empty,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if chunked:
        return result_iterator
    return [path for paths in result_iterator for path in paths]


def list_buckets(boto3_session: boto3.Session | None = None) -> list[str]:
    """List Amazon S3 buckets.

    Parameters
    ----------
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session to use, default to None.

    Returns
    -------
    List[str]
        List of bucket names.

    """
    client_s3 = _utils.client(service_name="s3", session=boto3_session)
    buckets = client_s3.list_buckets()["Buckets"]
    return [bucket["Name"] for bucket in buckets]
