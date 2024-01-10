"""Amazon S3 Delete Module (PRIVATE)."""

from __future__ import annotations

import datetime
import itertools
import logging
from typing import TYPE_CHECKING, Any

import boto3

from awswrangler import _utils, exceptions
from awswrangler._distributed import engine
from awswrangler._executor import _BaseExecutor, _get_executor
from awswrangler.distributed.ray import ray_get
from awswrangler.s3._fs import get_botocore_valid_kwargs
from awswrangler.s3._list import _path2list

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _split_paths_by_bucket(paths: list[str]) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = {}
    bucket: str
    for path in paths:
        bucket = _utils.parse_path(path=path)[0]
        if bucket not in buckets:
            buckets[bucket] = []
        buckets[bucket].append(path)
    return buckets


@engine.dispatch_on_engine
def _delete_objects(
    s3_client: "S3Client" | None,
    paths: list[str],
    s3_additional_kwargs: dict[str, Any] | None,
) -> None:
    s3_client = s3_client if s3_client else _utils.client(service_name="s3")

    if s3_additional_kwargs:
        extra_kwargs: dict[str, Any] = get_botocore_valid_kwargs(
            function_name="list_objects_v2", s3_additional_kwargs=s3_additional_kwargs
        )
    else:
        extra_kwargs = {}
    bucket = _utils.parse_path(path=paths[0])[0]
    batch: list[dict[str, str]] = [{"Key": _utils.parse_path(path)[1]} for path in paths]
    res = s3_client.delete_objects(
        Bucket=bucket,
        Delete={"Objects": batch},  # type: ignore[typeddict-item]
        **extra_kwargs,
    )
    deleted = res.get("Deleted", [])
    _logger.debug("Deleted %s objects", len(deleted))
    errors = res.get("Errors", [])
    for error in errors:
        _logger.debug("error: %s", error)
        if "Code" not in error or error["Code"] != "InternalError":
            raise exceptions.ServiceApiError(errors)


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def delete_objects(
    path: str | list[str],
    use_threads: bool | int = True,
    last_modified_begin: datetime.datetime | None = None,
    last_modified_end: datetime.datetime | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Note
    ----
    The filter by last_modified begin last_modified end is applied after list all S3 files

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.s3.delete_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Delete both objects
    >>> wr.s3.delete_objects('s3://bucket/prefix')  # Delete all objects under the received prefix

    """
    s3_client = _utils.client(service_name="s3", session=boto3_session)
    paths: list[str] = _path2list(
        path=path,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    paths_by_bucket: dict[str, list[str]] = _split_paths_by_bucket(paths)

    chunks = []
    for _, paths in paths_by_bucket.items():
        chunks += _utils.chunkify(lst=paths, max_length=1_000)

    executor: _BaseExecutor = _get_executor(use_threads=use_threads)
    ray_get(
        executor.map(
            _delete_objects,
            s3_client,
            chunks,
            itertools.repeat(s3_additional_kwargs),
        )
    )
