"""Amazon S3 Delete Module (PRIVATE)."""

import datetime
import itertools
import logging
from typing import Any, Dict, List, Optional, Union

import boto3

from awswrangler import _utils, exceptions
from awswrangler._distributed import engine
from awswrangler._threading import _get_executor
from awswrangler.distributed.ray import ray_get
from awswrangler.s3._fs import get_botocore_valid_kwargs
from awswrangler.s3._list import _path2list

_logger: logging.Logger = logging.getLogger(__name__)


def _split_paths_by_bucket(paths: List[str]) -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}
    bucket: str
    for path in paths:
        bucket = _utils.parse_path(path=path)[0]
        if bucket not in buckets:
            buckets[bucket] = []
        buckets[bucket].append(path)
    return buckets


@engine.dispatch_on_engine
def _delete_objects(
    boto3_session: Optional[boto3.Session],
    paths: List[str],
    s3_additional_kwargs: Optional[Dict[str, Any]],
) -> None:
    client_s3: boto3.client = _utils.client(
        service_name="s3",
        session=boto3_session,
    )
    _logger.debug("len(paths): %s", len(paths))
    if s3_additional_kwargs:
        extra_kwargs: Dict[str, Any] = get_botocore_valid_kwargs(
            function_name="list_objects_v2", s3_additional_kwargs=s3_additional_kwargs
        )
    else:
        extra_kwargs = {}
    bucket = _utils.parse_path(path=paths[0])[0]
    batch: List[Dict[str, str]] = [{"Key": _utils.parse_path(path)[1]} for path in paths]
    res = client_s3.delete_objects(Bucket=bucket, Delete={"Objects": batch}, **extra_kwargs)
    deleted: List[Dict[str, Any]] = res.get("Deleted", [])
    for obj in deleted:
        _logger.debug("s3://%s/%s has been deleted.", bucket, obj.get("Key"))
    errors: List[Dict[str, Any]] = res.get("Errors", [])
    for error in errors:
        _logger.debug("error: %s", error)
        if "Code" not in error or error["Code"] != "InternalError":
            raise exceptions.ServiceApiError(errors)


def delete_objects(
    path: Union[str, List[str]],
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
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
    s3_additional_kwargs : Optional[Dict[str, Any]]
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
    paths: List[str] = _path2list(
        path=path,
        boto3_session=boto3_session,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    paths_by_bucket: Dict[str, List[str]] = _split_paths_by_bucket(paths)

    chunks = []
    for _, paths in paths_by_bucket.items():
        chunks += _utils.chunkify(lst=paths, max_length=1_000)

    executor = _get_executor(use_threads=use_threads)
    ray_get(
        executor.map(
            _delete_objects,
            boto3_session,
            chunks,
            itertools.repeat(s3_additional_kwargs),
        )
    )
