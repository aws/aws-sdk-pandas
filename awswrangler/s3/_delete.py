"""Amazon S3 CopDeletey Module (PRIVATE)."""

import concurrent.futures
import itertools
import logging
from typing import Dict, List, Optional, Union

import boto3  # type: ignore

from awswrangler import _utils, exceptions
from awswrangler.s3._list import path2list

_logger: logging.Logger = logging.getLogger(__name__)


def _split_paths_by_bucket(paths: List[str]) -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}
    bucket: str
    key: str
    for path in paths:
        bucket, key = _utils.parse_path(path=path)
        if bucket not in buckets:
            buckets[bucket] = []
        buckets[bucket].append(key)
    return buckets


def _delete_objects(bucket: str, keys: List[str], client_s3: boto3.client) -> None:
    _logger.debug("len(keys): %s", len(keys))
    batch: List[Dict[str, str]] = [{"Key": key} for key in keys]
    res = client_s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    deleted = res.get("Deleted")
    if deleted is not None:
        for i in deleted:
            _logger.debug("s3://%s/%s has been deleted.", bucket, i.get("Key"))
    errors = res.get("Errors")
    if errors is not None:  # pragma: no cover
        raise exceptions.ServiceApiError(errors)


def delete_objects(
    path: Union[str, List[str]], use_threads: bool = True, boto3_session: Optional[boto3.Session] = None
) -> None:
    """Delete Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
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
    paths: List[str] = path2list(path=path, boto3_session=boto3_session)
    if len(paths) < 1:
        return
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    buckets: Dict[str, List[str]] = _split_paths_by_bucket(paths=paths)
    for bucket, keys in buckets.items():
        chunks: List[List[str]] = _utils.chunkify(lst=keys, max_length=1_000)
        if use_threads is False:
            for chunk in chunks:
                _delete_objects(bucket=bucket, keys=chunk, client_s3=client_s3)
        else:
            cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
            with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
                list(executor.map(_delete_objects, itertools.repeat(bucket), chunks, itertools.repeat(client_s3)))
