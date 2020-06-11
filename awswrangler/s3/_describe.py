"""Amazon S3 Describe Module (INTERNAL)."""

import concurrent.futures
import logging
import time
from itertools import repeat
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3  # type: ignore
import botocore.exceptions  # type: ignore

from awswrangler import _utils
from awswrangler.s3._list import path2list

_logger: logging.Logger = logging.getLogger(__name__)


def _describe_object(
    path: str, wait_time: Optional[Union[int, float]], client_s3: boto3.client
) -> Tuple[str, Dict[str, Any]]:
    wait_time = int(wait_time) if isinstance(wait_time, float) else wait_time
    tries: int = wait_time if (wait_time is not None) and (wait_time > 0) else 1
    bucket: str
    key: str
    bucket, key = _utils.parse_path(path=path)
    desc: Dict[str, Any] = {}
    for i in range(tries, 0, -1):
        try:
            desc = client_s3.head_object(Bucket=bucket, Key=key)
            break
        except botocore.exceptions.ClientError as e:  # pragma: no cover
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:  # Not Found
                _logger.debug("Object not found. %s seconds remaining to wait.", i)
                if i == 1:  # Last try, there is no more need to sleep
                    break
                time.sleep(1)
            else:
                raise e
    return path, desc


def describe_objects(
    path: Union[str, List[str]],
    wait_time: Optional[Union[int, float]] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Dict[str, Any]]:
    """Describe Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    Fetch attributes like ContentLength, DeleteMarker, LastModified, ContentType, etc
    The full list of attributes can be explored under the boto3 head_object documentation:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    wait_time : Union[int,float], optional
        How much time (seconds) should Wrangler try to reach this objects.
        Very useful to overcome eventual consistence issues.
        `None` means only a single try will be done.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Return a dictionary of objects returned from head_objects where the key is the object path.
        The response object can be explored here:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    Examples
    --------
    >>> import awswrangler as wr
    >>> descs0 = wr.s3.describe_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Describe both objects
    >>> descs1 = wr.s3.describe_objects('s3://bucket/prefix')  # Describe all objects under the prefix
    >>> descs2 = wr.s3.describe_objects('s3://bucket/prefix', wait_time=30)  # Overcoming eventual consistence issues

    """
    paths: List[str] = path2list(path=path, boto3_session=boto3_session)
    if len(paths) < 1:
        return {}
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    resp_list: List[Tuple[str, Dict[str, Any]]]
    if use_threads is False:
        resp_list = [_describe_object(path=p, wait_time=wait_time, client_s3=client_s3) for p in paths]
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            resp_list = list(executor.map(_describe_object, paths, repeat(wait_time), repeat(client_s3)))
    desc_dict: Dict[str, Dict[str, Any]] = dict(resp_list)
    return desc_dict


def size_objects(
    path: Union[str, List[str]],
    wait_time: Optional[Union[int, float]] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Optional[int]]:
    """Get the size (ContentLength) in bytes of Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    Note
    ----
    In case of `use_threads=True` the number of threads that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    wait_time : Union[int,float], optional
        How much time (seconds) should Wrangler try to reach this objects.
        Very useful to overcome eventual consistence issues.
        `None` means only a single try will be done.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Optional[int]]
        Dictionary where the key is the object path and the value is the object size.

    Examples
    --------
    >>> import awswrangler as wr
    >>> sizes0 = wr.s3.size_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Get the sizes of both objects
    >>> sizes1 = wr.s3.size_objects('s3://bucket/prefix')  # Get the sizes of all objects under the received prefix
    >>> sizes2 = wr.s3.size_objects('s3://bucket/prefix', wait_time=30)  # Overcoming eventual consistence issues

    """
    desc_list: Dict[str, Dict[str, Any]] = describe_objects(
        path=path, wait_time=wait_time, use_threads=use_threads, boto3_session=boto3_session
    )
    size_dict: Dict[str, Optional[int]] = {k: d.get("ContentLength", None) for k, d in desc_list.items()}
    return size_dict


def get_bucket_region(bucket: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get bucket region name.

    Parameters
    ----------
    bucket : str
        Bucket name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Region code (e.g. 'us-east-1').

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> region = wr.s3.get_bucket_region('bucket-name')

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> region = wr.s3.get_bucket_region('bucket-name', boto3_session=boto3.Session())

    """
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    _logger.debug("bucket: %s", bucket)
    region: str = client_s3.get_bucket_location(Bucket=bucket)["LocationConstraint"]
    region = "us-east-1" if region is None else region
    _logger.debug("region: %s", region)
    return region
