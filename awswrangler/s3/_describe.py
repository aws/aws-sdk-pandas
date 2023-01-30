"""Amazon S3 Describe Module (INTERNAL)."""

import concurrent.futures
import datetime
import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3

from awswrangler import _utils
from awswrangler.s3 import _fs
from awswrangler.s3._list import _path2list

_logger: logging.Logger = logging.getLogger(__name__)


def _describe_object(
    path: str,
    s3_client: boto3.client,
    s3_additional_kwargs: Optional[Dict[str, Any]],
    version_id: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    bucket: str
    key: str
    bucket, key = _utils.parse_path(path=path)
    if s3_additional_kwargs:
        extra_kwargs: Dict[str, Any] = _fs.get_botocore_valid_kwargs(
            function_name="head_object", s3_additional_kwargs=s3_additional_kwargs
        )
    else:
        extra_kwargs = {}
    desc: Dict[str, Any]
    if version_id:
        extra_kwargs["VersionId"] = version_id
    desc = _utils.try_it(
        f=s3_client.head_object, ex=s3_client.exceptions.NoSuchKey, Bucket=bucket, Key=key, **extra_kwargs
    )
    return path, desc


def _describe_object_concurrent(
    path: str,
    s3_client: boto3.client,
    s3_additional_kwargs: Optional[Dict[str, Any]],
    version_id: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    return _describe_object(
        path=path, s3_client=s3_client, s3_additional_kwargs=s3_additional_kwargs, version_id=version_id
    )


def _describe_objects(
    path: Union[str, List[str]],
    s3_client: boto3.client,
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    paths: List[str] = _path2list(
        path=path,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if len(paths) < 1:
        return {}
    resp_list: List[Tuple[str, Dict[str, Any]]]
    if len(paths) == 1:
        resp_list = [
            _describe_object(
                path=paths[0],
                version_id=version_id.get(paths[0]) if isinstance(version_id, dict) else version_id,
                s3_client=s3_client,
                s3_additional_kwargs=s3_additional_kwargs,
            )
        ]
    elif use_threads is False:
        resp_list = [
            _describe_object(
                path=p,
                version_id=version_id.get(p) if isinstance(version_id, dict) else version_id,
                s3_client=s3_client,
                s3_additional_kwargs=s3_additional_kwargs,
            )
            for p in paths
        ]
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        versions = [version_id.get(p) if isinstance(version_id, dict) else version_id for p in paths]
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            resp_list = list(
                executor.map(
                    _describe_object_concurrent,
                    paths,
                    itertools.repeat(s3_client),
                    itertools.repeat(s3_additional_kwargs),
                    versions,
                )
            )
    desc_dict: Dict[str, Dict[str, Any]] = dict(resp_list)
    return desc_dict


def describe_objects(
    path: Union[str, List[str]],
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Dict[str, Any]]:
    """Describe Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    Fetch attributes like ContentLength, DeleteMarker, last_modified, ContentType, etc
    The full list of attributes can be explored under the boto3 head_object documentation:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

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
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
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
    Dict[str, Dict[str, Any]]
        Return a dictionary of objects returned from head_objects where the key is the object path.
        The response object can be explored here:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    Examples
    --------
    >>> import awswrangler as wr
    >>> descs0 = wr.s3.describe_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Describe both objects
    >>> descs1 = wr.s3.describe_objects('s3://bucket/prefix')  # Describe all objects under the prefix

    """
    s3_client: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    return _describe_objects(
        path=path,
        s3_client=s3_client,
        version_id=version_id,
        use_threads=use_threads,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        s3_additional_kwargs=s3_additional_kwargs,
    )


def _size_objects(
    path: Union[str, List[str]],
    s3_client: boto3.client,
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    use_threads: Union[bool, int] = True,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Optional[int]]:
    desc_list: Dict[str, Dict[str, Any]] = _describe_objects(
        path=path,
        s3_client=s3_client,
        version_id=version_id,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    size_dict: Dict[str, Optional[int]] = {k: d.get("ContentLength", None) for k, d in desc_list.items()}
    return size_dict


def size_objects(
    path: Union[str, List[str]],
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    use_threads: Union[bool, int] = True,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Optional[int]]:
    """Get the size (ContentLength) in bytes of Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
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

    """
    s3_client: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    return _size_objects(
        path=path,
        s3_client=s3_client,
        version_id=version_id,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )


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
