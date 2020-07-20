"""Amazon S3 Wait Module (PRIVATE)."""

import concurrent.futures
import itertools
import logging
from typing import List, Optional, Tuple, Union

import boto3  # type: ignore

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def _wait_objects(
    waiter_name: str,
    paths: List[str],
    delay: Optional[Union[int, float]] = None,
    max_attempts: Optional[int] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    delay = 5 if delay is None else delay
    max_attempts = 20 if max_attempts is None else max_attempts
    _delay: int = int(delay) if isinstance(delay, float) else delay
    if len(paths) < 1:
        return None
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    _paths: List[Tuple[str, str]] = [_utils.parse_path(path=p) for p in paths]
    if use_threads is False:
        waiter = client_s3.get_waiter(waiter_name)
        for bucket, key in _paths:
            waiter.wait(Bucket=bucket, Key=key, WaiterConfig={"Delay": _delay, "MaxAttempts": max_attempts})
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            list(
                executor.map(
                    _wait_objects_concurrent,
                    _paths,
                    itertools.repeat(waiter_name),
                    itertools.repeat(client_s3),
                    itertools.repeat(_delay),
                    itertools.repeat(max_attempts),
                )
            )
    return None


def _wait_objects_concurrent(
    path: Tuple[str, str], waiter_name: str, client_s3: boto3.client, delay: int, max_attempts: int
) -> None:
    waiter = client_s3.get_waiter(waiter_name)
    bucket, key = path
    waiter.wait(Bucket=bucket, Key=key, WaiterConfig={"Delay": delay, "MaxAttempts": max_attempts})


def wait_objects_exist(
    paths: List[str],
    delay: Optional[Union[int, float]] = None,
    max_attempts: Optional[int] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Wait Amazon S3 objects exist.

    Polls S3.Client.head_object() every 5 seconds (default) until a successful
    state is reached. An error is returned after 20 (default) failed checks.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Waiter.ObjectExists

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    paths : List[str]
        List of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    delay : Union[int,float], optional
        The amount of time in seconds to wait between attempts. Default: 5
    max_attempts : int, optional
        The maximum number of attempts to be made. Default: 20
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
    >>> wr.s3.wait_objects_exist(['s3://bucket/key0', 's3://bucket/key1'])  # wait both objects

    """
    return _wait_objects(
        waiter_name="object_exists",
        paths=paths,
        delay=delay,
        max_attempts=max_attempts,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )


def wait_objects_not_exist(
    paths: List[str],
    delay: Optional[Union[int, float]] = None,
    max_attempts: Optional[int] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Wait Amazon S3 objects not exist.

    Polls S3.Client.head_object() every 5 seconds (default) until a successful
    state is reached. An error is returned after 20 (default) failed checks.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Waiter.ObjectNotExists

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    paths : List[str]
        List of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    delay : Union[int,float], optional
        The amount of time in seconds to wait between attempts. Default: 5
    max_attempts : int, optional
        The maximum number of attempts to be made. Default: 20
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
    >>> wr.s3.wait_objects_not_exist(['s3://bucket/key0', 's3://bucket/key1'])  # wait both objects not exist

    """
    return _wait_objects(
        waiter_name="object_not_exists",
        paths=paths,
        delay=delay,
        max_attempts=max_attempts,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )
