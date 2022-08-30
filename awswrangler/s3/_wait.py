"""Amazon S3 Wait Module (PRIVATE)."""

import itertools
import logging
from typing import List, Optional, Union

import boto3

from awswrangler import _utils
from awswrangler._config import config
from awswrangler.distributed import ray_get, ray_remote
from awswrangler._threading import _get_executor

_logger: logging.Logger = logging.getLogger(__name__)


def _wait_object(
    boto3_session: Optional[boto3.Session], path: str, waiter_name: str, delay: int, max_attempts: int
) -> None:
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    waiter = client_s3.get_waiter(waiter_name)

    bucket, key = _utils.parse_path(path=path)
    waiter.wait(Bucket=bucket, Key=key, WaiterConfig={"Delay": delay, "MaxAttempts": max_attempts})


@ray_remote
def _wait_object_batch(
    boto3_session: Optional[boto3.Session], paths: List[str], waiter_name: str, delay: int, max_attempts: int
) -> None:
    for path in paths:
        _wait_object(boto3_session, path, waiter_name, delay, max_attempts)


def _wait_objects(
    waiter_name: str,
    paths: List[str],
    delay: Optional[float] = None,
    max_attempts: Optional[int] = None,
    use_threads: Union[bool, int] = True,
    parallelism: Optional[int] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    delay = 5 if delay is None else delay
    max_attempts = 20 if max_attempts is None else max_attempts
    parallelism = 100 if parallelism is None else parallelism
    _delay: int = int(delay) if isinstance(delay, float) else delay

    if len(paths) < 1:
        return None

    if config.distributed and len(paths) > parallelism:
        path_batches = _utils.chunkify(paths, parallelism)
    else:
        path_batches = [[path] for path in paths]

    executor = _get_executor(use_threads=use_threads)
    ray_get(
        executor.map(
            _wait_object_batch,
            boto3_session,
            path_batches,
            itertools.repeat(waiter_name),
            itertools.repeat(_delay),
            itertools.repeat(max_attempts),
        )
    )

    return None


def wait_objects_exist(
    paths: List[str],
    delay: Optional[float] = None,
    max_attempts: Optional[int] = None,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    parallelism: Optional[int] = None,
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
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    parallelism: int, optional
        The requested parallelism of the wait. Only used when `distributed` add-on is installed.
        Parallelism may be limited by the number of files of the dataset. 100 by default.
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
    delay: Optional[float] = None,
    max_attempts: Optional[int] = None,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    parallelism: Optional[int] = None,
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
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    parallelism: int, optional
        The requested parallelism of the wait. Only used when `distributed` add-on is installed.
        Parallelism may be limited by the number of files of the dataset. 100 by default.
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
