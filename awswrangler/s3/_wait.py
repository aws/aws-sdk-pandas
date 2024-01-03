"""Amazon S3 Wait Module (PRIVATE)."""

from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING

import boto3
import botocore.exceptions

from awswrangler import _utils, exceptions
from awswrangler._distributed import engine
from awswrangler._executor import _BaseExecutor, _get_executor
from awswrangler.distributed.ray import ray_get

if TYPE_CHECKING:
    from mypy_boto3_s3 import ObjectExistsWaiter, ObjectNotExistsWaiter, S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _wait_object(
    waiter: "ObjectExistsWaiter" | "ObjectNotExistsWaiter", path: str, delay: int, max_attempts: int
) -> None:
    bucket, key = _utils.parse_path(path=path)
    try:
        waiter.wait(Bucket=bucket, Key=key, WaiterConfig={"Delay": delay, "MaxAttempts": max_attempts})
    except botocore.exceptions.WaiterError:
        raise exceptions.NoFilesFound(f"No files found: {key}.")


@engine.dispatch_on_engine
def _wait_object_batch(
    s3_client: "S3Client" | None, paths: list[str], waiter_name: str, delay: int, max_attempts: int
) -> None:
    s3_client = s3_client if s3_client else _utils.client(service_name="s3")
    waiter = s3_client.get_waiter(waiter_name)  # type: ignore[call-overload]
    for path in paths:
        _wait_object(waiter, path, delay, max_attempts)


def _wait_objects(
    waiter_name: str,
    paths: list[str],
    delay: float | None,
    max_attempts: int | None,
    use_threads: bool | int,
    s3_client: "S3Client",
) -> None:
    delay = 5 if delay is None else delay
    max_attempts = 20 if max_attempts is None else max_attempts

    concurrency = _utils.ensure_worker_or_thread_count(use_threads)

    if len(paths) < 1:
        return None

    path_batches = (
        _utils.chunkify(paths, num_chunks=concurrency)
        if len(paths) > concurrency
        else _utils.chunkify(paths, max_length=1)
    )

    executor: _BaseExecutor = _get_executor(use_threads=use_threads)
    ray_get(
        executor.map(
            _wait_object_batch,
            s3_client,
            path_batches,
            itertools.repeat(waiter_name),
            itertools.repeat(int(delay)),
            itertools.repeat(max_attempts),
        )
    )


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def wait_objects_exist(
    paths: list[str],
    delay: float | None = None,
    max_attempts: int | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
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
    s3_client = _utils.client(service_name="s3", session=boto3_session)
    return _wait_objects(
        waiter_name="object_exists",
        paths=paths,
        delay=delay,
        max_attempts=max_attempts,
        use_threads=use_threads,
        s3_client=s3_client,
    )


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def wait_objects_not_exist(
    paths: list[str],
    delay: float | None = None,
    max_attempts: int | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
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
    s3_client = _utils.client(service_name="s3", session=boto3_session)
    return _wait_objects(
        waiter_name="object_not_exists",
        paths=paths,
        delay=delay,
        max_attempts=max_attempts,
        use_threads=use_threads,
        s3_client=s3_client,
    )
