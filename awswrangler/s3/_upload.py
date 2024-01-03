"""Amazon S3 Upload Module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import Any

import boto3

from awswrangler.s3._fs import open_s3_object

_logger: logging.Logger = logging.getLogger(__name__)


def upload(
    local_file: str | Any,
    path: str,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
) -> None:
    """Upload file from a local file to received S3 path.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    local_file : Union[str, Any]
        A file-like object in binary mode or a path to local file (e.g. ``./local/path/to/key0``).
    path : str
        S3 path (e.g. ``s3://bucket/key0``).
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    pyarrow_additional_kwargs: dict[str, Any], optional
        Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.

    Returns
    -------
    None

    Examples
    --------
    Uploading a file using a path to local file

    >>> import awswrangler as wr
    >>> wr.s3.upload(local_file='./key', path='s3://bucket/key')

    Uploading a file using a file-like object

    >>> import awswrangler as wr
    >>> with open(file='./key', mode='wb') as local_f:
    >>>     wr.s3.upload(local_file=local_f, path='s3://bucket/key')

    """
    _logger.debug("path: %s", path)
    with open_s3_object(
        path=path,
        mode="wb",
        use_threads=use_threads,
        s3_block_size=-1,  # One shot download
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
    ) as s3_f:
        if isinstance(local_file, str):
            _logger.debug("Uploading local_file: %s", local_file)
            with open(file=local_file, mode="rb") as local_f:
                s3_f.write(local_f.read())  # type: ignore[arg-type]
        else:
            _logger.debug("Uploading file-like object.")
            s3_f.write(local_file.read())
