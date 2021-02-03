"""Amazon S3 Download Module (PRIVATE)."""

import logging
from typing import Any, Dict, Optional, Union

import boto3

from awswrangler import _utils
from awswrangler.s3._fs import open_s3_object

_logger: logging.Logger = logging.getLogger(__name__)


def download(
    path: str,
    local_file: Union[str, Any],
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    """Download file from from a received S3 path to local file.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    path : str
        S3 path (e.g. ``s3://bucket/key0``).
    local_file : Union[str, Any]
        A file-like object in binary mode or a path to local file (e.g. ``./local/path/to/key0``).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests, only "SSECustomerAlgorithm", "SSECustomerKey" and "RequestPayer"
        arguments will be considered.

    Returns
    -------
    None

    Examples
    --------
    Downloading a file using a path to local file

    >>> import awswrangler as wr
    >>> wr.s3.download(path='s3://bucket/key', local_file='./key')

    Downloading a file using a file-like object

    >>> import awswrangler as wr
    >>> with open(file='./key', mode='wb') as local_f:
    >>>     wr.s3.download(path='s3://bucket/key', local_file=local_f)

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    _logger.debug("path: %s", path)
    with open_s3_object(
        path=path,
        mode="rb",
        use_threads=use_threads,
        s3_block_size=-1,  # One shot download
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=session,
    ) as s3_f:
        if isinstance(local_file, str):
            _logger.debug("Downloading local_file: %s", local_file)
            with open(file=local_file, mode="wb") as local_f:
                local_f.write(s3_f.read())
        else:
            _logger.debug("Downloading file-like object.")
            local_file.write(s3_f.read())
