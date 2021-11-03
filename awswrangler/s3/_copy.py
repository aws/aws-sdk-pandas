"""Amazon S3 Copy Module (PRIVATE)."""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
from boto3.s3.transfer import TransferConfig

from awswrangler import _utils, exceptions
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._fs import get_botocore_valid_kwargs
from awswrangler.s3._list import list_objects

_logger: logging.Logger = logging.getLogger(__name__)


def _copy_objects(
    batch: List[Tuple[str, str]],
    use_threads: Union[bool, int],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, Any]],
) -> None:
    _logger.debug("len(batch): %s", len(batch))
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    resource_s3: boto3.resource = _utils.resource(service_name="s3", session=boto3_session)
    if s3_additional_kwargs is None:
        boto3_kwargs: Optional[Dict[str, Any]] = None
    else:
        boto3_kwargs = get_botocore_valid_kwargs(function_name="copy_object", s3_additional_kwargs=s3_additional_kwargs)
    for source, target in batch:
        source_bucket, source_key = _utils.parse_path(path=source)
        copy_source: Dict[str, str] = {"Bucket": source_bucket, "Key": source_key}
        target_bucket, target_key = _utils.parse_path(path=target)
        resource_s3.meta.client.copy(
            CopySource=copy_source,
            Bucket=target_bucket,
            Key=target_key,
            SourceClient=client_s3,
            ExtraArgs=boto3_kwargs,
            Config=TransferConfig(num_download_attempts=10, use_threads=use_threads),
        )


def merge_datasets(
    source_path: str,
    target_path: str,
    mode: str = "append",
    ignore_empty: bool = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Merge a source dataset into a target dataset.

    This function accepts Unix shell-style wildcards in the source_path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(source_path)` before passing the path to this function.

    Note
    ----
    If you are merging tables (S3 datasets + Glue Catalog metadata),
    remember that you will also need to update your partitions metadata in some cases.
    (e.g. wr.athena.repair_table(table='...', database='...'))

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    source_path : str,
        S3 Path for the source directory.
    target_path : str,
        S3 Path for the target directory.
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``.
    ignore_empty: bool
        Ignore files with 0 bytes.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}

    Returns
    -------
    List[str]
        List of new objects paths.

    Examples
    --------
    Merging

    >>> import awswrangler as wr
    >>> wr.s3.merge_datasets(
    ...     source_path="s3://bucket0/dir0/",
    ...     target_path="s3://bucket1/dir1/",
    ...     mode="append"
    ... )
    ["s3://bucket1/dir1/key0", "s3://bucket1/dir1/key1"]

    Merging with a KMS key

    >>> import awswrangler as wr
    >>> wr.s3.merge_datasets(
    ...     source_path="s3://bucket0/dir0/",
    ...     target_path="s3://bucket1/dir1/",
    ...     mode="append",
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )
    ["s3://bucket1/dir1/key0", "s3://bucket1/dir1/key1"]

    """
    source_path = source_path[:-1] if source_path[-1] == "/" else source_path
    target_path = target_path[:-1] if target_path[-1] == "/" else target_path
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    paths: List[str] = list_objects(  # type: ignore
        path=f"{source_path}/", ignore_empty=ignore_empty, boto3_session=session
    )
    _logger.debug("len(paths): %s", len(paths))
    if len(paths) < 1:
        return []

    if mode == "overwrite":
        _logger.debug("Deleting to overwrite: %s/", target_path)
        delete_objects(path=f"{target_path}/", use_threads=use_threads, boto3_session=session)
    elif mode == "overwrite_partitions":
        paths_wo_prefix: List[str] = [x.replace(f"{source_path}/", "") for x in paths]
        paths_wo_filename: List[str] = [f"{x.rpartition('/')[0]}/" for x in paths_wo_prefix]
        partitions_paths: List[str] = list(set(paths_wo_filename))
        target_partitions_paths = [f"{target_path}/{x}" for x in partitions_paths]
        for path in target_partitions_paths:
            _logger.debug("Deleting to overwrite_partitions: %s", path)
            delete_objects(path=path, use_threads=use_threads, boto3_session=session)
    elif mode != "append":
        raise exceptions.InvalidArgumentValue(f"{mode} is a invalid mode option.")

    new_objects: List[str] = copy_objects(
        paths=paths,
        source_path=source_path,
        target_path=target_path,
        use_threads=use_threads,
        boto3_session=session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    _logger.debug("len(new_objects): %s", len(new_objects))
    return new_objects


def copy_objects(
    paths: List[str],
    source_path: str,
    target_path: str,
    replace_filenames: Optional[Dict[str, str]] = None,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Copy a list of S3 objects to another S3 directory.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    paths : List[str]
        List of S3 objects paths (e.g. [s3://bucket/dir0/key0, s3://bucket/dir0/key1]).
    source_path : str,
        S3 Path for the source directory.
    target_path : str,
        S3 Path for the target directory.
    replace_filenames : Dict[str, str], optional
        e.g. {"old_name.csv": "new_name.csv", "old_name2.csv": "new_name2.csv"}
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}

    Returns
    -------
    List[str]
        List of new objects paths.

    Examples
    --------
    Copying

    >>> import awswrangler as wr
    >>> wr.s3.copy_objects(
    ...     paths=["s3://bucket0/dir0/key0", "s3://bucket0/dir0/key1"],
    ...     source_path="s3://bucket0/dir0/",
    ...     target_path="s3://bucket1/dir1/"
    ... )
    ["s3://bucket1/dir1/key0", "s3://bucket1/dir1/key1"]

    Copying with a KMS key

    >>> import awswrangler as wr
    >>> wr.s3.copy_objects(
    ...     paths=["s3://bucket0/dir0/key0", "s3://bucket0/dir0/key1"],
    ...     source_path="s3://bucket0/dir0/",
    ...     target_path="s3://bucket1/dir1/",
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )
    ["s3://bucket1/dir1/key0", "s3://bucket1/dir1/key1"]

    """
    _logger.debug("len(paths): %s", len(paths))
    if len(paths) < 1:
        return []
    source_path = source_path[:-1] if source_path[-1] == "/" else source_path
    target_path = target_path[:-1] if target_path[-1] == "/" else target_path
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    batch: List[Tuple[str, str]] = []
    new_objects: List[str] = []
    for path in paths:
        path_wo_prefix: str = path.replace(f"{source_path}/", "")
        path_final: str = f"{target_path}/{path_wo_prefix}"
        if replace_filenames is not None:
            parts: List[str] = path_final.rsplit(sep="/", maxsplit=1)
            if len(parts) == 2:
                path_wo_filename: str = parts[0]
                filename: str = parts[1]
                if filename in replace_filenames:
                    new_filename: str = replace_filenames[filename]
                    _logger.debug("Replacing filename: %s -> %s", filename, new_filename)
                    path_final = f"{path_wo_filename}/{new_filename}"
        new_objects.append(path_final)
        batch.append((path, path_final))
    _logger.debug("len(new_objects): %s", len(new_objects))
    _copy_objects(
        batch=batch, use_threads=use_threads, boto3_session=session, s3_additional_kwargs=s3_additional_kwargs
    )
    return new_objects
