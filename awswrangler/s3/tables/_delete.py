"""Amazon S3 Tables Delete Module (PRIVATE)."""

from __future__ import annotations

import logging

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def delete_table_bucket(
    table_bucket_arn: str,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete an S3 Table Bucket.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the table bucket to delete.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.s3.tables.delete_table_bucket(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ... )
    """
    s3tables_client = _utils.client(service_name="s3tables", session=boto3_session)
    s3tables_client.delete_table_bucket(tableBucketARN=table_bucket_arn)
    _logger.debug("Deleted table bucket %s", table_bucket_arn)


def delete_namespace(
    table_bucket_arn: str,
    namespace: str,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete a namespace from an S3 Table Bucket.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the table bucket.
    namespace : str
        The name of the namespace to delete.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.s3.tables.delete_namespace(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ... )
    """
    s3tables_client = _utils.client(service_name="s3tables", session=boto3_session)
    s3tables_client.delete_namespace(
        tableBucketARN=table_bucket_arn,
        namespace=namespace,
    )
    _logger.debug("Deleted namespace %s from table bucket %s", namespace, table_bucket_arn)


def delete_table(
    table_bucket_arn: str,
    namespace: str,
    table_name: str,
    version_token: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete a table from an S3 Table Bucket namespace.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the table bucket.
    namespace : str
        The namespace of the table.
    table_name : str
        The name of the table to delete.
    version_token : str, optional
        The version token of the table. If not provided, the current version is deleted.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.s3.tables.delete_table(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ...     table_name="my_table",
    ... )
    """
    s3tables_client = _utils.client(service_name="s3tables", session=boto3_session)
    kwargs: dict[str, str] = {
        "tableBucketARN": table_bucket_arn,
        "namespace": namespace,
        "name": table_name,
    }
    if version_token is not None:
        kwargs["versionToken"] = version_token
    s3tables_client.delete_table(**kwargs)
    _logger.debug("Deleted table %s.%s from table bucket %s", namespace, table_name, table_bucket_arn)
