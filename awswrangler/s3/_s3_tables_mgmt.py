"""Amazon S3 Tables Management Module (PRIVATE)."""

from __future__ import annotations

import logging

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def create_table_bucket(
    name: str,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Create an S3 Table Bucket.

    Parameters
    ----------
    name : str
        The name of the table bucket to create.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Returns
    -------
    str
        The ARN of the created table bucket.

    Examples
    --------
    >>> import awswrangler as wr
    >>> arn = wr.s3.create_table_bucket(name="my-table-bucket")
    """
    s3tables_client = _utils.client(service_name="s3tables", session=boto3_session)
    response = s3tables_client.create_table_bucket(name=name)  # type: ignore[attr-defined]
    arn: str = response["arn"]
    _logger.debug("Created table bucket %s with ARN: %s", name, arn)
    return arn


def create_namespace(
    table_bucket_arn: str,
    namespace: str,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Create a namespace in an S3 Table Bucket.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the table bucket.
    namespace : str
        The name of the namespace to create.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Returns
    -------
    str
        The namespace name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> ns = wr.s3.create_namespace(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ... )
    """
    s3tables_client = _utils.client(service_name="s3tables", session=boto3_session)
    s3tables_client.create_namespace(  # type: ignore[attr-defined]
        tableBucketARN=table_bucket_arn,
        namespace=[namespace],
    )
    _logger.debug("Created namespace %s in table bucket %s", namespace, table_bucket_arn)
    return namespace


def create_table(
    table_bucket_arn: str,
    namespace: str,
    table_name: str,
    format: str = "ICEBERG",
    boto3_session: boto3.Session | None = None,
) -> str:
    """Create a table in an S3 Table Bucket namespace.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the table bucket.
    namespace : str
        The namespace in which to create the table.
    table_name : str
        The name of the table to create.
    format : str, optional
        The table format. Default is ``"ICEBERG"``.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Returns
    -------
    str
        The ARN of the created table.

    Examples
    --------
    >>> import awswrangler as wr
    >>> table_arn = wr.s3.create_table(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ...     table_name="my_table",
    ... )
    """
    s3tables_client = _utils.client(service_name="s3tables", session=boto3_session)
    response = s3tables_client.create_table(  # type: ignore[attr-defined]
        tableBucketARN=table_bucket_arn,
        namespace=namespace,
        name=table_name,
        format=format,
    )
    table_arn: str = response["tableARN"]
    _logger.debug("Created table %s.%s with ARN: %s", namespace, table_name, table_arn)
    return table_arn


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
    >>> wr.s3.delete_table_bucket(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ... )
    """
    s3tables_client = _utils.client(service_name="s3tables", session=boto3_session)
    s3tables_client.delete_table_bucket(tableBucketARN=table_bucket_arn)  # type: ignore[attr-defined]
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
    >>> wr.s3.delete_namespace(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ... )
    """
    s3tables_client = _utils.client(service_name="s3tables", session=boto3_session)
    s3tables_client.delete_namespace(  # type: ignore[attr-defined]
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
    >>> wr.s3.delete_table(
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
    s3tables_client.delete_table(**kwargs)  # type: ignore[attr-defined]
    _logger.debug("Deleted table %s.%s from table bucket %s", namespace, table_name, table_bucket_arn)
