"""Amazon S3 Tables PyIceberg Catalog Bridge (PRIVATE)."""

from __future__ import annotations

import logging
import re

import boto3

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_ARN_REGION_PATTERN = re.compile(r"^arn:aws:s3tables:([a-z0-9-]+):\d{12}:bucket/.+$")


def _extract_region_from_arn(table_bucket_arn: str) -> str:
    """Extract the AWS region from an S3 Tables bucket ARN.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the table bucket (e.g. ``arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket``).

    Returns
    -------
    str
        The AWS region.
    """
    match = _ARN_REGION_PATTERN.match(table_bucket_arn)
    if not match:
        raise exceptions.InvalidArgumentValue(
            f"Cannot extract region from ARN: {table_bucket_arn}. "
            "Expected format: arn:aws:s3tables:<region>:<account>:bucket/<name>"
        )
    return match.group(1)


def _build_catalog_properties(
    table_bucket_arn: str,
    boto3_session: boto3.Session | None = None,
) -> dict[str, str]:
    """Build PyIceberg REST catalog properties from a table bucket ARN and boto3 session.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the table bucket.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Returns
    -------
    dict[str, str]
        Properties dictionary suitable for ``pyiceberg.catalog.rest.RestCatalog``.
    """
    region = _extract_region_from_arn(table_bucket_arn)

    properties: dict[str, str] = {
        "type": "rest",
        "warehouse": table_bucket_arn,
        "uri": f"https://s3tables.{region}.amazonaws.com/iceberg",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3tables",
        "rest.signing-region": region,
    }

    if boto3_session is not None:
        credentials = boto3_session.get_credentials()
        if credentials is not None:
            frozen = credentials.get_frozen_credentials()
            properties["rest.signing-region"] = region
            if frozen.access_key:
                properties["s3tables.access-key-id"] = frozen.access_key
            if frozen.secret_key:
                properties["s3tables.secret-access-key"] = frozen.secret_key
            if frozen.token:
                properties["s3tables.session-token"] = frozen.token

    return properties


def _load_catalog(
    table_bucket_arn: str,
    boto3_session: boto3.Session | None = None,
) -> "RestCatalog":  # type: ignore[name-defined]  # noqa: F821
    """Create and return a PyIceberg RestCatalog configured for S3 Tables.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the table bucket.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Returns
    -------
    pyiceberg.catalog.rest.RestCatalog
        A configured PyIceberg REST catalog instance.
    """
    from pyiceberg.catalog.rest import RestCatalog

    properties = _build_catalog_properties(table_bucket_arn, boto3_session)
    catalog = RestCatalog(name="s3tables", **properties)
    _logger.debug("Loaded PyIceberg REST catalog for %s", table_bucket_arn)
    return catalog
