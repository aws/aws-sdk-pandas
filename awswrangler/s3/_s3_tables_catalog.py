"""Amazon S3 Tables PyIceberg Catalog Bridge (PRIVATE)."""

from __future__ import annotations

import logging
import re

from awswrangler import _config, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_ARN_PATTERN = re.compile(r"^arn:aws:s3tables:([a-z0-9-]+):(\d{12}):bucket/(.+)$")


def _parse_table_bucket_arn(table_bucket_arn: str) -> tuple[str, str, str]:
    """Parse region, account ID, and bucket name from an S3 Tables bucket ARN.

    Returns
    -------
    tuple[str, str, str]
        (region, account_id, bucket_name)
    """
    match = _ARN_PATTERN.match(table_bucket_arn)
    if not match:
        raise exceptions.InvalidArgumentValue(
            f"Cannot parse ARN: {table_bucket_arn}. Expected format: arn:aws:s3tables:<region>:<account>:bucket/<name>"
        )
    return match.group(1), match.group(2), match.group(3)


def _build_catalog_properties(table_bucket_arn: str) -> dict[str, str]:
    """Build PyIceberg REST catalog properties for S3 Tables or Glue endpoint."""
    region, account_id, bucket_name = _parse_table_bucket_arn(table_bucket_arn)
    endpoint = _config.config.s3tables_catalog_endpoint_url

    props: dict[str, str] = {
        "type": "rest",
        "rest.sigv4-enabled": "true",
        "rest.signing-region": region,
    }

    if endpoint and "glue." in endpoint:
        # Glue Iceberg REST endpoint: warehouse uses account:catalog/bucket format
        props["uri"] = endpoint
        props["warehouse"] = f"{account_id}:s3tablescatalog/{bucket_name}"
        props["rest.signing-name"] = "glue"
    else:
        # S3 Tables REST endpoint (default): warehouse is the bucket ARN
        props["uri"] = endpoint or f"https://s3tables.{region}.amazonaws.com/iceberg"
        props["warehouse"] = table_bucket_arn
        props["rest.signing-name"] = "s3tables"

    return props


def _load_catalog(table_bucket_arn: str) -> "RestCatalog":  # type: ignore[name-defined]  # noqa: F821
    """Create and return a PyIceberg RestCatalog configured for S3 Tables."""
    from pyiceberg.catalog.rest import RestCatalog  # noqa: PLC0415

    properties = _build_catalog_properties(table_bucket_arn)
    catalog = RestCatalog(name="s3tables", **properties)
    _logger.debug("Loaded PyIceberg REST catalog for %s", table_bucket_arn)
    return catalog
