"""Amazon S3 Vectors - Management (control plane) Module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import Any

import boto3

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.s3._vectors._utils import _resolve_target

_logger: logging.Logger = logging.getLogger(__name__)


def _bucket_kwargs(name: str | None, arn: str | None) -> dict[str, str]:
    if (name is None) == (arn is None):
        raise exceptions.InvalidArgumentCombination("Exactly one of `name` and `arn` must be provided.")
    if arn is not None:
        return {"vectorBucketArn": arn}
    return {"vectorBucketName": name}  # type: ignore[dict-item]


def _encryption_block(sse_type: str | None, kms_key_arn: str | None) -> dict[str, str] | None:
    if sse_type is None and kms_key_arn is None:
        return None
    block: dict[str, str] = {}
    if sse_type is not None:
        block["sseType"] = sse_type
    if kms_key_arn is not None:
        block["kmsKeyArn"] = kms_key_arn
    return block


# ---------------------------------------------------------------------------
# Vector buckets
# ---------------------------------------------------------------------------


@apply_configs
def create_vector_bucket(
    name: str,
    *,
    encryption_kms_key_arn: str | None = None,
    sse_type: str | None = None,
    tags: dict[str, str] | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Create an Amazon S3 Vectors bucket.

    Parameters
    ----------
    name
        Name of the vector bucket to create. 3-63 chars.
    encryption_kms_key_arn
        Optional KMS key ARN for SSE-KMS encryption. Implies ``sse_type='aws:kms'`` if not specified.
    sse_type
        Server-side encryption type. ``'AES256'`` (default if encryption block omitted) or ``'aws:kms'``.
    tags
        Resource tags as a dict.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        ARN of the created vector bucket.

    Examples
    --------
    >>> import awswrangler as wr
    >>> arn = wr.s3.create_vector_bucket("my-vector-bucket")
    """
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    kwargs: dict[str, Any] = {"vectorBucketName": name}
    enc = _encryption_block(sse_type, encryption_kms_key_arn)
    if enc is not None:
        kwargs["encryptionConfiguration"] = enc
    if tags:
        kwargs["tags"] = tags
    response = client.create_vector_bucket(**kwargs)  # type: ignore[attr-defined]
    arn: str = response["vectorBucketArn"]
    _logger.debug("Created vector bucket %s with ARN: %s", name, arn)
    return arn


@apply_configs
def delete_vector_bucket(
    name: str | None = None,
    *,
    arn: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete an Amazon S3 Vectors bucket. Specify either ``name`` or ``arn``."""
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    client.delete_vector_bucket(**_bucket_kwargs(name, arn))  # type: ignore[attr-defined]
    _logger.debug("Deleted vector bucket %s", name or arn)


@apply_configs
def list_vector_buckets(
    prefix: str | None = None,
    *,
    boto3_session: boto3.Session | None = None,
) -> list[dict[str, Any]]:
    """List all Amazon S3 Vectors buckets in the account/region (paginates internally).

    Parameters
    ----------
    prefix
        Optional name prefix filter.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        List of vector bucket summaries (each a dict with ``vectorBucketName``, ``vectorBucketArn``, ``creationTime``).
    """
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    buckets: list[dict[str, Any]] = []
    next_token: str | None = None
    while True:
        kwargs: dict[str, Any] = {}
        if prefix:
            kwargs["prefix"] = prefix
        if next_token:
            kwargs["nextToken"] = next_token
        response = client.list_vector_buckets(**kwargs)  # type: ignore[attr-defined]
        buckets.extend(response.get("vectorBuckets", []))
        next_token = response.get("nextToken")
        if not next_token:
            break
    return buckets


@apply_configs
def get_vector_bucket(
    name: str | None = None,
    *,
    arn: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Get attributes of a vector bucket. Specify either ``name`` or ``arn``."""
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    response = client.get_vector_bucket(**_bucket_kwargs(name, arn))  # type: ignore[attr-defined]
    bucket: dict[str, Any] = response["vectorBucket"]
    return bucket


# ---------------------------------------------------------------------------
# Vector indexes
# ---------------------------------------------------------------------------


@apply_configs
def create_vector_index(
    *,
    name: str,
    dimension: int,
    distance_metric: str = "cosine",
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    data_type: str = "float32",
    non_filterable_metadata_keys: list[str] | None = None,
    encryption_kms_key_arn: str | None = None,
    sse_type: str | None = None,
    tags: dict[str, str] | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Create a vector index inside an Amazon S3 Vectors bucket.

    Parameters
    ----------
    name
        Index name (3-63 chars).
    dimension
        Vector dimension (1-4096). All vectors written to the index must match.
    distance_metric
        ``'cosine'`` (default) or ``'euclidean'``.
    vector_bucket / vector_bucket_arn
        Target vector bucket. Specify exactly one.
    data_type
        Vector element type. Currently only ``'float32'`` is supported.
    non_filterable_metadata_keys
        Metadata keys excluded from filtering (up to 10). Cannot be changed after index creation.
    encryption_kms_key_arn, sse_type
        Encryption overrides; default is to inherit from the bucket.
    tags
        Resource tags.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        ARN of the created vector index.

    Examples
    --------
    >>> import awswrangler as wr
    >>> arn = wr.s3.create_vector_index(
    ...     vector_bucket="my-bucket", name="my-index", dimension=384
    ... )
    """
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    kwargs: dict[str, Any] = {
        "indexName": name,
        "dataType": data_type,
        "dimension": dimension,
        "distanceMetric": distance_metric,
        **_bucket_kwargs(vector_bucket, vector_bucket_arn),
    }
    if non_filterable_metadata_keys:
        kwargs["metadataConfiguration"] = {"nonFilterableMetadataKeys": list(non_filterable_metadata_keys)}
    enc = _encryption_block(sse_type, encryption_kms_key_arn)
    if enc is not None:
        kwargs["encryptionConfiguration"] = enc
    if tags:
        kwargs["tags"] = tags
    response = client.create_index(**kwargs)  # type: ignore[attr-defined]
    arn: str = response["indexArn"]
    _logger.debug("Created vector index %s in bucket %s (ARN: %s)", name, vector_bucket or vector_bucket_arn, arn)
    return arn


@apply_configs
def delete_vector_index(
    *,
    name: str | None = None,
    arn: str | None = None,
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete a vector index. Specify either ``arn``, or ``name`` together with ``vector_bucket``/``vector_bucket_arn``."""
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    target = _resolve_target(
        vector_bucket=vector_bucket, vector_bucket_arn=vector_bucket_arn, index=name, index_arn=arn
    )
    client.delete_index(**target)  # type: ignore[attr-defined]
    _logger.debug("Deleted vector index %s", name or arn)


@apply_configs
def list_vector_indexes(
    *,
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    prefix: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> list[dict[str, Any]]:
    """List all vector indexes in a bucket (paginates internally)."""
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    indexes: list[dict[str, Any]] = []
    next_token: str | None = None
    base = _bucket_kwargs(vector_bucket, vector_bucket_arn)
    while True:
        kwargs: dict[str, Any] = {**base}
        if prefix:
            kwargs["prefix"] = prefix
        if next_token:
            kwargs["nextToken"] = next_token
        response = client.list_indexes(**kwargs)  # type: ignore[attr-defined]
        indexes.extend(response.get("indexes", []))
        next_token = response.get("nextToken")
        if not next_token:
            break
    return indexes


@apply_configs
def get_vector_index(
    *,
    name: str | None = None,
    arn: str | None = None,
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Get attributes of a vector index."""
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    target = _resolve_target(
        vector_bucket=vector_bucket, vector_bucket_arn=vector_bucket_arn, index=name, index_arn=arn
    )
    response = client.get_index(**target)  # type: ignore[attr-defined]
    index: dict[str, Any] = response["index"]
    return index
