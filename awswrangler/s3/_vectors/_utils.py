"""Amazon S3 Vectors - Shared internal helpers (PRIVATE)."""

from __future__ import annotations

from typing import Any

import numpy as np

import awswrangler.pandas as pd
from awswrangler import exceptions


def _resolve_target(
    *,
    vector_bucket: str | None,
    vector_bucket_arn: str | None,
    index: str | None,
    index_arn: str | None,
) -> dict[str, str]:
    """Resolve mutually-exclusive targeting params into a kwargs dict for the s3vectors client."""
    if index_arn is not None:
        if index is not None or vector_bucket is not None or vector_bucket_arn is not None:
            raise exceptions.InvalidArgumentCombination(
                "When `index_arn` is provided, `index`, `vector_bucket` and `vector_bucket_arn` must be omitted."
            )
        return {"indexArn": index_arn}

    if index is None:
        raise exceptions.InvalidArgumentCombination("Provide either `index_arn`, or `index` plus a vector bucket.")

    if (vector_bucket is None) == (vector_bucket_arn is None):
        raise exceptions.InvalidArgumentCombination(
            "Exactly one of `vector_bucket` or `vector_bucket_arn` is required when targeting an index by name."
        )

    if vector_bucket_arn is not None:
        return {"indexName": index, "vectorBucketArn": vector_bucket_arn}
    return {"indexName": index, "vectorBucketName": vector_bucket}  # type: ignore[dict-item]


def _to_float32_list(value: Any) -> list[float]:
    """Coerce a vector-like value to a Python list[float] in float32 precision.

    Non-finite values (``inf``, ``-inf``, ``nan``) are rejected — S3 Vectors will reject them
    server-side anyway, and a clear client-side error beats an opaque ``ValidationException``.
    """
    arr = np.asarray(value, dtype=np.float32)
    if arr.ndim != 1:
        raise exceptions.InvalidArgumentValue(f"Vector data must be one-dimensional; got shape {arr.shape}.")
    if not np.isfinite(arr).all():
        raise exceptions.InvalidArgumentValue("Vector data contains non-finite values (inf or NaN).")
    return [float(x) for x in arr.tolist()]


def _vectors_response_to_df(
    records: list[dict[str, Any]],
    *,
    include_data: bool = False,
    include_metadata: bool = False,
    include_distance: bool = False,
) -> pd.DataFrame:
    """Shape a list of s3vectors response records into a DataFrame."""
    columns = ["key"]
    if include_distance:
        columns.append("distance")
    if include_data:
        columns.append("vector")
    if include_metadata:
        columns.append("metadata")

    rows: list[dict[str, Any]] = []
    for r in records:
        row: dict[str, Any] = {"key": r.get("key")}
        if include_distance and "distance" in r:
            row["distance"] = r["distance"]
        if include_data:
            data = r.get("data")
            row["vector"] = list(data["float32"]) if isinstance(data, dict) and "float32" in data else data
        if include_metadata:
            row["metadata"] = r.get("metadata")
        rows.append(row)

    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
