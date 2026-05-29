"""Amazon S3 Vectors - Write & delete operations on vectors (PRIVATE)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import boto3
import numpy as np

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler._executor import _get_executor
from awswrangler.s3._vectors._bedrock import embed_texts
from awswrangler.s3._vectors._utils import _resolve_target, _to_float32_list

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger: logging.Logger = logging.getLogger(__name__)

# API limits for write/delete operations.
_VECTORS_PER_PUT = 500
_KEYS_PER_DELETE = 500


def _drop_nan(metadata: dict[str, Any]) -> dict[str, Any]:
    """Drop NaN / pd.NA / None metadata values (per-row).

    Lists, dicts, and falsy scalars (0, False, '') are preserved.
    """
    cleaned: dict[str, Any] = {}
    for k, v in metadata.items():
        if v is None:
            continue
        # `pd.isna` handles float('nan'), np.nan, pd.NA, pd.NaT — but it returns an array
        # for sequences, so guard iterables first.
        if not isinstance(v, (list, tuple, dict, set, np.ndarray)) and pd.isna(v):
            continue
        cleaned[k] = v
    return cleaned


def _put_chunk(client: "BaseClient", chunk: list[dict[str, Any]], target: dict[str, str]) -> None:
    client.put_vectors(**target, vectors=chunk)  # type: ignore[attr-defined]


def _normalise_put_items(vectors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate and float32-coerce the vector payload; pass metadata through unchanged."""
    out: list[dict[str, Any]] = []
    for v in vectors:
        if "key" not in v:
            raise exceptions.InvalidArgumentValue("Each vector must have a `key`.")
        data = v.get("data")
        # Accept either {"data": {"float32": [...]}} or {"data": [...]} or {"data": np.ndarray}.
        if isinstance(data, dict) and "float32" in data:
            floats = _to_float32_list(data["float32"])
        elif data is None:
            raise exceptions.InvalidArgumentValue(f"Vector '{v['key']}' is missing `data`.")
        else:
            floats = _to_float32_list(data)
        item: dict[str, Any] = {"key": v["key"], "data": {"float32": floats}}
        if "metadata" in v and v["metadata"] is not None:
            item["metadata"] = v["metadata"]
        out.append(item)
    return out


@apply_configs
def put_vectors(
    *,
    vectors: list[dict[str, Any]],
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    index: str | None = None,
    index_arn: str | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Insert one or more vectors into an Amazon S3 Vectors index.

    Parameters
    ----------
    vectors
        List of dicts, each shaped ``{"key": str, "data": list[float] | dict[str, list[float]] | np.ndarray, "metadata": dict | None}``.
        ``data`` is automatically cast to float32. Up to 500 vectors per underlying API call.
    vector_bucket / vector_bucket_arn / index / index_arn
        Target index. See module docstring for resolution rules.
    use_threads
        Concurrency for batched calls.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.
    """
    if not vectors:
        return
    target = _resolve_target(
        vector_bucket=vector_bucket, vector_bucket_arn=vector_bucket_arn, index=index, index_arn=index_arn
    )
    items = _normalise_put_items(vectors)

    client = _utils.client(service_name="s3vectors", session=boto3_session)
    chunks = _utils.chunkify(items, max_length=_VECTORS_PER_PUT)
    executor = _get_executor(use_threads=use_threads)
    executor.map(_put_chunk, client, chunks, [target] * len(chunks))


@apply_configs
def put_vectors_from_df(
    df: pd.DataFrame,
    *,
    key_column: str,
    vector_column: str | None = None,
    metadata_columns: list[str] | None = None,
    text_column: str | None = None,
    bedrock_model_id: str | None = None,
    bedrock_model_kwargs: dict[str, Any] | None = None,
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    index: str | None = None,
    index_arn: str | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Insert all rows of a DataFrame into an Amazon S3 Vectors index.

    Either ``vector_column`` (precomputed embeddings) or ``text_column`` + ``bedrock_model_id``
    (embed via Amazon Bedrock on the fly) must be provided.

    Parameters
    ----------
    df
        Input DataFrame.
    key_column
        Column containing the per-row vector key (string).
    vector_column
        Column containing the precomputed embedding (list[float] / np.ndarray per row).
    metadata_columns
        Columns to attach as filterable/non-filterable metadata. ``None`` means "all columns
        except ``key_column``, ``vector_column`` and ``text_column``" — note ``text_column``
        is excluded by default; pass it explicitly here (e.g. for RAG citations) to keep it.
        NaN / ``pd.NA`` / ``None`` cells are dropped per row.
    text_column
        Column containing input text to embed via Bedrock. Mutually exclusive with ``vector_column``.
    bedrock_model_id, bedrock_model_kwargs
        Bedrock embedding model and optional model-specific kwargs (e.g. ``{"dimensions": 256}``).
    vector_bucket / vector_bucket_arn / index / index_arn
        Target index.
    use_threads
        Concurrency for batched put calls and for parallel Bedrock embedding.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Examples
    --------
    Pre-computed vectors:

    >>> import awswrangler as wr
    >>> wr.s3.put_vectors_from_df(
    ...     df=my_df,
    ...     key_column="id",
    ...     vector_column="embedding",
    ...     vector_bucket="my-bucket",
    ...     index="my-index",
    ... )

    Embed-on-write via Bedrock Titan:

    >>> wr.s3.put_vectors_from_df(
    ...     df=my_df,
    ...     key_column="id",
    ...     text_column="content",
    ...     bedrock_model_id="amazon.titan-embed-text-v2:0",
    ...     vector_bucket="my-bucket",
    ...     index="my-index",
    ... )
    """
    if (vector_column is None) == (text_column is None):
        raise exceptions.InvalidArgumentCombination("Provide exactly one of `vector_column` or `text_column`.")
    if text_column is not None and not bedrock_model_id:
        raise exceptions.InvalidArgumentCombination("`bedrock_model_id` is required when `text_column` is provided.")

    if df.empty:
        return

    # Determine metadata columns.
    excluded = {key_column}
    if vector_column is not None:
        excluded.add(vector_column)
    if text_column is not None:
        excluded.add(text_column)
    meta_cols: list[str]
    if metadata_columns is None:
        meta_cols = [c for c in df.columns if c not in excluded]
    else:
        meta_cols = list(metadata_columns)

    # Compute embeddings if needed.
    vectors_iter: list[Any]
    if vector_column is not None:
        vectors_iter = df[vector_column].tolist()
    else:
        texts: list[str] = [str(t) for t in df[text_column].tolist()]
        vectors_iter = embed_texts(
            texts=texts,
            model_id=bedrock_model_id,  # type: ignore[arg-type]
            model_kwargs=bedrock_model_kwargs,
            use_threads=use_threads,
            boto3_session=boto3_session,
        )

    # Build raw vector items. `to_dict(orient="records")` is faster than .iterrows() and
    # avoids constructing per-row Series.
    keys = df[key_column].astype(str).tolist()
    metas = df[meta_cols].to_dict(orient="records") if meta_cols else [{}] * len(df)
    items: list[dict[str, Any]] = []
    for key, vec, meta in zip(keys, vectors_iter, metas):
        item: dict[str, Any] = {"key": key, "data": vec}
        cleaned = _drop_nan(meta)
        if cleaned:
            item["metadata"] = cleaned
        items.append(item)

    put_vectors(
        vectors=items,
        vector_bucket=vector_bucket,
        vector_bucket_arn=vector_bucket_arn,
        index=index,
        index_arn=index_arn,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )


def _delete_chunk(client: "BaseClient", chunk: list[str], target: dict[str, str]) -> None:
    client.delete_vectors(**target, keys=chunk)  # type: ignore[attr-defined]


@apply_configs
def delete_vectors(
    *,
    keys: list[str],
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    index: str | None = None,
    index_arn: str | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete vectors by key (chunks at 500 per underlying call)."""
    if not keys:
        return
    target = _resolve_target(
        vector_bucket=vector_bucket, vector_bucket_arn=vector_bucket_arn, index=index, index_arn=index_arn
    )
    client = _utils.client(service_name="s3vectors", session=boto3_session)
    chunks = _utils.chunkify(list(keys), max_length=_KEYS_PER_DELETE)
    executor = _get_executor(use_threads=use_threads)
    executor.map(_delete_chunk, client, chunks, [target] * len(chunks))
