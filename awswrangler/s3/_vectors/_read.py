"""Amazon S3 Vectors - Read & query operations on vectors (PRIVATE)."""

from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING, Any, Iterator

import boto3
import numpy as np

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler._executor import _get_executor
from awswrangler.s3._vectors._bedrock import embed_texts
from awswrangler.s3._vectors._utils import _resolve_target, _to_float32_list, _vectors_response_to_df

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger: logging.Logger = logging.getLogger(__name__)

# API limits for read operations.
_KEYS_PER_GET = 100
_VECTORS_PER_LIST_PG = 1000
_MAX_LIST_SEGMENTS = 16
_MAX_TOPK = 100


def _get_chunk(
    client: "BaseClient",
    chunk: list[str],
    target: dict[str, str],
    return_data: bool,
    return_metadata: bool,
) -> list[dict[str, Any]]:
    response = client.get_vectors(  # type: ignore[attr-defined]
        **target,
        keys=chunk,
        returnData=return_data,
        returnMetadata=return_metadata,
    )
    return list(response.get("vectors", []))


@apply_configs
def get_vectors(
    *,
    keys: list[str],
    return_data: bool = False,
    return_metadata: bool = False,
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    index: str | None = None,
    index_arn: str | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """Retrieve vectors by key. Returns a DataFrame with columns ``key`` and (optionally) ``vector``, ``metadata``.

    Up to 100 keys per underlying API call (chunked automatically).
    """
    if not keys:
        return _vectors_response_to_df([], include_data=return_data, include_metadata=return_metadata)

    target = _resolve_target(
        vector_bucket=vector_bucket, vector_bucket_arn=vector_bucket_arn, index=index, index_arn=index_arn
    )

    client = _utils.client(service_name="s3vectors", session=boto3_session)
    chunks = _utils.chunkify(list(keys), max_length=_KEYS_PER_GET)
    executor = _get_executor(use_threads=use_threads)
    results: list[list[dict[str, Any]]] = executor.map(
        _get_chunk,
        client,
        chunks,
        [target] * len(chunks),
        [return_data] * len(chunks),
        [return_metadata] * len(chunks),
    )
    return _vectors_response_to_df(
        list(itertools.chain.from_iterable(results)),
        include_data=return_data,
        include_metadata=return_metadata,
    )


def _iter_list_pages(
    client: "BaseClient",
    target: dict[str, str],
    return_data: bool,
    return_metadata: bool,
    segment_count: int | None,
    segment_index: int | None,
    max_items: int | None,
) -> Iterator[list[dict[str, Any]]]:
    """Yield one page of raw vector records per call, honouring ``max_items`` across pages."""
    next_token: str | None = None
    emitted = 0
    while True:
        kwargs: dict[str, Any] = {
            **target,
            "returnData": return_data,
            "returnMetadata": return_metadata,
            "maxResults": _VECTORS_PER_LIST_PG,
        }
        if segment_count is not None and segment_index is not None:
            kwargs["segmentCount"] = segment_count
            kwargs["segmentIndex"] = segment_index
        if next_token:
            kwargs["nextToken"] = next_token
        response = client.list_vectors(**kwargs)  # type: ignore[attr-defined]
        page = list(response.get("vectors", []))
        if max_items is not None and emitted + len(page) >= max_items:
            yield page[: max_items - emitted]
            return
        emitted += len(page)
        if page:
            yield page
        next_token = response.get("nextToken")
        if not next_token:
            return


def _list_segment(
    client: "BaseClient",
    target: dict[str, str],
    return_data: bool,
    return_metadata: bool,
    segment_count: int | None,
    segment_index: int | None,
    max_items: int | None,
) -> list[dict[str, Any]]:
    return list(
        itertools.chain.from_iterable(
            _iter_list_pages(client, target, return_data, return_metadata, segment_count, segment_index, max_items)
        )
    )


def _iter_list_vectors_chunked(
    client: "BaseClient",
    target: dict[str, str],
    return_data: bool,
    return_metadata: bool,
    max_items: int | None,
    chunksize: int | None,
) -> Iterator[pd.DataFrame]:
    """Stream list_vectors results as DataFrames.

    ``chunksize=None`` emits one frame per API page; otherwise emits frames of exactly
    ``chunksize`` rows (final frame may be shorter).
    """
    pages = _iter_list_pages(client, target, return_data, return_metadata, None, None, max_items)
    if chunksize is None:
        for page in pages:
            yield _vectors_response_to_df(page, include_data=return_data, include_metadata=return_metadata)
        return

    buffer: list[dict[str, Any]] = []
    for page in pages:
        buffer.extend(page)
        while len(buffer) >= chunksize:
            yield _vectors_response_to_df(
                buffer[:chunksize], include_data=return_data, include_metadata=return_metadata
            )
            buffer = buffer[chunksize:]
    if buffer:
        yield _vectors_response_to_df(buffer, include_data=return_data, include_metadata=return_metadata)


@apply_configs
def list_vectors(
    *,
    return_data: bool = False,
    return_metadata: bool = False,
    max_items: int | None = None,
    chunked: bool | int = False,
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    index: str | None = None,
    index_arn: str | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """List all vectors in an index. Uses parallel segments (up to 16) when ``use_threads`` enables it.

    Parameters
    ----------
    return_data, return_metadata
        Whether to include each vector's data and metadata.
    max_items
        Optional cap on total vectors returned across all pages/segments.
    chunked
        Batching (memory-friendly). Returns an iterator of DataFrames instead of one frame:

        - ``True`` — yield one DataFrame per underlying API page.
        - ``INTEGER`` — yield DataFrames of exactly this many rows (final frame may be shorter).

        Chunked streaming is single-segment (sequential) regardless of ``use_threads``.
    vector_bucket / vector_bucket_arn / index / index_arn
        Target index.
    use_threads
        Concurrency for parallel-segment listing. Ignored when ``chunked`` is truthy.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        DataFrame with columns ``key`` and (optionally) ``vector``, ``metadata`` — or an iterator of such DataFrames when ``chunked`` is truthy.
    """
    target = _resolve_target(
        vector_bucket=vector_bucket, vector_bucket_arn=vector_bucket_arn, index=index, index_arn=index_arn
    )
    client = _utils.client(service_name="s3vectors", session=boto3_session)

    if chunked:
        chunksize = chunked if isinstance(chunked, int) and not isinstance(chunked, bool) else None
        return _iter_list_vectors_chunked(client, target, return_data, return_metadata, max_items, chunksize)

    n_segments = min(_MAX_LIST_SEGMENTS, _utils.ensure_worker_or_thread_count(use_threads=use_threads))

    if n_segments <= 1:
        items = _list_segment(client, target, return_data, return_metadata, None, None, max_items)
    else:
        # Cap per-segment fetch so the total isn't ~max_items × n_segments before truncation.
        # Add 1 to handle uneven splits; final truncation guarantees the cap.
        per_segment_cap = max_items // n_segments + 1 if max_items is not None else None
        executor = _get_executor(use_threads=use_threads)
        # Each worker handles one segment with its own pagination.
        results: list[list[dict[str, Any]]] = executor.map(
            _list_segment,
            client,
            [target] * n_segments,
            [return_data] * n_segments,
            [return_metadata] * n_segments,
            [n_segments] * n_segments,
            list(range(n_segments)),
            [per_segment_cap] * n_segments,
        )
        items = list(itertools.chain.from_iterable(results))
        if max_items is not None and len(items) > max_items:
            items = items[:max_items]
    return _vectors_response_to_df(items, include_data=return_data, include_metadata=return_metadata)


@apply_configs
def query_vectors(
    *,
    query_vector: list[float] | np.ndarray[Any, Any] | None = None,
    query_text: str | None = None,
    top_k: int = 10,
    filter: dict[str, Any] | None = None,
    return_distance: bool = True,
    return_metadata: bool = True,
    bedrock_model_id: str | None = None,
    bedrock_model_kwargs: dict[str, Any] | None = None,
    vector_bucket: str | None = None,
    vector_bucket_arn: str | None = None,
    index: str | None = None,
    index_arn: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """Approximate-nearest-neighbour query against an Amazon S3 Vectors index.

    Parameters
    ----------
    query_vector
        Pre-computed query embedding.
    query_text
        Text to embed via Bedrock (requires ``bedrock_model_id``).
    top_k
        Number of nearest neighbours to return (1-100).
    filter
        Metadata filter (MongoDB-style operators: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $and, $or).
    return_distance, return_metadata
        Whether to include each result's distance and metadata.
    bedrock_model_id, bedrock_model_kwargs
        Bedrock embedding configuration when using ``query_text``.
    vector_bucket / vector_bucket_arn / index / index_arn
        Target index.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        DataFrame with columns ``key`` and (optionally) ``distance``, ``metadata``.
        The configured distance metric is exposed via ``df.attrs['distance_metric']``.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.s3.query_vectors(
    ...     query_vector=[0.1, 0.2, 0.3],
    ...     top_k=5,
    ...     filter={"genre": {"$eq": "documentary"}},
    ...     vector_bucket="my-bucket",
    ...     index="my-index",
    ... )
    """
    if (query_vector is None) == (query_text is None):
        raise exceptions.InvalidArgumentCombination("Provide exactly one of `query_vector` or `query_text`.")
    if query_text is not None and not bedrock_model_id:
        raise exceptions.InvalidArgumentCombination("`bedrock_model_id` is required when `query_text` is provided.")
    if not (1 <= top_k <= _MAX_TOPK):
        raise exceptions.InvalidArgumentValue(f"`top_k` must be between 1 and {_MAX_TOPK}; got {top_k}.")

    target = _resolve_target(
        vector_bucket=vector_bucket, vector_bucket_arn=vector_bucket_arn, index=index, index_arn=index_arn
    )

    if query_vector is None:
        query_vector = embed_texts(
            texts=[query_text],  # type: ignore[list-item]
            model_id=bedrock_model_id,  # type: ignore[arg-type]
            model_kwargs=bedrock_model_kwargs,
            use_threads=False,
            boto3_session=boto3_session,
        )[0]
    floats = _to_float32_list(query_vector)

    client = _utils.client(service_name="s3vectors", session=boto3_session)
    kwargs: dict[str, Any] = {
        **target,
        "topK": top_k,
        "queryVector": {"float32": floats},
        "returnDistance": return_distance,
        "returnMetadata": return_metadata,
    }
    if filter is not None:
        kwargs["filter"] = filter
    response = client.query_vectors(**kwargs)  # type: ignore[attr-defined]

    df_out = _vectors_response_to_df(
        response.get("vectors", []),
        include_data=False,
        include_metadata=return_metadata,
        include_distance=return_distance,
    )
    distance_metric = response.get("distanceMetric")
    if distance_metric is not None:
        df_out.attrs["distance_metric"] = distance_metric
    return df_out
