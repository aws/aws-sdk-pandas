"""Live integration tests for awswrangler S3 Vectors entry points.

These tests hit real AWS — they require credentials and a region where Amazon S3 Vectors is
available. They use the ``vector_bucket`` (session) and ``vector_index`` (function) fixtures
from ``tests/conftest.py``, which create and tear down throwaway resources via the awswrangler
control-plane functions themselves (no CDK stack needed).

Pure-mock unit tests live in ``tests/unit/test_s3_vectors_mocked.py``.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest

import awswrangler as wr


def _wait_for_indexed(
    vector_bucket: str,
    index_name: str,
    expected_keys: set[str],
    timeout: float = 30.0,
) -> None:
    """Poll list_vectors until all expected keys are visible (writes are strongly consistent
    once the call returns, but listings may briefly trail; tolerate up to `timeout` seconds)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        df = wr.s3.list_vectors(vector_bucket=vector_bucket, index=index_name, use_threads=False)
        if expected_keys.issubset(set(df["key"].tolist())):
            return
        time.sleep(1)
    raise AssertionError(f"Vectors not indexed within {timeout}s; expected {expected_keys}")


def test_get_vector_bucket(vector_bucket: str) -> None:
    info = wr.s3.get_vector_bucket(name=vector_bucket)
    assert info["vectorBucketName"] == vector_bucket
    assert "vectorBucketArn" in info


def test_list_vector_buckets_includes_test_bucket(vector_bucket: str) -> None:
    buckets = wr.s3.list_vector_buckets()
    names = {b["vectorBucketName"] for b in buckets}
    assert vector_bucket in names


def test_get_vector_index(vector_index: tuple[str, str]) -> None:
    bucket, index = vector_index
    info = wr.s3.get_vector_index(name=index, vector_bucket=bucket)
    assert info["indexName"] == index
    assert info["dimension"] == 4
    assert info["distanceMetric"] == "cosine"
    assert info["dataType"] == "float32"


def test_list_vector_indexes(vector_index: tuple[str, str]) -> None:
    bucket, index = vector_index
    indexes = wr.s3.list_vector_indexes(vector_bucket=bucket)
    names = {i["indexName"] for i in indexes}
    assert index in names


def test_put_query_round_trip(vector_index: tuple[str, str]) -> None:
    bucket, index = vector_index
    df = pd.DataFrame(
        {
            "id": ["doc-1", "doc-2", "doc-3", "doc-4"],
            "embedding": [
                np.array([0.10, 0.20, 0.30, 0.40], dtype=np.float32),
                np.array([0.15, 0.25, 0.35, 0.45], dtype=np.float32),
                np.array([0.90, 0.80, 0.70, 0.60], dtype=np.float32),
                np.array([0.05, 0.05, 0.05, 0.05], dtype=np.float32),
            ],
            "genre": ["documentary", "documentary", "drama", "comedy"],
            "year": [2021, 2023, 2019, 2024],
        }
    )

    wr.s3.put_vectors_from_df(
        df=df,
        key_column="id",
        vector_column="embedding",
        vector_bucket=bucket,
        index=index,
        use_threads=False,
    )

    # Closest match to doc-1's vector should be doc-1 itself.
    results = wr.s3.query_vectors(
        query_vector=[0.10, 0.20, 0.30, 0.40],
        top_k=1,
        return_distance=True,
        return_metadata=True,
        vector_bucket=bucket,
        index=index,
    )
    assert results.iloc[0]["key"] == "doc-1"
    assert results.attrs.get("distance_metric") == "cosine"


def test_query_with_metadata_filter(vector_index: tuple[str, str]) -> None:
    bucket, index = vector_index
    df = pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "embedding": [
                [0.1, 0.2, 0.3, 0.4],
                [0.11, 0.21, 0.31, 0.41],
                [0.9, 0.9, 0.9, 0.9],
            ],
            "year": [2018, 2024, 2025],
        }
    )
    wr.s3.put_vectors_from_df(
        df=df,
        key_column="id",
        vector_column="embedding",
        vector_bucket=bucket,
        index=index,
        use_threads=False,
    )

    # Even though "a" is closest, the filter excludes it.
    results = wr.s3.query_vectors(
        query_vector=[0.1, 0.2, 0.3, 0.4],
        top_k=2,
        filter={"year": {"$gte": 2024}},
        return_metadata=True,
        vector_bucket=bucket,
        index=index,
    )
    assert "a" not in set(results["key"].tolist())
    assert {"b", "c"}.issuperset(set(results["key"].tolist()))


def test_get_and_delete_vectors(vector_index: tuple[str, str]) -> None:
    bucket, index = vector_index
    keys = [f"k{i}" for i in range(5)]
    vectors = [{"key": k, "data": [float(i + 1) / 10, 0.0, 0.0, 0.0], "metadata": {"i": i}} for i, k in enumerate(keys)]
    wr.s3.put_vectors(vectors=vectors, vector_bucket=bucket, index=index, use_threads=False)

    fetched = wr.s3.get_vectors(
        keys=keys,
        return_data=True,
        return_metadata=True,
        vector_bucket=bucket,
        index=index,
        use_threads=False,
    )
    assert set(fetched["key"].tolist()) == set(keys)
    assert all(isinstance(v, list) and len(v) == 4 for v in fetched["vector"])

    wr.s3.delete_vectors(keys=keys[:3], vector_bucket=bucket, index=index, use_threads=False)
    remaining = wr.s3.get_vectors(keys=keys, vector_bucket=bucket, index=index, use_threads=False)
    assert set(remaining["key"].tolist()) == set(keys[3:])


def test_list_vectors_returns_all_with_parallel_segments(vector_index: tuple[str, str]) -> None:
    bucket, index = vector_index
    keys = [f"row-{i}" for i in range(20)]
    vectors = [{"key": k, "data": [float(i + 1), 0.0, 0.0, 0.0]} for i, k in enumerate(keys)]
    wr.s3.put_vectors(vectors=vectors, vector_bucket=bucket, index=index, use_threads=False)
    _wait_for_indexed(bucket, index, set(keys))

    df = wr.s3.list_vectors(vector_bucket=bucket, index=index, use_threads=4)
    assert set(df["key"].tolist()) >= set(keys)


@pytest.mark.parametrize("use_threads", [False, True])
def test_put_vectors_chunks_above_api_limit(vector_index: tuple[str, str], use_threads: bool) -> None:
    bucket, index = vector_index
    n = 1100  # > 500 (API limit) so awswrangler must chunk.
    vectors = [{"key": f"bulk-{i}", "data": [float(i + 1), 0.0, 0.0, 0.0]} for i in range(n)]
    wr.s3.put_vectors(vectors=vectors, vector_bucket=bucket, index=index, use_threads=use_threads)
    fetched = wr.s3.get_vectors(
        keys=[f"bulk-{i}" for i in range(n)],
        vector_bucket=bucket,
        index=index,
        use_threads=use_threads,
    )
    assert len(fetched) == n
