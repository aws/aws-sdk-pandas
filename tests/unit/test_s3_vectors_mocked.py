"""Unit tests for awswrangler.s3 S3 Vectors entry points.

Moto does not yet support s3vectors, so these tests mock the boto3 client returned by
`awswrangler._utils.client` and assert on call arguments and result shaping.
"""

from __future__ import annotations

import io
import json
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

import awswrangler as wr
from awswrangler import exceptions
from awswrangler.s3._vectors import _bedrock, _write
from awswrangler.s3._vectors import _utils as _vutils

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_client(mock_client: MagicMock) -> Any:
    """Patch awswrangler._utils.client so any service request returns mock_client."""
    return patch("awswrangler._utils.client", return_value=mock_client)


# ---------------------------------------------------------------------------
# _resolve_target
# ---------------------------------------------------------------------------


def test_resolve_target_by_index_arn() -> None:
    out = _vutils._resolve_target(
        vector_bucket=None, vector_bucket_arn=None, index=None, index_arn="arn:aws:s3vectors:::bucket/b/index/i"
    )
    assert out == {"indexArn": "arn:aws:s3vectors:::bucket/b/index/i"}


def test_resolve_target_by_index_and_bucket_name() -> None:
    out = _vutils._resolve_target(vector_bucket="my-b", vector_bucket_arn=None, index="my-i", index_arn=None)
    assert out == {"indexName": "my-i", "vectorBucketName": "my-b"}


def test_resolve_target_by_index_and_bucket_arn() -> None:
    out = _vutils._resolve_target(
        vector_bucket=None,
        vector_bucket_arn="arn:aws:s3vectors:::bucket/b",
        index="my-i",
        index_arn=None,
    )
    assert out == {"indexName": "my-i", "vectorBucketArn": "arn:aws:s3vectors:::bucket/b"}


def test_resolve_target_index_arn_excludes_others() -> None:
    with pytest.raises(exceptions.InvalidArgumentCombination):
        _vutils._resolve_target(
            vector_bucket="b", vector_bucket_arn=None, index=None, index_arn="arn:aws:s3vectors:::bucket/b/index/i"
        )


def test_resolve_target_no_index() -> None:
    with pytest.raises(exceptions.InvalidArgumentCombination):
        _vutils._resolve_target(vector_bucket="b", vector_bucket_arn=None, index=None, index_arn=None)


def test_resolve_target_both_buckets() -> None:
    with pytest.raises(exceptions.InvalidArgumentCombination):
        _vutils._resolve_target(vector_bucket="b", vector_bucket_arn="arn:b", index="i", index_arn=None)


def test_resolve_target_neither_bucket() -> None:
    with pytest.raises(exceptions.InvalidArgumentCombination):
        _vutils._resolve_target(vector_bucket=None, vector_bucket_arn=None, index="i", index_arn=None)


# ---------------------------------------------------------------------------
# Float32 + metadata cleanup
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        [0.1, 0.2, 0.3],
        np.array([0.1, 0.2, 0.3], dtype=np.float64),
        np.array([0.1, 0.2, 0.3], dtype=np.float32),
        (0.1, 0.2, 0.3),
    ],
)
def test_to_float32_list(value: Any) -> None:
    result = _vutils._to_float32_list(value)
    assert isinstance(result, list)
    assert len(result) == 3
    assert all(isinstance(x, float) for x in result)


def test_to_float32_rejects_2d() -> None:
    with pytest.raises(exceptions.InvalidArgumentValue):
        _vutils._to_float32_list(np.array([[0.1], [0.2]]))


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), float("-inf")])
def test_to_float32_rejects_non_finite(bad_value: float) -> None:
    with pytest.raises(exceptions.InvalidArgumentValue, match="non-finite"):
        _vutils._to_float32_list([0.1, bad_value, 0.3])


def test_drop_nan_keeps_falsy_values_and_collections() -> None:
    cleaned = _write._drop_nan(
        {
            "a": 0,
            "b": False,
            "c": "",
            "d": None,
            "e": float("nan"),
            "f": "x",
            "g": pd.NA,
            "h": [1, 2, 3],
            "i": {"nested": "dict"},
        }
    )
    assert cleaned == {"a": 0, "b": False, "c": "", "f": "x", "h": [1, 2, 3], "i": {"nested": "dict"}}


# ---------------------------------------------------------------------------
# Aliasing identity (the public surface mirrors the private functions)
# ---------------------------------------------------------------------------


def test_public_aliases_point_to_private_implementations() -> None:
    from awswrangler.s3 import _vectors

    for name in (
        "create_vector_bucket",
        "delete_vector_bucket",
        "list_vector_buckets",
        "get_vector_bucket",
        "create_vector_index",
        "delete_vector_index",
        "list_vector_indexes",
        "get_vector_index",
        "put_vectors",
        "put_vectors_from_df",
        "get_vectors",
        "delete_vectors",
        "list_vectors",
        "query_vectors",
    ):
        assert getattr(wr.s3, name) is getattr(_vectors, name), name


# ---------------------------------------------------------------------------
# put_vectors — chunking + payload shape
# ---------------------------------------------------------------------------


def test_put_vectors_chunks_at_500() -> None:
    client = MagicMock()
    client.put_vectors.return_value = {}
    n = 1200
    vectors = [{"key": f"k{i}", "data": [float(i), float(i)]} for i in range(n)]

    with _patch_client(client):
        wr.s3.put_vectors(vectors=vectors, vector_bucket="b", index="i", use_threads=False)

    # 1200 items @ max 500/call → ceil(1200/500) = 3 calls. chunkify balances sizes.
    assert client.put_vectors.call_count == 3
    sizes = [len(call.kwargs["vectors"]) for call in client.put_vectors.call_args_list]
    assert sum(sizes) == n
    assert all(0 < s <= 500 for s in sizes)
    # Targeting passed through.
    for call in client.put_vectors.call_args_list:
        assert call.kwargs["vectorBucketName"] == "b"
        assert call.kwargs["indexName"] == "i"


def test_put_vectors_coerces_to_float32() -> None:
    client = MagicMock()
    client.put_vectors.return_value = {}
    payload = [{"key": "k1", "data": np.array([1.0, 2.0], dtype=np.float64), "metadata": {"x": 1}}]

    with _patch_client(client):
        wr.s3.put_vectors(vectors=payload, vector_bucket="b", index="i", use_threads=False)

    item = client.put_vectors.call_args.kwargs["vectors"][0]
    assert item["key"] == "k1"
    assert item["data"]["float32"] == [1.0, 2.0]
    assert item["metadata"] == {"x": 1}


def test_put_vectors_accepts_explicit_float32_envelope() -> None:
    client = MagicMock()
    client.put_vectors.return_value = {}
    payload = [{"key": "k1", "data": {"float32": [0.5, 0.5]}}]

    with _patch_client(client):
        wr.s3.put_vectors(vectors=payload, vector_bucket="b", index="i", use_threads=False)

    item = client.put_vectors.call_args.kwargs["vectors"][0]
    assert item["data"] == {"float32": [0.5, 0.5]}


def test_put_vectors_empty_is_noop() -> None:
    client = MagicMock()
    with _patch_client(client):
        wr.s3.put_vectors(vectors=[], vector_bucket="b", index="i")
    client.put_vectors.assert_not_called()


# ---------------------------------------------------------------------------
# put_vectors_from_df
# ---------------------------------------------------------------------------


def test_put_vectors_from_df_with_vector_column() -> None:
    client = MagicMock()
    client.put_vectors.return_value = {}
    df = pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "embedding": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
            "genre": ["doc", "drama", None],
            "year": [2020, 2021, float("nan")],
        }
    )

    with _patch_client(client):
        wr.s3.put_vectors_from_df(
            df=df,
            key_column="id",
            vector_column="embedding",
            vector_bucket="b",
            index="i",
            use_threads=False,
        )

    sent = client.put_vectors.call_args.kwargs["vectors"]
    assert [v["key"] for v in sent] == ["a", "b", "c"]
    assert all(v["data"]["float32"] for v in sent)
    # Row a: both metadata fields present.
    assert sent[0]["metadata"] == {"genre": "doc", "year": 2020}
    # Row c: NaN year and None genre dropped → no metadata at all.
    assert "metadata" not in sent[2]


def test_put_vectors_from_df_requires_one_of_vector_or_text() -> None:
    df = pd.DataFrame({"id": ["a"], "embedding": [[0.1]]})
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.s3.put_vectors_from_df(df=df, key_column="id", vector_bucket="b", index="i")
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.s3.put_vectors_from_df(
            df=df,
            key_column="id",
            vector_column="embedding",
            text_column="x",
            vector_bucket="b",
            index="i",
        )


def test_put_vectors_from_df_text_column_requires_bedrock_model() -> None:
    df = pd.DataFrame({"id": ["a"], "text": ["hello"]})
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.s3.put_vectors_from_df(df=df, key_column="id", text_column="text", vector_bucket="b", index="i")


def test_put_vectors_from_df_with_text_column_calls_bedrock() -> None:
    s3v_client = MagicMock()
    s3v_client.put_vectors.return_value = {}

    def fake_client(service_name: str, **_: Any) -> MagicMock:
        if service_name == "bedrock-runtime":
            br = MagicMock()
            br.invoke_model.side_effect = lambda **kw: {
                "body": io.BytesIO(json.dumps({"embedding": [0.1, 0.2, 0.3]}).encode())
            }
            return br
        return s3v_client

    df = pd.DataFrame({"id": ["a", "b"], "text": ["hello", "world"]})

    def fake_client_v2(service_name: str, **_: Any) -> MagicMock:
        if service_name == "bedrock-runtime":
            br = MagicMock()
            # Return values that round-trip exactly through float32.
            br.invoke_model.side_effect = lambda **kw: {
                "body": io.BytesIO(json.dumps({"embedding": [0.5, 0.25, 0.125]}).encode())
            }
            return br
        return s3v_client

    with patch("awswrangler._utils.client", side_effect=fake_client_v2):
        wr.s3.put_vectors_from_df(
            df=df,
            key_column="id",
            text_column="text",
            bedrock_model_id="amazon.titan-embed-text-v2:0",
            vector_bucket="b",
            index="i",
            use_threads=False,
        )
    sent = s3v_client.put_vectors.call_args.kwargs["vectors"]
    assert [v["key"] for v in sent] == ["a", "b"]
    assert all(v["data"]["float32"] == [0.5, 0.25, 0.125] for v in sent)


# ---------------------------------------------------------------------------
# get_vectors / delete_vectors — chunking
# ---------------------------------------------------------------------------


def test_get_vectors_chunks_at_100_and_returns_dataframe() -> None:
    client = MagicMock()
    # Each call returns a couple of records keyed by the requested keys.
    client.get_vectors.side_effect = lambda **kw: {
        "vectors": [{"key": k, "data": {"float32": [1.0, 2.0]}, "metadata": {"x": 1}} for k in kw["keys"]]
    }
    keys = [f"k{i}" for i in range(250)]

    with _patch_client(client):
        df = wr.s3.get_vectors(
            keys=keys,
            return_data=True,
            return_metadata=True,
            vector_bucket="b",
            index="i",
            use_threads=False,
        )

    # 250 keys @ max 100/call → ceil(250/100) = 3 calls. chunkify balances sizes.
    assert client.get_vectors.call_count == 3
    sizes = [len(call.kwargs["keys"]) for call in client.get_vectors.call_args_list]
    assert sum(sizes) == 250
    assert all(0 < s <= 100 for s in sizes)
    assert list(df.columns) == ["key", "vector", "metadata"]
    assert len(df) == 250


def test_get_vectors_empty_keys_returns_empty_df() -> None:
    client = MagicMock()
    with _patch_client(client):
        df = wr.s3.get_vectors(keys=[], vector_bucket="b", index="i")
    assert df.empty
    assert list(df.columns) == ["key"]
    client.get_vectors.assert_not_called()


def test_delete_vectors_chunks_at_500() -> None:
    client = MagicMock()
    client.delete_vectors.return_value = {}
    keys = [f"k{i}" for i in range(600)]

    with _patch_client(client):
        wr.s3.delete_vectors(keys=keys, vector_bucket="b", index="i", use_threads=False)

    # 600 keys @ max 500/call → ceil(600/500) = 2 calls.
    assert client.delete_vectors.call_count == 2
    sizes = [len(call.kwargs["keys"]) for call in client.delete_vectors.call_args_list]
    assert sum(sizes) == 600
    assert all(0 < s <= 500 for s in sizes)


# ---------------------------------------------------------------------------
# list_vectors — segments + pagination
# ---------------------------------------------------------------------------


def test_list_vectors_paginates_single_segment() -> None:
    client = MagicMock()
    client.list_vectors.side_effect = [
        {"vectors": [{"key": "k1"}, {"key": "k2"}], "nextToken": "tok"},
        {"vectors": [{"key": "k3"}]},
    ]
    with _patch_client(client):
        df = wr.s3.list_vectors(vector_bucket="b", index="i", use_threads=False)
    assert client.list_vectors.call_count == 2
    # No segment params on a single-threaded list.
    for call in client.list_vectors.call_args_list:
        assert "segmentCount" not in call.kwargs
    assert df["key"].tolist() == ["k1", "k2", "k3"]


def test_list_vectors_uses_parallel_segments() -> None:
    client = MagicMock()
    # Each segment returns one record so we can count calls per segment.
    client.list_vectors.side_effect = lambda **kw: {"vectors": [{"key": f"seg{kw['segmentIndex']}"}]}
    with _patch_client(client):
        df = wr.s3.list_vectors(vector_bucket="b", index="i", use_threads=4)
    assert client.list_vectors.call_count == 4
    seg_indexes = sorted(call.kwargs["segmentIndex"] for call in client.list_vectors.call_args_list)
    assert seg_indexes == [0, 1, 2, 3]
    assert all(call.kwargs["segmentCount"] == 4 for call in client.list_vectors.call_args_list)
    assert sorted(df["key"].tolist()) == ["seg0", "seg1", "seg2", "seg3"]


def test_list_vectors_max_items_truncates() -> None:
    client = MagicMock()
    client.list_vectors.return_value = {
        "vectors": [{"key": f"k{i}"} for i in range(10)],
    }
    with _patch_client(client):
        df = wr.s3.list_vectors(vector_bucket="b", index="i", max_items=3, use_threads=False)
    assert len(df) == 3


def test_list_vectors_chunked_true_yields_one_frame_per_page() -> None:
    client = MagicMock()
    client.list_vectors.side_effect = [
        {"vectors": [{"key": "k1"}, {"key": "k2"}], "nextToken": "tok"},
        {"vectors": [{"key": "k3"}]},
    ]
    with _patch_client(client):
        it = wr.s3.list_vectors(vector_bucket="b", index="i", chunked=True, use_threads=8)
        frames = list(it)
    # Chunked forces single-segment sequential streaming regardless of use_threads.
    for call in client.list_vectors.call_args_list:
        assert "segmentCount" not in call.kwargs
        assert "segmentIndex" not in call.kwargs
    assert [f["key"].tolist() for f in frames] == [["k1", "k2"], ["k3"]]


def test_list_vectors_chunked_integer_yields_fixed_size_frames() -> None:
    client = MagicMock()
    client.list_vectors.side_effect = [
        {"vectors": [{"key": f"k{i}"} for i in range(3)], "nextToken": "tok"},
        {"vectors": [{"key": f"k{i}"} for i in range(3, 7)]},
    ]
    with _patch_client(client):
        it = wr.s3.list_vectors(vector_bucket="b", index="i", chunked=2)
        frames = list(it)
    assert [len(f) for f in frames] == [2, 2, 2, 1]
    assert [k for f in frames for k in f["key"].tolist()] == [f"k{i}" for i in range(7)]


def test_list_vectors_chunked_is_lazy() -> None:
    client = MagicMock()
    client.list_vectors.side_effect = [
        {"vectors": [{"key": "k1"}], "nextToken": "tok"},
        {"vectors": [{"key": "k2"}]},
    ]
    with _patch_client(client):
        it = wr.s3.list_vectors(vector_bucket="b", index="i", chunked=True)
        # No API call should have happened yet.
        assert client.list_vectors.call_count == 0
        next(it)
        assert client.list_vectors.call_count == 1
        next(it)
        assert client.list_vectors.call_count == 2


def test_list_vectors_chunked_respects_max_items() -> None:
    client = MagicMock()
    client.list_vectors.side_effect = [
        {"vectors": [{"key": f"k{i}"} for i in range(4)], "nextToken": "tok"},
        {"vectors": [{"key": f"k{i}"} for i in range(4, 8)]},
    ]
    with _patch_client(client):
        frames = list(wr.s3.list_vectors(vector_bucket="b", index="i", chunked=True, max_items=5))
    keys = [k for f in frames for k in f["key"].tolist()]
    assert keys == ["k0", "k1", "k2", "k3", "k4"]


# ---------------------------------------------------------------------------
# query_vectors
# ---------------------------------------------------------------------------


def test_query_vectors_happy_path() -> None:
    client = MagicMock()
    client.query_vectors.return_value = {
        "distanceMetric": "cosine",
        "vectors": [
            {"key": "a", "distance": 0.01, "metadata": {"genre": "doc"}},
            {"key": "b", "distance": 0.05, "metadata": {"genre": "drama"}},
        ],
    }
    # Use values that round-trip exactly through float32 to avoid precision noise in assertions.
    qv = [0.5, 0.25, 0.125]
    with _patch_client(client):
        df = wr.s3.query_vectors(
            query_vector=qv,
            top_k=5,
            filter={"year": {"$gte": 2020}},
            vector_bucket="b",
            index="i",
        )
    sent = client.query_vectors.call_args.kwargs
    assert sent["topK"] == 5
    assert sent["queryVector"] == {"float32": qv}
    assert sent["filter"] == {"year": {"$gte": 2020}}
    assert list(df.columns) == ["key", "distance", "metadata"]
    assert df.attrs["distance_metric"] == "cosine"


def test_query_vectors_top_k_bounds() -> None:
    with pytest.raises(exceptions.InvalidArgumentValue):
        wr.s3.query_vectors(query_vector=[0.1], top_k=0, vector_bucket="b", index="i")
    with pytest.raises(exceptions.InvalidArgumentValue):
        wr.s3.query_vectors(query_vector=[0.1], top_k=101, vector_bucket="b", index="i")


def test_query_vectors_either_vector_or_text_required() -> None:
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.s3.query_vectors(vector_bucket="b", index="i")
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.s3.query_vectors(query_vector=[0.1], query_text="hi", vector_bucket="b", index="i")


def test_query_vectors_text_requires_bedrock_model() -> None:
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.s3.query_vectors(query_text="hi", vector_bucket="b", index="i")


def test_query_vectors_with_text_calls_bedrock_then_query() -> None:
    s3v_client = MagicMock()
    s3v_client.query_vectors.return_value = {"vectors": [{"key": "a"}]}

    def fake_client(service_name: str, **_: Any) -> MagicMock:
        if service_name == "bedrock-runtime":
            br = MagicMock()
            br.invoke_model.return_value = {"body": io.BytesIO(json.dumps({"embedding": [0.5, 0.25, 0.125]}).encode())}
            return br
        return s3v_client

    with patch("awswrangler._utils.client", side_effect=fake_client):
        df = wr.s3.query_vectors(
            query_text="hello",
            bedrock_model_id="amazon.titan-embed-text-v2:0",
            top_k=1,
            vector_bucket="b",
            index="i",
        )
    sent = s3v_client.query_vectors.call_args.kwargs
    assert sent["queryVector"] == {"float32": [0.5, 0.25, 0.125]}
    assert df["key"].tolist() == ["a"]


# ---------------------------------------------------------------------------
# Bedrock helper
# ---------------------------------------------------------------------------


def _mock_bedrock_response(payload: dict[str, Any]) -> Any:
    return {"body": io.BytesIO(json.dumps(payload).encode())}


def test_bedrock_titan_request_and_response_shape() -> None:
    client = MagicMock()
    client.invoke_model.return_value = _mock_bedrock_response({"embedding": [0.1, 0.2]})
    with _patch_client(client):
        out = _bedrock.embed_texts(
            ["hello"], "amazon.titan-embed-text-v2:0", model_kwargs={"dimensions": 256}, use_threads=False
        )
    body = json.loads(client.invoke_model.call_args.kwargs["body"])
    assert body == {"inputText": "hello", "dimensions": 256}
    assert client.invoke_model.call_args.kwargs["modelId"] == "amazon.titan-embed-text-v2:0"
    assert out == [[0.1, 0.2]]


def test_bedrock_cohere_request_and_response_shape() -> None:
    client = MagicMock()
    client.invoke_model.return_value = _mock_bedrock_response({"embeddings": [[0.7, 0.8]]})
    with _patch_client(client):
        out = _bedrock.embed_texts(
            ["hello"], "cohere.embed-english-v3", model_kwargs={"input_type": "search_query"}, use_threads=False
        )
    body = json.loads(client.invoke_model.call_args.kwargs["body"])
    assert body == {"texts": ["hello"], "input_type": "search_query"}
    assert out == [[0.7, 0.8]]


def test_bedrock_unknown_model_raises() -> None:
    with pytest.raises(exceptions.InvalidArgument):
        _bedrock.embed_texts(["hello"], "unknown.model", use_threads=False)


def test_bedrock_empty_texts_skips_invoke() -> None:
    client = MagicMock()
    with _patch_client(client):
        out = _bedrock.embed_texts([], "amazon.titan-embed-text-v2:0", use_threads=False)
    assert out == []
    client.invoke_model.assert_not_called()


# ---------------------------------------------------------------------------
# Management — buckets and indexes
# ---------------------------------------------------------------------------


def test_create_vector_bucket_returns_arn() -> None:
    client = MagicMock()
    client.create_vector_bucket.return_value = {"vectorBucketArn": "arn:aws:s3vectors:::bucket/b"}
    with _patch_client(client):
        out = wr.s3.create_vector_bucket(
            "b", tags={"env": "test"}, sse_type="aws:kms", encryption_kms_key_arn="arn:aws:kms:::key/abc"
        )
    sent = client.create_vector_bucket.call_args.kwargs
    assert sent["vectorBucketName"] == "b"
    assert sent["encryptionConfiguration"] == {"sseType": "aws:kms", "kmsKeyArn": "arn:aws:kms:::key/abc"}
    assert sent["tags"] == {"env": "test"}
    assert out == "arn:aws:s3vectors:::bucket/b"


def test_list_vector_buckets_paginates() -> None:
    client = MagicMock()
    client.list_vector_buckets.side_effect = [
        {"vectorBuckets": [{"vectorBucketName": "b1"}], "nextToken": "tok"},
        {"vectorBuckets": [{"vectorBucketName": "b2"}]},
    ]
    with _patch_client(client):
        out = wr.s3.list_vector_buckets()
    assert [b["vectorBucketName"] for b in out] == ["b1", "b2"]


def test_create_vector_index_payload() -> None:
    client = MagicMock()
    client.create_index.return_value = {"indexArn": "arn:idx"}
    with _patch_client(client):
        out = wr.s3.create_vector_index(
            vector_bucket="b",
            name="i",
            dimension=384,
            distance_metric="cosine",
            non_filterable_metadata_keys=["raw_text"],
        )
    sent = client.create_index.call_args.kwargs
    assert sent["indexName"] == "i"
    assert sent["dimension"] == 384
    assert sent["dataType"] == "float32"
    assert sent["distanceMetric"] == "cosine"
    assert sent["vectorBucketName"] == "b"
    assert sent["metadataConfiguration"] == {"nonFilterableMetadataKeys": ["raw_text"]}
    assert out == "arn:idx"


def test_delete_vector_index_accepts_arn() -> None:
    client = MagicMock()
    client.delete_index.return_value = {}
    with _patch_client(client):
        wr.s3.delete_vector_index(arn="arn:aws:s3vectors:::bucket/b/index/i")
    assert client.delete_index.call_args.kwargs == {"indexArn": "arn:aws:s3vectors:::bucket/b/index/i"}


def test_delete_vector_index_accepts_name_plus_bucket() -> None:
    client = MagicMock()
    client.delete_index.return_value = {}
    with _patch_client(client):
        wr.s3.delete_vector_index(name="i", vector_bucket="b")
    sent = client.delete_index.call_args.kwargs
    assert sent == {"indexName": "i", "vectorBucketName": "b"}


def test_delete_vector_index_arn_with_name_raises() -> None:
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.s3.delete_vector_index(arn="arn:idx", name="i", vector_bucket="b")
