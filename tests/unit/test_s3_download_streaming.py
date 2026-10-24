# mypy: disable-error-code=no-untyped-def
"""Tests for the streaming behavior of ``wr.s3.download`` (issue #2831).

``download`` must not load the whole object into memory in a single
``get_object`` call: on a small host that OOMs for large files. These tests
assert the content round-trips AND that the object is fetched in bounded
byte-range chunks rather than one shot.
"""

from __future__ import annotations

import io

import boto3
import moto
import pytest

import awswrangler as wr
from awswrangler.s3._download import _DOWNLOAD_CHUNK_SIZE


@pytest.fixture(scope="function")
def moto_s3_client():
    with moto.mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="bucket")
        yield s3_client


def _largest_range_bytes(monkeypatch) -> list[int]:
    """Patch _fetch_range to record the size of every byte-range fetched."""
    from awswrangler.s3 import _fs

    sizes: list[int] = []
    original = _fs._fetch_range

    def spy(range_values, *args, **kwargs):
        start, end = range_values
        sizes.append(end - start)
        return original(range_values, *args, **kwargs)

    monkeypatch.setattr(_fs, "_fetch_range", spy)
    return sizes


def test_download_to_path_streams_in_bounded_chunks(moto_s3_client, tmp_path, monkeypatch):
    # An object several blocks large.
    size = _DOWNLOAD_CHUNK_SIZE * 3 + 12345
    payload = b"x" * size
    moto_s3_client.put_object(Bucket="bucket", Key="big.bin", Body=payload)

    sizes = _largest_range_bytes(monkeypatch)

    local = tmp_path / "big.bin"
    wr.s3.download(path="s3://bucket/big.bin", local_file=str(local), use_threads=False)

    # Content is preserved.
    assert local.read_bytes() == payload
    # No single range request pulled the whole object; peak fetch is bounded.
    assert sizes, "expected at least one range fetch"
    assert max(sizes) <= _DOWNLOAD_CHUNK_SIZE, (
        f"a single range fetched {max(sizes)} bytes, expected <= {_DOWNLOAD_CHUNK_SIZE}"
    )


def test_download_to_fileobj_preserves_content(moto_s3_client):
    size = _DOWNLOAD_CHUNK_SIZE + 777
    payload = bytes(bytearray(i % 256 for i in range(size)))
    moto_s3_client.put_object(Bucket="bucket", Key="obj.bin", Body=payload)

    buf = io.BytesIO()
    wr.s3.download(path="s3://bucket/obj.bin", local_file=buf, use_threads=False)

    assert buf.getvalue() == payload
