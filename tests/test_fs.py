import logging

import boto3
import pytest

import awswrangler as wr
from awswrangler.s3._fs import open_s3_object

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("use_threads", [True, False])
def test_io_intense(path, use_threads):
    path = f"{path}0.txt"
    data = b"0" * 10_000_000 + b"1" * 10_000_000 + b"2" * 10_000_000

    with open_s3_object(path, mode="wb", use_threads=use_threads) as s3obj:
        s3obj.write(data)

    with open_s3_object(path, mode="rb", use_threads=use_threads) as s3obj:
        assert s3obj.read() == data

    bucket, key = wr._utils.parse_path(path)
    assert boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read() == data


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("mode", ["r", "rb"])
def test_read_full(path, mode, use_threads):
    client_s3 = boto3.client("s3")
    path = f"{path}0.txt"
    bucket, key = wr._utils.parse_path(path)
    text = "AHDG*AWY&GD*A&WGd*AWgd87AGWD*GA*G*g*AGˆˆ&ÂDTW&ˆˆD&ÂTW7ˆˆTAWˆˆDAW&ˆˆAWGDIUHWOD#N"
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    with open_s3_object(path, mode=mode, s3_read_ahead_size=100, newline="\n", use_threads=use_threads) as s3obj:
        if mode == "r":
            assert s3obj.read() == text
        else:
            assert s3obj.read() == text.encode("utf-8")
    if "b" in mode:
        assert s3obj._cache == b""


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("mode", ["r", "rb"])
@pytest.mark.parametrize("block_size", [100, 2])
def test_read_chunked(path, mode, block_size, use_threads):
    client_s3 = boto3.client("s3")
    path = f"{path}0.txt"
    bucket, key = wr._utils.parse_path(path)
    text = "0123456789"
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    with open_s3_object(path, mode=mode, s3_read_ahead_size=block_size, newline="\n", use_threads=use_threads) as s3obj:
        if mode == "r":
            for i in range(3):
                assert s3obj.read(1) == text[i]
        else:
            for i in range(3):
                assert s3obj.read(1) == text[i].encode("utf-8")
        if "b" in mode:
            assert len(s3obj._cache) <= block_size
    if "b" in mode:
        assert s3obj._cache == b""


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("mode", ["r", "rb"])
@pytest.mark.parametrize("block_size", [1, 2, 3, 10, 23, 48, 65, 100])
def test_read_line(path, mode, block_size, use_threads):
    client_s3 = boto3.client("s3")
    path = f"{path}0.txt"
    bucket, key = wr._utils.parse_path(path)
    text = "0\n11\n22222\n33333333333333\n44444444444444444444444444444444444444444444\n55555"
    expected = ["0\n", "11\n", "22222\n", "33333333333333\n", "44444444444444444444444444444444444444444444\n", "55555"]
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    with open_s3_object(path, mode=mode, s3_read_ahead_size=block_size, newline="\n", use_threads=use_threads) as s3obj:
        for i, line in enumerate(s3obj):
            if mode == "r":
                assert line == expected[i]
            else:
                assert line == expected[i].encode("utf-8")
            if "b" in mode:
                assert len(s3obj._cache) < (len(expected[i]) + block_size)
        s3obj.seek(0)
        lines = s3obj.readlines()
        if mode == "r":
            assert lines == expected
        else:
            assert [line.decode("utf-8") for line in lines] == expected
    if "b" in mode:
        assert s3obj._cache == b""


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("mode", ["wb", "w"])
def test_write_full(path, mode, use_threads):
    client_s3 = boto3.client("s3")
    path = f"{path}0.txt"
    bucket, key = wr._utils.parse_path(path)
    text = "ajdaebdiebdkibaekdbekfbksbfksebkfjebkfjbekjfbkjebfkebwkfbewkjfbkjwebf"
    with open_s3_object(path, mode=mode, newline="\n", use_threads=use_threads) as s3obj:
        if mode == "wb":
            s3obj.write(text.encode("utf-8"))
        else:
            s3obj.write(text)
    assert client_s3.get_object(Bucket=bucket, Key=key)["Body"].read() == text.encode("utf-8")


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("mode", ["wb", "w"])
@pytest.mark.parametrize("data_size", [6_000_000, 10_000_000, 12_000_000])
def test_write_chunked(path, mode, data_size, use_threads):
    client_s3 = boto3.client("s3")
    path = f"{path}0.txt"
    bucket, key = wr._utils.parse_path(path)
    chunks = ["a", "jdae", "bdiebdkibaekdbekfbksbfk", "sebkf", "jebkfjbekjfbkjebfkebwkfbe", "f", "0" * data_size]
    expected = b"ajdaebdiebdkibaekdbekfbksbfksebkfjebkfjbekjfbkjebfkebwkfbef" + (b"0" * data_size)
    with open_s3_object(path, mode=mode, newline="\n", use_threads=use_threads) as s3obj:
        for chunk in chunks:
            if mode == "wb":
                s3obj.write(chunk.encode("utf-8"))
            else:
                s3obj.write(chunk)
    assert client_s3.get_object(Bucket=bucket, Key=key)["Body"].read() == expected


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
def test_additional_kwargs(path, kms_key_id, s3_additional_kwargs, use_threads):
    if s3_additional_kwargs is not None and "SSEKMSKeyId" in s3_additional_kwargs:
        s3_additional_kwargs["SSEKMSKeyId"] = kms_key_id
    path = f"{path}0.txt"
    with open_s3_object(path, mode="w", s3_additional_kwargs=s3_additional_kwargs, use_threads=use_threads) as s3obj:
        s3obj.write("foo")
    with open_s3_object(
        path,
        mode="r",
        s3_read_ahead_size=10_000_000,
        s3_additional_kwargs=s3_additional_kwargs,
        use_threads=use_threads,
    ) as s3obj:
        assert s3obj.read() == "foo"
    desc = wr.s3.describe_objects([path])[path]
    if s3_additional_kwargs is None:
        assert desc.get("ServerSideEncryption") is None
    elif s3_additional_kwargs["ServerSideEncryption"] == "aws:kms":
        assert desc.get("ServerSideEncryption") == "aws:kms"
    elif s3_additional_kwargs["ServerSideEncryption"] == "AES256":
        assert desc.get("ServerSideEncryption") == "AES256"
