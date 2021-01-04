import logging

import boto3
import pandas as pd
import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize(
    "encoding,strings,wrong_encoding,exception",
    [
        ("utf-8", ["漢字", "ãóú", "г, д, ж, з, к, л"], "ISO-8859-1", AssertionError),
        ("ISO-8859-1", ["Ö, ö, Ü, ü", "ãóú", "øe"], "utf-8", UnicodeDecodeError),
        ("ISO-8859-1", ["Ö, ö, Ü, ü", "ãóú", "øe"], None, UnicodeDecodeError),
    ],
)
@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("chunksize", [None, 2])
@pytest.mark.parametrize("line_terminator", ["\n", "\r"])
def test_csv_encoding(path, encoding, strings, wrong_encoding, exception, line_terminator, chunksize, use_threads):
    file_path = f"{path}0.csv"
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": strings})
    wr.s3.to_csv(
        df, file_path, index=False, encoding=encoding, line_terminator=line_terminator, use_threads=use_threads
    )
    df2 = wr.s3.read_csv(
        file_path, encoding=encoding, lineterminator=line_terminator, use_threads=use_threads, chunksize=chunksize
    )
    if isinstance(df2, pd.DataFrame) is False:
        df2 = pd.concat(df2, ignore_index=True)
    assert df.equals(df2)
    with pytest.raises(exception):
        df2 = wr.s3.read_csv(file_path, encoding=wrong_encoding, use_threads=use_threads, chunksize=chunksize)
        if isinstance(df2, pd.DataFrame) is False:
            df2 = pd.concat(df2, ignore_index=True)
        assert df.equals(df2)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_json(path, use_threads, chunksize):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.json" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        wr.s3.to_json(df, p, orient="records", lines=True)
    df2 = wr.s3.read_json(path, dataset=True, use_threads=use_threads, chunksize=chunksize)
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_csv(path, use_threads, chunksize):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.csv" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        wr.s3.to_csv(df, p, index=False)
    df2 = wr.s3.read_csv(path, dataset=True, use_threads=use_threads, chunksize=chunksize)
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_fwf(path, use_threads, chunksize):
    text = "0foo\n1boo"
    client_s3 = boto3.client("s3")
    paths = [f"{path}year={y}/month={m}/0.csv" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        bucket, key = wr._utils.parse_path(p)
        client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    df2 = wr.s3.read_fwf(
        path, dataset=True, use_threads=use_threads, chunksize=chunksize, widths=[1, 3], names=["c0", "c1"]
    )
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)


def test_csv(path):
    session = boto3.Session()
    df = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"{path}test_csv0.csv"
    path1 = f"{path}test_csv1.csv"
    path2 = f"{path}test_csv2.csv"
    wr.s3.to_csv(df=df, path=path0, index=False)
    assert wr.s3.does_object_exist(path=path0) is True
    assert wr.s3.size_objects(path=[path0], use_threads=False)[path0] == 9
    assert wr.s3.size_objects(path=[path0], use_threads=True)[path0] == 9
    wr.s3.to_csv(df=df, path=path1, index=False, boto3_session=None)
    wr.s3.to_csv(df=df, path=path2, index=False, boto3_session=session)
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=False))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=True))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=False, boto3_session=session))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=True, boto3_session=session))
    paths = [path0, path1, path2]
    df2 = pd.concat(objs=[df, df, df], sort=False, ignore_index=True)
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=False))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=True))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=False, boto3_session=session))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=True, boto3_session=session))
    with pytest.raises(wr.exceptions.InvalidArgumentType):
        wr.s3.read_csv(path=1)
    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.read_csv(path=paths, iterator=True)


def test_json(path):
    df0 = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"{path}test_json0.json"
    path1 = f"{path}test_json1.json"
    wr.s3.to_json(df=df0, path=path0)
    wr.s3.to_json(df=df0, path=path1)
    assert df0.equals(wr.s3.read_json(path=path0, use_threads=False))
    df1 = pd.concat(objs=[df0, df0], sort=False, ignore_index=False)
    assert df1.equals(wr.s3.read_json(path=[path0, path1], use_threads=True))


def test_fwf(path):
    text = "1 Herfelingen27-12-18\n2   Lambusart14-06-18\n3Spormaggiore15-04-18"
    client_s3 = boto3.client("s3")
    path0 = f"{path}0.txt"
    bucket, key = wr._utils.parse_path(path0)
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    path1 = f"{path}1.txt"
    bucket, key = wr._utils.parse_path(path1)
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    df = wr.s3.read_fwf(path=path0, use_threads=False, widths=[1, 12, 8], names=["id", "name", "date"])
    assert df.shape == (3, 3)
    df = wr.s3.read_fwf(path=[path0, path1], use_threads=True, widths=[1, 12, 8], names=["id", "name", "date"])
    assert df.shape == (6, 3)


def test_json_chunksize(path):
    num_files = 10
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    paths = [f"{path}{i}.json" for i in range(num_files)]
    for p in paths:
        wr.s3.to_json(df, p, orient="records", lines=True)
    dfs = list(wr.s3.read_json(paths, lines=True, chunksize=1))
    assert len(dfs) == (3 * num_files)
    for d in dfs:
        assert len(d.columns) == 2
        assert d.id.iloc[0] in (1, 2, 3)
        assert d.value.iloc[0] in ("foo", "boo", "bar")


def test_read_csv_index(path):
    df0 = pd.DataFrame({"id": [4, 5, 6], "value": ["foo", "boo", "bar"]})
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    path0 = f"{path}test_csv0.csv"
    path1 = f"{path}test_csv1.csv"
    paths = [path0, path1]
    wr.s3.to_csv(df=df0, path=path0, index=False)
    wr.s3.to_csv(df=df1, path=path1, index=False)
    df = wr.s3.read_csv(paths, index_col=["id"])
    assert df.shape == (6, 1)


def test_read_json_index(path):
    df0 = pd.DataFrame({"id": [4, 5, 6], "value": ["foo", "boo", "bar"]})
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    path0 = f"{path}test0.csv"
    path1 = f"{path}test1.csv"
    paths = [path0, path1]
    wr.s3.to_json(df=df0, path=path0, orient="index")
    wr.s3.to_json(df=df1, path=path1, orient="index")
    df = wr.s3.read_json(paths, orient="index")
    assert df.shape == (6, 2)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
def test_csv_additional_kwargs(path, kms_key_id, s3_additional_kwargs, use_threads):
    if s3_additional_kwargs is not None and "SSEKMSKeyId" in s3_additional_kwargs:
        s3_additional_kwargs["SSEKMSKeyId"] = kms_key_id
    path = f"{path}0.txt"
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.s3.to_csv(df, path, index=False, s3_additional_kwargs=s3_additional_kwargs)
    assert df.equals(wr.s3.read_csv([path]))
    desc = wr.s3.describe_objects([path])[path]
    if s3_additional_kwargs is None:
        assert desc.get("ServerSideEncryption") is None
    elif s3_additional_kwargs["ServerSideEncryption"] == "aws:kms":
        assert desc.get("ServerSideEncryption") == "aws:kms"
    elif s3_additional_kwargs["ServerSideEncryption"] == "AES256":
        assert desc.get("ServerSideEncryption") == "AES256"


@pytest.mark.parametrize("line_terminator", ["\n", "\r", "\n\r"])
def test_csv_line_terminator(path, line_terminator):
    file_path = f"{path}0.csv"
    df = pd.DataFrame(data={"reading": ["col1", "col2"], "timestamp": [1601379427618, 1601379427625], "value": [1, 2]})
    wr.s3.to_csv(df=df, path=file_path, index=False, line_terminator=line_terminator)
    df2 = wr.s3.read_csv(file_path)
    assert df.equals(df2)
