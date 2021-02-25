import bz2
import gzip
import logging
import lzma
from io import BytesIO, TextIOWrapper
from sys import version_info

import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import get_df_csv

EXT = {"gzip": ".gz", "bz2": ".bz2", "xz": ".xz", "zip": ".zip"}

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("compression", ["gzip", "bz2", "xz"])
def test_csv_read(bucket, path, compression):
    key_prefix = path.replace(f"s3://{bucket}/", "")
    wr.s3.delete_objects(path=path)
    df = get_df_csv()
    if compression == "gzip":
        buffer = BytesIO()
        with gzip.GzipFile(mode="w", fileobj=buffer) as zipped_file:
            df.to_csv(TextIOWrapper(zipped_file, "utf8"), index=False, header=None)
        s3_resource = boto3.resource("s3")
        s3_object = s3_resource.Object(bucket, f"{key_prefix}test.csv.gz")
        s3_object.put(Body=buffer.getvalue())
        file_path = f"{path}test.csv.gz"
    elif compression == "bz2":
        buffer = BytesIO()
        with bz2.BZ2File(mode="w", filename=buffer) as zipped_file:
            df.to_csv(TextIOWrapper(zipped_file, "utf8"), index=False, header=None)
        s3_resource = boto3.resource("s3")
        s3_object = s3_resource.Object(bucket, f"{key_prefix}test.csv.bz2")
        s3_object.put(Body=buffer.getvalue())
        file_path = f"{path}test.csv.bz2"
    elif compression == "xz":
        buffer = BytesIO()
        with lzma.LZMAFile(mode="w", filename=buffer) as zipped_file:
            df.to_csv(TextIOWrapper(zipped_file, "utf8"), index=False, header=None)
        s3_resource = boto3.resource("s3")
        s3_object = s3_resource.Object(bucket, f"{key_prefix}test.csv.xz")
        s3_object.put(Body=buffer.getvalue())
        file_path = f"{path}test.csv.xz"
    else:
        file_path = f"{path}test.csv"
        wr.s3.to_csv(df=df, path=file_path, index=False, header=None)

    df2 = wr.s3.read_csv(path=[file_path], names=df.columns)
    assert df2.shape == (3, 10)
    dfs = wr.s3.read_csv(path=[file_path], names=df.columns, chunksize=1)
    for df3 in dfs:
        assert len(df3.columns) == 10


@pytest.mark.parametrize("compression", ["gzip", "bz2", "xz", "zip", None])
def test_csv_write(path, compression):
    path_file = f"{path}test.csv{EXT.get(compression, '')}"
    df = get_df_csv()
    if version_info < (3, 7) and compression:
        with pytest.raises(wr.exceptions.InvalidArgument):
            wr.s3.to_csv(df, path_file, compression=compression, index=False, header=None)
    else:
        wr.s3.to_csv(df, path_file, compression=compression, index=False, header=None)
        df2 = pd.read_csv(path_file, names=df.columns)
        df3 = wr.s3.read_csv([path_file], names=df.columns)
        assert df.shape == df2.shape == df3.shape


# @pytest.mark.parametrize("compression", ["gzip", "bz2", "xz", "zip", None])  # Removed due a Pandas bug
@pytest.mark.parametrize("compression", [None])
def test_json(path, compression):
    path_file = f"{path}test.json{EXT.get(compression, '')}"
    df = pd.DataFrame({"id": [1, 2, 3]})
    if version_info < (3, 7) and compression:
        with pytest.raises(wr.exceptions.InvalidArgument):
            wr.s3.to_json(df=df, path=path_file, compression=compression)
    else:
        wr.s3.to_json(df=df, path=path_file)
        df2 = pd.read_json(path_file, compression=compression)
        df3 = wr.s3.read_json(path=[path_file])
        assert df.shape == df2.shape == df3.shape


@pytest.mark.parametrize("chunksize", [None, 1])
# @pytest.mark.parametrize("compression", ["gzip", "bz2", "xz", "zip", None])  # Removed due a Pandas bug
@pytest.mark.parametrize("compression", [None])
def test_partitioned_json(path, compression, chunksize):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.json{EXT.get(compression, '')}" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    if version_info < (3, 7) and compression:
        with pytest.raises(wr.exceptions.InvalidArgument):
            for p in paths:
                wr.s3.to_json(df, p, orient="records", lines=True, compression=compression)
    else:
        for p in paths:
            wr.s3.to_json(df, p, orient="records", lines=True, compression=compression)
        df2 = wr.s3.read_json(path, dataset=True, chunksize=chunksize)
        if chunksize is None:
            assert df2.shape == (6, 4)
            assert df2.c0.sum() == 3
        else:
            for d in df2:
                assert d.shape == (1, 4)


@pytest.mark.parametrize("chunksize", [None, 1])
@pytest.mark.parametrize("compression", ["gzip", "bz2", "xz", "zip", None])
def test_partitioned_csv(path, compression, chunksize):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.csv{EXT.get(compression, '')}" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    if version_info < (3, 7) and compression:
        with pytest.raises(wr.exceptions.InvalidArgument):
            for p in paths:
                wr.s3.to_csv(df, p, index=False, compression=compression)
    else:
        for p in paths:
            wr.s3.to_csv(df, p, index=False, compression=compression, header=True)
        df2 = wr.s3.read_csv(path, dataset=True, chunksize=chunksize, header=0)
        if chunksize is None:
            assert df2.shape == (6, 4)
            assert df2.c0.sum() == 3
        else:
            for d in df2:
                assert d.shape == (1, 4)
