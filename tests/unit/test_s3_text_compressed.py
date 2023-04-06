import bz2
import gzip
import logging
import lzma
from io import BytesIO, TextIOWrapper
from typing import Optional

import boto3
import pyarrow as pa
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import get_df_csv, is_ray_modin

EXT = {"gzip": ".gz", "bz2": ".bz2", "xz": ".xz", "zip": ".zip"}

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


# XFail issue: https://github.com/aws/aws-sdk-pandas/issues/2005
@pytest.mark.parametrize(
    "compression",
    [
        "gzip",
        "bz2",
        pytest.param(
            "xz", marks=pytest.mark.xfail(is_ray_modin, reason="Arrow compression errors", raises=pa.lib.ArrowInvalid)
        ),
    ],
)
def test_csv_read(bucket: str, path: str, compression: str) -> None:
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


# XFail issue: https://github.com/aws/aws-sdk-pandas/issues/2005
@pytest.mark.parametrize(
    "compression",
    [
        "gzip",
        "bz2",
        pytest.param(
            "xz", marks=pytest.mark.xfail(is_ray_modin, reason="Arrow compression errors", raises=pa.lib.ArrowInvalid)
        ),
        pytest.param(
            "zip", marks=pytest.mark.xfail(is_ray_modin, reason="Arrow compression errors", raises=pa.lib.ArrowInvalid)
        ),
        None,
    ],
)
def test_csv_write(path: str, compression: Optional[str]) -> None:
    # Ensure we use the pd.read_csv native to Pandas, not Modin.
    # Modin's read_csv has an issue in this scenario, making the test fail.
    import pandas as pd

    path_file = f"{path}test.csv{EXT.get(compression, '')}"
    df = get_df_csv()
    wr.s3.to_csv(df, path_file, compression=compression, index=False, header=None)
    df2 = pd.read_csv(path_file, names=df.columns)
    df3 = wr.s3.read_csv([path_file], names=df.columns)
    assert df.shape == df2.shape == df3.shape


@pytest.mark.parametrize("compression", ["gzip", "bz2", "xz", "zip", None])
def test_csv_write_dataset_filename_extension(path: str, compression: Optional[str]) -> None:
    df = get_df_csv()
    result = wr.s3.to_csv(df, path, compression=compression, index=False, dataset=True)
    for p in result["paths"]:
        assert p.endswith(f".csv{EXT.get(compression, '')}")


@pytest.mark.parametrize("compression", ["gzip", "bz2", "xz", "zip", None])
def test_json(path: str, compression: Optional[str]) -> None:
    path_file = f"{path}test.json{EXT.get(compression, '')}"
    df = pd.DataFrame({"id": [1, 2, 3]})
    wr.s3.to_json(df=df, path=path_file)
    df2 = pd.read_json(path_file, compression=compression)
    df3 = wr.s3.read_json(path=[path_file])
    assert df.shape == df2.shape == df3.shape


@pytest.mark.parametrize("chunksize", [None, 1])
@pytest.mark.parametrize("compression", ["gzip", "bz2", "xz", "zip", None])
def test_partitioned_json(path: str, compression: Optional[str], chunksize: Optional[int]) -> None:
    df = pd.DataFrame(
        {
            "c0": [0, 1, 2, 3],
            "c1": ["foo", "boo", "bar", "baz"],
            "year": [2020, 2020, 2021, 2021],
            "month": [1, 2, 1, 2],
        }
    )
    wr.s3.to_json(
        df,
        path=path,
        orient="records",
        lines=True,
        compression=compression,
        dataset=True,
        partition_cols=["year", "month"],
    )
    df2 = wr.s3.read_json(path, dataset=True, chunksize=chunksize)
    if chunksize is None:
        assert df2.shape == (4, 4)
        assert df2.c0.sum() == 6
    else:
        for d in df2:
            assert d.shape == (1, 4)


@pytest.mark.parametrize("chunksize", [None, 1])
@pytest.mark.parametrize("compression", ["gzip", "bz2", "xz", "zip", None])
def test_partitioned_csv(path: str, compression: Optional[str], chunksize: Optional[int]) -> None:
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.csv{EXT.get(compression, '')}" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        wr.s3.to_csv(df, p, index=False, compression=compression, header=True)
    df2 = wr.s3.read_csv(path, dataset=True, chunksize=chunksize, header=0)
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)
