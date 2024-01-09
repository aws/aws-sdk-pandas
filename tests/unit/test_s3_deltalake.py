from __future__ import annotations

from typing import Any

import pytest

import awswrangler as wr
import awswrangler.pandas as pd


@pytest.mark.parametrize("s3_additional_kwargs", [None, {"ServerSideEncryption": "AES256"}])
@pytest.mark.parametrize(
    "pyarrow_additional_kwargs", [{"safe": True, "deduplicate_objects": False, "types_mapper": None}]
)
def test_read_deltalake(
    path: str, s3_additional_kwargs: dict[str, Any] | None, pyarrow_additional_kwargs: dict[str, Any]
) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", None, "bar"], "c2": [3.0, 4.0, 5.0], "c3": [True, False, None]})
    wr.s3.to_deltalake(path=path, df=df, s3_additional_kwargs=s3_additional_kwargs, s3_allow_unsafe_rename=True)

    df2 = wr.s3.read_deltalake(
        path=path, s3_additional_kwargs=s3_additional_kwargs, pyarrow_additional_kwargs=pyarrow_additional_kwargs
    )
    assert df2.equals(df)


@pytest.mark.parametrize("pyarrow_additional_kwargs", [{"types_mapper": None}])
def test_read_deltalake_versioned(path: str, pyarrow_additional_kwargs: dict[str, Any]) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "baz", "bar"]})
    wr.s3.to_deltalake(path=path, df=df, s3_allow_unsafe_rename=True)

    df2 = wr.s3.read_deltalake(path=path, pyarrow_additional_kwargs=pyarrow_additional_kwargs)
    assert df2.equals(df)

    df["c2"] = [True, False, True]
    wr.s3.to_deltalake(path=path, df=df, mode="overwrite", overwrite_schema=True, s3_allow_unsafe_rename=True)

    df3 = wr.s3.read_deltalake(path=path, version=0, pyarrow_additional_kwargs=pyarrow_additional_kwargs)
    assert df3.equals(df.drop("c2", axis=1))

    df4 = wr.s3.read_deltalake(path=path, version=1, pyarrow_additional_kwargs=pyarrow_additional_kwargs)
    assert df4.equals(df)


def test_read_deltalake_partitions(path: str) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [True, False, True], "par0": ["foo", "foo", "bar"], "par1": [1, 2, 2]})
    wr.s3.to_deltalake(path=path, df=df, partition_cols=["par0", "par1"], s3_allow_unsafe_rename=True)

    df2 = wr.s3.read_deltalake(path=path, columns=["c0"], partitions=[("par0", "=", "foo"), ("par1", "=", "1")])
    assert df2.shape == (1, 1)
