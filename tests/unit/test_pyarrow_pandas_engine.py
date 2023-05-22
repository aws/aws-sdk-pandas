import logging

import pyarrow as pa
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import assert_pandas_equals, is_pandas_2_x

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = [
    pytest.mark.distributed,
    pytest.mark.skipif(condition=not is_pandas_2_x, reason="PyArrow backed types are only supported in Pandas 2.x"),
]


def get_arrow_backed_df() -> pd.DataFrame:
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    df.id = df.id.astype(pd.ArrowDtype(pa.int64()))
    df.val = df.val.astype(pd.ArrowDtype(pa.string()))
    return df


def test_s3_read_csv(path: str) -> None:
    df = get_arrow_backed_df()
    wr.s3.to_csv(df=df, path=f"{path}.csv", index=False)

    df2 = wr.s3.read_csv(path=path, dtype_backend="pyarrow")

    assert_pandas_equals(df, df2)


def test_s3_read_json(path: str) -> None:
    df = get_arrow_backed_df()
    wr.s3.to_json(df=df, path=f"{path}.json", orient="records", lines=True)

    df2 = wr.s3.read_json(path=path, dtype_backend="pyarrow", orient="records", lines=True, use_threads=False)

    assert_pandas_equals(df, df2)
