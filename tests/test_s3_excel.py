import logging

import pandas as pd
import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("use_threads", [True, False])
def test_excel(path, use_threads):
    file_path = f"{path}0.xlsx"
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.s3.to_excel(df, file_path, use_threads=use_threads, index=False)
    df2 = wr.s3.read_excel(file_path, use_threads=use_threads)
    assert df.equals(df2)
