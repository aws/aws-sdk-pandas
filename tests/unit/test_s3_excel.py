import logging

import pandas as pd
import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("ext", ["xlsx", "xlsm", "xls", "odf"])
@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_excel(path, ext, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    file_path = f"{path}0.{ext}"
    pandas_kwargs = {}

    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.to_excel(df, file_path, use_threads=use_threads, index=False, pandas_kwargs=pandas_kwargs)

    wr.s3.to_excel(df, file_path, use_threads=use_threads, index=False, **pandas_kwargs)

    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.read_excel(file_path, use_threads=use_threads, pandas_kwargs=pandas_kwargs)

    df2 = wr.s3.read_excel(file_path, use_threads=use_threads, **pandas_kwargs)
    assert df.equals(df2)


def test_read_xlsx_versioned(path) -> None:
    path_file = f"{path}0.xlsx"
    dfs = [pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]}), pd.DataFrame({"c0": [3, 4, 5], "c1": [6, 7, 8]})]
    pandas_kwargs = {}
    for df in dfs:
        wr.s3.to_excel(df=df, path=path_file, index=False, **pandas_kwargs)
        version_id = wr.s3.describe_objects(path=path_file)[path_file]["VersionId"]
        df_temp = wr.s3.read_excel(path_file, version_id=version_id, **pandas_kwargs)
        assert df_temp.equals(df)
        assert version_id == wr.s3.describe_objects(path=path_file, version_id=version_id)[path_file]["VersionId"]
