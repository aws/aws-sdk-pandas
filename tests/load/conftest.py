import modin.pandas as pd
import pytest
import ray

import awswrangler as wr


@pytest.fixture(scope="function")
def df_s() -> pd.DataFrame:
    # Data frame with 100000 rows
    return wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2010/02/data.parquet")


@pytest.fixture(scope="function")
def df_xl() -> pd.DataFrame:
    # Data frame with 8759874 rows
    return wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2018/01/data.parquet")


@pytest.fixture(scope="function")
def big_modin_df() -> pd.DataFrame:
    pandas_refs = ray.data.range_table(100_000).to_pandas_refs()
    dataset = ray.data.from_pandas_refs(pandas_refs)

    frame = dataset.to_modin()
    frame["foo"] = frame.value * 2
    frame["bar"] = frame.value % 2

    return frame
