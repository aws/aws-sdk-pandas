import modin.pandas as pd
import pytest
import ray
from pyarrow import csv

import awswrangler as wr


@pytest.fixture(scope="function")
def df_timestream() -> pd.DataFrame:
    # Data frame with 126_000 rows
    return (
        ray.data.read_csv(
            "https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/mainline/sample_apps/data/sample.csv",
            **{
                "read_options": csv.ReadOptions(
                    column_names=[
                        "ignore0",
                        "region",
                        "ignore1",
                        "az",
                        "ignore2",
                        "hostname",
                        "measure_kind",
                        "measure",
                        "ignore3",
                        "ignore4",
                        "ignore5",
                    ]
                )
            },
        )
        .to_modin()
        .loc[:, ["region", "az", "hostname", "measure_kind", "measure"]]
    )


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
    pandas_refs = ray.data.range(100_000).to_pandas_refs()
    dataset = ray.data.from_pandas_refs(pandas_refs)

    frame = dataset.to_modin()
    frame["foo"] = frame.id * 2
    frame["bar"] = frame.id % 2

    return frame
