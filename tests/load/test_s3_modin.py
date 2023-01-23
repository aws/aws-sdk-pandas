from typing import List, Optional

import modin.pandas as pd
import pytest
import ray

from .._utils import ExecutionTimer


@pytest.fixture(scope="function")
def df_s() -> pd.DataFrame:
    # Data frame with 100000 rows
    ray_ds = ray.data.read_parquet("s3://ursa-labs-taxi-data/2010/02/data.parquet")
    return ray_ds.to_modin()


@pytest.mark.parametrize("benchmark_time", [40])
def test_modin_s3_read_parquet_simple(benchmark_time: float, request: pytest.FixtureRequest) -> None:
    path = "s3://ursa-labs-taxi-data/2018/"
    with ExecutionTimer(request, data_paths=path) as timer:
        ray_ds = ray.data.read_parquet(path)
        ray_ds.to_modin()

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_modin_s3_write_parquet_simple(
    df_s: pd.DataFrame, path: str, benchmark_time: float, request: pytest.FixtureRequest
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        df_s.to_parquet(path)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [30])
@pytest.mark.parametrize("partition_cols", [None, ["payment_type"], ["payment_type", "passenger_count"]])
def test_modin_s3_write_parquet_dataset(
    df_s: pd.DataFrame,
    path: str,
    partition_cols: Optional[List[str]],
    benchmark_time: float,
    request: pytest.FixtureRequest,
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        df_s.to_parquet(path, partition_cols=partition_cols)

    assert timer.elapsed_time < benchmark_time
