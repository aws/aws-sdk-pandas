from __future__ import annotations

import modin.pandas as pd
import pytest
import ray

import awswrangler as wr

from .._utils import ExecutionTimer


@pytest.mark.parametrize("benchmark_time", [40])
def test_modin_s3_read_parquet_simple(benchmark_time: float, request: pytest.FixtureRequest) -> None:
    path = "s3://ursa-labs-taxi-data/2018/"
    with ExecutionTimer(request, data_paths=path) as timer:
        ray_ds = ray.data.read_parquet(path)
        ray_ds.to_modin()

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [180])
@pytest.mark.parametrize(
    "bulk_read",
    [
        pytest.param(False, id="regular"),
        pytest.param(True, id="bulk_read"),
    ],
)
def test_modin_s3_read_parquet_many_files(
    data_gen_bucket: str,
    benchmark_time: float,
    bulk_read: bool,
    request: pytest.FixtureRequest,
) -> None:
    path_prefix = f"s3://{data_gen_bucket}/parquet/small/partitioned/10000/"
    file_prefix = "input_1"

    paths = [path for path in wr.s3.list_objects(path_prefix) if path[len(path_prefix) :].startswith(file_prefix)]

    with ExecutionTimer(request, data_paths=paths) as timer:
        if bulk_read:
            ray_ds = ray.data.read_parquet_bulk(paths)
        else:
            ray_ds = ray.data.read_parquet(paths)

        frame: pd.DataFrame = ray_ds.to_modin()

    num_files = len(paths)
    assert len(frame) == num_files  # each file contains just one row

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_modin_s3_write_parquet_simple(
    df_s: pd.DataFrame, path: str, benchmark_time: float, request: pytest.FixtureRequest
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        df_s.to_parquet(path[:-1])  # path[:-1] due to Modin not properly handling S3 prefixes

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [30])
@pytest.mark.parametrize("partition_cols", [None, ["payment_type"], ["payment_type", "passenger_count"]])
def test_modin_s3_write_parquet_dataset(
    df_s: pd.DataFrame,
    path: str,
    partition_cols: list[str] | None,
    benchmark_time: float,
    request: pytest.FixtureRequest,
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        df_s.to_parquet(
            path[:-1], partition_cols=partition_cols
        )  # path[:-1] due to Modin not properly handling S3 prefixes

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [20])
def test_modin_s3_read_csv_simple(benchmark_time: float, request: pytest.FixtureRequest) -> None:
    paths = [f"s3://nyc-tlc/csv_backup/yellow_tripdata_2021-0{i}.csv" for i in range(1, 10)]
    with ExecutionTimer(request, data_paths=paths) as timer:
        ray_ds = ray.data.read_csv(paths)
        ray_ds.to_modin()

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [15])
def test_modin_s3_read_json_simple(benchmark_time: float, request: pytest.FixtureRequest) -> None:
    path = "s3://covid19-lake/covid_knowledge_graph/json/edges/paper_to_concept/*.json"
    with ExecutionTimer(request, data_paths=path) as timer:
        file_paths = wr.s3.list_objects(path)
        ray_ds = ray.data.read_json(file_paths)
        ray_ds.to_modin()

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_modin_s3_write_csv(
    path: str, big_modin_df: pd.DataFrame, benchmark_time: int, request: pytest.FixtureRequest
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        ray_ds = ray.data.from_modin(big_modin_df)
        ray_ds.write_csv(path)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_modin_s3_write_json(
    path: str, big_modin_df: pd.DataFrame, benchmark_time: int, request: pytest.FixtureRequest
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        # modin.DataFrame.to_json does not support PandasOnRay yet
        ray_ds = ray.data.from_modin(big_modin_df)
        ray_ds.write_json(path)

    assert timer.elapsed_time < benchmark_time
