import pandas as pd
import pytest

import awswrangler as wr

from .._utils import ExecutionTimer


@pytest.fixture(scope="function")
def df_s():
    # Data frame with 100000 rows
    return wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2010/02/data.parquet")


@pytest.fixture(scope="function")
def df_xl():
    # Data frame with 8759874 rows
    return wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2018/01/data.parquet")


@pytest.mark.repeat(1)
@pytest.mark.parametrize("benchmark_time", [180])
def test_s3_select(benchmark_time):
    path = "s3://ursa-labs-taxi-data/2018/1*.parquet"
    with ExecutionTimer("elapsed time of wr.s3.select_query()") as timer:
        wr.s3.select_query(
            sql="SELECT * FROM s3object",
            path=path,
            input_serialization="Parquet",
            input_serialization_params={},
            scan_range_chunk_size=16 * 1024 * 1024,
        )

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [90])
def test_s3_read_parquet_simple(benchmark_time):
    path = "s3://ursa-labs-taxi-data/2018/"
    with ExecutionTimer("elapsed time of wr.s3.read_parquet() simple") as timer:
        wr.s3.read_parquet(path=path)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [240])
def test_s3_read_parquet_partition_filter(benchmark_time):
    path = "s3://amazon-reviews-pds/parquet/"
    with ExecutionTimer("elapsed time of wr.s3.read_parquet() partition filter") as timer:
        filter = lambda x: True if x["product_category"].startswith("Wireless") else False  # noqa: E731
        wr.s3.read_parquet(path=path, dataset=True, partition_filter=filter)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [10])
@pytest.mark.parametrize("path_suffix", [None, "df.parquet"])
def test_s3_write_parquet_simple(df_s, path, path_suffix, benchmark_time):
    # Write into either a key or a prefix
    path = f"{path}{path_suffix}" if path_suffix else path

    with ExecutionTimer("elapsed time of wr.s3.to_parquet() simple") as timer:
        result = wr.s3.to_parquet(df_s, path=path)

    assert len(result["paths"]) == 1
    assert result["paths"][0].endswith(".parquet")
    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [200])
@pytest.mark.parametrize("partition_cols", [None, ["payment_type"], ["payment_type", "passenger_count"]])
@pytest.mark.parametrize("bucketing_info", [None, (["vendor_id"], 2), (["vendor_id", "rate_code_id"], 2)])
def test_s3_write_parquet_dataset(df_s, path, partition_cols, bucketing_info, benchmark_time):
    with ExecutionTimer("elapsed time of wr.s3.to_parquet() with partitioning and/or bucketing") as timer:
        wr.s3.to_parquet(df_s, path=path, dataset=True, partition_cols=partition_cols, bucketing_info=bucketing_info)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_s3_delete_objects(path, path2, benchmark_time):
    df = pd.DataFrame({"id": [1, 2, 3]})
    objects_per_bucket = 505
    paths1 = [f"{path}delete-test{i}.json" for i in range(objects_per_bucket)]
    paths2 = [f"{path2}delete-test{i}.json" for i in range(objects_per_bucket)]
    paths = paths1 + paths2
    for path in paths:
        wr.s3.to_json(df, path)
    with ExecutionTimer("elapsed time of wr.s3.delete_objects()") as timer:
        wr.s3.delete_objects(path=paths)
    assert timer.elapsed_time < benchmark_time
    assert len(wr.s3.list_objects(f"{path}delete-test*")) == 0
    assert len(wr.s3.list_objects(f"{path2}delete-test*")) == 0


@pytest.mark.parametrize("benchmark_time", [240])
def test_s3_read_csv_simple(benchmark_time):
    path = "s3://nyc-tlc/csv_backup/yellow_tripdata_2021-0*.csv"
    with ExecutionTimer("elapsed time of wr.s3.read_csv() simple") as timer:
        wr.s3.read_csv(path=path)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [30])
def test_s3_read_json_simple(benchmark_time):
    path = "s3://covid19-lake/covid_knowledge_graph/json/edges/paper_to_concept/*.json"
    with ExecutionTimer("elapsed time of wr.s3.read_json() simple") as timer:
        wr.s3.read_json(path=path, lines=True, orient="records")

    assert timer.elapsed_time < benchmark_time


@pytest.mark.timeout(300)
@pytest.mark.parametrize("benchmark_time", [30])
def test_wait_object_exists(path: str, benchmark_time: int) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})

    num_objects = 200
    file_paths = [f"{path}{i}.txt" for i in range(num_objects)]

    for file_path in file_paths:
        wr.s3.to_csv(df, file_path, index=False)

    with ExecutionTimer("elapsed time of wr.s3.wait_objects_exist()") as timer:
        wr.s3.wait_objects_exist(file_paths, parallelism=16)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.timeout(60)
@pytest.mark.parametrize("benchmark_time", [30])
def test_wait_object_not_exists(path: str, benchmark_time: int) -> None:
    num_objects = 200
    file_paths = [f"{path}{i}.txt" for i in range(num_objects)]

    with ExecutionTimer("elapsed time of wr.s3.wait_objects_not_exist()") as timer:
        wr.s3.wait_objects_not_exist(file_paths, parallelism=16)

    assert timer.elapsed_time < benchmark_time
