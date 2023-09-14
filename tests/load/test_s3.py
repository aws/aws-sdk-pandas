import math
from typing import List, Optional, Tuple

import modin.config as cfg
import modin.pandas as pd
import numpy as np
import pytest
import ray
from modin.distributed.dataframe.pandas import unwrap_partitions

import awswrangler as wr

from .._utils import ExecutionTimer


def _modin_repartition(df: pd.DataFrame, num_blocks: int) -> pd.DataFrame:
    """Repartition modin DataFrame into n blocks."""
    dataset = ray.data.from_modin(df)
    dataset = dataset.repartition(num_blocks)
    return dataset.to_modin()


@pytest.mark.parametrize("benchmark_time", [150])
def test_s3_select(benchmark_time: float, request: pytest.FixtureRequest) -> None:
    paths = [f"s3://ursa-labs-taxi-data/2018/{i}/data.parquet" for i in range(10, 13)]
    with ExecutionTimer(request, data_paths=paths) as timer:
        df = wr.s3.select_query(
            sql="SELECT * FROM s3object",
            path=paths,
            input_serialization="Parquet",
            input_serialization_params={},
            scan_range_chunk_size=16 * 1024 * 1024,
        )
        assert df.shape == (25139500, 17)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [40])
@pytest.mark.parametrize(
    "bulk_read,validate_schema",
    [
        pytest.param(False, False, id="regular"),
        pytest.param(True, False, id="bulk_read"),
        pytest.param(False, True, id="validate_schema"),
    ],
)
def test_s3_read_parquet_simple(
    benchmark_time: float,
    bulk_read: bool,
    validate_schema: bool,
    request: pytest.FixtureRequest,
) -> None:
    path = "s3://ursa-labs-taxi-data/2018/"
    with ExecutionTimer(request, data_paths=path) as timer:
        wr.s3.read_parquet(
            path=path,
            validate_schema=validate_schema,
            ray_args={"bulk_read": bulk_read},
        )

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [180])
@pytest.mark.parametrize(
    "bulk_read,validate_schema",
    [
        pytest.param(False, False, id="regular"),
        pytest.param(True, False, id="bulk_read"),
        pytest.param(False, True, id="validate_schema"),
    ],
)
def test_s3_read_parquet_many_files(
    data_gen_bucket: str,
    benchmark_time: float,
    bulk_read: bool,
    validate_schema: bool,
    request: pytest.FixtureRequest,
) -> None:
    path_prefix = f"s3://{data_gen_bucket}/parquet/small/partitioned/10000/"
    file_prefix = "input_1"

    paths = [path for path in wr.s3.list_objects(path_prefix) if path[len(path_prefix) :].startswith(file_prefix)]

    with ExecutionTimer(request, data_paths=paths) as timer:
        frame = wr.s3.read_parquet(
            path=paths,
            validate_schema=validate_schema,
            ray_args={"bulk_read": bulk_read},
        )

    num_files = len(paths)
    assert len(frame) == num_files  # each file contains just one row

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
@pytest.mark.parametrize("path_suffix", [None, "df.parquet"])
def test_s3_write_parquet_simple(
    df_s: pd.DataFrame, path: str, path_suffix: str, benchmark_time: float, request: pytest.FixtureRequest
) -> None:
    # Write into either a key or a prefix
    path = f"{path}{path_suffix}" if path_suffix else path

    with ExecutionTimer(request, data_paths=path) as timer:
        result = wr.s3.to_parquet(df_s, path=path)

    assert len(result["paths"]) == 1
    assert result["paths"][0].endswith(".parquet")
    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [30])
@pytest.mark.parametrize("partition_cols", [None, ["payment_type"], ["payment_type", "passenger_count"]])
@pytest.mark.parametrize("bucketing_info", [None, (["vendor_id"], 2), (["vendor_id", "rate_code_id"], 2)])
def test_s3_write_parquet_dataset(
    df_s: pd.DataFrame,
    path: str,
    partition_cols: Optional[List[str]],
    bucketing_info: Optional[wr.typing.BucketingInfoTuple],
    benchmark_time: float,
    request: pytest.FixtureRequest,
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        wr.s3.to_parquet(df_s, path=path, dataset=True, partition_cols=partition_cols, bucketing_info=bucketing_info)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [15])
@pytest.mark.parametrize("partition_cols", [None, ["payment_type"]])
@pytest.mark.parametrize("num_blocks", [None, 1, 5])
def test_s3_write_parquet_blocks(
    df_s: pd.DataFrame,
    path: str,
    partition_cols: Optional[List[str]],
    num_blocks: Optional[int],
    benchmark_time: float,
    request: pytest.FixtureRequest,
) -> None:
    if num_blocks:
        df_s = _modin_repartition(df_s, num_blocks)
    if partition_cols:
        dataset = True
        for col in partition_cols:
            df_s[col] = df_s[col].str.strip()
    else:
        dataset = False
    with ExecutionTimer(request, data_paths=path) as timer:
        wr.s3.to_parquet(df_s, path=path, dataset=dataset, partition_cols=partition_cols)
    df = wr.s3.read_parquet(path=path, dataset=dataset)
    assert df.shape == df_s.shape
    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_s3_delete_objects(path: str, path2: str, benchmark_time: float, request: pytest.FixtureRequest) -> None:
    df = pd.DataFrame({"id": range(0, 505)})
    paths1 = wr.s3.to_parquet(df=df, path=path, max_rows_by_file=1)["paths"]
    paths2 = wr.s3.to_parquet(df=df, path=path2, max_rows_by_file=1)["paths"]
    paths = paths1 + paths2
    with ExecutionTimer(request) as timer:
        wr.s3.delete_objects(path=paths)
    assert timer.elapsed_time < benchmark_time
    assert len(wr.s3.list_objects(path)) == 0
    assert len(wr.s3.list_objects(path2)) == 0


@pytest.mark.parametrize("benchmark_time", [20])
def test_s3_read_csv_simple(benchmark_time: float, request: pytest.FixtureRequest) -> None:
    paths = [f"s3://nyc-tlc/csv_backup/yellow_tripdata_2021-0{i}.csv" for i in range(1, 10)]
    with ExecutionTimer(request, data_paths=paths) as timer:
        wr.s3.read_csv(path=paths)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [15])
def test_s3_read_json_simple(benchmark_time: float, request: pytest.FixtureRequest) -> None:
    path = "s3://covid19-lake/covid_knowledge_graph/json/edges/paper_to_concept/*.json"
    with ExecutionTimer(request, data_paths=path) as timer:
        wr.s3.read_json(path=path, lines=True, orient="records")

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_s3_write_csv(
    path: str, big_modin_df: pd.DataFrame, benchmark_time: int, request: pytest.FixtureRequest
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        wr.s3.to_csv(big_modin_df, path, dataset=True)

    objects = wr.s3.list_objects(path)
    assert len(objects) > 1
    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_s3_write_json(
    path: str, big_modin_df: pd.DataFrame, benchmark_time: int, request: pytest.FixtureRequest
) -> None:
    with ExecutionTimer(request, data_paths=path) as timer:
        wr.s3.to_json(big_modin_df, path=path, dataset=True, lines=True, orient="records")

    objects = wr.s3.list_objects(path)
    assert len(objects) > 1
    assert timer.elapsed_time < benchmark_time


@pytest.mark.timeout(300)
@pytest.mark.parametrize("benchmark_time", [15])
def test_wait_object_exists(path: str, benchmark_time: int, request: pytest.FixtureRequest) -> None:
    df = pd.DataFrame({"c0": range(0, 200)})

    paths = wr.s3.to_parquet(df=df, path=path, max_rows_by_file=1)["paths"]

    with ExecutionTimer(request) as timer:
        wr.s3.wait_objects_exist(paths, use_threads=16)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.timeout(60)
@pytest.mark.parametrize("benchmark_time", [15])
def test_wait_object_not_exists(path: str, benchmark_time: int, request: pytest.FixtureRequest) -> None:
    num_objects = 200
    file_paths = [f"{path}{i}.txt" for i in range(num_objects)]

    with ExecutionTimer(request) as timer:
        wr.s3.wait_objects_not_exist(file_paths, use_threads=16)

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("size", [(5000, 5000), (1, 5000), (5000, 1), (1, 1)])
def test_wide_df(size: Tuple[int, int], path: str) -> None:
    df = pd.DataFrame(np.random.randint(0, 100, size=size))
    df.columns = df.columns.map(str)

    num_cols = size[0]
    df["int"] = np.random.choice(["1", "2", None], num_cols)
    df["decimal"] = np.random.choice(["1.0", "2.0", None], num_cols)
    df["date"] = np.random.choice(["2020-01-01", "2020-01-02", None], num_cols)
    df["par0"] = np.random.choice(["A", "B"], num_cols)

    partitions_shape = np.array(unwrap_partitions(df)).shape
    assert partitions_shape[1] == min(math.ceil(len(df.columns) / cfg.MinPartitionSize.get()), cfg.NPartitions.get())

    dtype = {
        "int": "tinyint",
        "decimal": "double",
        "date": "date",
    }

    result = wr.s3.to_csv(df=df, path=path, dataset=True, dtype=dtype, partition_cols=["par0"])
    assert len(result["paths"]) == partitions_shape[0] * len(df["par0"].unique())
