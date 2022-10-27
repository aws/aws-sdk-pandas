from datetime import datetime

import pytest
import ray
from pyarrow import csv

import awswrangler as wr

from .._utils import ExecutionTimer


@pytest.mark.parametrize("benchmark_time", [180])
def test_real_csv_load_scenario(benchmark_time: int, timestream_database_and_table: str) -> None:
    name = timestream_database_and_table
    df = (
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

    df["time"] = datetime.now()
    df.reset_index(inplace=True, drop=False)
    df_cpu = df[df.measure_kind == "cpu_utilization"]
    df_memory = df[df.measure_kind == "memory_utilization"]

    with ExecutionTimer("elapsed time of wr.timestream.write()") as timer:
        rejected_records = wr.timestream.write(
            df=df_cpu,
            database=name,
            table=name,
            time_col="time",
            measure_col="measure",
            dimensions_cols=["index", "region", "az", "hostname"],
        )
        assert len(rejected_records) == 0
        rejected_records = wr.timestream.write(
            df=df_memory,
            database=name,
            table=name,
            time_col="time",
            measure_col="measure",
            dimensions_cols=["index", "region", "az", "hostname"],
        )
        assert len(rejected_records) == 0
    assert timer.elapsed_time < benchmark_time

    df = wr.timestream.query(f'SELECT COUNT(*) AS counter FROM "{name}"."{name}"')
    assert df["counter"].iloc[0] == 126_000
