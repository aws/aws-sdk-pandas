import logging

import pytest

import awswrangler as wr

from .._utils import ExecutionTimer

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("benchmark_time", [2000])
def test_timestream_write(timestream_database_and_table, benchmark_time):
    # path = "s3://artifacts-s3artifacts6faa504f-265103l9fk5d/artifacts/ray/timestream2.csv"
    path = "s3://cfait-temp-ray/timestream2.csv"
    df = wr.s3.read_csv(path, parse_dates=["time"])

    name = timestream_database_and_table
    with ExecutionTimer("elapsed time of wr.timestream.write() simple") as timer:
        rejected_records = wr.timestream.write(
            df=df,
            database=name,
            table=name,
            time_col="time",
            measure_col="measure",
            dimensions_cols=["dim0", "dim1"],
        )

    assert timer.elapsed_time < benchmark_time
    assert len(rejected_records) == 0
