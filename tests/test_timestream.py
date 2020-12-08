import logging
from datetime import datetime

import pandas as pd

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_basic_scenario(timestream_database_and_table):
    name = timestream_database_and_table
    df = pd.DataFrame(
        {
            "time": [datetime.now(), datetime.now(), datetime.now()],
            "dim0": ["foo", "boo", "bar"],
            "dim1": [1, 2, 3],
            "measure": [1.0, 1.1, 1.2],
        }
    )
    rejected_records = wr.timestream.write(
        df=df,
        database=name,
        table=name,
        time_col="time",
        measure_col="measure",
        dimensions_cols=["dim0", "dim1"],
    )
    assert len(rejected_records) == 0
    df = wr.timestream.query(
        f"""
        SELECT
            1 as col_int,
            try_cast(now() as time) as col_time,
            TRUE as col_bool,
            current_date as col_date,
            'foo' as col_str,
            measure_value::double,
            measure_name,
            time
        FROM "{name}"."{name}"
        ORDER BY time
        DESC LIMIT 10
    """
    )
    assert df.shape == (3, 8)


def test_real_csv_load_scenario(timestream_database_and_table):
    name = timestream_database_and_table
    df = pd.read_csv(
        "https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/sample_apps/data/sample.csv",
        names=[
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
        ],
        usecols=["region", "az", "hostname", "measure_kind", "measure"],
    )
    df["time"] = datetime.now()
    df.reset_index(inplace=True, drop=False)
    df_cpu = df[df.measure_kind == "cpu_utilization"]
    df_memory = df[df.measure_kind == "memory_utilization"]
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
    df = wr.timestream.query(f'SELECT COUNT(*) AS counter FROM "{name}"."{name}"')
    assert df["counter"].iloc[0] == 126_000
