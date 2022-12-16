import datetime as dt
import logging
from datetime import datetime

import boto3
import pandas as pd
import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("pagination", [None, {}, {"MaxItems": 3, "PageSize": 2}])
def test_basic_scenario(timestream_database_and_table, pagination):
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
        """,
        pagination_config=pagination,
    )
    assert df.shape == (3, 8)
    assert df.attrs == {}


@pytest.mark.parametrize("chunked", [False, True])
def test_empty_query(timestream_database_and_table: str, chunked: bool) -> None:
    df = pd.DataFrame(
        {
            "time": [datetime.now() for _ in range(5)],
            "dim0": ["foo", "boo", "bar", "fizz", "buzz"],
            "dim1": [1, 2, 3, 4, 5],
            "measure": [1.0, 1.1, 1.2, 1.3, 1.4],
        }
    )
    rejected_records = wr.timestream.write(
        df=df,
        database=timestream_database_and_table,
        table=timestream_database_and_table,
        time_col="time",
        measure_col="measure",
        dimensions_cols=["dim0", "dim1"],
    )
    assert len(rejected_records) == 0

    output = wr.timestream.query(
        f"""SELECT *
            FROM "{timestream_database_and_table}"."{timestream_database_and_table}"
            WHERE dim0 = 'non_existing_test_dimension'
        """,
    )

    if chunked:
        assert list(output) == []
    else:
        assert output.empty


def test_chunked_scenario(timestream_database_and_table):
    df = pd.DataFrame(
        {
            "time": [datetime.now() for _ in range(5)],
            "dim0": ["foo", "boo", "bar", "fizz", "buzz"],
            "dim1": [1, 2, 3, 4, 5],
            "measure": [1.0, 1.1, 1.2, 1.3, 1.4],
        }
    )
    rejected_records = wr.timestream.write(
        df=df,
        database=timestream_database_and_table,
        table=timestream_database_and_table,
        time_col="time",
        measure_col="measure",
        dimensions_cols=["dim0", "dim1"],
    )
    assert len(rejected_records) == 0
    shapes = [(3, 5), (2, 5)]
    for df, shape in zip(
        wr.timestream.query(
            f"""
        SELECT
            *
        FROM "{timestream_database_and_table}"."{timestream_database_and_table}"
        ORDER BY time ASC
        """,
            chunked=True,
            pagination_config={"MaxItems": 5, "PageSize": 3},
        ),
        shapes,
    ):
        assert "QueryId" in df.attrs
        assert df.shape == shape


def test_versioned(timestream_database_and_table):
    name = timestream_database_and_table
    time = [datetime.now(), datetime.now(), datetime.now()]
    dfs = [
        pd.DataFrame(
            {
                "time": time,
                "dim0": ["foo", "boo", "bar"],
                "dim1": [1, 2, 3],
                "measure": [1.0, 1.1, 1.2],
            }
        ),
        pd.DataFrame(
            {
                "time": time,
                "dim0": ["foo", "boo", "bar"],
                "dim1": [1, 2, 3],
                "measure": [1.0, 1.1, 1.9],
            }
        ),
        pd.DataFrame(
            {
                "time": time,
                "dim0": ["foo", "boo", "bar"],
                "dim1": [1, 2, 3],
                "measure": [1.0, 1.1, 1.9],
            }
        ),
    ]
    versions = [1, 1, 2]
    rejected_rec_nums = [0, 1, 0]
    for df, version, rejected_rec_num in zip(dfs, versions, rejected_rec_nums):
        rejected_records = wr.timestream.write(
            df=df,
            database=name,
            table=name,
            time_col="time",
            measure_col="measure",
            dimensions_cols=["dim0", "dim1"],
            version=version,
        )
        assert len(rejected_records) == rejected_rec_num
        df_out = wr.timestream.query(
            f"""
            SELECT
                *
            FROM "{name}"."{name}"
            DESC LIMIT 10
        """
        )
        assert df_out.shape == (3, 5)


def test_real_csv_load_scenario(timestream_database_and_table):
    name = timestream_database_and_table
    df = pd.read_csv(
        "https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/mainline/sample_apps/data/sample.csv",
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


def test_multimeasure_scenario(timestream_database_and_table):
    df = pd.DataFrame(
        {
            "time": [datetime.now(), datetime.now(), datetime.now()],
            "dim0": ["foo", "boo", "bar"],
            "dim1": [1, 2, 3],
            "measure1": [1.0, 1.1, 1.2],
            "measure2": [2.0, 2.1, 2.2],
        }
    )
    rejected_records = wr.timestream.write(
        df=df,
        database=timestream_database_and_table,
        table=timestream_database_and_table,
        time_col="time",
        measure_col=["measure1", "measure2"],
        dimensions_cols=["dim0", "dim1"],
    )
    assert len(rejected_records) == 0
    df = wr.timestream.query(
        f"""
        SELECT
            *
        FROM "{timestream_database_and_table}"."{timestream_database_and_table}"
        ORDER BY time
        DESC LIMIT 10
        """,
    )
    assert df.shape == (3, 6)


def test_list_databases(timestream_database_and_table, timestream_database):
    dbs = wr.timestream.list_databases()

    assert timestream_database_and_table in dbs
    assert timestream_database in dbs

    wr.timestream.delete_database(timestream_database)

    dbs_tmp = wr.timestream.list_databases()
    assert timestream_database_and_table in dbs_tmp
    assert timestream_database not in dbs_tmp


def test_list_tables(timestream_database_and_table):
    all_tables = wr.timestream.list_tables()

    assert timestream_database_and_table in all_tables

    tables_in_db = wr.timestream.list_tables(database=timestream_database_and_table)
    assert timestream_database_and_table in tables_in_db
    assert len(tables_in_db) <= len(all_tables)

    wr.timestream.create_table(
        database=timestream_database_and_table,
        table=f"{timestream_database_and_table}_2",
        memory_retention_hours=1,
        magnetic_retention_days=1,
    )

    tables_in_db = wr.timestream.list_tables(database=timestream_database_and_table)
    assert f"{timestream_database_and_table}_2" in tables_in_db

    wr.timestream.delete_table(database=timestream_database_and_table, table=f"{timestream_database_and_table}_2")

    tables_in_db = wr.timestream.list_tables(database=timestream_database_and_table)
    assert f"{timestream_database_and_table}_2" not in tables_in_db


@pytest.mark.parametrize(
    "timestream_additional_kwargs",
    [None, {"MagneticStoreWriteProperties": {"EnableMagneticStoreWrites": True}}],
)
def test_create_table_additional_kwargs(timestream_database_and_table, timestream_additional_kwargs):
    client_timestream = boto3.client("timestream-write")
    wr.timestream.create_table(
        database=timestream_database_and_table,
        table=f"{timestream_database_and_table}_3",
        memory_retention_hours=1,
        magnetic_retention_days=1,
        timestream_additional_kwargs=timestream_additional_kwargs,
    )

    desc = client_timestream.describe_table(
        DatabaseName=timestream_database_and_table, TableName=f"{timestream_database_and_table}_3"
    )["Table"]
    if timestream_additional_kwargs is None:
        assert desc["MagneticStoreWriteProperties"].get("EnableMagneticStoreWrites") is False
    elif timestream_additional_kwargs["MagneticStoreWriteProperties"]["EnableMagneticStoreWrites"] is True:
        assert desc["MagneticStoreWriteProperties"].get("EnableMagneticStoreWrites") is True

    wr.timestream.delete_table(database=timestream_database_and_table, table=f"{timestream_database_and_table}_3")
    tables_in_db = wr.timestream.list_tables(database=timestream_database_and_table)
    assert f"{timestream_database_and_table}_3" not in tables_in_db


def test_timestamp_measure_column(timestream_database_and_table):
    df = pd.DataFrame(
        {
            "time": [datetime.now()] * 3,
            "dim0": ["foo", "boo", "bar"],
            "dim1": [1, 2, 3],
            "measure_f": [1.1, 1.2, 1.3],
            "measure_t": [datetime.now(dt.timezone.utc)] * 3,
        }
    )

    rejected_records = wr.timestream.write(
        df=df,
        database=timestream_database_and_table,
        table=timestream_database_and_table,
        time_col="time",
        measure_col=["measure_f", "measure_t"],
        dimensions_cols=["dim0", "dim1"],
    )
    assert len(rejected_records) == 0

    df = wr.timestream.query(
        f"""
        SELECT
            *
        FROM "{timestream_database_and_table}"."{timestream_database_and_table}"
        """,
    )
    assert df["measure_t"].dtype == "datetime64[ns]"
