import datetime as dt
import logging
import time
from datetime import datetime

import boto3
import numpy as np
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


# This test covers every possible combination of common_attributes and data frame inputs
@pytest.mark.parametrize("record_type", ["SCALAR", "MULTI"])
@pytest.mark.parametrize(
    "common_attributes_details",
    [
        ({}, (4, 6)),
        ({"MeasureName": "cpu_util"}, (4, 6)),
        ({"MeasureValue": "13.1", "MeasureValueType": "DOUBLE"}, (4, 5)),
        (
            {
                "MeasureValues": [
                    {"Name": "cpu_util", "Value": "12.0", "Type": "DOUBLE"},
                    {"Name": "mem_util", "Value": "45.6", "Type": "DOUBLE"},
                ],
                "MeasureValueType": "MULTI",
            },
            (5, 6),
        ),
        ({"Time": str(round(time.time())), "TimeUnit": "SECONDS"}, (4, 5)),
        ({"Time": str(round(time.time()) * 1_000), "MeasureValue": "13.1", "MeasureValueType": "DOUBLE"}, (4, 5)),
        ({"Dimensions": [{"Name": "region", "Value": "us-east-1"}]}, (4, 5)),
        ({"Dimensions": [{"Name": "region", "Value": "us-east-1"}, {"Name": "host", "Value": "linux"}]}, (5, 6)),
        (
            {
                "Dimensions": [{"Name": "region", "Value": "us-east-1"}],
                "MeasureValue": "13.1",
                "MeasureValueType": "DOUBLE",
            },
            (4, 4),
        ),
        (
            {
                "Dimensions": [{"Name": "region", "Value": "us-east-1"}, {"Name": "host", "Value": "linux"}],
                "MeasureValues": [
                    {"Name": "cpu_util", "Value": "12.0", "Type": "DOUBLE"},
                    {"Name": "mem_util", "Value": "45.6", "Type": "DOUBLE"},
                ],
                "MeasureValueType": "MULTI",
            },
            (6, 6),
        ),
    ],
)
def test_common_attributes(timestream_database_and_table, record_type, common_attributes_details):
    common_attributes, num_cols = common_attributes_details
    df = pd.DataFrame({"dummy": ["a"] * 3})

    kwargs = {
        "df": df,
        "database": timestream_database_and_table,
        "table": timestream_database_and_table,
        "common_attributes": common_attributes,
        "measure_name": "cpu",
    }
    if "Time" not in common_attributes:
        df["time"] = [datetime.now() + dt.timedelta(seconds=(60 * c)) for c in range(3)]
        kwargs["time_col"] = "time"
    if all(k not in common_attributes for k in ("MeasureValue", "MeasureValues")):
        df["cpu_utilization"] = [13.1] * 3
        measure_cols = ["cpu_utilization"]
        if record_type == "MULTI":
            df["mem_utilization"] = [45.2] * 3
            measure_cols += ["mem_utilization"]
        kwargs["measure_col"] = measure_cols
    if "Dimensions" not in common_attributes:
        df["region"] = ["us-east-1", "us-east-2", "us-west-2"]
        dimensions_cols = ["region"]
        if record_type == "MULTI":
            df["host"] = ["AL2", "Ubuntu", "Debian"]
            dimensions_cols += ["host"]
        kwargs["dimensions_cols"] = dimensions_cols

    rejected_records = wr.timestream.write(**kwargs)
    assert len(rejected_records) == 0

    df = wr.timestream.query(f"""SELECT * FROM "{timestream_database_and_table}"."{timestream_database_and_table}" """)
    assert df.shape == (3, num_cols[1] if record_type == "MULTI" else num_cols[0])


@pytest.mark.parametrize("record_type", ["SCALAR", "MULTI"])
def test_merge_dimensions(timestream_database_and_table, record_type):
    df = pd.DataFrame(
        {
            "time": [datetime.now()] * 3,
            "measure": [13.1, 13.2, 13.3],
            "dim1": ["az1", "az2", "az3"],
        }
    )

    kwargs = {
        "df": df,
        "database": timestream_database_and_table,
        "table": timestream_database_and_table,
        "common_attributes": {
            "Dimensions": [{"Name": "region", "Value": "us-east-1"}, {"Name": "host", "Value": "linux"}]
        },
        "time_col": "time",
        "measure_col": ["measure"],
        "dimensions_cols": ["dim1"],
    }
    if record_type == "MULTI":
        df["dim2"] = ["arm", "intel", "arm"]
        kwargs["dimensions_cols"] = ["dim1", "dim2"]

    rejected_records = wr.timestream.write(**kwargs)
    assert len(rejected_records) == 0

    df = wr.timestream.query(f"""SELECT * FROM "{timestream_database_and_table}"."{timestream_database_and_table}" """)
    assert df.shape == (3, 7) if record_type == "MULTI" else (3, 6)


def test_exceptions():
    df = pd.DataFrame(
        {
            "time": [datetime.now(), datetime.now(), datetime.now()],
            "measure1": [1.0, 1.1, 1.2],
        }
    )

    # No columns supplied
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.timestream.write(
            df=df,
            database="test",
            table="test",
            common_attributes={"MeasureName": "test", "MeasureValueType": "Double"},
        )

    # Missing MeasureValueType
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.timestream.write(
            df=df, database="test", table="test", time_col="time", common_attributes={"MeasureValue": "13.1"}
        )

    # Missing MeasureName
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.timestream.write(
            df=df,
            database="test",
            table="test",
            time_col="time",
            common_attributes={"MeasureValue": "13.1", "MeasureValueType": "DOUBLE"},
        )

    # None timestamp time_col
    with pytest.raises(wr.exceptions.InvalidArgumentType):
        df["time"] = ["a"] * 3
        wr.timestream.write(
            df=df,
            database="test",
            table="test",
            time_col="time",
            common_attributes={"MeasureName": "test", "MeasureValue": "13.1", "MeasureValueType": "Double"},
        )


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


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (
            {
                "df": pd.DataFrame(
                    {
                        "time": [datetime.now(), datetime.now(), datetime.now()],
                        "dim0": [1, 2, 3],
                        "measure1": [None, np.nan, 1.2],
                        "measure2": [pd.NaT, 2.1, 2.2],
                    }
                ),
                "measure_col": ["measure1", "measure2"],
            },
            (2, 5),
        ),
        (
            {
                "df": pd.DataFrame(
                    {
                        "time": [datetime.now(), datetime.now(), datetime.now()],
                        "dim0": [1, 2, 3],
                        "measure1": [None, 1.1, 1.2],
                    }
                ),
                "measure_col": ["measure1"],
            },
            (2, 4),
        ),
    ],
)
def test_nans(timestream_database_and_table, test_input, expected):
    rejected_records = wr.timestream.write(
        **test_input,
        database=timestream_database_and_table,
        table=timestream_database_and_table,
        time_col="time",
        dimensions_cols=["dim0"],
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
    assert df.shape == expected


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
        tags={"foo": "boo", "bar": "xoo"},
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
            "dim1": [1, None, 3],
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


@pytest.mark.parametrize(
    "record_type",
    ["MULTI", "SCALAR"],
)
def test_measure_name(timestream_database_and_table, record_type):
    data = {"time": [datetime.now()] * 3}
    args = {
        "database": timestream_database_and_table,
        "table": timestream_database_and_table,
        "time_col": "time",
    }
    if record_type == "MULTI":
        data.update(
            {
                "dim0": ["foo", "boo", "bar"],
                "dim1": [1, None, 3],
                "measure_0": [1.1, 1.2, 1.3],
                "measure_1": [2.1, 2.2, 2.3],
            }
        )
        args.update(
            {
                "measure_col": ["measure_0", "measure_1"],
                "measure_name": "example",
                "dimensions_cols": ["dim0", "dim1"],
            }
        )
    else:
        data.update(
            {
                "dim": ["foo", "boo", "bar"],
                "measure": [1.1, 1.2, 1.3],
            }
        )
        args.update(
            {
                "measure_col": ["measure"],
                "measure_name": "example",
                "dimensions_cols": ["dim"],
            }
        )

    df = pd.DataFrame(data)
    rejected_records = wr.timestream.write(
        df=df,
        **args,
    )

    assert len(rejected_records) == 0

    df = wr.timestream.query(
        f"""
        SELECT
            *
        FROM "{timestream_database_and_table}"."{timestream_database_and_table}"
        """,
    )
    for measure_name in df["measure_name"].tolist():
        assert measure_name == "example"
