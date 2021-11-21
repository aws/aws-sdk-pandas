import logging

import pandas as pd

import awswrangler as wr

from ._utils import dt, ts

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_to_parquet_projection_integer(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 100, 200], "c3": [0, 1, 2]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c1", "c2", "c3"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "integer", "c2": "integer", "c3": "integer"},
        projection_ranges={"c1": "0,2", "c2": "0,200", "c3": "0,2"},
        projection_intervals={"c2": "100"},
        projection_digits={"c3": "1"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.sum()
    assert df.c2.sum() == df2.c2.sum()
    assert df.c3.sum() == df2.c3.sum()


def test_to_parquet_projection_enum(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [1, 2, 3], "c2": ["foo", "boo", "bar"]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "enum", "c2": "enum"},
        projection_values={"c1": "1,2,3", "c2": "foo,boo,bar"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.sum()


def test_to_parquet_projection_date(glue_database, glue_table, path):
    df = pd.DataFrame(
        {
            "c0": [0, 1, 2],
            "c1": [dt("2020-01-01"), dt("2020-01-02"), dt("2020-01-03")],
            "c2": [ts("2020-01-01 01:01:01.0"), ts("2020-01-01 01:01:02.0"), ts("2020-01-01 01:01:03.0")],
        }
    )
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "date", "c2": "date"},
        projection_ranges={"c1": "2020-01-01,2020-01-03", "c2": "2020-01-01 01:01:00,2020-01-01 01:01:03"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()


def test_to_parquet_projection_injected(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["foo", "boo", "bar"], "c2": ["0", "1", "2"]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "injected", "c2": "injected"},
    )
    df2 = wr.athena.read_sql_query(f"SELECT * FROM {glue_table} WHERE c1='foo' AND c2='0'", glue_database)
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 0


def test_to_parquet_storage_location(glue_database, glue_table, path):
    df1 = pd.DataFrame({"c0": [0], "c1": ["foo"], "c2": ["0"]})
    df2 = pd.DataFrame({"c0": [1], "c1": ["foo"], "c2": ["1"]})
    df3 = pd.DataFrame({"c0": [2], "c1": ["boo"], "c2": ["2"]})
    df4 = pd.DataFrame({"c0": [3], "c1": ["boo"], "c2": ["3"]})

    wr.s3.to_parquet(df=df1, path=f"{path}foo/0/file0.parquet")
    wr.s3.to_parquet(df=df2, path=f"{path}foo/1/file1.parquet")
    wr.s3.to_parquet(df=df3, path=f"{path}boo/2/file2.parquet")
    wr.s3.to_parquet(df=df4, path=f"{path}boo/3/file3.parquet")
    column_types, partitions_types = wr.catalog.extract_athena_types(df1)
    wr.catalog.create_parquet_table(
        database=glue_database,
        table=glue_table,
        path=path,
        columns_types=column_types,
        projection_enabled=True,
        projection_types={"c1": "injected", "c2": "injected"},
        projection_storage_location_template=f"{path}${{c1}}/${{c2}}",
    )

    df5 = wr.athena.read_sql_query(f"SELECT * FROM {glue_table} WHERE c1='foo' AND c2='0'", glue_database)
    pd.testing.assert_frame_equal(df1, df5, check_dtype=False)
