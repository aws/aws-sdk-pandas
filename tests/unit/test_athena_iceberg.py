from __future__ import annotations

import datetime
import logging
from typing import Any

import numpy as np
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import (
    assert_pandas_equals,
    pandas_equals,
    ts,
)

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.mark.parametrize("partition_cols", [None, ["name"], ["name", "day(ts)"]])
@pytest.mark.parametrize(
    "additional_table_properties",
    [None, {"write_target_data_file_size_bytes": 536870912, "optimize_rewrite_delete_file_threshold": 10}],
)
def test_athena_to_iceberg(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
    partition_cols: list[str] | None,
    additional_table_properties: dict[str, Any] | None,
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "ts": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:00:00.0")],
        }
    )
    df["id"] = df["id"].astype("Int64")  # Cast as nullable int64 type
    df["name"] = df["name"].astype("string")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=partition_cols,
        additional_table_properties=additional_table_properties,
    )

    df_out = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}" ORDER BY id',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert_pandas_equals(df, df_out)


@pytest.mark.parametrize("partition_cols", [None, ["name"]])
def test_athena_to_iceberg_append(
    path: str,
    path2: str,
    path3: str,
    glue_database: str,
    glue_table: str,
    partition_cols: list[str] | None,
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["a", "b", "c", "a", "c"],
        }
    )
    df["id"] = df["id"].astype("Int64")  # Cast as nullable int64 type
    df["name"] = df["name"].astype("string")

    split_index = 4

    wr.athena.to_iceberg(
        df=df.iloc[:split_index],
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=partition_cols,
        keep_files=False,
    )

    wr.athena.to_iceberg(
        df=df.iloc[split_index:],
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=partition_cols,
        keep_files=False,
        mode="append",
        s3_output=path3,
    )

    df_actual = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}" ORDER BY id',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert_pandas_equals(df, df_actual)


@pytest.mark.parametrize("partition_cols", [None, ["name"]])
def test_athena_to_iceberg_overwrite(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
    partition_cols: list[str] | None,
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["a", "b", "c", "a", "c"],
        }
    )
    df["id"] = df["id"].astype("Int64")  # Cast as nullable int64 type
    df["name"] = df["name"].astype("string")

    split_index = 4

    wr.athena.to_iceberg(
        df=df.iloc[:split_index],
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=partition_cols,
        keep_files=False,
    )

    wr.athena.to_iceberg(
        df=df.iloc[split_index:],
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=partition_cols,
        keep_files=False,
        mode="overwrite",
    )

    df_actual = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}" ORDER BY id',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )
    df_expected = df.iloc[split_index:].reset_index(drop=True)

    assert_pandas_equals(df_expected, df_actual)


def test_athena_to_iceberg_overwrite_partitions(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    df1 = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "ts": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:15:00.0")],
        }
    )
    df1["id"] = df1["id"].astype("Int64")  # Cast as nullable int64 type
    df1["day"] = df1["ts"].dt.day.astype("string")

    wr.athena.to_iceberg(
        df=df1,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=["day"],
        keep_files=False,
    )

    df2 = pd.DataFrame(
        {
            "id": [4, 5],
            "ts": [ts("2020-01-03 12:30:00.0"), ts("2020-01-03 16:45:00.0")],
        }
    )
    df2["id"] = df2["id"].astype("Int64")  # Cast as nullable int64 type
    df2["day"] = df2["ts"].dt.day.astype("string")

    wr.athena.to_iceberg(
        df=df2,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=["day"],
        keep_files=False,
        mode="overwrite_partitions",
    )

    df_actual = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}" ORDER BY id',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    df_expected = pd.DataFrame(
        {
            "id": [1, 2, 4, 5],
            "ts": [
                ts("2020-01-01 00:00:00.0"),
                ts("2020-01-02 00:00:01.0"),
                ts("2020-01-03 12:30:00.0"),
                ts("2020-01-03 16:45:00.0"),
            ],
        }
    )
    df_expected["id"] = df_expected["id"].astype("Int64")  # Cast as nullable int64 type
    df_expected["day"] = df_expected["ts"].dt.day.astype("string")

    assert_pandas_equals(df_expected, df_actual)


def test_athena_to_iceberg_overwrite_partitions_no_partition_error(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    df1 = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["foo", "bar", "baz"],
        }
    )

    wr.athena.to_iceberg(
        df=df1,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=["name"],
        keep_files=False,
    )

    df2 = pd.DataFrame(
        {
            "id": [4, 5],
            "name": ["foo", "bar"],
        }
    )

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.athena.to_iceberg(
            df=df2,
            database=glue_database,
            table=glue_table,
            table_location=path,
            temp_path=path2,
            partition_cols=["name"],
            merge_cols=["id"],
            keep_files=False,
            mode="overwrite_partitions",
        )


def test_athena_to_iceberg_overwrite_partitions_merge_cols_error(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    df1 = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "ts": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:15:00.0")],
        }
    )

    wr.athena.to_iceberg(
        df=df1,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    df2 = pd.DataFrame(
        {
            "id": [4, 5],
            "ts": [ts("2020-01-03 12:30:00.0"), ts("2020-01-03 16:45:00.0")],
        }
    )

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.athena.to_iceberg(
            df=df2,
            database=glue_database,
            table=glue_table,
            table_location=path,
            temp_path=path2,
            keep_files=False,
            mode="overwrite_partitions",
        )


def test_athena_to_iceberg_schema_evolution_add_columns(
    path: str, path2: str, glue_database: str, glue_table: str
) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    df["c2"] = [6, 7, 8]
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    column_types = wr.catalog.get_table_types(glue_database, glue_table)
    assert len(column_types) == len(df.columns)

    df_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )
    assert len(df_out) == len(df) * 2

    df["c3"] = [9, 10, 11]
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.to_iceberg(
            df=df,
            database=glue_database,
            table=glue_table,
            table_location=path,
            temp_path=path2,
            keep_files=False,
            schema_evolution=False,
            fill_missing_columns_in_df=False,
        )


def test_athena_to_iceberg_schema_evolution_modify_columns(
    path: str, path2: str, glue_database: str, glue_table: str
) -> None:
    # Version 1
    df = pd.DataFrame({"c1": pd.Series([1.0, 2.0], dtype="float32"), "c2": pd.Series([-1, -2], dtype="int32")})

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    df_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert len(df_out) == 2
    assert len(df_out.columns) == 2
    assert str(df_out["c1"].dtype).startswith("float32")
    assert str(df_out["c2"].dtype).startswith("Int32")

    # Version 2
    df2 = pd.DataFrame({"c1": pd.Series([3.0, 4.0], dtype="float64"), "c2": pd.Series([-3, -4], dtype="int64")})

    wr.athena.to_iceberg(
        df=df2,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    df2_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert len(df2_out) == 4
    assert len(df2_out.columns) == 2
    assert str(df2_out["c1"].dtype).startswith("float64")
    assert str(df2_out["c2"].dtype).startswith("Int64")


@pytest.mark.parametrize("schema_evolution", [False, True])
def test_athena_to_iceberg_schema_evolution_fill_missing_columns(
    path: str, path2: str, glue_database: str, glue_table: str, schema_evolution: bool
) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["foo", "bar", "baz"], "c2": [10, 11, 12]})
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    df = pd.DataFrame({"c0": [3, 4, 5]})
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=schema_evolution,
        fill_missing_columns_in_df=True,
    )

    df_actual = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )
    df_actual = df_actual.sort_values("c0").reset_index(drop=True)
    df_actual["c0"] = df_actual["c0"].astype("int64")

    df_expected = pd.DataFrame(
        {
            "c0": [0, 1, 2, 3, 4, 5],
            "c1": ["foo", "bar", "baz", None, None, None],
            "c2": [10, 11, 12, None, None, None],
        },
    )
    df_expected["c1"] = df_expected["c1"].astype("string")
    df_expected["c2"] = df_expected["c2"].astype("Int64")

    assert_pandas_equals(df_actual, df_expected)


def test_athena_to_iceberg_schema_evolution_drop_columns_error(
    path: str, path2: str, glue_database: str, glue_table: str
) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    df = pd.DataFrame({"c0": [6, 7, 8]})

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.athena.to_iceberg(
            df=df,
            database=glue_database,
            table=glue_table,
            table_location=path,
            temp_path=path2,
            keep_files=False,
            schema_evolution=True,
            fill_missing_columns_in_df=False,
        )


def test_to_iceberg_cast(path: str, path2: str, glue_table: str, glue_database: str) -> None:
    df = pd.DataFrame(
        {
            "c0": [
                datetime.date(4000, 1, 1),
                datetime.datetime(2000, 1, 1, 10),
                "2020",
                "2020-01",
                1,
                None,
                pd.NA,
                pd.NaT,
                np.nan,
                np.inf,
            ]
        }
    )
    df_expected = pd.DataFrame(
        {
            "c0": [
                datetime.date(1970, 1, 1),
                datetime.date(2000, 1, 1),
                datetime.date(2020, 1, 1),
                datetime.date(2020, 1, 1),
                datetime.date(4000, 1, 1),
                None,
                None,
                None,
                None,
                None,
            ]
        }
    )
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        dtype={"c0": "date"},
    )
    df2 = wr.athena.read_sql_table(database=glue_database, table=glue_table, ctas_approach=False)
    assert pandas_equals(df_expected, df2.sort_values("c0").reset_index(drop=True))


def test_athena_to_iceberg_with_hyphenated_table_name(
    path: str, path2: str, glue_database: str, glue_table_with_hyphenated_name: str
) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3, 4], "c1": ["foo", "bar", "baz", "boo"]})
    df["c0"] = df["c0"].astype("int")
    df["c1"] = df["c1"].astype("string")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table_with_hyphenated_name,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    df_out = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table_with_hyphenated_name}"',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert len(df) == len(df_out)
    assert len(df.columns) == len(df_out.columns)


def test_athena_to_iceberg_column_comments(path: str, path2: str, glue_database: str, glue_table: str) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    column_comments = {
        "c0": "comment 0",
        "c1": "comment 1",
    }
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        glue_table_settings={
            "columns_comments": column_comments,
        },
    )

    column_comments_actual = wr.catalog.get_columns_comments(glue_database, glue_table)

    assert column_comments_actual == column_comments


def test_athena_to_iceberg_merge_into(path: str, path2: str, glue_database: str, glue_table: str) -> None:
    df = pd.DataFrame({"title": ["Dune", "Fargo"], "year": ["1984", "1996"], "gross": [35_000_000, 60_000_000]})
    df["title"] = df["title"].astype("string")
    df["year"] = df["year"].astype("string")
    df["gross"] = df["gross"].astype("Int64")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    # Perform MERGE INTO
    df2 = pd.DataFrame({"title": ["Dune", "Fargo"], "year": ["2021", "1996"], "gross": [400_000_000, 60_000_001]})
    df2["title"] = df2["title"].astype("string")
    df2["year"] = df2["year"].astype("string")
    df2["gross"] = df2["gross"].astype("Int64")

    wr.athena.to_iceberg(
        df=df2,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        merge_cols=["title", "year"],
    )

    # Expected output
    df_expected = pd.DataFrame(
        {
            "title": ["Dune", "Fargo", "Dune"],
            "year": ["1984", "1996", "2021"],
            "gross": [35_000_000, 60_000_001, 400_000_000],
        }
    )
    df_expected["title"] = df_expected["title"].astype("string")
    df_expected["year"] = df_expected["year"].astype("string")
    df_expected["gross"] = df_expected["gross"].astype("Int64")

    df_out = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}" ORDER BY year',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert_pandas_equals(df_expected, df_out)


def test_athena_to_iceberg_merge_into_nulls(path: str, path2: str, glue_database: str, glue_table: str) -> None:
    df = pd.DataFrame(
        {
            "col1": ["a", "a", "a", np.nan],
            "col2": [0.0, 1.1, np.nan, 2.2],
            "action": ["insert", "insert", "insert", "insert"],
        }
    )
    df["col1"] = df["col1"].astype("string")
    df["col2"] = df["col2"].astype("float64")
    df["action"] = df["action"].astype("string")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    # Perform MERGE INTO
    df2 = pd.DataFrame(
        {
            "col1": ["a", "a", np.nan, "b"],
            "col2": [1.1, np.nan, 2.2, 3.3],
            "action": ["update", "update", "update", "insert"],
        }
    )
    df2["col1"] = df2["col1"].astype("string")
    df2["col2"] = df2["col2"].astype("float64")
    df2["action"] = df2["action"].astype("string")

    wr.athena.to_iceberg(
        df=df2,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        merge_cols=["col1", "col2"],
        merge_match_nulls=True,
    )

    # Expected output
    df_expected = pd.DataFrame(
        {
            "col1": ["a", "a", "a", np.nan, "b"],
            "col2": [0.0, 1.1, np.nan, 2.2, 3.3],
            "action": ["insert", "update", "update", "update", "insert"],
        }
    )
    df_expected["col1"] = df_expected["col1"].astype("string")
    df_expected["col2"] = df_expected["col2"].astype("float64")
    df_expected["action"] = df_expected["action"].astype("string")

    df_out = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}"',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert_pandas_equals(
        df_out.sort_values(df_out.columns.to_list()).reset_index(drop=True),
        df_expected.sort_values(df_expected.columns.to_list()).reset_index(drop=True),
    )


def test_athena_to_iceberg_merge_into_ignore(path: str, path2: str, glue_database: str, glue_table: str) -> None:
    df = pd.DataFrame({"title": ["Dune", "Fargo"], "year": ["1984", "1996"], "gross": [35_000_000, 60_000_000]})
    df["title"] = df["title"].astype("string")
    df["year"] = df["year"].astype("string")
    df["gross"] = df["gross"].astype("Int64")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    # Perform MERGE INTO
    df2 = pd.DataFrame({"title": ["Dune", "Fargo"], "year": ["2021", "1996"], "gross": [400_000_000, 60_000_001]})
    df2["title"] = df2["title"].astype("string")
    df2["year"] = df2["year"].astype("string")
    df2["gross"] = df2["gross"].astype("Int64")

    wr.athena.to_iceberg(
        df=df2,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        merge_cols=["title", "year"],
        merge_condition="ignore",
    )

    # Expected output
    df_expected = pd.DataFrame(
        {
            "title": ["Dune", "Fargo", "Dune"],
            "year": ["1984", "1996", "2021"],
            "gross": [35_000_000, 60_000_000, 400_000_000],
        }
    )
    df_expected["title"] = df_expected["title"].astype("string")
    df_expected["year"] = df_expected["year"].astype("string")
    df_expected["gross"] = df_expected["gross"].astype("Int64")

    df_out = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}" ORDER BY year',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert_pandas_equals(df_expected, df_out)


def test_athena_to_iceberg_cols_order(path: str, path2: str, glue_database: str, glue_table: str) -> None:
    kwargs = {
        "database": glue_database,
        "table": glue_table,
        "table_location": path,
        "temp_path": path2,
        "partition_cols": ["partition"],
        "schema_evolution": True,
        "keep_files": False,
    }

    df = pd.DataFrame(
        {
            "partition": [1, 1, 2, 2],
            "column1": ["X", "Y", "Z", "Z"],
            "column2": ["A", "B", "C", "D"],
        }
    )
    wr.athena.to_iceberg(df=df, mode="overwrite_partitions", **kwargs)

    # Adding a column
    df_new_col_last = pd.DataFrame(
        {
            "partition": [2, 2],
            "column1": ["Z", "Z"],
            "column2": ["C", "D"],
            "new_column": [True, False],
        }
    )
    wr.athena.to_iceberg(df=df_new_col_last, mode="overwrite_partitions", **kwargs)

    # Switching the order of columns
    df_new_col_not_last = pd.DataFrame(
        {
            "partition": [2, 2],
            "column1": ["Z", "Z"],
            "new_column": [True, False],
            "column2": ["C", "D"],
        }
    )
    wr.athena.to_iceberg(df=df_new_col_not_last, mode="overwrite_partitions", **kwargs)

    df_out = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}"',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )
    assert len(df) == len(df_out)
    assert len(df.columns) + 1 == len(df_out.columns)


def test_athena_to_iceberg_empty_df_error(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    with pytest.raises(wr.exceptions.EmptyDataFrame):
        wr.athena.to_iceberg(
            df=pd.DataFrame(),
            database=glue_database,
            table=glue_table,
            table_location=path,
            temp_path=path2,
        )


def test_athena_to_iceberg_no_table_location_error(
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.to_iceberg(
            df=pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]}),
            database=glue_database,
            table=glue_table,
            table_location=None,
            temp_path=path2,
        )


@pytest.mark.parametrize("partition_cols", [None, ["name"], ["name", "day(ts)"]])
def test_athena_delete_from_iceberg_table(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
    partition_cols: list[str] | None,
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "ts": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:00:00.0")],
        }
    )
    df["id"] = df["id"].astype("Int64")  # Cast as nullable int64 type
    df["name"] = df["name"].astype("string")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=partition_cols,
        keep_files=False,
    )

    wr.athena.delete_from_iceberg_table(
        df=pd.DataFrame({"id": [1, 2]}),
        database=glue_database,
        table=glue_table,
        temp_path=path2,
        merge_cols=["id"],
        keep_files=False,
    )

    df_actual = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}" ORDER BY id',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    df_expected = pd.DataFrame(
        {
            "id": [3],
            "name": ["c"],
            "ts": [ts("2020-01-03 00:00:00.0")],
        }
    )
    df_expected["id"] = df_expected["id"].astype("Int64")  # Cast as nullable int64 type
    df_expected["name"] = df_expected["name"].astype("string")

    assert_pandas_equals(df_expected, df_actual)


@pytest.mark.parametrize("partition_cols", [None, ["name"]])
def test_athena_delete_from_iceberg_table_no_merge_cols_error(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
    partition_cols: list[str] | None,
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "ts": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:00:00.0")],
        }
    )
    df["id"] = df["id"].astype("Int64")  # Cast as nullable int64 type
    df["name"] = df["name"].astype("string")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=partition_cols,
        keep_files=False,
    )

    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.delete_from_iceberg_table(
            df=pd.DataFrame({"id": [1, 2]}),
            database=glue_database,
            table=glue_table,
            temp_path=path2,
            merge_cols=[],
            keep_files=False,
        )


def test_athena_delete_from_iceberg_table_no_table_error(
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    with pytest.raises(wr.exceptions.InvalidTable):
        wr.athena.delete_from_iceberg_table(
            df=pd.DataFrame({"id": [1, 2]}),
            database=glue_database,
            table=glue_table,
            temp_path=path2,
            merge_cols=["id"],
            keep_files=False,
        )


def test_athena_delete_from_iceberg_empty_df_error(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    wr.athena.to_iceberg(
        df=pd.DataFrame({"id": [1, 2, 3]}),
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    with pytest.raises(wr.exceptions.EmptyDataFrame):
        wr.athena.delete_from_iceberg_table(
            df=pd.DataFrame(),
            database=glue_database,
            table=glue_table,
            merge_cols=["id"],
            temp_path=path2,
            keep_files=False,
        )


def test_athena_iceberg_use_partition_function(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "ts": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:00:00.0")],
        }
    )

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=["day(ts)"],
        keep_files=False,
    )

    df2 = pd.DataFrame(
        {
            "id": [4, 5],
            "name": ["d", "e"],
            "ts": [ts("2020-01-03 12:30:00.0"), ts("2020-01-03 16:45:00.0")],
        }
    )

    wr.athena.to_iceberg(
        df=df2,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=["day(ts)"],
        keep_files=False,
    )

    df_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert len(df_out) == len(df) + len(df2)
    assert len(df_out.columns) == len(df.columns)


def test_to_iceberg_uppercase_columns(
    path: str,
    path2: str,
    path3: str,
    glue_database: str,
    glue_table: str,
) -> None:
    df = pd.DataFrame(
        {
            "ID": [1, 2, 3, 4, 5],
            "TS": [
                ts("2020-01-01 00:00:00.0"),
                ts("2020-01-02 00:00:01.0"),
                ts("2020-01-03 00:00:00.0"),
                ts("2020-01-03 12:30:00.0"),
                ts("2020-01-03 16:45:00.0"),
            ],
        }
    )
    df["ID"] = df["ID"].astype("Int64")  # Cast as nullable int64 type

    split_index = 4

    wr.athena.to_iceberg(
        df=df.iloc[:split_index],
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    wr.athena.to_iceberg(
        df=df.iloc[split_index:],
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        s3_output=path3,
        keep_files=False,
        mode="append",
        schema_evolution=True,
    )

    df_output = wr.athena.read_sql_query(
        sql=f'SELECT ID, TS FROM "{glue_table}" ORDER BY ID',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert_pandas_equals(df, df_output)


def test_to_iceberg_fill_missing_columns_with_complex_types(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    df_with_col = pd.DataFrame(
        {
            "partition": [1, 1, 2, 2],
            "column2": ["A", "B", "C", "D"],
            "map_col": [{"s": "d"}, {"s": "h"}, {"i": "l"}, {}],
            "struct_col": [
                {"a": "val1", "b": {"c": "val21"}},
                {"a": "val1", "b": {"c": None}},
                {"a": "val1", "b": None},
                {},
            ],
        }
    )
    df_missing_col = pd.DataFrame(
        {
            "partition": [2, 2],
            "column2": ["Z", "X"],
        }
    )

    glue_dtypes = {
        "partition": "int",
        "column2": "string",
        "map_col": "map<string, string>",
        "struct_col": "struct<a: string, b: struct<c: string>>",
    }

    wr.athena.to_iceberg(
        df=df_with_col,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        dtype=glue_dtypes,
        mode="overwrite_partitions",
        partition_cols=["partition"],
    )

    wr.athena.to_iceberg(
        df=df_missing_col,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        dtype=glue_dtypes,
        mode="overwrite_partitions",
        partition_cols=["partition"],
        schema_evolution=True,
        fill_missing_columns_in_df=True,
    )


def test_athena_to_iceberg_alter_schema(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
) -> None:
    df = pd.DataFrame(
        {
            "id": pd.Series([1, 2, 3, 4, 5], dtype="Int64"),
            "name": pd.Series(["a", "b", "c", "d", "e"], dtype="string"),
        },
    ).reset_index(drop=True)

    split_index = 3

    wr.athena.to_iceberg(
        df=df[:split_index],
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        schema_evolution=True,
        keep_files=False,
    )

    wr.athena.start_query_execution(
        sql=f"ALTER TABLE {glue_table} CHANGE COLUMN id new_id bigint",
        database=glue_database,
        wait=True,
    )

    df = df.rename(columns={"id": "new_id"})

    wr.athena.to_iceberg(
        df=df[split_index:],
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        schema_evolution=True,
        keep_files=False,
    )

    df_actual = wr.athena.read_sql_query(
        sql=f"SELECT new_id, name FROM {glue_table} ORDER BY new_id",
        database=glue_database,
        ctas_approach=False,
    )

    assert_pandas_equals(df, df_actual)
