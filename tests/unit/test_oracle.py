from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import boto3
import oracledb
import pyarrow as pa
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import assert_pandas_equals, ensure_data_types, get_df, pandas_equals

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.fixture(scope="function")
def oracle_con() -> "oracledb.Connection":
    with wr.oracle.connect("aws-sdk-pandas-oracle") as con:
        yield con


def test_connection() -> None:
    with wr.oracle.connect("aws-sdk-pandas-oracle", call_timeout=10000):
        pass


def test_read_sql_query_simple(databases_parameters: dict[str, Any], oracle_con: "oracledb.Connection") -> None:
    df = wr.oracle.read_sql_query("SELECT 1 FROM DUAL", con=oracle_con)
    assert df.shape == (1, 1)


def test_to_sql_simple(oracle_table: str, oracle_con: "oracledb.Connection") -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.oracle.to_sql(df, oracle_con, oracle_table, "TEST", "overwrite", True)


@pytest.mark.parametrize("first_to_sql_mode", ["append", "overwrite", "upsert"])
def test_to_sql_upsert(oracle_table: str, oracle_con: "oracledb.Connection", first_to_sql_mode: str) -> None:
    schema = "TEST"

    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"], "c2": [0] * 3})
    wr.oracle.to_sql(
        df=df,
        con=oracle_con,
        table=oracle_table,
        schema=schema,
        mode=first_to_sql_mode,
        use_column_names=True,
        primary_keys=["c0"],
    )

    df2 = pd.DataFrame({"c0": [2, 4], "c1": ["baz", "foo"], "c2": [0] * 2})
    wr.oracle.to_sql(
        df=df2,
        con=oracle_con,
        table=oracle_table,
        schema=schema,
        mode="upsert",
        use_column_names=True,
        primary_keys=["c0"],
    )

    df_expected = pd.DataFrame({"c0": [1, 2, 3, 4], "c1": ["foo", "baz", "bar", "foo"], "c2": [0] * 4})
    df_expected["c0"] = df_expected["c0"].astype("Int64")
    df_expected["c1"] = df_expected["c1"].astype("string")
    df_expected["c2"] = df_expected["c2"].astype("Int64")

    df_actual = wr.oracle.read_sql_table(table=oracle_table, con=oracle_con)

    assert_pandas_equals(df_expected, df_actual)


def test_sql_types(oracle_table: str, oracle_con: "oracledb.Connection") -> None:
    table = oracle_table
    df = get_df()
    wr.oracle.to_sql(
        df=df,
        con=oracle_con,
        table=table,
        schema="TEST",
        mode="overwrite",
        index=True,
        dtype={"iint32": "NUMBER(10)", "decimal": "NUMBER(3,2)"},
    )
    df = wr.oracle.read_sql_query(f'SELECT * FROM "TEST"."{table}"', oracle_con)
    # ensure_data_types(df, has_list=False)
    dfs = wr.oracle.read_sql_query(
        sql=f'SELECT * FROM "TEST"."{table}"',
        con=oracle_con,
        chunksize=1,
        dtype={
            "iint8": pa.int8(),
            "iint16": pa.int16(),
            "iint32": pa.int32(),
            "iint64": pa.int64(),
            "float": pa.float32(),
            "ddouble": pa.float64(),
            "decimal": pa.decimal128(3, 2),
            "string_object": pa.string(),
            "string": pa.string(),
            "date": pa.date32(),
            "timestamp": pa.timestamp(unit="ns"),
            "binary": pa.binary(),
            "category": pa.float64(),
        },
    )
    for df in dfs:
        ensure_data_types(df, has_list=False)


def test_to_sql_cast(oracle_table: str, oracle_con: "oracledb.Connection") -> None:
    table = oracle_table
    df = pd.DataFrame(
        {
            "col": [
                "".join([str(i)[-1] for i in range(1_024)]),
                "".join([str(i)[-1] for i in range(1_024)]),
                "".join([str(i)[-1] for i in range(1_024)]),
            ]
        },
        dtype="string",
    )
    wr.oracle.to_sql(
        df=df,
        con=oracle_con,
        table=table,
        schema="TEST",
        mode="overwrite",
        index=False,
        dtype={"col": "VARCHAR(1024)"},
    )
    df2 = wr.oracle.read_sql_query(sql=f'SELECT * FROM "TEST"."{table}"', con=oracle_con)
    assert df.equals(df2)


def test_null(oracle_table: str, oracle_con: "oracledb.Connection") -> None:
    table = oracle_table
    df = pd.DataFrame({"id": [1, 2, 3], "nothing": [None, None, None]})
    wr.oracle.to_sql(
        df=df,
        con=oracle_con,
        table=table,
        schema="TEST",
        mode="overwrite",
        index=False,
        dtype={"nothing": "NUMBER(10)"},
    )
    wr.oracle.to_sql(
        df=df,
        con=oracle_con,
        table=table,
        schema="TEST",
        mode="append",
        index=False,
    )

    df2 = wr.oracle.read_sql_table(table=table, schema="TEST", con=oracle_con)
    df["id"] = df["id"].astype("Int64")
    assert pandas_equals(pd.concat(objs=[df, df], ignore_index=True), df2)


def test_decimal_cast(oracle_table: str, oracle_con: "oracledb.Connection") -> None:
    table = oracle_table
    df = pd.DataFrame(
        {
            "col0": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col1": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        }
    )
    wr.oracle.to_sql(df, oracle_con, table, "TEST")
    df2 = wr.oracle.read_sql_table(
        schema="TEST", table=table, con=oracle_con, dtype={"col0": "float32", "col1": "float64", "col2": "Int64"}
    )
    assert df2.dtypes.to_list() == ["float32", "float64", "Int64"]
    assert 3.88 <= df2.col0.sum() <= 3.89
    assert 3.88 <= df2.col1.sum() <= 3.89
    assert df2.col2.sum() == 2


def test_read_retry(oracle_con: "oracledb.Connection") -> None:
    try:
        wr.oracle.read_sql_query("ERROR", oracle_con)
    except:  # noqa
        pass
    df = wr.oracle.read_sql_query("SELECT 1 FROM DUAL", oracle_con)
    assert df.shape == (1, 1)


def test_table_name(oracle_con: "oracledb.Connection") -> None:
    df = pd.DataFrame({"col0": [1]})
    wr.oracle.to_sql(df, oracle_con, "Test Name", "TEST", mode="overwrite")
    df = wr.oracle.read_sql_table(schema="TEST", con=oracle_con, table="Test Name")
    assert df.shape == (1, 1)
    with oracle_con.cursor() as cursor:
        cursor.execute('DROP TABLE "Test Name"')
    oracle_con.commit()


@pytest.mark.parametrize("dbname", [None, "ORCL"])
def test_connect_secret_manager(dbname: str) -> None:
    try:
        con = wr.oracle.connect(secret_id="aws-sdk-pandas/oracle", dbname=dbname)
        df = wr.oracle.read_sql_query("SELECT 1 FROM DUAL", con=con)
        assert df.shape == (1, 1)
    except boto3.client("secretsmanager").exceptions.ResourceNotFoundException:
        pass  # Workaround for secretmanager inconsistance


def test_insert_with_column_names(oracle_table: str, oracle_con: "oracledb.Connection") -> None:
    create_table_sql = (
        f'CREATE TABLE "TEST"."{oracle_table}" '
        '("c0" varchar2(100) NULL,'
        '"c1" NUMBER(10) DEFAULT 42 NULL,'
        '"c2" NUMBER(10) NOT NULL)'
    )
    with oracle_con.cursor() as cursor:
        cursor.execute(create_table_sql)
        oracle_con.commit()

    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    with pytest.raises(oracledb.Error):
        wr.oracle.to_sql(
            df=df, con=oracle_con, schema="TEST", table=oracle_table, mode="append", use_column_names=False
        )

    wr.oracle.to_sql(df=df, con=oracle_con, schema="TEST", table=oracle_table, mode="append", use_column_names=True)

    df2 = wr.oracle.read_sql_table(con=oracle_con, schema="TEST", table=oracle_table)

    df["c1"] = 42
    df["c0"] = df["c0"].astype("string")
    df["c1"] = df["c1"].astype("Int64")
    df["c2"] = df["c2"].astype("Int64")
    df = df.reindex(sorted(df.columns), axis=1)
    assert df.equals(df2)


@pytest.mark.parametrize("chunksize", [1, 10, 500])
def test_dfs_are_equal_for_different_chunksizes(
    oracle_table: str, oracle_con: "oracledb.Connection", chunksize: int
) -> None:
    df = pd.DataFrame({"c0": [i for i in range(64)], "c1": ["foo" for _ in range(64)]})
    wr.oracle.to_sql(df=df, con=oracle_con, schema="TEST", table=oracle_table, chunksize=chunksize)
    df2 = wr.oracle.read_sql_table(con=oracle_con, schema="TEST", table=oracle_table)
    df["c0"] = df["c0"].astype("Int64")
    df["c1"] = df["c1"].astype("string")

    assert df.equals(df2)
