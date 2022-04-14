import logging
from decimal import Decimal

import boto3
import cx_Oracle
import pandas as pd
import pyarrow as pa
import pytest

import awswrangler as wr

from ._utils import ensure_data_types, get_df

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="function")
def oracle_con():
    con = wr.oracle.connect("aws-data-wrangler-oracle")
    yield con
    con.close()


def test_connection():
    wr.oracle.connect("aws-data-wrangler-oracle", call_timeout=10000).close()


def test_read_sql_query_simple(databases_parameters, oracle_con):
    df = wr.oracle.read_sql_query("SELECT 1 FROM DUAL", con=oracle_con)
    assert df.shape == (1, 1)


def test_to_sql_simple(oracle_table, oracle_con):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.oracle.to_sql(df, oracle_con, oracle_table, "TEST", "overwrite", True)


def test_sql_types(oracle_table, oracle_con):
    table = oracle_table
    df = get_df()
    df.drop(["binary"], axis=1, inplace=True)
    wr.oracle.to_sql(
        df=df,
        con=oracle_con,
        table=table,
        schema="TEST",
        mode="overwrite",
        index=True,
        dtype={"iint32": "INTEGER"},
    )
    df = wr.oracle.read_sql_query(f'SELECT * FROM "TEST"."{table}"', oracle_con)
    ensure_data_types(df, has_list=False)
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


def test_to_sql_cast(oracle_table, oracle_con):
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


def test_null(oracle_table, oracle_con):
    table = oracle_table
    df = pd.DataFrame({"id": [1, 2, 3], "nothing": [None, None, None]})
    wr.oracle.to_sql(
        df=df,
        con=oracle_con,
        table=table,
        schema="TEST",
        mode="overwrite",
        index=False,
        dtype={"nothing": "INTEGER"},
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
    assert pd.concat(objs=[df, df], ignore_index=True).equals(df2)


def test_decimal_cast(oracle_table, oracle_con):
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


def test_read_retry(oracle_con):
    try:
        wr.oracle.read_sql_query("ERROR", oracle_con)
    except:  # noqa
        pass
    df = wr.oracle.read_sql_query("SELECT 1 FROM DUAL", oracle_con)
    assert df.shape == (1, 1)


def test_table_name(oracle_con):
    df = pd.DataFrame({"col0": [1]})
    wr.oracle.to_sql(df, oracle_con, "Test Name", "TEST", mode="overwrite")
    df = wr.oracle.read_sql_table(schema="TEST", con=oracle_con, table="Test Name")
    assert df.shape == (1, 1)
    with oracle_con.cursor() as cursor:
        cursor.execute('DROP TABLE "Test Name"')
    oracle_con.commit()


@pytest.mark.parametrize("dbname", [None, "ORCL"])
def test_connect_secret_manager(dbname):
    try:
        con = wr.oracle.connect(secret_id="aws-data-wrangler/oracle", dbname=dbname)
        df = wr.oracle.read_sql_query("SELECT 1 FROM DUAL", con=con)
        assert df.shape == (1, 1)
    except boto3.client("secretsmanager").exceptions.ResourceNotFoundException:
        pass  # Workaround for secretmanager inconsistance


def test_insert_with_column_names(oracle_table, oracle_con):
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

    with pytest.raises(cx_Oracle.Error):
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
def test_dfs_are_equal_for_different_chunksizes(oracle_table, oracle_con, chunksize):
    df = pd.DataFrame({"c0": [i for i in range(64)], "c1": ["foo" for _ in range(64)]})
    wr.oracle.to_sql(df=df, con=oracle_con, schema="TEST", table=oracle_table, chunksize=chunksize)
    df2 = wr.oracle.read_sql_table(con=oracle_con, schema="TEST", table=oracle_table)
    df["c0"] = df["c0"].astype("Int64")
    df["c1"] = df["c1"].astype("string")

    assert df.equals(df2)
