import logging
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pymysql
import pytest

import awswrangler as wr

from ._utils import ensure_data_types, get_df

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="function")
def mysql_con():
    con = wr.mysql.connect("aws-data-wrangler-mysql")
    yield con
    con.close()


def test_connection():
    wr.mysql.connect("aws-data-wrangler-mysql", connect_timeout=10).close()


def test_read_sql_query_simple(databases_parameters):
    con = pymysql.connect(
        host=databases_parameters["mysql"]["host"],
        port=int(databases_parameters["mysql"]["port"]),
        database=databases_parameters["mysql"]["database"],
        user=databases_parameters["user"],
        password=databases_parameters["password"],
    )
    df = wr.mysql.read_sql_query("SELECT 1", con=con)
    con.close()
    assert df.shape == (1, 1)


def test_to_sql_simple(mysql_table, mysql_con):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.mysql.to_sql(df, mysql_con, mysql_table, "test", "overwrite", True)


def test_sql_types(mysql_table, mysql_con):
    table = mysql_table
    df = get_df()
    df.drop(["binary"], axis=1, inplace=True)
    wr.mysql.to_sql(
        df=df,
        con=mysql_con,
        table=table,
        schema="test",
        mode="overwrite",
        index=True,
        dtype={"iint32": "INTEGER"},
    )
    df = wr.mysql.read_sql_query(f"SELECT * FROM test.{table}", mysql_con)
    ensure_data_types(df, has_list=False)
    dfs = wr.mysql.read_sql_query(
        sql=f"SELECT * FROM test.{table}",
        con=mysql_con,
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


def test_to_sql_cast(mysql_table, mysql_con):
    table = mysql_table
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
    wr.mysql.to_sql(
        df=df,
        con=mysql_con,
        table=table,
        schema="test",
        mode="overwrite",
        index=False,
        dtype={"col": "VARCHAR(1024)"},
    )
    df2 = wr.mysql.read_sql_query(sql=f"SELECT * FROM test.{table}", con=mysql_con)
    assert df.equals(df2)


def test_null(mysql_table, mysql_con):
    table = mysql_table
    df = pd.DataFrame({"id": [1, 2, 3], "nothing": [None, None, None]})
    wr.mysql.to_sql(
        df=df,
        con=mysql_con,
        table=table,
        schema="test",
        mode="overwrite",
        index=False,
        dtype={"nothing": "INTEGER"},
    )
    wr.mysql.to_sql(
        df=df,
        con=mysql_con,
        table=table,
        schema="test",
        mode="append",
        index=False,
    )
    df2 = wr.mysql.read_sql_table(table=table, schema="test", con=mysql_con)
    df["id"] = df["id"].astype("Int64")
    assert pd.concat(objs=[df, df], ignore_index=True).equals(df2)


def test_decimal_cast(mysql_table, mysql_con):
    df = pd.DataFrame(
        {
            "col0": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col1": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        }
    )
    wr.mysql.to_sql(df, mysql_con, mysql_table, "test")
    df2 = wr.mysql.read_sql_table(
        schema="test", table=mysql_table, con=mysql_con, dtype={"col0": "float32", "col1": "float64", "col2": "Int64"}
    )
    assert df2.dtypes.to_list() == ["float32", "float64", "Int64"]
    assert 3.88 <= df2.col0.sum() <= 3.89
    assert 3.88 <= df2.col1.sum() <= 3.89
    assert df2.col2.sum() == 2


def test_read_retry(mysql_con):
    try:
        wr.mysql.read_sql_query("ERROR", mysql_con)
    except:  # noqa
        pass
    df = wr.mysql.read_sql_query("SELECT 1", mysql_con)
    assert df.shape == (1, 1)


def test_table_name(mysql_con):
    df = pd.DataFrame({"col0": [1]})
    wr.mysql.to_sql(df, mysql_con, "Test Name", "test", mode="overwrite")
    df = wr.mysql.read_sql_table(con=mysql_con, schema="test", table="Test Name")
    assert df.shape == (1, 1)
    with mysql_con.cursor() as cursor:
        cursor.execute("DROP TABLE `Test Name`")
    mysql_con.commit()


@pytest.mark.parametrize("dbname", [None, "test"])
def test_connect_secret_manager(dbname):
    con = wr.mysql.connect(secret_id="aws-data-wrangler/mysql", dbname=dbname)
    df = wr.mysql.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


def test_insert_with_column_names(mysql_table, mysql_con):
    create_table_sql = (
        f"CREATE TABLE test.{mysql_table} " "(c0 varchar(100) NULL, " "c1 INT DEFAULT 42 NULL, " "c2 INT NOT NULL);"
    )
    with mysql_con.cursor() as cursor:
        cursor.execute(create_table_sql)
        mysql_con.commit()

    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    with pytest.raises(pymysql.err.OperationalError):
        wr.mysql.to_sql(df=df, con=mysql_con, schema="test", table=mysql_table, mode="append", use_column_names=False)

    wr.mysql.to_sql(df=df, con=mysql_con, schema="test", table=mysql_table, mode="append", use_column_names=True)

    df2 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)

    df["c1"] = 42
    df["c0"] = df["c0"].astype("string")
    df["c1"] = df["c1"].astype("Int64")
    df["c2"] = df["c2"].astype("Int64")
    df = df.reindex(sorted(df.columns), axis=1)
    assert df.equals(df2)


def test_upsert_distinct(mysql_table, mysql_con):
    create_table_sql = (
        f"CREATE TABLE test.{mysql_table} " "(c0 varchar(100) NULL, " "c1 INT DEFAULT 42 NULL, " "c2 INT NOT NULL);"
    )
    with mysql_con.cursor() as cursor:
        cursor.execute(create_table_sql)
        mysql_con.commit()

    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_distinct", use_column_names=True
    )
    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_distinct", use_column_names=True
    )
    df2 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df2) == 2)

    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_distinct", use_column_names=True
    )
    df3 = pd.DataFrame({"c0": ["baz", "bar"], "c2": [3, 2]})
    wr.mysql.to_sql(
        df=df3, con=mysql_con, schema="test", table=mysql_table, mode="upsert_distinct", use_column_names=True
    )
    df4 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df4) == 3)

    df5 = pd.DataFrame({"c0": ["foo", "bar"], "c2": [4, 5]})
    wr.mysql.to_sql(
        df=df5, con=mysql_con, schema="test", table=mysql_table, mode="upsert_distinct", use_column_names=True
    )
    df6 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df6) == 5)
    assert bool(len(df6.loc[(df6["c0"] == "foo")]) == 2)
    assert bool(len(df6.loc[(df6["c0"] == "foo") & (df6["c2"] == 4)]) == 1)
    assert bool(len(df6.loc[(df6["c0"] == "bar") & (df6["c2"] == 5)]) == 1)


def test_upsert_duplicate_key(mysql_table, mysql_con):
    create_table_sql = (
        f"CREATE TABLE test.{mysql_table} "
        "(c0 varchar(100) PRIMARY KEY, "
        "c1 INT DEFAULT 42 NULL, "
        "c2 INT NOT NULL);"
    )
    with mysql_con.cursor() as cursor:
        cursor.execute(create_table_sql)
        mysql_con.commit()

    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_duplicate_key", use_column_names=True
    )
    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_duplicate_key", use_column_names=True
    )
    df2 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df2) == 2)

    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_duplicate_key", use_column_names=True
    )
    df3 = pd.DataFrame({"c0": ["baz", "bar"], "c2": [3, 2]})
    wr.mysql.to_sql(
        df=df3, con=mysql_con, schema="test", table=mysql_table, mode="upsert_duplicate_key", use_column_names=True
    )
    df4 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df4) == 3)

    df5 = pd.DataFrame({"c0": ["foo", "bar"], "c2": [4, 5]})
    wr.mysql.to_sql(
        df=df5, con=mysql_con, schema="test", table=mysql_table, mode="upsert_duplicate_key", use_column_names=True
    )

    df6 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df6) == 3)
    assert bool(len(df6.loc[(df6["c0"] == "foo") & (df6["c2"] == 4)]) == 1)
    assert bool(len(df6.loc[(df6["c0"] == "bar") & (df6["c2"] == 5)]) == 1)


def test_upsert_replace(mysql_table, mysql_con):
    create_table_sql = (
        f"CREATE TABLE test.{mysql_table} "
        "(c0 varchar(100) PRIMARY KEY, "
        "c1 INT DEFAULT 42 NULL, "
        "c2 INT NOT NULL);"
    )
    with mysql_con.cursor() as cursor:
        cursor.execute(create_table_sql)
        mysql_con.commit()

    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_replace_into", use_column_names=True
    )
    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_replace_into", use_column_names=True
    )
    df2 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df2) == 2)

    wr.mysql.to_sql(
        df=df, con=mysql_con, schema="test", table=mysql_table, mode="upsert_replace_into", use_column_names=True
    )
    df3 = pd.DataFrame({"c0": ["baz", "bar"], "c2": [3, 2]})
    wr.mysql.to_sql(
        df=df3, con=mysql_con, schema="test", table=mysql_table, mode="upsert_replace_into", use_column_names=True
    )
    df4 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df4) == 3)

    df5 = pd.DataFrame({"c0": ["foo", "bar"], "c2": [4, 5]})
    wr.mysql.to_sql(
        df=df5, con=mysql_con, schema="test", table=mysql_table, mode="upsert_replace_into", use_column_names=True
    )

    df6 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)
    assert bool(len(df6) == 3)
    assert bool(len(df6.loc[(df6["c0"] == "foo") & (df6["c2"] == 4)]) == 1)
    assert bool(len(df6.loc[(df6["c0"] == "bar") & (df6["c2"] == 5)]) == 1)


@pytest.mark.parametrize("chunksize", [1, 10, 500])
def test_dfs_are_equal_for_different_chunksizes(mysql_table, mysql_con, chunksize):
    df = pd.DataFrame({"c0": [i for i in range(64)], "c1": ["foo" for _ in range(64)]})
    wr.mysql.to_sql(df=df, con=mysql_con, schema="test", table=mysql_table, chunksize=chunksize)

    df2 = wr.mysql.read_sql_table(con=mysql_con, schema="test", table=mysql_table)

    df["c0"] = df["c0"].astype("Int64")
    df["c1"] = df["c1"].astype("string")

    assert df.equals(df2)
