import logging
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pymysql
import pytest

import awswrangler as wr

from ._utils import ensure_data_types, get_df

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


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


def test_to_sql_simple(mysql_table):
    con = wr.mysql.connect("aws-data-wrangler-mysql")
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.mysql.to_sql(df, con, mysql_table, "test", "overwrite", True)
    con.close()


def test_sql_types(mysql_table):
    table = mysql_table
    df = get_df()
    df.drop(["binary"], axis=1, inplace=True)
    con = wr.mysql.connect("aws-data-wrangler-mysql")
    wr.mysql.to_sql(
        df=df,
        con=con,
        table=table,
        schema="test",
        mode="overwrite",
        index=True,
        dtype={"iint32": "INTEGER"},
    )
    df = wr.mysql.read_sql_query(f"SELECT * FROM test.{table}", con)
    ensure_data_types(df, has_list=False)
    dfs = wr.mysql.read_sql_query(
        sql=f"SELECT * FROM test.{table}",
        con=con,
        chunksize=1,
        dtype={
            "iint8": pa.int8(),
            "iint16": pa.int16(),
            "iint32": pa.int32(),
            "iint64": pa.int64(),
            "float": pa.float32(),
            "double": pa.float64(),
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


def test_to_sql_cast(mysql_table):
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
    con = wr.mysql.connect(connection="aws-data-wrangler-mysql")
    wr.mysql.to_sql(
        df=df,
        con=con,
        table=table,
        schema="test",
        mode="overwrite",
        index=False,
        dtype={"col": "VARCHAR(1024)"},
    )
    df2 = wr.mysql.read_sql_query(sql=f"SELECT * FROM test.{table}", con=con)
    assert df.equals(df2)
    con.close()


def test_null(mysql_table):
    table = mysql_table
    con = wr.mysql.connect(connection="aws-data-wrangler-mysql")
    df = pd.DataFrame({"id": [1, 2, 3], "nothing": [None, None, None]})
    wr.mysql.to_sql(
        df=df,
        con=con,
        table=table,
        schema="test",
        mode="overwrite",
        index=False,
        dtype={"nothing": "INTEGER"},
    )
    wr.mysql.to_sql(
        df=df,
        con=con,
        table=table,
        schema="test",
        mode="append",
        index=False,
    )
    df2 = wr.mysql.read_sql_table(table=table, schema="test", con=con)
    df["id"] = df["id"].astype("Int64")
    assert pd.concat(objs=[df, df], ignore_index=True).equals(df2)
    con.close()


def test_decimal_cast(mysql_table):
    df = pd.DataFrame(
        {
            "col0": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col1": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        }
    )
    con = wr.mysql.connect(connection="aws-data-wrangler-mysql")
    wr.mysql.to_sql(df, con, mysql_table, "test")
    df2 = wr.mysql.read_sql_table(
        schema="test", table=mysql_table, con=con, dtype={"col0": "float32", "col1": "float64", "col2": "Int64"}
    )
    assert df2.dtypes.to_list() == ["float32", "float64", "Int64"]
    assert 3.88 <= df2.col0.sum() <= 3.89
    assert 3.88 <= df2.col1.sum() <= 3.89
    assert df2.col2.sum() == 2
    con.close()


def test_read_retry():
    con = wr.mysql.connect(connection="aws-data-wrangler-mysql")
    try:
        wr.mysql.read_sql_query("ERROR", con)
    except:  # noqa
        pass
    df = wr.mysql.read_sql_query("SELECT 1", con)
    assert df.shape == (1, 1)
    con.close()


def test_table_name():
    df = pd.DataFrame({"col0": [1]})
    con = wr.mysql.connect(connection="aws-data-wrangler-mysql")
    wr.mysql.to_sql(df, con, "Test Name", "test", mode="overwrite")
    df = wr.mysql.read_sql_table(con=con, schema="test", table="Test Name")
    assert df.shape == (1, 1)
    with con.cursor() as cursor:
        cursor.execute("DROP TABLE `Test Name`")
    con.commit()
    con.close()


@pytest.mark.parametrize("dbname", [None, "test"])
def test_connect_secret_manager(dbname):
    con = wr.mysql.connect(secret_id="aws-data-wrangler/mysql", dbname=dbname)
    df = wr.mysql.read_sql_query("SELECT 1", con=con)
    con.close()
    assert df.shape == (1, 1)
