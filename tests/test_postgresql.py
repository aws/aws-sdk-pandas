import logging
from decimal import Decimal

import pandas as pd
import pg8000
import pyarrow as pa

import awswrangler as wr

from ._utils import ensure_data_types, get_df

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_connection():
    wr.postgresql.connect("aws-data-wrangler-postgresql", timeout=10).close()


def test_read_sql_query_simple(databases_parameters):
    con = pg8000.connect(
        host=databases_parameters["postgresql"]["host"],
        port=int(databases_parameters["postgresql"]["port"]),
        database=databases_parameters["postgresql"]["database"],
        user=databases_parameters["user"],
        password=databases_parameters["password"],
    )
    df = wr.postgresql.read_sql_query("SELECT 1", con=con)
    con.close()
    assert df.shape == (1, 1)


def test_to_sql_simple(postgresql_table):
    con = wr.postgresql.connect("aws-data-wrangler-postgresql")
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.postgresql.to_sql(df, con, postgresql_table, "public", "overwrite", True)
    con.close()


def test_sql_types(postgresql_table):
    table = postgresql_table
    df = get_df()
    df.drop(["binary"], axis=1, inplace=True)
    con = wr.postgresql.connect("aws-data-wrangler-postgresql")
    wr.postgresql.to_sql(
        df=df,
        con=con,
        table=table,
        schema="public",
        mode="overwrite",
        index=True,
        dtype={"iint32": "INTEGER"},
    )
    df = wr.postgresql.read_sql_query(f"SELECT * FROM public.{table}", con)
    ensure_data_types(df, has_list=False)
    dfs = wr.postgresql.read_sql_query(
        sql=f"SELECT * FROM public.{table}",
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


def test_to_sql_cast(postgresql_table):
    table = postgresql_table
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
    con = wr.postgresql.connect(connection="aws-data-wrangler-postgresql")
    wr.postgresql.to_sql(
        df=df,
        con=con,
        table=table,
        schema="public",
        mode="overwrite",
        index=False,
        dtype={"col": "VARCHAR(1024)"},
    )
    df2 = wr.postgresql.read_sql_query(sql=f"SELECT * FROM public.{table}", con=con)
    assert df.equals(df2)
    con.close()


def test_null(postgresql_table):
    table = postgresql_table
    con = wr.postgresql.connect(connection="aws-data-wrangler-postgresql")
    df = pd.DataFrame({"id": [1, 2, 3], "nothing": [None, None, None]})
    wr.postgresql.to_sql(
        df=df,
        con=con,
        table=table,
        schema="public",
        mode="overwrite",
        index=False,
        dtype={"nothing": "INTEGER"},
    )
    wr.postgresql.to_sql(
        df=df,
        con=con,
        table=table,
        schema="public",
        mode="append",
        index=False,
    )
    df2 = wr.postgresql.read_sql_table(table=table, schema="public", con=con)
    df["id"] = df["id"].astype("Int64")
    assert pd.concat(objs=[df, df], ignore_index=True).equals(df2)
    con.close()


def test_decimal_cast(postgresql_table):
    df = pd.DataFrame(
        {
            "col0": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col1": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        }
    )
    con = wr.postgresql.connect(connection="aws-data-wrangler-postgresql")
    wr.postgresql.to_sql(df, con, postgresql_table, "public")
    df2 = wr.postgresql.read_sql_table(
        schema="public", table=postgresql_table, con=con, dtype={"col0": "float32", "col1": "float64", "col2": "Int64"}
    )
    assert df2.dtypes.to_list() == ["float32", "float64", "Int64"]
    assert 3.88 <= df2.col0.sum() <= 3.89
    assert 3.88 <= df2.col1.sum() <= 3.89
    assert df2.col2.sum() == 2
    con.close()


def test_read_retry():
    con = wr.postgresql.connect(connection="aws-data-wrangler-postgresql")
    try:
        wr.postgresql.read_sql_query("ERROR", con)
    except:  # noqa
        pass
    df = wr.postgresql.read_sql_query("SELECT 1", con)
    assert df.shape == (1, 1)
    con.close()


def test_table_name():
    df = pd.DataFrame({"col0": [1]})
    con = wr.postgresql.connect(connection="aws-data-wrangler-postgresql")
    wr.postgresql.to_sql(df, con, "Test Name", "public", mode="overwrite")
    df = wr.postgresql.read_sql_table(schema="public", con=con, table="Test Name")
    assert df.shape == (1, 1)
    with con.cursor() as cursor:
        cursor.execute('DROP TABLE "Test Name"')
    con.commit()
    con.close()
