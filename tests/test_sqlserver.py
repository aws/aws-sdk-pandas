import logging

import pandas as pd
import pyarrow as pa
import pymssql

import awswrangler as wr

from ._utils import ensure_data_types, get_df

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_connection():
    wr.sqlserver.connect("aws-data-wrangler-sqlserver", timeout=10).close()


def test_read_sql_query_simple(databases_parameters):
    con = pymssql.connect(
        host=databases_parameters["sqlserver"]["host"],
        port=int(databases_parameters["sqlserver"]["port"]),
        database=databases_parameters["sqlserver"]["database"],
        user=databases_parameters["user"],
        password=databases_parameters["password"],
    )
    df = wr.sqlserver.read_sql_query("SELECT 1", con=con)
    con.close()
    assert df.shape == (1, 1)


def test_to_sql_simple(sqlserver_table):
    con = wr.sqlserver.connect("aws-data-wrangler-sqlserver")
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.sqlserver.to_sql(df, con, sqlserver_table, "dbo", "overwrite", True)
    con.close()


def test_sql_types(sqlserver_table):
    table = sqlserver_table
    df = get_df()
    df.drop(["binary"], axis=1, inplace=True)
    con = wr.sqlserver.connect("aws-data-wrangler-sqlserver")
    wr.sqlserver.to_sql(
        df=df,
        con=con,
        table=table,
        schema="dbo",
        mode="overwrite",
        index=True,
        dtype={"iint32": "INTEGER"},
    )
    df = wr.sqlserver.read_sql_query(f"SELECT * FROM dbo.{table}", con)
    ensure_data_types(df, has_list=False)
    dfs = wr.sqlserver.read_sql_query(
        sql=f"SELECT * FROM dbo.{table}",
        con=con,
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
