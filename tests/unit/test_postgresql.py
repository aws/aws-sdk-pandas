import logging
from datetime import datetime
from decimal import Decimal
from typing import Iterator

import pg8000
import pyarrow as pa
import pytest
from packaging import version
from pg8000.dbapi import ProgrammingError

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import ensure_data_types, get_df, is_ray_modin, pandas_equals

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.fixture(scope="function")
def postgresql_con() -> Iterator[pg8000.Connection]:
    with wr.postgresql.connect("aws-sdk-pandas-postgresql") as con:
        yield con


def test_glue_connection() -> None:
    with wr.postgresql.connect("aws-sdk-pandas-postgresql", timeout=10):
        pass


def test_glue_connection_ssm_credential_type() -> None:
    with wr.postgresql.connect("aws-sdk-pandas-postgresql-ssm", timeout=10):
        pass


def test_read_sql_query_simple(databases_parameters):
    with pg8000.connect(
        host=databases_parameters["postgresql"]["host"],
        port=int(databases_parameters["postgresql"]["port"]),
        database=databases_parameters["postgresql"]["database"],
        user=databases_parameters["user"],
        password=databases_parameters["password"],
    ) as con:
        df = wr.postgresql.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


def test_to_sql_simple(postgresql_table, postgresql_con):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        table=postgresql_table,
        schema="public",
        mode="overwrite",
        index=True,
    )


@pytest.mark.parametrize("overwrite_method", ["drop", "cascade", "truncate", "truncate cascade"])
def test_to_sql_overwrite(postgresql_table, postgresql_con, overwrite_method):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        table=postgresql_table,
        schema="public",
        mode="overwrite",
        overwrite_method=overwrite_method,
    )
    df = pd.DataFrame({"c0": [4, 5, 6], "c1": ["xoo", "yoo", "zoo"]})
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        table=postgresql_table,
        schema="public",
        mode="overwrite",
        overwrite_method=overwrite_method,
    )
    df = wr.postgresql.read_sql_table(table=postgresql_table, schema="public", con=postgresql_con)
    assert df.shape == (3, 2)


def test_unknown_overwrite_method_error(postgresql_table, postgresql_con):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.postgresql.to_sql(
            df=df,
            con=postgresql_con,
            table=postgresql_table,
            schema="public",
            mode="overwrite",
            overwrite_method="unknown",
        )


@pytest.mark.xfail(is_ray_modin, raises=ProgrammingError, reason="Broken export of values in Modin")
def test_sql_types(postgresql_table, postgresql_con):
    table = postgresql_table
    df = get_df()
    df["arrint"] = pd.Series([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    df["arrstr"] = pd.Series([["a", "b", "c"], ["d", "e", "f"], ["g", "h", "i"]])
    df.drop(["binary"], axis=1, inplace=True)
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        table=table,
        schema="public",
        mode="overwrite",
        index=True,
        dtype={"iint32": "INTEGER", "arrint": "INTEGER[]", "arrstr": "VARCHAR[]"},
    )
    df = wr.postgresql.read_sql_query(f"SELECT * FROM public.{table}", postgresql_con)
    ensure_data_types(df, has_list=False)
    dfs = wr.postgresql.read_sql_query(
        sql=f"SELECT * FROM public.{table}",
        con=postgresql_con,
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
            "arrint": pa.list_(pa.int64()),
            "arrstr": pa.list_(pa.string()),
        },
    )
    for df in dfs:
        ensure_data_types(df, has_list=False)


def test_to_sql_cast(postgresql_table, postgresql_con):
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
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        table=table,
        schema="public",
        mode="overwrite",
        index=False,
        dtype={"col": "VARCHAR(1024)"},
    )
    df2 = wr.postgresql.read_sql_query(sql=f"SELECT * FROM public.{table}", con=postgresql_con)
    assert df.equals(df2)


def test_null(postgresql_table, postgresql_con):
    table = postgresql_table
    df = pd.DataFrame({"id": [1, 2, 3], "nothing": [None, None, None]})
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        table=table,
        schema="public",
        mode="overwrite",
        index=False,
        dtype={"nothing": "INTEGER"},
    )
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        table=table,
        schema="public",
        mode="append",
        index=False,
    )
    df2 = wr.postgresql.read_sql_table(table=table, schema="public", con=postgresql_con)
    df["id"] = df["id"].astype("Int64")
    assert pandas_equals(pd.concat(objs=[df, df], ignore_index=True), df2)


def test_decimal_cast(postgresql_table, postgresql_con):
    df = pd.DataFrame(
        {
            "col0": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col1": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        }
    )
    wr.postgresql.to_sql(df, postgresql_con, postgresql_table, "public")
    df2 = wr.postgresql.read_sql_table(
        schema="public",
        table=postgresql_table,
        con=postgresql_con,
        dtype={"col0": "float32", "col1": "float64", "col2": "Int64"},
    )
    assert df2.dtypes.to_list() == ["float32", "float64", "Int64"]
    assert 3.88 <= df2.col0.sum() <= 3.89
    assert 3.88 <= df2.col1.sum() <= 3.89
    assert df2.col2.sum() == 2


def test_read_retry(postgresql_con):
    try:
        wr.postgresql.read_sql_query("ERROR", postgresql_con)
    except:  # noqa
        pass
    df = wr.postgresql.read_sql_query("SELECT 1", postgresql_con)
    assert df.shape == (1, 1)


def test_table_name(postgresql_con):
    df = pd.DataFrame({"col0": [1]})
    wr.postgresql.to_sql(df, postgresql_con, "Test Name", "public", mode="overwrite")
    df = wr.postgresql.read_sql_table(schema="public", con=postgresql_con, table="Test Name")
    assert df.shape == (1, 1)
    with postgresql_con.cursor() as cursor:
        cursor.execute('DROP TABLE "Test Name"')
    postgresql_con.commit()


@pytest.mark.parametrize("dbname", [None, "postgres"])
def test_connect_secret_manager(dbname):
    con = wr.postgresql.connect(secret_id="aws-sdk-pandas/postgresql", dbname=dbname)
    df = wr.postgresql.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


def test_insert_with_column_names(postgresql_table, postgresql_con):
    create_table_sql = (
        f"CREATE TABLE public.{postgresql_table} (c0 varchar NULL,c1 int NULL DEFAULT 42,c2 int NOT NULL);"
    )
    with postgresql_con.cursor() as cursor:
        cursor.execute(create_table_sql)
        postgresql_con.commit()

    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    with pytest.raises(ProgrammingError):
        wr.postgresql.to_sql(
            df=df, con=postgresql_con, schema="public", table=postgresql_table, mode="append", use_column_names=False
        )

    wr.postgresql.to_sql(
        df=df, con=postgresql_con, schema="public", table=postgresql_table, mode="append", use_column_names=True
    )

    df2 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)

    df["c1"] = 42
    df["c0"] = df["c0"].astype("string")
    df["c1"] = df["c1"].astype("Int64")
    df["c2"] = df["c2"].astype("Int64")
    df = df.reindex(sorted(df.columns), axis=1)
    assert df.equals(df2)


@pytest.mark.parametrize("chunksize", [1, 10, 500])
def test_dfs_are_equal_for_different_chunksizes(postgresql_table, postgresql_con, chunksize):
    df = pd.DataFrame({"c0": [i for i in range(64)], "c1": ["foo" for _ in range(64)]})
    wr.postgresql.to_sql(df=df, con=postgresql_con, schema="public", table=postgresql_table, chunksize=chunksize)

    df2 = pd.concat(
        list(
            wr.postgresql.read_sql_table(
                con=postgresql_con,
                schema="public",
                table=postgresql_table,
                chunksize=chunksize,
            )
        ),
        ignore_index=True,
    )

    df["c0"] = df["c0"].astype("Int64")
    df["c1"] = df["c1"].astype("string")

    assert df.equals(df2)


def test_upsert(postgresql_table, postgresql_con):
    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.postgresql.to_sql(
            df=df,
            con=postgresql_con,
            schema="public",
            table=postgresql_table,
            mode="upsert",
            upsert_conflict_columns=None,
            use_column_names=True,
        )

    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=["c0"],
        use_column_names=True,
    )
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=["c0"],
        use_column_names=True,
    )
    df2 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df2) == 2)

    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=["c0"],
        use_column_names=True,
    )
    df3 = pd.DataFrame({"c0": ["baz", "bar"], "c2": [3, 2]})
    wr.postgresql.to_sql(
        df=df3,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=["c0"],
        use_column_names=True,
    )
    df4 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df4) == 3)

    df5 = pd.DataFrame({"c0": ["foo", "bar"], "c2": [4, 5]})
    wr.postgresql.to_sql(
        df=df5,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=["c0"],
        use_column_names=True,
    )

    df6 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df6) == 3)
    assert bool(len(df6.loc[(df6["c0"] == "foo") & (df6["c2"] == 4)]) == 1)
    assert bool(len(df6.loc[(df6["c0"] == "bar") & (df6["c2"] == 5)]) == 1)


def test_upsert_multiple_conflict_columns(postgresql_table, postgresql_con):
    df = pd.DataFrame({"c0": ["foo", "bar"], "c1": [1, 2], "c2": [3, 4]})
    upsert_conflict_columns = ["c1", "c2"]

    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=upsert_conflict_columns,
        use_column_names=True,
    )
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=upsert_conflict_columns,
        use_column_names=True,
    )
    df2 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df2) == 2)

    df3 = pd.DataFrame({"c0": ["baz", "spam"], "c1": [1, 5], "c2": [3, 2]})
    wr.postgresql.to_sql(
        df=df3,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=upsert_conflict_columns,
        use_column_names=True,
    )
    df4 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df4) == 3)

    df5 = pd.DataFrame({"c0": ["egg", "spam"], "c1": [2, 5], "c2": [4, 2]})
    wr.postgresql.to_sql(
        df=df5,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="upsert",
        upsert_conflict_columns=upsert_conflict_columns,
        use_column_names=True,
    )

    df6 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    df7 = pd.DataFrame({"c0": ["baz", "egg", "spam"], "c1": [1, 2, 5], "c2": [3, 4, 2]})
    df7["c0"] = df7["c0"].astype("string")
    df7["c1"] = df7["c1"].astype("Int64")
    df7["c2"] = df7["c2"].astype("Int64")
    assert pandas_equals(df6, df7)


def test_insert_ignore_duplicate_columns(postgresql_table, postgresql_con):
    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="append",
        insert_conflict_columns=["c0"],
        use_column_names=True,
    )
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="append",
        insert_conflict_columns=["c0"],
        use_column_names=True,
    )
    df2 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df2) == 2)

    df3 = pd.DataFrame({"c0": ["baz", "bar"], "c2": [30, 20]})
    wr.postgresql.to_sql(
        df=df3,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="append",
        insert_conflict_columns=["c0"],
        use_column_names=True,
    )
    df4 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df4) == 3)

    df5 = pd.DataFrame({"c0": ["foo", "bar"], "c2": [4, 5]})
    wr.postgresql.to_sql(
        df=df5,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="append",
        insert_conflict_columns=["c0"],
        use_column_names=True,
    )

    df6 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df6) == 3)
    assert bool(len(df6.loc[(df6["c0"] == "foo") & (df6["c2"] == 1)]) == 1)
    assert bool(len(df6.loc[(df6["c0"] == "bar") & (df6["c2"] == 2)]) == 1)


def test_insert_ignore_duplicate_multiple_columns(postgresql_table, postgresql_con):
    df = pd.DataFrame({"c0": ["foo", "bar"], "c1": [1, 2], "c2": [3, 4]})
    insert_conflict_columns = ["c1", "c2"]

    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="append",
        insert_conflict_columns=insert_conflict_columns,
        use_column_names=True,
    )
    wr.postgresql.to_sql(
        df=df,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="append",
        insert_conflict_columns=insert_conflict_columns,
        use_column_names=True,
    )
    df2 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df2) == 2)

    df3 = pd.DataFrame({"c0": ["baz", "spam"], "c1": [1, 5], "c2": [3, 2]})
    wr.postgresql.to_sql(
        df=df3,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="append",
        insert_conflict_columns=insert_conflict_columns,
        use_column_names=True,
    )
    df4 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    assert bool(len(df4) == 3)

    df5 = pd.DataFrame({"c0": ["egg", "spam"], "c1": [2, 5], "c2": [4, 2]})
    wr.postgresql.to_sql(
        df=df5,
        con=postgresql_con,
        schema="public",
        table=postgresql_table,
        mode="append",
        insert_conflict_columns=insert_conflict_columns,
        use_column_names=True,
    )

    df6 = wr.postgresql.read_sql_table(con=postgresql_con, schema="public", table=postgresql_table)
    df7 = pd.DataFrame({"c0": ["foo", "bar", "spam"], "c1": [1, 2, 5], "c2": [3, 4, 2]})
    df7["c0"] = df7["c0"].astype("string")
    df7["c1"] = df7["c1"].astype("Int64")
    df7["c2"] = df7["c2"].astype("Int64")
    assert pandas_equals(df6, df7)


@pytest.mark.skipif(version.parse(pa.__version__) > version.parse("14.0.0"), reason="Fixed in pyarrow 14+")
def test_timestamp_overflow(postgresql_table, postgresql_con):
    df = pd.DataFrame({"c0": [datetime.strptime("1677-01-01 00:00:00.0", "%Y-%m-%d %H:%M:%S.%f")]})
    wr.postgresql.to_sql(df=df, con=postgresql_con, schema="public", table=postgresql_table)

    with pytest.raises(pa._lib.ArrowInvalid):
        wr.postgresql.read_sql_table(
            con=postgresql_con, schema="public", table=postgresql_table, timestamp_as_object=False
        )

    df2 = wr.postgresql.read_sql_table(
        con=postgresql_con, schema="public", table=postgresql_table, timestamp_as_object=True
    )
    assert df.c0.values[0] == df2.c0.values[0]


def test_column_with_reserved_keyword(postgresql_table: str, postgresql_con: pg8000.Connection) -> None:
    df = pd.DataFrame({"col0": [1], "end": ["foo"]})
    wr.postgresql.to_sql(
        df=df, con=postgresql_con, table=postgresql_table, schema="public", mode="append", use_column_names=True
    )

    df2 = wr.postgresql.read_sql_table(con=postgresql_con, table=postgresql_table, schema="public")
    assert (df.columns == df2.columns).all()
