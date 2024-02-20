import logging
from typing import Iterator

import adbc_driver_manager.dbapi as dbapi
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import assert_pandas_equals, get_time_str_with_random_suffix

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.fixture(scope="function", params=["aws-sdk-pandas-postgresql"])
def adbc_con(request: pytest.FixtureRequest) -> Iterator[dbapi.Connection]:
    connection: str = request.param
    with wr.adbc.connect(connection) as con:
        yield con


@pytest.fixture(scope="function")
def table(adbc_con: dbapi.Connection) -> Iterator[str]:
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")

    yield name

    with adbc_con.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS public.{name}")
    adbc_con.commit()


@pytest.mark.parametrize("connection", ["aws-sdk-pandas-postgresql"])
def test_glue_connection(connection: str) -> None:
    with wr.adbc.connect(connection, timeout=10):
        pass


@pytest.mark.parametrize("secret_id", ["postgresql"])
def test_connect_secret_manager(secret_id: str) -> None:
    with wr.adbc.connect(secret_id=f"aws-sdk-pandas/{secret_id}") as con:
        df = wr.adbc.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


def test_read_sql_query_simple(adbc_con: dbapi.Connection) -> None:
    df = wr.adbc.read_sql_query("SELECT 1", con=adbc_con)
    assert df.shape == (1, 1)


def test_to_sql_simple(adbc_con: dbapi.Connection, table: str) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.adbc.to_sql(df=df, con=adbc_con, table=table, schema="public", if_exists="replace", index=True)


def test_read_write_equality(adbc_con: dbapi.Connection, table: str) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.adbc.to_sql(df=df, con=adbc_con, table=table, schema="public", if_exists="replace")

    df_out = wr.adbc.read_sql_table(table=table, con=adbc_con, schema="public")
    assert_pandas_equals(df, df_out)
