import logging
from typing import Iterator

import adbc_driver_manager.dbapi as dbapi
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.fixture(scope="function", params=["aws-sdk-pandas-postgresql"])
def adbc_con(request: pytest.FixtureRequest) -> Iterator[dbapi.Connection]:
    connection: str = request.param
    with wr.adbc.connect(connection) as con:
        yield con


@pytest.mark.parametrize("connection", ["aws-sdk-pandas-postgresql"])
def test_glue_connection(connection: str) -> None:
    with wr.adbc.connect(connection, timeout=10):
        pass


@pytest.mark.parametrize("secret_id", ["postgresql"])
def test_connect_secret_manager(secret_id: str):
    con = wr.adbc.connect(secret_id=f"aws-sdk-pandas/{secret_id}")
    df = wr.adbc.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


def test_read_sql_query_simple(adbc_con: dbapi.Connection):
    df = wr.adbc.read_sql_query("SELECT 1", con=adbc_con)
    assert df.shape == (1, 1)


def test_to_sql_simple(postgresql_table: str, adbc_con: dbapi.Connection):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.adbc.to_sql(df=df, con=adbc_con, table=postgresql_table, schema="public", if_exists="replace", index=True)
