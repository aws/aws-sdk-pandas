from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

import awswrangler as wr

pg8000 = pytest.importorskip("pg8000")


def _mocked_connection() -> tuple[MagicMock, MagicMock]:
    con = MagicMock(spec=pg8000.Connection)
    cursor = con.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [(True,)]  # table exists, so no CREATE TABLE path
    return con, cursor


def test_to_sql_escapes_conflict_columns() -> None:
    con, cursor = _mocked_connection()
    wr.postgresql.to_sql(
        df=pd.DataFrame({"id": [1], "value": ["a"]}),
        con=con,
        table="t",
        schema="s",
        mode="upsert",
        upsert_conflict_columns=["id"],
    )
    sql = cursor.executemany.call_args.args[0]
    assert 'ON CONFLICT ("id") DO UPDATE SET' in sql


@pytest.mark.parametrize(
    "mode,param",
    [("upsert", "upsert_conflict_columns"), ("append", "insert_conflict_columns")],
)
@pytest.mark.parametrize(
    "payload",
    [
        "id) DO NOTHING; DROP TABLE s.t; --",
        'id") DO NOTHING; --',
        "id; SELECT pg_sleep(10)",
    ],
)
def test_to_sql_rejects_unsafe_conflict_columns(mode: str, param: str, payload: str) -> None:
    con, cursor = _mocked_connection()
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.postgresql.to_sql(
            df=pd.DataFrame({"id": [1], "value": ["a"]}),
            con=con,
            table="t",
            schema="s",
            mode=mode,
            **{param: [payload]},
        )
    cursor.executemany.assert_not_called()
