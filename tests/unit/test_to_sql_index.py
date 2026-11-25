from __future__ import annotations

import inspect
from contextlib import ExitStack
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import awswrangler as wr


def _raw(func: Callable[..., Any]) -> Callable[..., Any]:
    # Skip the optional dependency guard so the test does not need the database drivers installed.
    return inspect.unwrap(func)


def _call_postgresql(df: pd.DataFrame, con: MagicMock) -> None:
    with ExitStack() as stack:
        stack.enter_context(patch("awswrangler.postgresql._validate_connection"))
        stack.enter_context(patch("awswrangler.postgresql._create_table"))
        _raw(wr.postgresql.to_sql)(df=df, con=con, table="t", schema="s", index=True)


def _call_mysql(df: pd.DataFrame, con: MagicMock) -> None:
    with ExitStack() as stack:
        stack.enter_context(patch("awswrangler.mysql._validate_connection"))
        stack.enter_context(patch("awswrangler.mysql._create_table"))
        stack.enter_context(patch("awswrangler.mysql.pymysql"))
        _raw(wr.mysql.to_sql)(df=df, con=con, table="t", schema="s", index=True)


def _call_sqlserver(df: pd.DataFrame, con: MagicMock) -> None:
    with ExitStack() as stack:
        stack.enter_context(patch("awswrangler.sqlserver._validate_connection"))
        stack.enter_context(patch("awswrangler.sqlserver._create_table"))
        _raw(wr.sqlserver.to_sql)(df=df, con=con, table="t", schema="s", index=True)


def _call_oracle(df: pd.DataFrame, con: MagicMock) -> None:
    with ExitStack() as stack:
        stack.enter_context(patch("awswrangler.oracle._validate_connection"))
        stack.enter_context(patch("awswrangler.oracle._create_table"))
        _raw(wr.oracle.to_sql)(df=df, con=con, table="t", schema="s", index=True)


def _call_redshift(df: pd.DataFrame, con: MagicMock) -> None:
    with ExitStack() as stack:
        stack.enter_context(patch("awswrangler.redshift._write._validate_connection"))
        stack.enter_context(patch("awswrangler.redshift._write._create_table", return_value=("t", "s")))
        _raw(wr.redshift.to_sql)(df=df, con=con, table="t", schema="s", index=True)


@pytest.mark.parametrize(
    "to_sql",
    [_call_postgresql, _call_mysql, _call_sqlserver, _call_oracle, _call_redshift],
)
def test_to_sql_index_true_does_not_mutate_dataframe(to_sql: Callable[[pd.DataFrame, MagicMock], Any]) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    expected = df.copy()
    con = MagicMock()

    # A second call with the same frame used to fail because the index had already been
    # moved into a column ("cannot insert index, already exists").
    to_sql(df, con)
    to_sql(df, con)

    pd.testing.assert_frame_equal(df, expected)
    assert list(df.columns) == ["c0", "c1"]
