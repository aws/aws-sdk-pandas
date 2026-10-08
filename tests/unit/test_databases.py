"""Unit tests for the generic database helpers in ``awswrangler._databases``.

These tests use an in-memory stub connection (DB-API shaped), so they require no
database server and no AWS credentials.
"""

from __future__ import annotations

from typing import Any, Iterator

import pytest

from awswrangler import _databases as _db_utils


class _StubCursor:
    """Minimal DB-API cursor. Raises on ``execute`` or on the Nth ``fetchmany`` call."""

    description = [("col0",)]

    def __init__(self, fail_on_execute: bool, fail_on_fetch: bool) -> None:
        self._fail_on_execute = fail_on_execute
        self._fail_on_fetch = fail_on_fetch
        self._fetch_calls = 0
        self.closed = False

    def __enter__(self) -> "_StubCursor":
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def execute(self, *args: Any, **kwargs: Any) -> None:
        if self._fail_on_execute:
            raise RuntimeError("syntax error at or near ...")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return [(1,), (2,)]

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        self._fetch_calls += 1
        if self._fetch_calls == 1:
            return [(1,)]
        if self._fail_on_fetch:
            raise RuntimeError("connection reset by peer")
        return []


class _StubConnection:
    def __init__(self, fail_on_execute: bool = False, fail_on_fetch: bool = False) -> None:
        self._fail_on_execute = fail_on_execute
        self._fail_on_fetch = fail_on_fetch
        self.rollback_count = 0
        self.cursors: list[_StubCursor] = []

    def cursor(self) -> _StubCursor:
        cursor = _StubCursor(fail_on_execute=self._fail_on_execute, fail_on_fetch=self._fail_on_fetch)
        self.cursors.append(cursor)
        return cursor

    def rollback(self) -> None:
        self.rollback_count += 1


@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_sql_query_rolls_back_on_execute_error(chunksize: int | None) -> None:
    con = _StubConnection(fail_on_execute=True)

    with pytest.raises(RuntimeError):
        result = _db_utils.read_sql_query("SELECT 1", con=con, chunksize=chunksize)
        if chunksize is not None:
            list(result)

    assert con.rollback_count == 1
    assert all(cursor.closed for cursor in con.cursors)


def test_read_sql_query_chunked_rolls_back_on_fetch_error() -> None:
    con = _StubConnection(fail_on_fetch=True)
    iterator: Iterator[Any] = _db_utils.read_sql_query("SELECT 1", con=con, chunksize=1)

    # The first chunk is produced normally; the failure happens mid-iteration.
    next(iterator)
    assert con.rollback_count == 0

    with pytest.raises(RuntimeError):
        next(iterator)

    assert con.rollback_count == 1
    assert all(cursor.closed for cursor in con.cursors)


def test_read_sql_query_chunked_does_not_roll_back_on_success() -> None:
    con = _StubConnection()

    chunks = list(_db_utils.read_sql_query("SELECT 1", con=con, chunksize=1))

    assert len(chunks) == 1
    assert chunks[0]["col0"].to_list() == [1]
    assert con.rollback_count == 0


def test_read_sql_query_chunked_does_not_roll_back_on_early_exit() -> None:
    con = _StubConnection(fail_on_fetch=True)
    iterator: Iterator[Any] = _db_utils.read_sql_query("SELECT 1", con=con, chunksize=1)

    next(iterator)
    # Abandoning the iterator throws GeneratorExit into it, which is not an error.
    iterator.close()  # type: ignore[union-attr]

    assert con.rollback_count == 0
