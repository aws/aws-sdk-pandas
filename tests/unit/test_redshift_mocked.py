from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import awswrangler as wr

redshift_connector = pytest.importorskip("redshift_connector")


def test_unload_to_files_escapes_partition_columns() -> None:
    con = MagicMock(spec=redshift_connector.Connection)
    cursor = con.cursor.return_value.__enter__.return_value
    wr.redshift.unload_to_files(
        sql="SELECT * FROM public.events",
        path="s3://bucket/prefix/",
        con=con,
        iam_role="arn:aws:iam::123456789012:role/r",
        partition_cols=["group", "event date"],
    )
    sql = cursor.execute.call_args.args[0]
    assert 'PARTITION BY ("group","event date")' in sql
