from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pytest

from awswrangler.athena import _read, _utils

_SUCCEEDED_PAYLOAD = {
    "QueryExecutionId": "query-id",
    "Status": {"State": "SUCCEEDED", "SubmissionDateTime": datetime(2026, 1, 1, tzinfo=timezone.utc)},
    "ResultConfiguration": {"OutputLocation": "s3://bucket/query-id.csv"},
}


@pytest.mark.parametrize(
    "values",
    [
        [Decimal("123456789012345.67"), Decimal("9007199254740993.00"), Decimal("0.00"), None],
        [None, None],
        [],
    ],
)
def test_fix_csv_types_casts_decimal_columns(values: list[Decimal | None]) -> None:
    df = pd.DataFrame({"amount": pd.Series(values, dtype=object), "name": pd.Series(["x"] * len(values), dtype=object)})

    out = _read._fix_csv_types(
        df, parse_dates=[], binaries=[], parse_geometry=[], decimal_types={"amount": "decimal(20,2)"}
    )

    assert out["amount"].dtype == pd.ArrowDtype(pa.decimal128(20, 2))
    assert out["name"].dtype == object
    for actual, expected in zip(out["amount"], values):
        if expected is None:
            assert pd.isna(actual)
        else:
            assert actual == expected


def test_fix_csv_types_without_decimal_types_leaves_objects() -> None:
    df = pd.DataFrame({"amount": pd.Series([Decimal("1.50")], dtype=object)})

    out = _read._fix_csv_types(df, parse_dates=[], binaries=[], parse_geometry=[])

    assert out["amount"].dtype == object


@pytest.mark.parametrize(
    "dtype_backend,expected_decimal_types",
    [("pyarrow", {"amount": "decimal(20,2)"}), ("numpy_nullable", {})],
)
def test_get_query_metadata_decimal_types(dtype_backend: str, expected_decimal_types: dict[str, str]) -> None:
    with patch.object(_utils, "get_query_columns_types", return_value={"amount": "decimal(20,2)", "name": "varchar"}):
        meta = _utils._get_query_metadata(
            query_execution_id="query-id",
            query_execution_payload=_SUCCEEDED_PAYLOAD,
            dtype_backend=dtype_backend,
        )

    assert meta.decimal_types == expected_decimal_types
    assert "amount" in meta.converters  # values are parsed as Decimal on both backends
    assert "amount" not in meta.dtype
    assert "name" in meta.dtype


def test_get_query_metadata_category_takes_precedence_over_decimal() -> None:
    with patch.object(_utils, "get_query_columns_types", return_value={"amount": "decimal(20,2)"}):
        meta = _utils._get_query_metadata(
            query_execution_id="query-id",
            query_execution_payload=_SUCCEEDED_PAYLOAD,
            categories=["amount"],
            dtype_backend="pyarrow",
        )

    assert meta.dtype == {"amount": "category"}
    assert meta.decimal_types == {}
