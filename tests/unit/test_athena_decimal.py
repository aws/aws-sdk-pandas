import csv
import io
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest

import awswrangler as wr
from awswrangler.athena import _read, _utils
from awswrangler.athena._cache import _LocalMetadataCacheManager


@pytest.fixture
def decimal_query(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(wr._utils, "client", lambda **kwargs: client)
    monkeypatch.setattr(_read, "_cache_manager", _LocalMetadataCacheManager())
    monkeypatch.setattr(_read, "_start_query_execution", lambda **kwargs: "decimal-query")
    payload = {
        "QueryExecutionId": "decimal-query",
        "Status": {"State": "SUCCEEDED", "SubmissionDateTime": datetime(2026, 1, 1, tzinfo=timezone.utc)},
        "ResultConfiguration": {"OutputLocation": "s3://test-bucket/decimal-query.csv"},
    }
    monkeypatch.setattr(_utils._executions, "wait_query", lambda **kwargs: payload)
    monkeypatch.setattr(
        wr.s3._read_text, "_path2list", lambda **kwargs: [payload["ResultConfiguration"]["OutputLocation"]]
    )

    def read(values, precision, scale, *, managed, chunksize, dtype_backend, categories=None):
        monkeypatch.setattr(
            _read,
            "_get_workgroup_config",
            lambda **kwargs: _utils._WorkGroupConfig(False, "s3://test-bucket/", None, None, managed),
        )
        monkeypatch.setattr(
            _utils, "get_query_columns_types", lambda **kwargs: {"amount": f"decimal({precision},{scale})"}
        )
        buffer = io.StringIO()
        writer = csv.writer(buffer, quoting=csv.QUOTE_ALL)
        writer.writerow(["amount"])
        writer.writerows([[value] for value in values])
        monkeypatch.setattr(wr.s3._read_text_core, "open_s3_object", lambda **kwargs: io.StringIO(buffer.getvalue()))
        client.get_paginator.return_value.paginate.return_value = [
            {
                "ResultSet": {
                    "ResultSetMetadata": {"ColumnInfo": [{"Name": "amount"}]},
                    "Rows": [{"Data": [{"VarCharValue": "amount"}]}]
                    + [{"Data": [{"VarCharValue": value}] if value is not None else [{}]} for value in values],
                }
            }
        ]
        result = wr.athena.read_sql_query(
            "SELECT amount FROM test_table",
            database="test_database",
            ctas_approach=False,
            client_request_token="decimal-query",
            chunksize=chunksize,
            dtype_backend=dtype_backend,
            categories=categories,
            use_threads=False,
        )
        return [result] if chunksize is None else list(result)

    return read


@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("chunksize", [None, 2])
@pytest.mark.parametrize("dtype_backend", ["numpy_nullable", "pyarrow"])
@pytest.mark.parametrize(
    "precision,scale,values",
    [
        (20, 2, ["123456789012345.67", "9007199254740993.00", "-123456789012345.67", "0.00", None]),
        (20, 0, ["9007199254740993", "-9007199254740993", None]),
        (38, 18, ["12345678901234567890.123456789012345678", "-0.000000000000000001", None]),
        (38, 38, ["0.12345678901234567890123456789012345678", None]),
        (20, 2, [None, None]),
        (20, 2, []),
    ],
)
def test_read_sql_query_decimal_precision(decimal_query, managed, chunksize, dtype_backend, precision, scale, values):
    chunks = decimal_query(values, precision, scale, managed=managed, chunksize=chunksize, dtype_backend=dtype_backend)
    actual = [value for chunk in chunks for value in chunk["amount"]]
    assert len(actual) == len(values)
    for value, expected in zip(actual, values):
        if expected is None:
            assert pd.isna(value)
        else:
            assert isinstance(value, Decimal)
            assert value == Decimal(expected)
    for chunk in chunks:
        if dtype_backend == "pyarrow":
            assert chunk["amount"].dtype == pd.ArrowDtype(pa.decimal128(precision, scale))
        else:
            assert chunk["amount"].dtype == object
        assert chunk.query_metadata["QueryExecutionId"] == "decimal-query"


@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("chunksize", [None, 2])
def test_read_sql_query_decimal_category(decimal_query, managed, chunksize):
    values = ["123456789012345.67", "12.50"]
    chunks = decimal_query(
        values, 20, 2, managed=managed, chunksize=chunksize, dtype_backend="pyarrow", categories=["amount"]
    )
    assert [value for chunk in chunks for value in chunk["amount"]] == values
    assert all(isinstance(chunk["amount"].dtype, pd.CategoricalDtype) for chunk in chunks)
