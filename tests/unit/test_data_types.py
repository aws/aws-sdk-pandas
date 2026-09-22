import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from awswrangler import _data_types
from awswrangler._data_types import athena2pandas, athena2pyarrow, cast_pandas_with_athena_types
from awswrangler.exceptions import UnsupportedType


@pytest.mark.parametrize(
    "dtype,expected",
    [
        ("binary", pa.binary()),
        ("varbinary", pa.binary()),
        ("BINARY", pa.binary()),
        ("VARBINARY", pa.binary()),
    ],
)
def test_athena2pyarrow_binary_types(dtype, expected):
    assert athena2pyarrow(dtype) == expected


@pytest.mark.parametrize("dtype", ["i", "n", "ary", "bin"])
def test_athena2pyarrow_rejects_binary_substrings(dtype):
    with pytest.raises(UnsupportedType, match=f"Unsupported Athena type: {dtype}"):
        athena2pyarrow(dtype)


@pytest.mark.parametrize("dtype", ["binary", "varbinary"])
def test_athena2pandas_binary_types(dtype):
    assert athena2pandas(dtype) == "bytes"


@pytest.mark.parametrize("series_dtype", [object, pd.StringDtype()])
def test_cast_pandas_date_vectorizes_iso_strings(monkeypatch, series_dtype):
    df = pd.DataFrame({"event_date": pd.Series(["2024-01-02", None, np.nan, pd.NaT, "2000-02-29"], dtype=series_dtype)})

    def fail_scalar_cast(value):
        raise AssertionError(f"unexpected scalar date cast for {value!r}")

    monkeypatch.setattr(_data_types, "_cast2date", fail_scalar_cast)

    result = cast_pandas_with_athena_types(df, {"event_date": "date"})

    assert result["event_date"].tolist() == [
        datetime.date(2024, 1, 2),
        None,
        None,
        None,
        datetime.date(2000, 2, 29),
    ]
    assert result["event_date"].dtype == object


@pytest.mark.skipif(str(pd.Series(["2024-01-02"]).dtype) != "str", reason="pandas does not infer str dtype")
def test_cast_pandas_date_vectorizes_pandas_3_string_dtype(monkeypatch):
    df = pd.DataFrame({"event_date": ["2024-01-02", None]})

    def fail_scalar_cast(value):
        raise AssertionError(f"unexpected scalar date cast for {value!r}")

    monkeypatch.setattr(_data_types, "_cast2date", fail_scalar_cast)

    result = cast_pandas_with_athena_types(df, {"event_date": "date"})

    assert result["event_date"].tolist() == [datetime.date(2024, 1, 2), None]


@pytest.mark.parametrize(
    "values",
    [
        [],
        [None, pd.NA],
    ],
)
def test_cast_pandas_date_preserves_empty_string_dtype(values):
    df = pd.DataFrame({"event_date": pd.Series(values, dtype="string")})
    expected = df["event_date"].apply(lambda value: _data_types._cast2date(value)).replace(to_replace={pd.NaT: None})

    result = cast_pandas_with_athena_types(df, {"event_date": "date"})

    pd.testing.assert_series_equal(result["event_date"], expected)


def test_cast_pandas_date_falls_back_for_mixed_values(monkeypatch):
    original_cast2date = _data_types._cast2date
    calls = []

    def spy_scalar_cast(value):
        calls.append(value)
        return original_cast2date(value)

    values = [
        "2024-01-02",
        "01/03/2024",
        "",
        datetime.date(2024, 1, 4),
        datetime.datetime(2024, 1, 5, 12, 30),
        pd.Timestamp("2024-01-06 12:30"),
        "2024-01-07T23:30:00-05:00",
        0,
        1.5,
        None,
        np.nan,
        pd.NaT,
        np.inf,
    ]
    df = pd.DataFrame({"event_date": pd.Series(values, dtype=object)})
    expected = df["event_date"].apply(lambda value: original_cast2date(value)).replace(to_replace={pd.NaT: None})
    monkeypatch.setattr(_data_types, "_cast2date", spy_scalar_cast)

    result = cast_pandas_with_athena_types(df, {"event_date": "date"})

    assert len(calls) == len(values)
    pd.testing.assert_series_equal(result["event_date"], expected)


def test_cast2date_preserves_date_subclasses():
    values = [
        datetime.date(2024, 1, 4),
        datetime.datetime(2024, 1, 5, 12, 30),
        pd.Timestamp("2024-01-06 12:30"),
    ]

    for value in values:
        assert _data_types._cast2date(value) is value


def test_cast_pandas_date_falls_back_for_mixed_format_string_dtype(monkeypatch):
    original_cast2date = _data_types._cast2date
    calls = []

    def spy_scalar_cast(value):
        calls.append(value)
        return original_cast2date(value)

    df = pd.DataFrame({"event_date": pd.Series(["2024-01-02", "01/03/2024"], dtype="string")})
    monkeypatch.setattr(_data_types, "_cast2date", spy_scalar_cast)

    result = cast_pandas_with_athena_types(df, {"event_date": "date"})

    assert len(calls) == 2
    assert result["event_date"].tolist() == [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]


def test_cast_pandas_date_falls_back_for_native_numeric_dtype(monkeypatch):
    original_cast2date = _data_types._cast2date
    calls = []

    def spy_scalar_cast(value):
        calls.append(value)
        return original_cast2date(value)

    df = pd.DataFrame({"event_date": pd.Series([0.0, 1.5, np.nan, np.inf], dtype="float64")})
    monkeypatch.setattr(_data_types, "_cast2date", spy_scalar_cast)

    result = cast_pandas_with_athena_types(df, {"event_date": "date"})

    assert len(calls) == 4
    assert result["event_date"].tolist() == [
        datetime.date(1970, 1, 1),
        datetime.date(1970, 1, 1),
        None,
        None,
    ]


def test_cast_pandas_date_preserves_invalid_iso_exception(monkeypatch):
    original_cast2date = _data_types._cast2date
    calls = []

    def spy_scalar_cast(value):
        calls.append(value)
        return original_cast2date(value)

    df = pd.DataFrame({"event_date": pd.Series(["2024-02-30"], dtype="string")})
    monkeypatch.setattr(_data_types, "_cast2date", spy_scalar_cast)

    with pytest.raises(ValueError):
        cast_pandas_with_athena_types(df, {"event_date": "date"})

    assert calls == ["2024-02-30"]
