"""Offline, table-driven tests for the type mapping and casting helpers in ``awswrangler._data_types``."""

import datetime
import uuid
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pytest

from awswrangler import _data_types, exceptions
from awswrangler._data_types import (
    _cast2date,
    _cast_pandas_column,
    _cast_pandas_date,
    _normalize_pandas_dtype_name,
    athena2pandas,
    athena2pyarrow,
    cast_pandas_with_athena_types,
    database_types_from_pandas,
    pyarrow2athena,
    pyarrow2mysql,
)


class _IntExtension(pa.ExtensionType):
    def __init__(self) -> None:
        super().__init__(pa.int32(), "tests.int_extension")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> "_IntExtension":
        return cls()


# --------------------------------------------------------------------------------------
# athena2pandas
# --------------------------------------------------------------------------------------

# (athena type, default backend, pyarrow backend)
ATHENA2PANDAS_TABLE = [
    ("tinyint", "Int8", "int8[pyarrow]"),
    ("smallint", "Int16", "int16[pyarrow]"),
    ("int", "Int32", "int32[pyarrow]"),
    ("integer", "Int32", "int32[pyarrow]"),
    ("bigint", "Int64", "int64[pyarrow]"),
    ("float", "float32", "double[pyarrow]"),
    ("real", "float32", "double[pyarrow]"),
    ("double", "float64", "double[pyarrow]"),
    ("boolean", "boolean", "bool[pyarrow]"),
    ("string", "string", "string[pyarrow]"),
    ("char(10)", "string", "string[pyarrow]"),
    ("varchar(10)", "string", "string[pyarrow]"),
    ("timestamp", "datetime64", "timestamp[ns][pyarrow]"),
    ("timestamp with time zone", "datetime64", "timestamp[ns][pyarrow]"),
    ("date", "date", "date32[pyarrow]"),
    ("time", "string", "time32[ms][pyarrow]"),
    ("decimal(10,2)", "decimal", "double[pyarrow]"),
    ("binary", "bytes", "binary[pyarrow]"),
    ("varbinary", "bytes", "binary[pyarrow]"),
    ("array<int>", "object", "object"),
    ("row(a int)", "object", "object"),
    ("map<string,int>", "object", "object"),
    ("struct<a:int>", "object", "object"),
    ("json", "object", "object"),
    ("geometry", "string", "string"),
]


@pytest.mark.parametrize("dtype,expected,_", ATHENA2PANDAS_TABLE)
def test_athena2pandas_default_backend(dtype, expected, _):
    assert athena2pandas(dtype) == expected
    assert athena2pandas(dtype, dtype_backend=None) == expected
    assert athena2pandas(dtype, dtype_backend="numpy_nullable") == expected


@pytest.mark.parametrize("dtype,_,expected", ATHENA2PANDAS_TABLE)
def test_athena2pandas_pyarrow_backend(dtype, _, expected):
    assert athena2pandas(dtype, dtype_backend="pyarrow") == expected


@pytest.mark.parametrize("dtype,expected,_", ATHENA2PANDAS_TABLE)
def test_athena2pandas_is_case_insensitive(dtype, expected, _):
    assert athena2pandas(dtype.upper()) == expected


@pytest.mark.parametrize("dtype", ["", "unknown", "interval", "uuid"])
def test_athena2pandas_unsupported(dtype):
    with pytest.raises(exceptions.UnsupportedType, match="Unsupported Athena type"):
        athena2pandas(dtype)


# --------------------------------------------------------------------------------------
# athena2pyarrow
# --------------------------------------------------------------------------------------

ATHENA2PYARROW_TABLE = [
    ("tinyint", pa.int8()),
    ("smallint", pa.int16()),
    ("int", pa.int32()),
    ("integer", pa.int32()),
    ("bigint", pa.int64()),
    ("float", pa.float32()),
    ("real", pa.float32()),
    ("double", pa.float64()),
    ("boolean", pa.bool_()),
    ("string", pa.string()),
    ("uuid", pa.string()),
    ("char(5)", pa.string()),
    ("varchar(5)", pa.string()),
    ("timestamp", pa.timestamp("ns")),
    ("date", pa.date32()),
    ("binary", pa.binary()),
    ("varbinary", pa.binary()),
    ("decimal(10,2)", pa.decimal128(10, 2)),
    ("decimal(38, 18)", pa.decimal128(38, 18)),
    ("array<int>", pa.list_(pa.int32())),
    ("array<array<string>>", pa.list_(pa.list_(pa.string()))),
    ("map<string,bigint>", pa.map_(pa.string(), pa.int64())),
    ("struct<a:int,b:string>", pa.struct([("a", pa.int32()), ("b", pa.string())])),
    ("struct<a:int, b:string>", pa.struct([("a", pa.int32()), ("b", pa.string())])),
    (
        "struct<a:array<decimal(5,2)>,b:map<string,struct<c:boolean>>>",
        pa.struct(
            [
                ("a", pa.list_(pa.decimal128(5, 2))),
                ("b", pa.map_(pa.string(), pa.struct([("c", pa.bool_())]))),
            ]
        ),
    ),
]


@pytest.mark.parametrize("dtype,expected", ATHENA2PYARROW_TABLE)
def test_athena2pyarrow_table(dtype, expected):
    assert athena2pyarrow(dtype) == expected


@pytest.mark.parametrize("dtype,expected", ATHENA2PYARROW_TABLE)
def test_athena2pyarrow_ignores_case_and_surrounding_whitespace(dtype, expected):
    assert athena2pyarrow(f"  {dtype.upper()}  ") == expected


@pytest.mark.parametrize(
    "df_type,unit",
    [
        ("datetime64[ns]", "ns"),
        ("datetime64[us]", "us"),
        ("datetime64[ms]", "ms"),
        ("datetime64[s]", "s"),
        ("datetime64[D]", "ns"),
        (None, "ns"),
    ],
)
def test_athena2pyarrow_timestamp_unit(df_type, unit):
    assert athena2pyarrow("timestamp", df_type=df_type) == pa.timestamp(unit)


def test_athena2pyarrow_struct_field_names_are_lowercased():
    # The whole type string is lowercased before parsing, which matches Athena's case-insensitive identifiers.
    result = athena2pyarrow("struct<MyField:int>")
    assert result == pa.struct([("myfield", pa.int32())])


@pytest.mark.parametrize("dtype", ["", "unknown", "interval", "time"])
def test_athena2pyarrow_unsupported(dtype):
    with pytest.raises(exceptions.UnsupportedType, match="Unsupported Athena type"):
        athena2pyarrow(dtype)


def test_athena2pyarrow_invalid_map():
    with pytest.raises(RuntimeError, match="Invalid map fields"):
        athena2pyarrow("map<string,int,int>")


@pytest.mark.parametrize(
    "value,expected",
    [
        ("a,b", ["a", "b"]),
        ("a", ["a"]),
        ("a,b<c,d>,e(f,g),h", ["a", "b<c,d>", "e(f,g)", "h"]),
        ("a:array<struct<b:int,c:int>>,d:int", ["a:array<struct<b:int,c:int>>", "d:int"]),
    ],
)
def test_split_struct(value, expected):
    assert _data_types._split_struct(value) == expected


def test_split_map():
    assert _data_types._split_map("string,map<string,int>") == ["string", "map<string,int>"]


# --------------------------------------------------------------------------------------
# pyarrow2athena
# --------------------------------------------------------------------------------------

PYARROW2ATHENA_TABLE = [
    (pa.int8(), "tinyint"),
    (pa.int16(), "smallint"),
    (pa.uint8(), "smallint"),
    (pa.int32(), "int"),
    (pa.uint16(), "int"),
    (pa.int64(), "bigint"),
    (pa.uint32(), "bigint"),
    (pa.float32(), "float"),
    (pa.float64(), "double"),
    (pa.bool_(), "boolean"),
    (pa.string(), "string"),
    (pa.large_string(), "string"),
    (pa.timestamp("ns"), "timestamp"),
    (pa.timestamp("s", tz="UTC"), "timestamp"),
    (pa.date32(), "date"),
    (pa.date64(), "date"),
    (pa.binary(), "binary"),
    (pa.large_binary(), "binary"),
    (pa.binary(4), "binary"),
    (pa.decimal128(10, 2), "decimal(10,2)"),
    (pa.decimal128(38, 0), "decimal(38,0)"),
    (pa.dictionary(pa.int8(), pa.string()), "string"),
    (pa.dictionary(pa.int8(), pa.int64()), "bigint"),
    (pa.list_(pa.int32()), "array<int>"),
    (pa.large_list(pa.string()), "array<string>"),
    (pa.list_(pa.list_(pa.int8())), "array<array<tinyint>>"),
    (pa.struct([("a", pa.int32()), ("b", pa.string())]), "struct<a:int,b:string>"),
    (pa.map_(pa.string(), pa.int64()), "map<string,bigint>"),
    (_IntExtension(), "int"),
]


@pytest.mark.parametrize("dtype,expected", PYARROW2ATHENA_TABLE)
def test_pyarrow2athena_table(dtype, expected):
    assert pyarrow2athena(dtype) == expected


def test_pyarrow2athena_uint64_unsupported():
    with pytest.raises(
        exceptions.UnsupportedType,
        match=r"^There is no support for uint64, please consider int64 or uint32\.$",
    ):
        pyarrow2athena(pa.uint64())


def test_pyarrow2athena_null_type():
    with pytest.raises(
        exceptions.UndetectedType,
        match=r"^We can not infer the data type from an entire null object column$",
    ):
        pyarrow2athena(pa.null())
    assert pyarrow2athena(pa.null(), ignore_null=True) == ""


def test_pyarrow2athena_ignore_null_propagates_to_nested_types():
    # With ignore_null the null element is skipped instead of raising. The exact string for the empty element is
    # an implementation detail, so only the outer shape is checked.
    assert pyarrow2athena(pa.list_(pa.null()), ignore_null=True).startswith("array<")
    assert pyarrow2athena(pa.struct([pa.field("a", pa.null())]), ignore_null=True).startswith("struct<a:")
    assert pyarrow2athena(pa.map_(pa.string(), pa.null()), ignore_null=True).startswith("map<string,")
    assert pyarrow2athena(pa.dictionary(pa.int8(), pa.null()), ignore_null=True) == ""
    with pytest.raises(exceptions.UndetectedType):
        pyarrow2athena(pa.list_(pa.null()))
    with pytest.raises(exceptions.UndetectedType):
        pyarrow2athena(pa.struct([pa.field("a", pa.null())]))
    with pytest.raises(exceptions.UndetectedType):
        pyarrow2athena(pa.map_(pa.string(), pa.null()))


@pytest.mark.parametrize("dtype", [pa.time32("s"), pa.duration("s"), pa.float16()])
def test_pyarrow2athena_unsupported(dtype):
    with pytest.raises(exceptions.UnsupportedType, match="Unsupported Pyarrow type"):
        pyarrow2athena(dtype)


# --------------------------------------------------------------------------------------
# pyarrow2mysql
# --------------------------------------------------------------------------------------

PYARROW2MYSQL_TABLE = [
    (pa.int8(), "TINYINT"),
    (pa.uint8(), "TINYINT UNSIGNED"),
    (pa.int16(), "SMALLINT"),
    (pa.uint16(), "SMALLINT UNSIGNED"),
    (pa.int32(), "INTEGER"),
    (pa.uint32(), "INTEGER UNSIGNED"),
    (pa.int64(), "BIGINT"),
    (pa.uint64(), "BIGINT UNSIGNED"),
    (pa.float32(), "FLOAT"),
    (pa.float64(), "DOUBLE PRECISION"),
    (pa.bool_(), "BOOLEAN"),
    (pa.string(), "VARCHAR(7)"),
    (pa.large_string(), "VARCHAR(7)"),
    (pa.timestamp("ns"), "TIMESTAMP"),
    (pa.date32(), "DATE"),
    (pa.decimal128(12, 3), "DECIMAL(12,3)"),
    (pa.dictionary(pa.int8(), pa.string()), "VARCHAR(7)"),
    (pa.dictionary(pa.int8(), pa.int16()), "SMALLINT"),
    (pa.binary(), "BLOB"),
]


@pytest.mark.parametrize("dtype,expected", PYARROW2MYSQL_TABLE)
def test_pyarrow2mysql_table(dtype, expected):
    assert pyarrow2mysql(dtype, string_type="VARCHAR(7)") == expected


@pytest.mark.parametrize("dtype", [pa.large_binary(), pa.list_(pa.int8()), pa.null(), pa.time32("s")])
def test_pyarrow2mysql_unsupported(dtype):
    with pytest.raises(exceptions.UnsupportedType, match="Unsupported MySQL type"):
        pyarrow2mysql(dtype, string_type="VARCHAR(7)")


# --------------------------------------------------------------------------------------
# casting helpers
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dtype,expected",
    [
        ("datetime64[ns]", "datetime64"),
        ("datetime64[ns, UTC]", "datetime64"),
        ("datetime64[us]", "datetime64"),
        ("decimal", "decimal"),
        ("decimal128(10, 2)", "decimal"),
        ("int64", "int64"),
        ("string", "string"),
        ("object", "object"),
        ("Xdatetime64", "Xdatetime64"),
        ("Xdecimal", "Xdecimal"),
    ],
)
def test_normalize_pandas_dtype_name(dtype, expected):
    assert _normalize_pandas_dtype_name(dtype) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        (float("nan"), None),
        (float("inf"), None),
        (float("-inf"), None),
        (pd.NaT, None),
        (pd.NA, None),
        (datetime.date(2020, 1, 2), datetime.date(2020, 1, 2)),
        ("2020-01-02", datetime.date(2020, 1, 2)),
        ("2020/01/02", datetime.date(2020, 1, 2)),
    ],
)
def test_cast2date(value, expected):
    assert _cast2date(value) == expected


def test_cast2date_returns_same_date_instance():
    value = datetime.date(2020, 1, 2)
    assert _cast2date(value) is value


def test_cast2date_invalid():
    with pytest.raises(ValueError):
        _cast2date("not a date")


def test_cast_pandas_date_iso_strings_fast_path():
    result = _cast_pandas_date(pd.Series(["2020-01-02", None, "2021-12-31"]))
    assert result.tolist() == [datetime.date(2020, 1, 2), None, datetime.date(2021, 12, 31)]


def test_cast_pandas_date_string_dtype():
    result = _cast_pandas_date(pd.Series(["2020-01-02", pd.NA], dtype="string"))
    assert result.tolist() == [datetime.date(2020, 1, 2), None]


def test_cast_pandas_date_non_iso_strings_use_scalar_path():
    result = _cast_pandas_date(pd.Series(["2020/01/02", "2021-12-31", None]))
    assert result.tolist() == [datetime.date(2020, 1, 2), datetime.date(2021, 12, 31), None]


def test_cast_pandas_date_invalid_iso_shape_falls_back_to_scalar_error():
    with pytest.raises(ValueError):
        _cast_pandas_date(pd.Series(["2020-13-45"]))


def test_cast_pandas_date_all_null_strings():
    result = _cast_pandas_date(pd.Series([None, None], dtype="string"))
    assert result.tolist() == [None, None]


def test_cast_pandas_date_datetimes_keep_their_time_part():
    # Intentional: date subclasses are returned unchanged (see test_cast2date_preserves_date_subclasses).
    result = _cast_pandas_date(pd.Series(pd.to_datetime(["2020-01-02 10:00", None])))
    assert result.iloc[0] == pd.Timestamp("2020-01-02 10:00")
    assert pd.isna(result.iloc[1])


def test_cast_pandas_column_datetime():
    df = pd.DataFrame({"c": ["2020-01-02", "2020-01-03"]})
    result = _cast_pandas_column(df, "c", "string", "datetime64")
    assert str(result["c"].dtype).startswith("datetime64")
    assert result["c"].tolist() == [pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")]


def test_cast_pandas_column_date():
    df = pd.DataFrame({"c": ["2020-01-02", None]})
    result = _cast_pandas_column(df, "c", "string", "date")
    assert result["c"].tolist() == [datetime.date(2020, 1, 2), None]


def test_cast_pandas_column_bytes():
    df = pd.DataFrame({"c": ["ab", None, "é"]})
    result = _cast_pandas_column(df, "c", "string", "bytes")
    assert result["c"].tolist() == [b"ab", None, "é".encode()]


@pytest.mark.parametrize(
    "values,expected",
    [
        ([1, 2], [Decimal("1"), Decimal("2")]),
        ([1.5, None], [Decimal("1.5"), None]),
        (["1.10", "", " ", "None", "none"], [Decimal("1.10"), None, None, None, None]),
        ([Decimal("3.14"), Decimal("0")], [Decimal("3.14"), Decimal("0")]),
    ],
)
def test_cast_pandas_column_decimal(values, expected):
    df = pd.DataFrame({"c": values})
    result = _cast_pandas_column(df, "c", "object", "decimal")
    _assert_decimals(result["c"].tolist(), expected)


def _assert_decimals(actual, expected):
    # Missing values are None or pd.NA depending on the pandas version, so only their position is checked.
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        if e is None:
            assert pd.isna(a)
        else:
            assert isinstance(a, Decimal)
            assert a == e


def test_cast_pandas_column_decimal_pd_na():
    df = pd.DataFrame({"c": pd.array([1, None], dtype="Int64")})
    result = _cast_pandas_column(df, "c", "Int64", "decimal")
    _assert_decimals(result["c"].tolist(), [Decimal("1"), None])


def test_cast_pandas_column_astype():
    df = pd.DataFrame({"c": ["1", "2"]})
    result = _cast_pandas_column(df, "c", "object", "Int32")
    assert str(result["c"].dtype) == "Int32"
    assert result["c"].tolist() == [1, 2]


def test_cast_pandas_column_modifies_and_returns_same_frame():
    df = pd.DataFrame({"c": ["1", "2"]})
    result = _cast_pandas_column(df, "c", "object", "Int64")
    assert result is df
    assert str(df["c"].dtype) == "Int64"


def test_cast_pandas_column_integer_with_missing_values_warns(monkeypatch):
    # Recent pandas versions convert this input directly, so the pandas error that triggers the fallback is simulated.
    original_astype = pd.Series.astype
    calls = []

    def flaky_astype(self, dtype, *args, **kwargs):
        calls.append(dtype)
        if len(calls) == 1:
            raise TypeError("object cannot be converted to an IntegerDtype")
        return original_astype(self, dtype, *args, **kwargs)

    monkeypatch.setattr(pd.Series, "astype", flaky_astype)
    df = pd.DataFrame({"c": pd.Series(["1", "", "3", None, "<NA>", "none", "None", " "], dtype="object")})
    with pytest.warns(UserWarning, match="Object cannot be converted to an IntegerDtype"):
        result = _cast_pandas_column(df, "c", "object", "Int64")
    assert calls == ["Int64", "Int64"]
    assert str(result["c"].dtype) == "Int64"
    assert result["c"].isna().tolist() == [False, True, False, True, True, True, True, True]
    assert result["c"].dropna().tolist() == [1, 3]


def test_cast_pandas_column_other_type_errors_are_reraised(monkeypatch):
    df = pd.DataFrame({"c": [1, 2]})

    def boom(self, *args, **kwargs):
        raise TypeError("something else")

    monkeypatch.setattr(pd.Series, "astype", boom)
    with pytest.raises(TypeError, match="something else"):
        _cast_pandas_column(df, "c", "int64", "Int32")


def test_cast_pandas_with_athena_types_casts_requested_columns():
    df = pd.DataFrame(
        {
            "i": ["1", "2"],
            "d": ["2020-01-02", "2020-01-03"],
            "s": [1, 2],
            "untouched": ["x", "y"],
        }
    )
    result = cast_pandas_with_athena_types(df, {"i": "int", "d": "date", "s": "string", "missing": "int"})
    assert str(result["i"].dtype) == "Int32"
    assert result["d"].tolist() == [datetime.date(2020, 1, 2), datetime.date(2020, 1, 3)]
    assert result["s"].tolist() == ["1", "2"]
    assert result["untouched"].tolist() == ["x", "y"]
    assert "missing" not in result.columns


def test_cast_pandas_with_athena_types_skips_nested_types():
    values = [[1, 2], [3]]
    df = pd.DataFrame({"a": values, "s": [{"x": 1}, {"x": 2}], "m": [{"k": 1}, {"k": 2}]})
    result = cast_pandas_with_athena_types(df, {"a": "array<int>", "s": "struct<x:int>", "m": "map<string,int>"})
    assert result["a"].tolist() == values
    assert result["s"].tolist() == [{"x": 1}, {"x": 2}]
    assert result["m"].tolist() == [{"k": 1}, {"k": 2}]
    assert result["a"].dtype == object


def test_cast_pandas_with_athena_types_skips_matching_types(monkeypatch):
    calls = []
    monkeypatch.setattr(_data_types, "_cast_pandas_column", lambda **kwargs: calls.append(kwargs))
    df = pd.DataFrame({"i": pd.array([1, 2], dtype="Int32"), "s": pd.array(["a"] * 2, dtype="string")})
    cast_pandas_with_athena_types(df, {"i": "int", "s": "string"})
    assert calls == []


def test_cast_pandas_with_athena_types_passes_current_and_desired_types(monkeypatch):
    calls = []
    monkeypatch.setattr(_data_types, "_cast_pandas_column", lambda **kwargs: calls.append(kwargs))
    df = pd.DataFrame({"t": pd.to_datetime(["2020-01-01"])})
    cast_pandas_with_athena_types(df, {"t": "date"})
    assert len(calls) == 1
    assert calls[0]["col"] == "t"
    assert calls[0]["current_type"] == "datetime64"
    assert calls[0]["desired_type"] == "date"
    assert calls[0]["df"] is df or calls[0]["df"].equals(df)


def test_cast_pandas_with_athena_types_pyarrow_backend():
    df = pd.DataFrame({"i": [1, 2]})
    result = cast_pandas_with_athena_types(df, {"i": "bigint"}, dtype_backend="pyarrow")
    assert str(result["i"].dtype) == "int64[pyarrow]"


# --------------------------------------------------------------------------------------
# database_types_from_pandas
# --------------------------------------------------------------------------------------


def _converter(dtype: pa.DataType, string_type: str) -> str:
    return pyarrow2mysql(dtype, string_type)


def _db_types(df, **kwargs):
    params = {
        "index": False,
        "dtype": None,
        "varchar_lengths_default": 256,
        "varchar_lengths": None,
        "converter_func": _converter,
    }
    params.update(kwargs)
    return database_types_from_pandas(df=df, **params)


def test_database_types_from_pandas_defaults():
    df = pd.DataFrame({"i": [1], "f": [1.5], "s": ["a"], "b": [True]})
    assert _db_types(df) == {"i": "BIGINT", "f": "DOUBLE PRECISION", "s": "VARCHAR(256)", "b": "BOOLEAN"}


def test_database_types_from_pandas_varchar_lengths():
    df = pd.DataFrame({"a": ["x"], "b": ["y"]})
    result = _db_types(df, varchar_lengths={"a": 10})
    assert result == {"a": "VARCHAR(10)", "b": "VARCHAR(256)"}


def test_database_types_from_pandas_varchar_lengths_numpy_integer():
    import numpy as np

    df = pd.DataFrame({"a": ["x"]})
    assert _db_types(df, varchar_lengths={"a": np.int64(12)}) == {"a": "VARCHAR(12)"}


def test_database_types_from_pandas_varchar_lengths_max():
    df = pd.DataFrame({"a": ["x"]})
    assert _db_types(df, varchar_lengths={"a": "MAX"}) == {"a": "VARCHAR(MAX)"}


@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "ten", None])
def test_database_types_from_pandas_invalid_varchar_length(bad):
    df = pd.DataFrame({"a": ["x"]})
    with pytest.raises(exceptions.InvalidArgumentValue, match="varchar_lengths"):
        _db_types(df, varchar_lengths={"a": bad})


def test_database_types_from_pandas_string_default():
    df = pd.DataFrame({"a": ["x"], "i": [1]})
    result = _db_types(df, varchar_lengths_default="TEXT")
    assert result == {"a": "TEXT", "i": "BIGINT"}


def test_database_types_from_pandas_string_default_is_validated():
    df = pd.DataFrame({"a": ["x"]})
    with pytest.raises(exceptions.InvalidArgumentValue, match="Invalid database type"):
        _db_types(df, varchar_lengths_default="TEXT; DROP TABLE t")


def test_database_types_from_pandas_dtype_override():
    df = pd.DataFrame({"a": [1], "b": ["x"]})
    result = _db_types(df, dtype={"a": "SMALLINT"})
    assert result == {"a": "SMALLINT", "b": "VARCHAR(256)"}


def test_database_types_from_pandas_dtype_override_is_validated():
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(exceptions.InvalidArgumentValue, match="Invalid database type"):
        _db_types(df, dtype={"a": "INT); DROP TABLE t; --"})


def test_database_types_from_pandas_dtype_override_skips_unsupported_column():
    # A column with an explicit dtype is not inspected, so an otherwise unsupported type must not raise.
    df = pd.DataFrame({"a": [[1, 2]], "b": [1]})
    assert _db_types(df, dtype={"a": "JSON"}) == {"a": "JSON", "b": "BIGINT"}


def test_database_types_from_pandas_index():
    df = pd.DataFrame({"a": [1]}, index=pd.Index([10], name="idx"))
    assert _db_types(df, index=True) == {"idx": "BIGINT", "a": "BIGINT"}
    assert _db_types(df, index=False) == {"a": "BIGINT"}


def test_database_types_from_pandas_uses_converter_func():
    df = pd.DataFrame({"a": [1], "s": ["x"]})
    seen = []

    def converter(dtype: pa.DataType, string_type: str) -> str:
        seen.append((dtype, string_type))
        return "T"

    result = _db_types(df, converter_func=converter, varchar_lengths={"s": 4})
    assert result == {"a": "T", "s": "T"}
    assert seen == [(pa.int64(), "VARCHAR(256)"), (pa.string(), "VARCHAR(4)")]


@pytest.mark.parametrize(
    "type_str",
    [
        "INT",
        "VARCHAR(10)",
        "DECIMAL(10,2)",
        'text COLLATE "C"',
        "ENUM('a','b')",
        '"MySchema"."MyType"',
    ],
)
def test_validate_database_type_accepts(type_str):
    assert _data_types._validate_database_type("c", type_str) == type_str


@pytest.mark.parametrize(
    "type_str",
    [
        "",
        "   ",
        "INT;",
        "INT`",
        "INT -- x",
        "INT /* x */",
        "INT # x",
        "INT\\",
        "$$INT$$",
        "INT\n",
        "INT\x00",
        "INT, other INT",
        "INT)",
        "VARCHAR(10",
        "'abc",
        '"abc',
        None,
        10,
    ],
)
def test_validate_database_type_rejects(type_str):
    with pytest.raises(exceptions.InvalidArgumentValue, match="Invalid database type"):
        _data_types._validate_database_type("c", type_str)


def test_validate_database_type_error_mentions_column_and_type():
    with pytest.raises(exceptions.InvalidArgumentValue) as exc:
        _data_types._validate_database_type("my_col", "INT;")
    assert "'INT;'" in str(exc.value)
    assert "'my_col'" in str(exc.value)


@pytest.mark.parametrize("value,expected", [(5, 5), ("max", "max"), ("MAX", "MAX"), ("Max", "Max")])
def test_validate_varchar_length_accepts(value, expected):
    assert _data_types._validate_varchar_length("c", value) == expected


def test_validate_varchar_length_returns_python_int():
    import numpy as np

    result = _data_types._validate_varchar_length("c", np.int32(3))
    assert result == 3
    assert type(result) is int


# --------------------------------------------------------------------------------------
# additional edge cases found by mutation testing
# --------------------------------------------------------------------------------------


class _NullExtension(pa.ExtensionType):
    def __init__(self) -> None:
        super().__init__(pa.null(), "tests.null_extension")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> "_NullExtension":
        return cls()


def test_pyarrow2athena_ignore_null_propagates_to_extension_storage_type():
    assert pyarrow2athena(_NullExtension(), ignore_null=True) == ""
    with pytest.raises(exceptions.UndetectedType):
        pyarrow2athena(_NullExtension())


@pytest.mark.parametrize("athena_type", ["array<int>", "struct<a:int>", "map<string,int>"])
def test_cast_pandas_with_athena_types_leaves_nested_type_columns_untouched(athena_type):
    df = pd.DataFrame({"c": [1, 2]})
    result = cast_pandas_with_athena_types(df, {"c": athena_type})
    assert str(result["c"].dtype) == "int64"
    assert result["c"].tolist() == [1, 2]


def test_cast_pandas_with_athena_types_ensures_mutability_once(monkeypatch):
    calls = []

    def fake_ensure(df):
        calls.append(df)
        return df

    monkeypatch.setattr(_data_types._arrow, "ensure_df_is_mutable", fake_ensure)
    df = pd.DataFrame({"a": ["1", "2"], "b": ["3", "4"], "c": ["5", "6"]})
    result = cast_pandas_with_athena_types(df, {"a": "int", "b": "int", "c": "bigint"})
    assert len(calls) == 1
    assert [str(result[c].dtype) for c in ("a", "b", "c")] == ["Int32", "Int32", "Int64"]


def test_cast_pandas_with_athena_types_does_not_touch_mutability_when_nothing_to_cast(monkeypatch):
    calls = []
    monkeypatch.setattr(_data_types._arrow, "ensure_df_is_mutable", lambda df: calls.append(df) or df)
    df = pd.DataFrame({"a": pd.array([1, 2], dtype="Int32")})
    cast_pandas_with_athena_types(df, {"a": "int", "missing": "int"})
    assert calls == []


def test_database_types_from_pandas_does_not_infer_overridden_columns():
    # A column of mixed types cannot be inferred, but must be ignored when the type is given explicitly.
    df = pd.DataFrame({"mixed": pd.Series([1, "a"], dtype=object), "a": [1, 2]})
    assert _db_types(df, dtype={"mixed": "VARCHAR(10)"}) == {"mixed": "VARCHAR(10)", "a": "BIGINT"}
    with pytest.raises(pa.ArrowInvalid):
        _db_types(df)


def test_database_types_from_pandas_index_comes_first():
    df = pd.DataFrame({"a": [1], "b": ["x"]}, index=pd.Index([10], name="idx"))
    assert list(_db_types(df, index=True)) == ["idx", "a", "b"]


def test_database_types_from_pandas_index_dtype_override():
    df = pd.DataFrame({"a": [1]}, index=pd.Index([10], name="idx"))
    result = _db_types(df, index=True, dtype={"idx": "SMALLINT"})
    assert result == {"idx": "SMALLINT", "a": "BIGINT"}
    assert list(result) == ["idx", "a"]


def test_database_types_from_pandas_errors_mention_the_column():
    df = pd.DataFrame({"col_x": ["x"]})
    with pytest.raises(exceptions.InvalidArgumentValue, match="'col_x'"):
        _db_types(df, dtype={"col_x": "INT;"})
    with pytest.raises(exceptions.InvalidArgumentValue, match="'col_x'"):
        _db_types(df, varchar_lengths={"col_x": 0})
    with pytest.raises(exceptions.InvalidArgumentValue, match="'col_x'"):
        _db_types(df, varchar_lengths_default="TEXT;")


def test_validate_database_type_accepts_nested_parentheses():
    for type_str in ("TEST(A(B))", "ARRAY(STRUCT(a INT, b DECIMAL(10,2)))", "T((A),(B))"):
        assert _data_types._validate_database_type("c", type_str) == type_str


def test_validate_database_type_rejects_unbalanced_nested_parentheses():
    for type_str in ("TEST(A(B)))", "TEST((A)", "TEST(A(B),C)D)"):
        with pytest.raises(exceptions.InvalidArgumentValue):
            _data_types._validate_database_type("c", type_str)


def test_validate_varchar_length_accepts_one():
    assert _data_types._validate_varchar_length("c", 1) == 1


# --------------------------------------------------------------------------------------
# process_not_inferred_dtype / process_not_inferred_array
# --------------------------------------------------------------------------------------


class _Unknown:
    pass


def _arrow_invalid_for(value, type_name):
    # Message produced by pyarrow versions that cannot infer the given Python type.
    return pa.ArrowInvalid(
        f"Could not convert {value!r} with type {type_name}: "
        "did not recognize Python value type when inferring an Arrow data type"
    )


def test_process_not_inferred_dtype_uuid_is_string():
    ex = _arrow_invalid_for(uuid.UUID("12345678-1234-5678-1234-567812345678"), "UUID")
    assert _data_types.process_not_inferred_dtype(ex) == pa.string()


def test_process_not_inferred_dtype_reraises_other_types():
    ex = _arrow_invalid_for(_Unknown(), "_Unknown")
    with pytest.raises(pa.ArrowInvalid) as exc:
        _data_types.process_not_inferred_dtype(ex)
    assert exc.value is ex


def test_process_not_inferred_dtype_reraises_unrelated_errors():
    ex = pa.ArrowInvalid("something else entirely")
    with pytest.raises(pa.ArrowInvalid) as exc:
        _data_types.process_not_inferred_dtype(ex)
    assert exc.value is ex


def test_process_not_inferred_array_converts_uuids_to_strings():
    values = [uuid.UUID("12345678-1234-5678-1234-567812345678"), None]
    ex = _arrow_invalid_for(values[0], "UUID")
    result = _data_types.process_not_inferred_array(ex, values=values)
    assert result.type == pa.string()
    assert result.to_pylist() == ["12345678-1234-5678-1234-567812345678", None]


def test_process_not_inferred_array_reraises_other_types():
    ex = _arrow_invalid_for(_Unknown(), "_Unknown")
    with pytest.raises(pa.ArrowInvalid) as exc:
        _data_types.process_not_inferred_array(ex, values=[_Unknown()])
    assert exc.value is ex
