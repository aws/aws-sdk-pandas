import datetime
import math
from decimal import Decimal

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from awswrangler import _data_types
from awswrangler._arrow import _df_to_table
from awswrangler._data_types import (
    athena2pandas,
    athena2pyarrow,
    cast_pandas_with_athena_types,
    pyarrow2athena,
    pyarrow2mysql,
    pyarrow_schema_from_pandas,
)
from awswrangler.athena._write_iceberg import _validate_athena_ddl_type as validate_athena_ddl_type
from awswrangler.exceptions import InvalidArgumentValue, UnsupportedType


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


@pytest.mark.parametrize("dtype", [pa.binary(), pa.large_binary(), pa.binary(16)])
def test_pyarrow2athena_binary_types(dtype):
    assert pyarrow2athena(dtype) == "binary"


@pytest.mark.parametrize(
    "dtype,expected",
    [
        (pa.uint8(), "TINYINT UNSIGNED"),
        (pa.uint16(), "SMALLINT UNSIGNED"),
        (pa.uint32(), "INTEGER UNSIGNED"),
        (pa.uint64(), "BIGINT UNSIGNED"),
    ],
)
def test_pyarrow2mysql_unsigned_types(dtype, expected):
    assert pyarrow2mysql(dtype, string_type="TEXT") == expected


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


@pytest.mark.parametrize(
    "type_str",
    [
        "INT",
        "DECIMAL(10,2)",
        "GEOMETRY(Point, 4326)",
        'text COLLATE "C"',
        '"MySchema"."MyType"',
        '"MySchema"."My,Type"',  # commas inside quoted identifiers are inert
        "ENUM('small','medium','large')",  # MySQL enum values require single quotes
        "SET('a','b','c')",
        "TIMESTAMP WITH TIME ZONE",
        "NUMBER(*,2)",  # Oracle star precision
        "[dbo].[MyType]",  # SQL Server bracket-quoted UDT
        "hierarchyid",
        "INT COMMENT 'it''s a col'",  # MySQL attribute with doubled-quote escaping
        "VARCHAR(256) ENCODE lzo",  # Redshift encoding attribute
        "DOUBLE(8,3) UNSIGNED",
        "int4range[]",
        "INTERVAL YEAR(4) TO MONTH",
    ],
)
def test_validate_database_type_accepts_legitimate_types(type_str):
    assert _data_types._validate_database_type("col", type_str) == type_str


@pytest.mark.parametrize(
    "type_str",
    [
        "INT); DROP TABLE users; --",
        "INT, evil INT REFERENCES other_table(id)",  # top-level comma: extra column defs
        "INT'",  # unbalanced single quote
        "INT DEFAULT '(', evil TEXT DEFAULT ')'",  # single-quote-hidden parens: comma is unquoted at depth 0
        'text COLLATE "C',  # unbalanced double quote
        "INT -- comment",
        "INT /* c */",
        "`INT`",
        "INT)",
        "(INT",
        "",
        "   ",
        None,
        5,
    ],
)
def test_validate_database_type_rejects_breakouts(type_str):
    with pytest.raises(InvalidArgumentValue):
        _data_types._validate_database_type("col", type_str)


def test_validate_varchar_length_integral_semantics():
    assert _data_types._validate_varchar_length("c", 256) == 256
    assert _data_types._validate_varchar_length("c", np.int64(256)) == 256  # pandas .str.len().max() idiom
    for bad in (True, "256); DROP TABLE t; --", 2.5, None):
        with pytest.raises(InvalidArgumentValue):
            _data_types._validate_varchar_length("c", bad)


def test_database_types_from_pandas_validates_dtype_overrides():
    df = pd.DataFrame({"c0": [1], "c1": ["x"]})
    with pytest.raises(InvalidArgumentValue):
        _data_types.database_types_from_pandas(
            df=df,
            index=False,
            dtype={"c0": "INT); DROP TABLE users; --"},
            varchar_lengths_default="VARCHAR(256)",
            varchar_lengths=None,
            converter_func=_data_types.pyarrow2postgresql,
        )


@pytest.mark.parametrize(
    "type_str",
    [
        "INT DEFAULT $$($$, evil text DEFAULT $$)$$",  # dollar-quoting depth manipulation (PG/Redshift)
        'INT DEFAULT "(", evil TEXT DEFAULT ")"',  # quote-hidden parens depth manipulation (MySQL)
        "VARCHAR($$)",
        "INT\x00",  # control characters surface as confusing driver errors downstream
        "INT\n, evil INT",
    ],
)
def test_validate_database_type_rejects_quoting_bypasses(type_str):
    with pytest.raises(InvalidArgumentValue):
        _data_types._validate_database_type("col", type_str)


def test_validate_varchar_length_max_and_positivity():
    assert _data_types._validate_varchar_length("c", "MAX") == "MAX"
    assert _data_types._validate_varchar_length("c", "max") == "max"
    for bad in (0, -1, np.int64(0)):
        with pytest.raises(InvalidArgumentValue):
            _data_types._validate_varchar_length("c", bad)


def test_database_types_from_pandas_validates_varchar_lengths_default():
    df = pd.DataFrame({"c1": ["x"]})
    with pytest.raises(InvalidArgumentValue):
        _data_types.database_types_from_pandas(
            df=df,
            index=False,
            dtype=None,
            varchar_lengths_default="VARCHAR(256)); DROP TABLE t; --",
            varchar_lengths=None,
            converter_func=_data_types.pyarrow2postgresql,
        )


@pytest.mark.parametrize(
    "type_str",
    [
        "int",
        "string",
        "decimal(38,2)",
        "array<struct<a:int,b_x:string>>",
        "map<string,int>",
        "struct<a:struct<b:decimal(10, 2)>>",
        "timestamp with time zone",
        "array<timestamp with time zone>",
        "double precision",
    ],
)
def test_validate_athena_ddl_type_accepts_grammar(type_str):
    assert validate_athena_ddl_type("col", type_str) == type_str


@pytest.mark.parametrize(
    "type_str",
    [
        "struct<a:int>) LOCATION 's3://evil'",  # report payload: quote + unbalanced paren
        "int) LOCATION (x",  # all-legal charset, unbalanced brackets
        "struct<a>b:int>",  # field name broke the angle grammar at build time
        "int; DROP TABLE t",
        "int -- comment",
        "`int`",
        'struct<a:int> TBLPROPERTIES ("k"="v")',
        "int\n",
        "",
        None,
        123,
    ],
)
def test_validate_athena_ddl_type_rejects_breakouts(type_str):
    with pytest.raises(InvalidArgumentValue):
        validate_athena_ddl_type("col", type_str)


def test_struct_field_name_breakout_is_caught_at_ddl_emission():
    # Data-derived field names flow through pyarrow2athena unvalidated (read paths must
    # accept arbitrary existing data); the DDL emitter is the enforcement point.
    malicious = pa.struct([pa.field("a>b:int,x", pa.int64())])
    built = pyarrow2athena(malicious)
    with pytest.raises(InvalidArgumentValue):
        validate_athena_ddl_type("col", built)


@pytest.mark.parametrize(
    "type_str",
    [
        "int, extra int",  # top-level comma: extra column definition
        "(<)>",  # interleaved brackets defeat independent counters
        "array<int)",
    ],
)
def test_validate_athena_ddl_type_rejects_structural_injection(type_str):
    with pytest.raises(InvalidArgumentValue):
        validate_athena_ddl_type("col", type_str)


def test_nested_decimal_dtype_casts_strings_to_decimal() -> None:
    df = pd.DataFrame(
        {
            "id": [1],
            "tax": [
                [
                    {
                        "a": "foo",
                        "b": "bar",
                        "c": "12.345678",
                    }
                ]
            ],
        }
    )
    dtype = {
        "id": "int",
        "tax": "array<struct<a:string,b:string,c:decimal(26,6)>>",
    }

    df = cast_pandas_with_athena_types(df=df, dtype=dtype)
    schema = pyarrow_schema_from_pandas(df=df, index=False, dtype=dtype)
    table = _df_to_table(df=df, schema=schema, index=False, dtype=dtype)

    assert table.column("tax").to_pylist() == [[{"a": "foo", "b": "bar", "c": Decimal("12.345678")}]]


def test_nested_decimal_cast_preserves_mixed_case_struct_fields() -> None:
    df = pd.DataFrame({"value": [{"Name": "x", "Amount": "1.5"}]})

    result = cast_pandas_with_athena_types(
        df=df,
        dtype={"value": "struct<Name:string,Amount:decimal(10,2)>"},
    )

    assert result["value"].iloc[0] == {"Name": "x", "Amount": Decimal("1.5")}


def test_nested_decimal_cast_preserves_undeclared_struct_fields() -> None:
    df = pd.DataFrame({"value": [{"a": 1, "extra": 2}]})

    result = cast_pandas_with_athena_types(df=df, dtype={"value": "struct<a:int>"})

    assert result["value"].iloc[0] == {"a": 1, "extra": 2}


def test_nested_decimal_cast_preserves_nan_in_non_decimal_leaf() -> None:
    nan = float("nan")
    inf = float("inf")
    df = pd.DataFrame({"value": [[1.0, nan, inf]]})

    result = cast_pandas_with_athena_types(df=df, dtype={"value": "array<double>"})
    values = result["value"].iloc[0]

    assert type(values) is list
    assert values[0] == 1.0
    assert values[1] is nan
    assert values[2] is inf and math.isinf(values[2])
    assert result["value"].dtype == df["value"].dtype


def test_nested_decimal_casts_array_decimal_values_and_nulls() -> None:
    existing_decimal = Decimal("2.50")
    values = ["1.25", existing_decimal, None, pd.NA, np.nan, np.inf]

    result = cast_pandas_with_athena_types(
        df=pd.DataFrame({"value": [values]}),
        dtype={"value": "array<decimal(10,2)>"},
    )
    cast_values = result["value"].iloc[0]

    assert type(cast_values) is list
    assert cast_values[0] == Decimal("1.25")
    assert cast_values[1] is existing_decimal
    assert cast_values[2:] == [None, None, None, None]


def test_nested_decimal_cast_preserves_tuple_for_array_decimal() -> None:
    values = ("1.25", Decimal("2.50"))

    result = _data_types._cast_nested_athena_value(values, "array<decimal(10,2)>")

    assert type(result) is tuple
    assert result == (Decimal("1.25"), Decimal("2.50"))

    df = pd.DataFrame({"value": [values]})
    cast_df = cast_pandas_with_athena_types(df, {"value": "array<decimal(10,2)>"})
    assert type(cast_df["value"].iloc[0]) is tuple


def test_nested_decimal_casts_multiple_fields_in_nested_struct_preserving_order() -> None:
    value = {"before": 7, "nested": {"First": "1.25", "Second": "2.50", "extra": True}, "after": "tail"}
    dtype = "struct<before:int,nested:struct<First:decimal(10,2),Second:decimal(10,2)>,after:string>"

    result = _data_types._cast_nested_athena_value(value, dtype)

    assert list(result) == ["before", "nested", "after"]
    assert list(result["nested"]) == ["First", "Second", "extra"]
    assert result["before"] == 7 and type(result["before"]) is int
    assert result["nested"] == {"First": Decimal("1.25"), "Second": Decimal("2.50"), "extra": True}
    assert result["after"] == "tail"


def test_nested_decimal_casts_decimal_inside_map() -> None:
    value = {"first": "1.25", "second": "2.50"}

    result = _data_types._cast_nested_athena_value(value, "map<string,decimal(10,2)>")

    assert type(result) is dict
    assert list(result) == ["first", "second"]
    assert result == {"first": Decimal("1.25"), "second": Decimal("2.50")}


def test_nested_decimal_casts_decimal_inside_map_array() -> None:
    value = {"amounts": ["1.25", None, "2.50"]}

    result = _data_types._cast_nested_athena_value(value, "map<string,array<decimal(10,2)>>")

    assert type(result) is dict
    assert type(result["amounts"]) is list
    assert result == {"amounts": [Decimal("1.25"), None, Decimal("2.50")]}


def test_nested_decimal_cast_preserves_map_pair_container_types() -> None:
    value = [["first", "1.25"], ("second", "2.50")]

    result = _data_types._cast_nested_athena_value(value, "map<string,decimal(10,2)>")

    assert type(result) is list
    assert type(result[0]) is list
    assert type(result[1]) is tuple
    assert result == [["first", Decimal("1.25")], ("second", Decimal("2.50"))]


def test_nested_decimal_cast_converts_nan_and_inf_only_for_decimal_fields() -> None:
    value = {
        "nan": np.nan,
        "inf": np.inf,
        "decimal_nan": Decimal("NaN"),
        "decimal_inf": Decimal("Infinity"),
        "value": None,
    }

    result = _data_types._cast_nested_athena_value(
        value,
        "struct<nan:decimal(10,2),inf:decimal(10,2),decimal_nan:decimal(10,2),"
        "decimal_inf:decimal(10,2),value:decimal(10,2)>",
    )

    assert result == {"nan": None, "inf": None, "decimal_nan": None, "decimal_inf": None, "value": None}


def test_nested_non_decimal_values_and_containers_preserve_identity() -> None:
    nan = float("nan")
    inf = float("inf")
    value = {
        "text": "text",
        "integer": 3,
        "float": 1.5,
        "boolean": True,
        "decimal": Decimal("4.25"),
        "decimal_nonfinite": Decimal("Infinity"),
        "none": None,
        "missing": pd.NA,
        "nan": nan,
        "inf": inf,
        "list": [1, 2],
        "tuple": ("x", 4),
        "dict": {"nested": False},
    }
    dtype = (
        "struct<text:string,integer:int,float:double,boolean:boolean,decimal:string,decimal_nonfinite:string,"
        "none:string,missing:string,"
        "nan:double,inf:double,list:array<int>,tuple:array<string>,dict:map<string,boolean>>"
    )

    result = _data_types._cast_nested_athena_value(value, dtype)

    assert result is value
    assert list(result) == list(value)
    assert result["text"] is value["text"]
    assert result["integer"] is value["integer"]
    assert result["float"] is value["float"]
    assert result["boolean"] is value["boolean"]
    assert result["decimal"] is value["decimal"]
    assert result["decimal_nonfinite"] is value["decimal_nonfinite"]
    assert result["none"] is None
    assert result["missing"] is pd.NA
    assert result["nan"] is nan and math.isnan(result["nan"])
    assert result["inf"] is inf and math.isinf(result["inf"])
    assert result["list"] is value["list"]
    assert result["tuple"] is value["tuple"]
    assert result["dict"] is value["dict"]

    df = pd.DataFrame({"value": [value]})
    cast_df = cast_pandas_with_athena_types(df, {"value": dtype})
    assert cast_df["value"].iloc[0] is value
    assert cast_df["value"].dtype == df["value"].dtype


def test_nested_decimal_cast_preserves_array_values_when_type_is_malformed_or_unsupported() -> None:
    values = ["1.25", None]
    struct_value = {"amount": "1.25", "extra": 3}
    map_value = {"amount": "1.25"}

    assert _data_types._cast_nested_athena_value(values, "array<decimal(10,2)") is values
    assert _data_types._cast_nested_athena_value(values, "array<unsupported>") is values
    assert _data_types._cast_nested_athena_value(struct_value, "struct<amount:unsupported>") is struct_value
    assert _data_types._cast_nested_athena_value(map_value, "map<string,unsupported>") is map_value


@pytest.mark.parametrize(
    "value,athena_type,error",
    [
        ({"a": 1}, "struct<a>", ValueError),
        ({"a": 1}, "map<string>", RuntimeError),
    ],
)
def test_nested_decimal_cast_preserves_errors_for_malformed_types(value, athena_type, error) -> None:
    with pytest.raises(error):
        _data_types._cast_nested_athena_value(value, athena_type)
