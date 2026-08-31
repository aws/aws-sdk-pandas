from decimal import Decimal

import pyarrow as pa
import pytest

import awswrangler.pandas as pd
from awswrangler._arrow import _df_to_table
from awswrangler._data_types import (
    athena2pandas,
    athena2pyarrow,
    cast_pandas_with_athena_types,
    pyarrow_schema_from_pandas,
)
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
