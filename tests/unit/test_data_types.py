import pyarrow as pa
import pytest

from awswrangler._data_types import athena2pandas, athena2pyarrow, pyarrow2mysql
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
