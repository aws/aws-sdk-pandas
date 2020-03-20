import logging

import pandas as pd
import pyarrow as pa
import pytest

import awswrangler as wr
from awswrangler._data_types import athena_types_from_pandas, pyarrow_types_from_pandas  # noqa

from ._utils import get_type_df

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


def test_types_from_pandas():
    df = pd.DataFrame({"a": [None, None]})
    assert pyarrow_types_from_pandas(df=df, index=False)["a"] == pa.lib.null()
    with pytest.raises(wr.exceptions.UndetectedType):
        athena_types_from_pandas(df=df, index=False)

    df = get_type_df()

    columns_types = pyarrow_types_from_pandas(df=df, index=False)
    assert len(columns_types) == 20
    assert pa.types.is_int8(columns_types["int8"])
    assert pa.types.is_int16(columns_types["int16"])
    assert pa.types.is_int32(columns_types["int32"])
    assert pa.types.is_int32(columns_types["iint32"])
    assert pa.types.is_int64(columns_types["int64"])
    assert pa.types.is_int64(columns_types["iint64"])
    assert pa.types.is_float32(columns_types["float"])
    assert pa.types.is_float64(columns_types["double"])
    assert pa.types.is_decimal(columns_types["decimal"])
    assert pa.types.is_string(columns_types["string_object"])
    assert pa.types.is_string(columns_types["string"])
    assert pa.types.is_date(columns_types["date"])
    assert pa.types.is_timestamp(columns_types["timestamp"])
    assert pa.types.is_boolean(columns_types["bool"])
    assert pa.types.is_binary(columns_types["binary"])
    assert pa.types.is_dictionary(columns_types["category"])
    assert pa.types.is_list(columns_types["list"])
    assert pa.types.is_int64(columns_types["list"].value_type)
    assert pa.types.is_struct(columns_types["struct"])
    assert pa.types.is_int64(list(columns_types["struct"])[0].type)
    assert pa.types.is_list(columns_types["list_struct"])
    assert pa.types.is_struct(columns_types["list_struct"].value_type)
    assert pa.types.is_int64(list(columns_types["list_struct"].value_type)[0].type)
    assert pa.types.is_struct(columns_types["struct_list"])
    assert pa.types.is_list(list(columns_types["struct_list"])[0].type)
    assert pa.types.is_int64(list(columns_types["struct_list"])[0].type.value_type)

    columns_types = pyarrow_types_from_pandas(df=df, index=True, ignore_cols=["int32", "int64"])
    assert len(columns_types) == 19
    assert pa.types.is_int8(columns_types["int8"])
    assert pa.types.is_int16(columns_types["int16"])
    assert pa.types.is_int32(columns_types["iint32"])
    assert pa.types.is_int64(columns_types["iint64"])
    assert pa.types.is_float32(columns_types["float"])
    assert pa.types.is_float64(columns_types["double"])
    assert pa.types.is_decimal(columns_types["decimal"])
    assert pa.types.is_string(columns_types["string_object"])
    assert pa.types.is_string(columns_types["string"])
    assert pa.types.is_date(columns_types["date"])
    assert pa.types.is_timestamp(columns_types["timestamp"])
    assert pa.types.is_boolean(columns_types["bool"])
    assert pa.types.is_binary(columns_types["binary"])
    assert pa.types.is_dictionary(columns_types["category"])
    assert pa.types.is_list(columns_types["list"])
    assert pa.types.is_int64(columns_types["list"].value_type)
    assert pa.types.is_struct(columns_types["struct"])
    assert pa.types.is_int64(list(columns_types["struct"])[0].type)
    assert pa.types.is_list(columns_types["list_struct"])
    assert pa.types.is_struct(columns_types["list_struct"].value_type)
    assert pa.types.is_int64(list(columns_types["list_struct"].value_type)[0].type)
    assert pa.types.is_struct(columns_types["struct_list"])
    assert pa.types.is_list(list(columns_types["struct_list"])[0].type)
    assert pa.types.is_int64(list(columns_types["struct_list"])[0].type.value_type)
    assert pa.types.is_int64(columns_types["__index_level_0__"])

    columns_types = athena_types_from_pandas(df=df, index=False)
    assert len(columns_types) == 20
    assert columns_types["int8"] == "tinyint"
    assert columns_types["int16"] == "smallint"
    assert columns_types["int32"] == "int"
    assert columns_types["iint32"] == "int"
    assert columns_types["int64"] == "bigint"
    assert columns_types["iint64"] == "bigint"
    assert columns_types["float"] == "float"
    assert columns_types["double"] == "double"
    assert columns_types["decimal"] == "decimal(3,2)"
    assert columns_types["string_object"] == "string"
    assert columns_types["string"] == "string"
    assert columns_types["date"] == "date"
    assert columns_types["timestamp"] == "timestamp"
    assert columns_types["bool"] == "boolean"
    assert columns_types["binary"] == "binary"
    assert columns_types["category"] == "bigint"
    assert columns_types["list"] == "array<bigint>"
    assert columns_types["struct"] == "struct<a: bigint>"
    assert columns_types["list_struct"] == "array<struct<a: bigint>>"
    assert columns_types["struct_list"] == "struct<a: array<bigint>>"

    columns_types = athena_types_from_pandas(df=df, index=True, ignore_cols=["int32", "int64"])
    assert len(columns_types) == 19
    assert columns_types["int8"] == "tinyint"
    assert columns_types["int16"] == "smallint"
    assert columns_types["iint32"] == "int"
    assert columns_types["iint64"] == "bigint"
    assert columns_types["float"] == "float"
    assert columns_types["double"] == "double"
    assert columns_types["decimal"] == "decimal(3,2)"
    assert columns_types["string_object"] == "string"
    assert columns_types["string"] == "string"
    assert columns_types["date"] == "date"
    assert columns_types["timestamp"] == "timestamp"
    assert columns_types["bool"] == "boolean"
    assert columns_types["binary"] == "binary"
    assert columns_types["category"] == "bigint"
    assert columns_types["list"] == "array<bigint>"
    assert columns_types["struct"] == "struct<a: bigint>"
    assert columns_types["list_struct"] == "array<struct<a: bigint>>"
    assert columns_types["struct_list"] == "struct<a: array<bigint>>"
    assert columns_types["__index_level_0__"] == "bigint"
