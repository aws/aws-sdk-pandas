import logging
from datetime import datetime
from decimal import Decimal

import pandas as pd
import pyarrow as pa

from awswrangler._data_types import athena_types_from_pandas, pyarrow_types_from_pandas  # noqa

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)

ts = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")  # noqa
dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()  # noqa


def test_types_from_pandas():
    df = pd.DataFrame(
        {
            "int32": [1, 2],
            "Int32": [1, 2],
            "int64": [1, 2],
            "Int64": [1, 2],
            "double": [0.0, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), Decimal((0, (1, 9, 0), -2))],
            "string_object": ["foo", "boo"],
            "string": ["foo", "boo"],
            "date": [dt("2020-01-01"), dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0")],
            "bool": [True, False],
        }
    )
    df["int32"] = df["int32"].astype("int32")
    df["Int32"] = df["Int32"].astype("Int32")
    df["Int64"] = df["Int64"].astype("Int64")
    df["string"] = df["string"].astype("string")
    columns_types = pyarrow_types_from_pandas(df=df, index=False)
    assert len(columns_types) == 11
    assert columns_types["int32"] == pa.lib.int32()
    assert columns_types["Int32"] == pa.lib.int32()
    assert columns_types["int64"] == pa.lib.int64()
    assert columns_types["Int64"] == pa.lib.int64()
    assert columns_types["double"] == pa.lib.float64()
    assert columns_types["decimal"] == pa.lib.decimal128(3, 2)
    assert columns_types["string_object"] == pa.lib.string()
    assert columns_types["string"] == pa.lib.string()
    assert columns_types["date"] == pa.lib.date32()
    assert columns_types["timestamp"] == pa.lib.timestamp(unit="ns")
    assert columns_types["bool"] == pa.lib.bool_()
    columns_types = pyarrow_types_from_pandas(df=df, index=True, ignore_cols=["int32", "int64"])
    assert len(columns_types) == 10
    assert columns_types["Int32"] == pa.lib.int32()
    assert columns_types["Int64"] == pa.lib.int64()
    assert columns_types["double"] == pa.lib.float64()
    assert columns_types["decimal"] == pa.lib.decimal128(3, 2)
    assert columns_types["string_object"] == pa.lib.string()
    assert columns_types["string"] == pa.lib.string()
    assert columns_types["date"] == pa.lib.date32()
    assert columns_types["timestamp"] == pa.lib.timestamp(unit="ns")
    assert columns_types["bool"] == pa.lib.bool_()
    assert columns_types["__index_level_0__"] == pa.lib.int64()
    columns_types = athena_types_from_pandas(df=df, index=False)
    assert len(columns_types) == 11
    assert columns_types["int32"] == "int"
    assert columns_types["Int32"] == "int"
    assert columns_types["int64"] == "bigint"
    assert columns_types["Int64"] == "bigint"
    assert columns_types["double"] == "double"
    assert columns_types["decimal"] == "decimal(3,2)"
    assert columns_types["string_object"] == "string"
    assert columns_types["string"] == "string"
    assert columns_types["date"] == "date"
    assert columns_types["timestamp"] == "timestamp"
    assert columns_types["bool"] == "boolean"
    columns_types = athena_types_from_pandas(df=df, index=True, ignore_cols=["int32", "int64"])
    assert len(columns_types) == 10
    assert columns_types["Int32"] == "int"
    assert columns_types["Int64"] == "bigint"
    assert columns_types["double"] == "double"
    assert columns_types["decimal"] == "decimal(3,2)"
    assert columns_types["string_object"] == "string"
    assert columns_types["string"] == "string"
    assert columns_types["date"] == "date"
    assert columns_types["timestamp"] == "timestamp"
    assert columns_types["bool"] == "boolean"
    assert columns_types["__index_level_0__"] == "bigint"
