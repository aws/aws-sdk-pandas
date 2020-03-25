from datetime import datetime
from decimal import Decimal

import pandas as pd

ts = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")  # noqa
dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()  # noqa


def get_type_df():
    df = pd.DataFrame(
        {
            "int8": [1, 2],
            "int16": [1, 2],
            "int32": [1, 2],
            "iint32": [1, 2],
            "int64": [1, 2],
            "iint64": [1, 2],
            "float": [0.0, 1.1],
            "double": [0.0, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), Decimal((0, (1, 9, 0), -2))],
            "string_object": ["foo", "boo"],
            "string": ["foo", "boo"],
            "date": [dt("2020-01-01"), dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0")],
            "bool": [True, False],
            "binary": [b"0", b"1"],
            "category": [1, 2],
            "list": [[1, 2], [3, 4]],
            "struct": [{"a": 1}, {"a": 2}],
            "list_struct": [[{"a": 1}, {"a": 2}], [{"a": 3}, {"a": 4}]],
            "struct_list": [{"a": [1, 2]}, {"a": [3, 4]}],
        }
    )
    df["int8"] = df["int8"].astype("int8")
    df["int16"] = df["int16"].astype("int16")
    df["int32"] = df["int32"].astype("int32")
    df["iint32"] = df["iint32"].astype("Int32")
    df["iint64"] = df["iint64"].astype("Int64")
    df["float"] = df["float"].astype("float32")
    df["string"] = df["string"].astype("string")
    df["category"] = df["category"].astype("category")
    return df


def get_parquet_df():
    df = pd.DataFrame(
        {
            "int8": [1, 2],
            "int16": [1, 2],
            "int32": [1, 2],
            "iint32": [1, 2],
            "int64": [1, 2],
            "iint64": [1, 2],
            "float": [0.0, 1.1],
            "double": [0.0, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), Decimal((0, (1, 9, 0), -2))],
            "string_object": ["foo", "boo"],
            "string": ["foo", "boo"],
            "date": [dt("2020-01-01"), dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0")],
            "bool": [True, False],
            "binary": [b"0", b"1"],
            "category": [1, 2],
            "list": [[1, 2], [3, 4]],
            "list_list": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
        }
    )
    df["int8"] = df["int8"].astype("int8")
    df["int16"] = df["int16"].astype("int16")
    df["int32"] = df["int32"].astype("int32")
    df["iint32"] = df["iint32"].astype("Int32")
    df["iint64"] = df["iint64"].astype("Int64")
    df["float"] = df["float"].astype("float32")
    df["string"] = df["string"].astype("string")
    df["category"] = df["category"].astype("category")
    return df


def get_athena_df():
    df = pd.DataFrame(
        {
            "iint8": [1, None, 2],
            "iint16": [1, None, 2],
            "iint32": [1, None, 2],
            "iint64": [1, None, 2],
            "float": [0.0, None, 1.1],
            "double": [0.0, None, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "string_object": ["foo", None, "boo"],
            "string": ["foo", None, "boo"],
            "date": [dt("2020-01-01"), None, dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), None, ts("2020-01-02 00:00:01.0")],
            "bool": [True, None, False],
            "binary": [b"0", None, b"1"],
            "category": [1.0, None, 2.0],
            "par0": [1, 1, 2],
            "par1": ["a", "b", "b"],
        }
    )
    df["iint8"] = df["iint8"].astype("Int8")
    df["iint16"] = df["iint16"].astype("Int16")
    df["iint32"] = df["iint32"].astype("Int32")
    df["iint64"] = df["iint64"].astype("Int64")
    df["float"] = df["float"].astype("float32")
    df["string"] = df["string"].astype("string")
    df["category"] = df["category"].astype("category")
    return df


def get_athena_ctas_df():
    df = pd.DataFrame(
        {
            "iint8": [1, None, 2],
            "iint16": [1, None, 2],
            "iint32": [1, None, 2],
            "iint64": [1, None, 2],
            "float": [0.0, None, 1.1],
            "double": [0.0, None, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "string_object": ["foo", None, "boo"],
            "string": ["foo", None, "boo"],
            "date": [dt("2020-01-01"), None, dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), None, ts("2020-01-02 00:00:01.0")],
            "bool": [True, None, False],
            "binary": [b"0", None, b"1"],
            "category": [1.0, None, 2.0],
            "list": [[1, 2], None, [3, 4]],
            "list_list": [[[1, 2], [3, 4]], None, [[5, 6], [7, 8]]],
            "par0": [1, 1, 2],
            "par1": ["a", "b", "b"],
        }
    )
    df["iint8"] = df["iint8"].astype("Int8")
    df["iint16"] = df["iint16"].astype("Int16")
    df["iint32"] = df["iint32"].astype("Int32")
    df["iint64"] = df["iint64"].astype("Int64")
    df["float"] = df["float"].astype("float32")
    df["string"] = df["string"].astype("string")
    df["category"] = df["category"].astype("category")
    return df
