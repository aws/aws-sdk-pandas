from datetime import datetime
from decimal import Decimal

import pandas as pd

ts = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")  # noqa
dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()  # noqa

CFN_VALID_STATUS = ["CREATE_COMPLETE", "ROLLBACK_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]


def get_df():
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


def get_df_list():
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


def get_df_cast():
    df = pd.DataFrame(
        {
            "iint8": [None, None, None],
            "iint16": [None, None, None],
            "iint32": [None, None, None],
            "iint64": [None, None, None],
            "float": [None, None, None],
            "double": [None, None, None],
            "decimal": [None, None, None],
            "string": [None, None, None],
            "date": [None, None, dt("2020-01-02")],
            "timestamp": [None, None, None],
            "bool": [True, None, None],
            "binary": [None, None, None],
            "category": [None, None, None],
            "par0": [1, 1, 2],
            "par1": ["a", "b", "b"],
        }
    )
    return df


def get_df_csv():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "string_object": ["foo", None, "boo"],
            "string": ["foo", None, "boo"],
            "float": [1.0, None, 2.0],
            "int": [1, None, 2],
            "date": [dt("2020-01-01"), None, dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), None, ts("2020-01-02 00:00:01.0")],
            "bool": [True, None, False],
            "par0": [1, 1, 2],
            "par1": ["a", "b", "b"],
        }
    )
    df["string"] = df["string"].astype("string")
    df["int"] = df["int"].astype("Int64")
    df["par1"] = df["par1"].astype("string")
    return df


def get_df_category():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "string_object": ["foo", None, "boo"],
            "string": ["foo", None, "boo"],
            "binary": [b"1", None, b"2"],
            "float": [1.0, None, 2.0],
            "int": [1, None, 2],
            "par0": [1, 1, 2],
            "par1": ["a", "b", "b"],
        }
    )
    df["string"] = df["string"].astype("string")
    df["int"] = df["int"].astype("Int64")
    df["par1"] = df["par1"].astype("string")
    return df


def get_query_long():
    return """
SELECT
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand()
        """


def ensure_data_types(df, has_list=False):
    if "iint8" in df.columns:
        assert str(df["iint8"].dtype).startswith("Int")
    assert str(df["iint16"].dtype).startswith("Int")
    assert str(df["iint32"].dtype).startswith("Int")
    assert str(df["iint64"].dtype) == "Int64"
    assert str(df["float"].dtype).startswith("float")
    assert str(df["double"].dtype) == "float64"
    assert str(df["decimal"].dtype) == "object"
    if "string_object" in df.columns:
        assert str(df["string_object"].dtype) == "string"
    assert str(df["string"].dtype) == "string"
    assert str(df["date"].dtype) == "object"
    assert str(df["timestamp"].dtype) == "datetime64[ns]"
    assert str(df["bool"].dtype) in ("boolean", "Int64", "object")
    if "binary" in df.columns:
        assert str(df["binary"].dtype) == "object"
    assert str(df["category"].dtype) == "float64"
    if has_list is True:
        assert str(df["list"].dtype) == "object"
        assert str(df["list_list"].dtype) == "object"
    if "__index_level_0__" in df.columns:
        assert str(df["__index_level_0__"].dtype) == "Int64"
    assert str(df["par0"].dtype) in ("Int64", "category")
    assert str(df["par1"].dtype) in ("string", "category")
    row = df[df["iint16"] == 1]
    if not row.empty:
        row = row.iloc[0]
        assert str(type(row["decimal"]).__name__) == "Decimal"
        assert str(type(row["date"]).__name__) == "date"
        if "binary" in df.columns:
            assert str(type(row["binary"]).__name__) == "bytes"
        if has_list is True:
            assert str(type(row["list"][0]).__name__) == "int64"
            assert str(type(row["list_list"][0][0]).__name__) == "int64"


def ensure_data_types_category(df):
    assert len(df.columns) in (7, 8)
    assert str(df["id"].dtype) in ("category", "Int64")
    assert str(df["string_object"].dtype) == "category"
    assert str(df["string"].dtype) == "category"
    if "binary" in df.columns:
        assert str(df["binary"].dtype) == "category"
    assert str(df["float"].dtype) == "category"
    assert str(df["int"].dtype) in ("category", "Int64")
    assert str(df["par0"].dtype) in ("category", "Int64")
    assert str(df["par1"].dtype) == "category"


def ensure_data_types_csv(df):
    if "__index_level_0__" in df:
        assert str(df["__index_level_0__"].dtype).startswith("Int")
    assert str(df["id"].dtype).startswith("Int")
    if "string_object" in df:
        assert str(df["string_object"].dtype) == "string"
    if "string" in df:
        assert str(df["string"].dtype) == "string"
    if "float" in df:
        assert str(df["float"].dtype).startswith("float")
    if "int" in df:
        assert str(df["int"].dtype).startswith("Int")
    assert str(df["date"].dtype) == "object"
    assert str(df["timestamp"].dtype).startswith("datetime")
    if "bool" in df:
        assert str(df["bool"].dtype) == "boolean"
    if "par0" in df:
        assert str(df["par0"].dtype).startswith("Int")
    if "par1" in df:
        assert str(df["par1"].dtype) == "string"
