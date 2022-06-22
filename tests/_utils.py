import random
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterator

import boto3
import botocore.exceptions
import pandas as pd

import awswrangler as wr
from awswrangler._utils import try_it

ts = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")  # noqa
dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()  # noqa

CFN_VALID_STATUS = ["CREATE_COMPLETE", "ROLLBACK_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]


def get_df(governed=False):
    df = pd.DataFrame(
        {
            "iint8": [1, None, 2],
            "iint16": [1, None, 2],
            "iint32": [1, None, 2],
            "iint64": [1, None, 2],
            "float": [0.0, None, 1.1],
            "ddouble": [0.0, None, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "string_object": ["foo", None, "boo"],
            "string": ["Seattle", None, "Washington"],
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

    if governed:
        df = df.drop(["iint8", "binary"], axis=1)  # tinyint & binary currently not supported
    return df


def get_df_list(governed=False):
    df = pd.DataFrame(
        {
            "iint8": [1, None, 2],
            "iint16": [1, None, 2],
            "iint32": [1, None, 2],
            "iint64": [1, None, 2],
            "float": [0.0, None, 1.1],
            "ddouble": [0.0, None, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "string_object": ["foo", None, "boo"],
            "string": ["foo", None, "boo"],
            "date": [dt("2020-01-01"), None, dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), None, ts("2020-01-02 00:00:01.0")],
            "timestamp2": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:00:01.0")],
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

    if governed:
        df = (df.drop(["iint8", "binary"], axis=1),)  # tinyint & binary currently not supported
    return df


def get_df_cast(governed=False):
    df = pd.DataFrame(
        {
            "iint8": [None, None, None],
            "iint16": [None, None, None],
            "iint32": [None, None, None],
            "iint64": [None, None, None],
            "float": [None, None, None],
            "ddouble": [None, None, None],
            "decimal": [None, None, None],
            "string": [None, None, None],
            "date": [None, None, dt("2020-01-02")],
            "timestamp": [None, None, None],
            "timestamp2": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:00:01.0")],
            "bool": [True, None, None],
            "binary": [None, None, None],
            "category": [None, None, None],
            "par0": [1, 1, 2],
            "par1": ["a", "b", "b"],
        }
    )
    if governed:
        df = (df.drop(["iint8", "binary"], axis=1),)  # tinyint & binary currently not supported
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


def get_df_txt():
    df = pd.DataFrame(
        {
            "col_name": [
                "iint8               ",
                "iint16              ",
                "iint32              ",
                "par0                ",
                "par1                ",
                "",
                "# Partition Information",
                "# col_name            ",
                "",
                "par0                ",
                "par1                ",
            ],
            "data_type": [
                "tinyint             ",
                "smallint            ",
                "int                 ",
                "bigint              ",
                "string              ",
                " ",
                " ",
                "data_type           ",
                " ",
                "bigint              ",
                "string              ",
            ],
            "comment": [
                "                    ",
                "                    ",
                "                    ",
                "                    ",
                "                    ",
                "                    ",
                " ",
                "comment             ",
                " ",
                "                    ",
                "                    ",
            ],
        }
    )
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


def get_df_quicksight():
    df = pd.DataFrame(
        {
            "iint8": [1, None, 2],
            "iint16": [1, None, 2],
            "iint32": [1, None, 2],
            "iint64": [1, None, 2],
            "float": [0.0, None, 1.1],
            "ddouble": [0.0, None, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "string_object": ["foo", None, "boo"],
            "string": ["foo", None, "boo"],
            "date": [dt("2020-01-01"), None, dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), None, ts("2020-01-02 00:00:01.0")],
            "bool": [True, None, False],
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
    assert str(df["ddouble"].dtype) == "float64"
    assert str(df["decimal"].dtype) in ("object", "float64")
    if "string_object" in df.columns:
        assert str(df["string_object"].dtype) == "string"
    assert str(df["string"].dtype) == "string"
    assert str(df["date"].dtype) in ("object", "O", "datetime64[ns]")
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


def ensure_data_types_csv(df, governed=False):
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
    if governed:
        assert str(df["date"].dtype).startswith("datetime")
    else:
        assert str(df["date"].dtype) == "object"
    assert str(df["timestamp"].dtype).startswith("datetime")
    if "bool" in df:
        assert str(df["bool"].dtype) == "boolean"
    if "par0" in df:
        assert str(df["par0"].dtype).startswith("Int")
    if "par1" in df:
        assert str(df["par1"].dtype) == "string"


def ensure_athena_ctas_table(ctas_query_info: Dict[str, Any], boto3_session: boto3.Session) -> None:
    query_metadata = (
        wr.athena._utils._get_query_metadata(
            query_execution_id=ctas_query_info["ctas_query_id"], boto3_session=boto3_session
        )
        if "ctas_query_id" in ctas_query_info
        else ctas_query_info["ctas_query_metadata"]
    )
    assert query_metadata.raw_payload["Status"]["State"] == "SUCCEEDED"
    wr.catalog.delete_table_if_exists(database=ctas_query_info["ctas_database"], table=ctas_query_info["ctas_table"])


def ensure_athena_query_metadata(df, ctas_approach=True, encrypted=False):
    assert df.query_metadata is not None
    assert isinstance(df.query_metadata, dict)
    assert df.query_metadata["QueryExecutionId"] is not None
    assert df.query_metadata["Query"] is not None
    assert df.query_metadata["StatementType"] is not None
    if encrypted:
        assert df.query_metadata["ResultConfiguration"]["EncryptionConfiguration"]
    assert df.query_metadata["QueryExecutionContext"] is not None
    assert df.query_metadata["Status"]["SubmissionDateTime"] is not None
    assert df.query_metadata["Status"]["CompletionDateTime"] is not None
    assert df.query_metadata["Statistics"] is not None
    assert df.query_metadata["WorkGroup"] is not None
    assert df.query_metadata["ResultConfiguration"]["OutputLocation"] is not None
    if ctas_approach:
        assert df.query_metadata["Statistics"]["DataManifestLocation"] is not None


def get_time_str_with_random_suffix() -> str:
    time_str = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    return f"{time_str}_{random.randrange(16**6):06x}"


def path_generator(bucket: str) -> Iterator[str]:
    s3_path = f"s3://{bucket}/{get_time_str_with_random_suffix()}/"
    print(f"S3 Path: {s3_path}")
    objs = wr.s3.list_objects(s3_path)
    wr.s3.delete_objects(path=objs)
    yield s3_path
    objs = wr.s3.list_objects(s3_path)
    wr.s3.delete_objects(path=objs)


def extract_cloudformation_outputs():
    outputs = {}
    client = boto3.client("cloudformation")
    response = try_it(client.describe_stacks, botocore.exceptions.ClientError, max_num_tries=5)
    for stack in response.get("Stacks"):
        if (
            stack["StackName"]
            in ["aws-data-wrangler-base", "aws-data-wrangler-databases", "aws-data-wrangler-opensearch"]
        ) and (stack["StackStatus"] in CFN_VALID_STATUS):
            for output in stack.get("Outputs"):
                outputs[output.get("OutputKey")] = output.get("OutputValue")
    return outputs


def list_workgroups():
    client = boto3.client("athena")
    attempt = 1
    while True:
        try:
            return client.list_work_groups()
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] != "ThrottlingException":
                raise ex
            if attempt > 5:
                raise ex
            time.sleep(attempt + random.randrange(start=0, stop=3, step=1))


def validate_workgroup_key(workgroup):
    if "ResultConfiguration" in workgroup["Configuration"]:
        if "EncryptionConfiguration" in workgroup["Configuration"]["ResultConfiguration"]:
            if "KmsKey" in workgroup["Configuration"]["ResultConfiguration"]["EncryptionConfiguration"]:
                kms_client = boto3.client("kms")
                key = try_it(
                    kms_client.describe_key,
                    kms_client.exceptions.NotFoundException,
                    KeyId=workgroup["Configuration"]["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"],
                )["KeyMetadata"]
                if key["KeyState"] != "Enabled":
                    return False
    return True


def create_workgroup(wkg_name, config):
    client = boto3.client("athena")
    wkgs = list_workgroups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    deleted = False
    if wkg_name in wkgs:
        wkg = try_it(client.get_work_group, botocore.exceptions.ClientError, max_num_tries=5, WorkGroup=wkg_name)[
            "WorkGroup"
        ]
        if validate_workgroup_key(workgroup=wkg) is False:
            client.delete_work_group(WorkGroup=wkg_name, RecursiveDeleteOption=True)
            deleted = True
    if wkg_name not in wkgs or deleted is True:
        client.create_work_group(
            Name=wkg_name,
            Configuration=config,
            Description=f"AWS Data Wrangler Test - {wkg_name}",
        )
    return wkg_name
