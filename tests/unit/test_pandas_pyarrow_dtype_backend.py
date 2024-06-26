from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import assert_pandas_equals, get_df_dtype_backend, is_pandas_2_x

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = [
    pytest.mark.distributed,
    pytest.mark.skipif(condition=not is_pandas_2_x, reason="PyArrow backed types are only supported in Pandas 2.x"),
]


def test_s3_read_parquet(path: str) -> None:
    df = get_df_dtype_backend(dtype_backend="pyarrow")
    wr.s3.to_parquet(df=df, path=f"{path}.parquet", index=False)

    df2 = wr.s3.read_parquet(path=path, dtype_backend="pyarrow")

    assert_pandas_equals(df, df2)


def test_s3_read_parquet_table(path: str, glue_database: str, glue_table: str) -> None:
    df = get_df_dtype_backend(dtype_backend="pyarrow")
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)

    df2 = wr.s3.read_parquet_table(database=glue_database, table=glue_table, dtype_backend="pyarrow")

    assert_pandas_equals(df, df2)


def test_s3_read_csv(path: str) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    wr.s3.to_csv(df=df, path=f"{path}.csv", index=False)

    df.id = df.id.astype(pd.ArrowDtype(pa.int64()))
    df.val = df.val.astype(pd.ArrowDtype(pa.string()))

    df2 = wr.s3.read_csv(path=path, dtype_backend="pyarrow")

    assert_pandas_equals(df, df2)


def test_s3_read_json(path: str) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    wr.s3.to_json(df=df, path=f"{path}.json", orient="records", lines=True)

    df.id = df.id.astype(pd.ArrowDtype(pa.int64()))
    df.val = df.val.astype(pd.ArrowDtype(pa.string()))

    df2 = wr.s3.read_json(path=path, dtype_backend="pyarrow", orient="records", lines=True)

    assert_pandas_equals(df, df2)


def test_s3_select(path: str) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    wr.s3.to_parquet(df=df, path=f"{path}.parquet", index=False)

    df.id = df.id.astype(pd.ArrowDtype(pa.int64()))
    df.val = df.val.astype(pd.ArrowDtype(pa.string()))

    df2 = wr.s3.select_query(
        sql="select * from s3object",
        path=path,
        input_serialization="Parquet",
        input_serialization_params={},
        dtype_backend="pyarrow",
    )

    assert_pandas_equals(df, df2)


@pytest.mark.parametrize("ctas_approach,unload_approach", [(False, False), (True, False), (False, True)])
def test_athena_csv_dtype_backend(
    path: str, path2: str, glue_table: str, glue_database: str, ctas_approach: bool, unload_approach: bool
) -> None:
    df = get_df_dtype_backend(dtype_backend="pyarrow")
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        index=False,
    )
    df2 = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        dtype_backend="pyarrow",
        ctas_approach=ctas_approach,
        unload_approach=unload_approach,
        s3_output=path2,
    )

    if not ctas_approach and not unload_approach:
        df["string_nullable"] = df["string_nullable"].astype("string[pyarrow]")

    if ctas_approach or unload_approach:
        df2["string_nullable"] = df2["string_nullable"].replace("", pa.NA)

    assert_pandas_equals(df, df2)


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}, {"AttributeName": "val", "KeyType": "RANGE"}],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "N"},
                {"AttributeName": "val", "AttributeType": "S"},
            ],
        }
    ],
)
def test_dynamodb_read_items(params: dict[str, Any], dynamodb_table: str) -> None:
    df = pd.DataFrame({"id": pa.array([1, 2, 3], type=pa.decimal128(1)), "val": ["foo", "boo", "bar"]})
    df.id = df.id.astype(pd.ArrowDtype(pa.decimal128(1)))
    df.val = df.val.astype(pd.ArrowDtype(pa.string()))

    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

    df2 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        allow_full_scan=True,
        dtype_backend="pyarrow",
        use_threads=False,
    )
    df2 = df2.sort_values(by="id", ascending=True).reset_index(drop=True)

    assert_pandas_equals(df, df2)
