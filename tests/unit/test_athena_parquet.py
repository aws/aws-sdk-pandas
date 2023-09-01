import datetime
import logging
import math
from decimal import Decimal
from typing import Optional

import boto3
import numpy as np
import pyarrow as pa
import pytest

import awswrangler as wr
import awswrangler.pandas as pd
from awswrangler._data_types import _split_fields

from .._utils import ensure_data_types, get_df, get_df_cast, get_df_list, is_ray_modin, pandas_equals

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


def test_parquet_catalog(path, path2, glue_table, glue_table2, glue_database):
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_parquet(
            df=pd.DataFrame({"A": [None]}),
            path=path,
            dataset=True,
            database=glue_database,
            table=glue_table,
        )
    df = get_df_list()
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(
            df=df,
            path=path[:-1],
            use_threads=True,
            dataset=False,
            mode="overwrite",
            database=glue_database,
            table=glue_table,
        )
    wr.s3.to_parquet(
        df=df,
        path=path,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
    )
    wr.s3.to_parquet(
        df=df,
        path=path2,
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table2,
        partition_cols=["iint8", "iint16"],
    )
    columns_types, partitions_types = wr.s3.read_parquet_metadata(path=path2, dataset=True)
    assert len(columns_types) == 18
    assert len(partitions_types) == 2
    columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
        path=path2, database=glue_database, table=glue_table2, dataset=True
    )
    assert len(columns_types) == 18
    assert len(partitions_types) == 2
    assert len(partitions_values) == 2


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("max_rows_by_file", [None, 0, 40, 250, 1000])
@pytest.mark.parametrize("partition_cols", [None, ["par0"], ["par0", "par1"]])
def test_file_size(path, glue_table, glue_database, use_threads, max_rows_by_file, partition_cols):
    df = get_df_list()
    df = pd.concat([df for _ in range(100)])

    # workaround for https://github.com/modin-project/modin/issues/5164
    if is_ray_modin:
        vanilla_pandas = df._to_pandas()
        df = pd.DataFrame(vanilla_pandas)

    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        index=False,
        dataset=True,
        database=glue_database,
        table=glue_table,
        max_rows_by_file=max_rows_by_file,
        use_threads=use_threads,
        partition_cols=partition_cols,
    )["paths"]
    if max_rows_by_file is not None and max_rows_by_file > 0:
        assert len(paths) >= math.floor(300 / max_rows_by_file)
    df2 = wr.s3.read_parquet(path=path, dataset=True, use_threads=use_threads)
    ensure_data_types(df2, has_list=True)
    assert df2.shape == (300, 19)
    assert df.iint8.sum() == df2.iint8.sum()
    df2 = wr.athena.read_sql_table(database=glue_database, table=glue_table, use_threads=use_threads)
    ensure_data_types(df2, has_list=True)
    assert df2.shape == (300, 19)
    assert df.iint8.sum() == df2.iint8.sum()


def test_parquet_catalog_duplicated(path, glue_table, glue_database):
    df = pd.DataFrame({"A": [1], "a": [1]})
    with pytest.raises(wr.exceptions.InvalidDataFrame):
        wr.s3.to_parquet(
            df=df,
            path=path,
            index=False,
            dataset=True,
            mode="overwrite",
            database=glue_database,
            table=glue_table,
        )


def test_parquet_catalog_casting(path, glue_database, glue_table):
    wr.s3.to_parquet(
        df=get_df_cast(),
        path=path,
        index=False,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        dtype={
            "iint8": "tinyint",
            "iint16": "smallint",
            "iint32": "int",
            "iint64": "bigint",
            "float": "float",
            "ddouble": "double",
            "decimal": "decimal(3,2)",
            "string": "string",
            "date": "date",
            "timestamp": "timestamp",
            "bool": "boolean",
            "binary": "binary",
            "category": "double",
            "par0": "bigint",
            "par1": "string",
        },
    )
    df = wr.s3.read_parquet(path=path)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=True)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=False)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)


def test_parquet_catalog_casting_to_string_with_null(path, glue_table, glue_database):
    data = [{"A": "foo"}, {"A": "boo", "B": "bar"}]
    df = pd.DataFrame(data)
    wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        dtype={"A": "string", "B": "string"},
    )
    df = wr.s3.read_parquet(path=path)
    assert df.shape == (2, 2)
    for dtype in df.dtypes.values:
        assert str(dtype) == "string"
    assert pd.isna(df[df["a"] == "foo"].b.iloc[0])
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=True)
    assert df.shape == (2, 2)
    for dtype in df.dtypes.values:
        assert str(dtype) == "string"
    assert pd.isna(df[df["a"] == "foo"].b.iloc[0])
    df = wr.athena.read_sql_query(
        f"SELECT count(*) as counter FROM {glue_table} WHERE b is NULL ", database=glue_database
    )
    assert df.counter.iloc[0] == 1


@pytest.mark.parametrize("compression", [None, "gzip", "snappy"])
def test_parquet_compress(path, glue_table, glue_database, compression):
    wr.s3.to_parquet(
        df=get_df(),
        path=path,
        compression=compression,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    ensure_data_types(df2)
    df2 = wr.s3.read_parquet(path=path)
    ensure_data_types(df2)


def test_parquet_char_length(path, glue_database, glue_table):
    df = pd.DataFrame(
        {"id": [1, 2], "cchar": ["foo", "boo"], "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2)]}
    )
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        partition_cols=["date"],
        dtype={"cchar": "char(3)"},
    )

    df2 = wr.s3.read_parquet(path, dataset=True)
    assert len(df2.index) == 2
    assert len(df2.columns) == 3
    assert df2.id.sum() == 3

    df2 = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df2.index) == 2
    assert len(df2.columns) == 3
    assert df2.id.sum() == 3


@pytest.mark.parametrize("col2", [[1, 1, 1, 1, 1], [1, 2, 3, 4, 5], [1, 1, 1, 1, 2], [1, 2, 2, 2, 2]])
@pytest.mark.parametrize("chunked", [True, 1, 2, 100])
def test_parquet_chunked(path, glue_database, glue_table, col2, chunked):
    values = list(range(5))
    df = pd.DataFrame({"col1": values, "col2": col2})
    wr.s3.to_parquet(
        df,
        path,
        index=False,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["col2"],
        mode="overwrite",
    )

    dfs = list(wr.s3.read_parquet(path=path, dataset=True, chunked=chunked))
    assert sum(values) == pd.concat(dfs, ignore_index=True).col1.sum()
    if chunked is not True:
        assert len(dfs) == int(math.ceil(len(df) / chunked))
        for df2 in dfs[:-1]:
            assert chunked == len(df2)
        assert chunked >= len(dfs[-1])
    else:
        assert len(dfs) == len(set(col2))

    dfs = list(wr.athena.read_sql_table(database=glue_database, table=glue_table, chunksize=chunked))
    assert sum(values) == pd.concat(dfs, ignore_index=True).col1.sum()
    if chunked is not True:
        assert len(dfs) == int(math.ceil(len(df) / chunked))
        for df2 in dfs[:-1]:
            assert chunked == len(df2)
        assert chunked >= len(dfs[-1])


@pytest.mark.xfail(is_ray_modin, raises=AssertionError, reason="Issue since upgrading to PyArrow 11.0")
def test_unsigned_parquet(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, 0, (2**8) - 1], "c1": [0, 0, (2**16) - 1], "c2": [0, 0, (2**32) - 1]})
    df["c0"] = df.c0.astype("uint8")
    df["c1"] = df.c1.astype("uint16")
    df["c2"] = df.c2.astype("uint32")
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
    )
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert df.c0.sum() == (2**8) - 1
    assert df.c1.sum() == (2**16) - 1
    assert df.c2.sum() == (2**32) - 1
    schema = wr.s3.read_parquet_metadata(path=path)[0]
    assert schema["c0"] == "smallint"
    assert schema["c1"] == "int"
    assert schema["c2"] == "bigint"
    df = wr.s3.read_parquet(path=path)
    assert df.c0.sum() == (2**8) - 1
    assert df.c1.sum() == (2**16) - 1
    assert df.c2.sum() == (2**32) - 1

    df = pd.DataFrame({"c0": [0, 0, (2**64) - 1]})
    df["c0"] = df.c0.astype("uint64")
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            database=glue_database,
            table=glue_table,
            mode="overwrite",
        )


def test_parquet_overwrite_partition_cols(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [1, 2, 1, 2], "c1": [1, 2, 1, 2], "c2": [2, 1, 2, 1]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        partition_cols=["c2"],
    )
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 4
    assert len(df.columns) == 3
    assert df.c0.sum() == 6
    assert df.c1.sum() == 6
    assert df.c2.sum() == 6

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        partition_cols=["c1", "c2"],
    )
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 4
    assert len(df.columns) == 3
    assert df.c0.sum() == 6
    assert df.c1.sum() == 6
    assert df.c2.sum() == 6


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["c1", "c2"]])
def test_store_metadata_partitions_dataset(glue_database, glue_table, path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)
    wr.s3.store_parquet_metadata(path=path, database=glue_database, table=glue_table, dataset=True)
    df2 = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == len(df2.index)
    assert len(df.columns) == len(df2.columns)
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.astype(int).sum()
    assert df.c2.sum() == df2.c2.astype(int).sum()


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["c1", "c2"]])
def test_store_metadata_partitions_sample_dataset(glue_database, glue_table, path, partition_cols):
    num_files = 10
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    for _ in range(num_files):
        wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)
    wr.s3.store_parquet_metadata(
        path=path,
        database=glue_database,
        table=glue_table,
        dtype={"c1": "bigint", "c2": "smallint"},
        sampling=0.25,
        dataset=True,
    )
    df2 = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) * num_files == len(df2.index)
    assert len(df.columns) == len(df2.columns)
    assert df.c0.sum() * num_files == df2.c0.sum()
    assert df.c1.sum() * num_files == df2.c1.sum()
    assert df.c2.sum() * num_files == df2.c2.sum()


def test_store_metadata_ignore_null_columns(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2_null": [None, None, None], "c3_null": [None, None, None]})
    wr.s3.to_parquet(df=df, path=path, dataset=True, dtype={"c2_null": "int", "c3_null": "int"})
    wr.s3.store_parquet_metadata(
        path=path,
        database=glue_database,
        table=glue_table,
        ignore_null=True,
        dataset=True,
        dtype={"c2_null": "int", "c3_null": "int"},
    )


@pytest.mark.parametrize("partition_cols", [None, ["c0"], ["c0", "c1"]])
def test_store_metadata_ignore_null_columns_partitions(glue_database, glue_table, path, partition_cols):
    # only partition on non-null columns
    num_files = 10
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2_null": [None, None, None], "c3_null": [None, None, None]})
    for _ in range(num_files):
        wr.s3.to_parquet(
            df=df, path=path, dataset=True, dtype={"c2_null": "int", "c3_null": "int"}, partition_cols=partition_cols
        )
    wr.s3.store_parquet_metadata(
        path=path,
        database=glue_database,
        table=glue_table,
        ignore_null=True,
        dtype={"c2_null": "int", "c3_null": "int"},
        dataset=True,
    )


@pytest.mark.parametrize("partition_cols", [None, ["c1"], ["c2"], ["c1", "c2"], ["c2", "c1"]])
def test_to_parquet_reverse_partitions(glue_database, glue_table, path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=partition_cols,
    )
    df2 = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.sum()
    assert df.c2.sum() == df2.c2.sum()


def test_to_parquet_nested_append(glue_database, glue_table, path):
    df = pd.DataFrame(
        {
            "c0": [[1, 2, 3], [4, 5, 6]],
            "c1": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
            "c2": [[["a", "b"], ["c", "d"]], [["e", "f"], ["g", "h"]]],
            "c3": [[], [[[[[[[[1]]]]]]]]],
            "c4": [{"a": 1}, {"a": 1}],
            "c5": [{"a": {"b": {"c": [1, 2]}}}, {"a": {"b": {"c": [3, 4]}}}],
        }
    )
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {glue_table}", database=glue_database)
    assert len(df2.index) == 2
    assert len(df2.columns) == 4
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {glue_table}", database=glue_database)
    assert len(df2.index) == 4
    assert len(df2.columns) == 4


def test_to_parquet_nested_cast(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [[1, 2, 3], [4, 5, 6]], "c1": [[], []], "c2": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        dtype={"c0": "array<double>", "c1": "array<string>", "c2": "struct<a:bigint, b:double>"},
    )
    df = pd.DataFrame({"c0": [[1, 2, 3], [4, 5, 6]], "c1": [["a"], ["b"]], "c2": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]})
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c2 FROM {glue_table}", database=glue_database)
    assert len(df2.index) == 4
    assert len(df2.columns) == 2


def test_parquet_catalog_casting_to_string(path, glue_table, glue_database):
    for df in [get_df(), get_df_cast()]:
        wr.s3.to_parquet(
            df=df,
            path=path,
            index=False,
            dataset=True,
            mode="overwrite",
            database=glue_database,
            table=glue_table,
            dtype={
                "iint8": "string",
                "iint16": "string",
                "iint32": "string",
                "iint64": "string",
                "float": "string",
                "ddouble": "string",
                "decimal": "string",
                "string": "string",
                "date": "string",
                "timestamp": "string",
                "timestamp2": "string",
                "bool": "string",
                "binary": "string",
                "category": "string",
                "par0": "string",
                "par1": "string",
            },
        )
        df = wr.s3.read_parquet(path=path)
        assert df.shape == (3, 16)
        for dtype in df.dtypes.values:
            assert str(dtype) == "string"
        df = wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=True)
        assert df.shape == (3, 16)
        for dtype in df.dtypes.values:
            assert str(dtype) == "string"
        df = wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=False)
        assert df.shape == (3, 16)
        for dtype in df.dtypes.values:
            assert str(dtype) == "string"


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("partition_cols", [["c2"], ["c1", "c2"]])
def test_read_parquet_filter_partitions(path, glue_table, glue_database, use_threads, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 1, 2]})
    wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        partition_cols=partition_cols,
        use_threads=use_threads,
        database=glue_database,
        table=glue_table,
    )
    for i in range(3):
        df2 = wr.s3.read_parquet_table(
            table=glue_table,
            database=glue_database,
            partition_filter=lambda x: True if x["c2"] == str(i) else False,
            use_threads=use_threads,
        )
        assert df2.shape == (1, 3)
        assert df2.c0.iloc[0] == i
        assert df2.c1.iloc[0] == i
        assert df2.c2.iloc[0] == i


@pytest.mark.parametrize("use_threads", [True, False])
def test_read_parquet_mutability(path, glue_table, glue_database, use_threads):
    sql = "SELECT timestamp '2012-08-08 01:00:00.000' AS c0"
    df = wr._arrow.ensure_df_is_mutable(wr.athena.read_sql_query(sql, "default", use_threads=use_threads))
    df["c0"] = df["c0"] + pd.DateOffset(months=-2)
    assert df.c0[0].value == 1339117200000000000


def test_glue_number_of_versions_created(path, glue_table, glue_database):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2]})
    for _ in range(5):
        wr.s3.to_parquet(
            df,
            path,
            dataset=True,
            table=glue_table,
            database=glue_database,
        )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1


def test_sanitize_index(path, glue_table, glue_database):
    df = pd.DataFrame({"id": [1, 2], "DATE": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2)]})
    df.set_index("DATE", inplace=True, verify_integrity=True)
    wr.s3.to_parquet(df, path, dataset=True, index=True, database=glue_database, table=glue_table, mode="overwrite")
    df = pd.DataFrame({"id": [1, 2], "DATE": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2)]})
    df.set_index("DATE", inplace=True, verify_integrity=True)
    wr.s3.to_parquet(df, path, dataset=True, index=True, database=glue_database, table=glue_table, mode="append")
    df2 = wr.athena.read_sql_table(database=glue_database, table=glue_table)
    assert df2.shape == (4, 2)
    assert df2.id.sum() == 6
    assert list(df2.columns) == ["id", "date"]


def test_to_parquet_sanitize(path, glue_database):
    df = pd.DataFrame({"C0": [0, 1], "camelCase": [2, 3], "c**--2": [4, 5]})
    table_name = "TableName*!"
    wr.catalog.delete_table_if_exists(database=glue_database, table="tablename_")
    wr.s3.to_parquet(
        df, path, dataset=True, database=glue_database, table=table_name, mode="overwrite", partition_cols=["c**--2"]
    )
    df2 = wr.athena.read_sql_table(database=glue_database, table=table_name)
    wr.catalog.delete_table_if_exists(database=glue_database, table="tablename_")
    assert df.shape == df2.shape
    assert list(df) != list(df2)  # make sure the original DataFrame is not modified by this
    assert list(df2.columns) == ["c0", "camelcase", "c_2"]
    assert df2.c0.sum() == 1
    assert df2.camelcase.sum() == 5
    assert df2.c_2.sum() == 9


def test_schema_evolution_disabled(path, glue_table, glue_database):
    wr.s3.to_parquet(
        df=pd.DataFrame({"c0": [1]}),
        path=path,
        dataset=True,
        table=glue_table,
        database=glue_database,
        schema_evolution=False,
    )
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_parquet(
            df=pd.DataFrame({"c0": [2], "c1": [2]}),
            path=path,
            dataset=True,
            table=glue_table,
            database=glue_database,
            schema_evolution=False,
        )
    wr.s3.to_parquet(
        df=pd.DataFrame({"c0": [2]}),
        path=path,
        dataset=True,
        table=glue_table,
        database=glue_database,
        schema_evolution=False,
    )
    df2 = wr.athena.read_sql_table(database=glue_database, table=glue_table)
    assert df2.shape == (2, 1)
    assert df2.c0.sum() == 3


@pytest.mark.modin_index
def test_date_cast(path, glue_table, glue_database):
    df = pd.DataFrame(
        {
            "c0": [
                datetime.date(4000, 1, 1),
                datetime.datetime(2000, 1, 1, 10),
                "2020",
                "2020-01",
                1,
                None,
                pd.NA,
                pd.NaT,
                np.nan,
                np.inf,
            ]
        }
    )
    df_expected = pd.DataFrame(
        {
            "c0": [
                datetime.date(4000, 1, 1),
                datetime.date(2000, 1, 1),
                datetime.date(2020, 1, 1),
                datetime.date(2020, 1, 1),
                datetime.date(1970, 1, 1),
                None,
                None,
                None,
                None,
                None,
            ]
        }
    )
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table, dtype={"c0": "date"})
    df2 = wr.s3.read_parquet(path=path, pyarrow_additional_kwargs={"ignore_metadata": True})
    assert pandas_equals(df_expected, df2)
    df3 = wr.athena.read_sql_table(
        database=glue_database,
        table=glue_table,
        ctas_approach=False,
        pyarrow_additional_kwargs={"ignore_metadata": True},
    )
    assert pandas_equals(df_expected, df3)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("partition_cols", [None, ["par0"], ["par0", "par1"]])
def test_partitions_overwrite(path, glue_table, glue_database, use_threads, partition_cols):
    df = get_df_list()
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        use_threads=use_threads,
        partition_cols=partition_cols,
        mode="overwrite_partitions",
    )
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        use_threads=use_threads,
        partition_cols=partition_cols,
        mode="overwrite_partitions",
    )
    df2 = wr.athena.read_sql_table(database=glue_database, table=glue_table, use_threads=use_threads)
    ensure_data_types(df2, has_list=True)
    assert df2.shape == (3, 19)
    assert df.iint8.sum() == df2.iint8.sum()


def test_empty_dataframe(path, glue_database, glue_table):
    df = get_df_list()
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    sql = f"SELECT * FROM {glue_table} WHERE par0 = :par0"
    df_uncached = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=True, params={"par0": 999})
    df_cached = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=True, params={"par0": 999})
    assert set(df.columns) == set(df_uncached.columns)
    assert set(df.columns) == set(df_cached.columns)


@pytest.mark.parametrize("use_threads", [True, False])
def test_empty_column(path, glue_table, glue_database, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [None, None, None], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_parquet(
            df,
            path,
            dataset=True,
            use_threads=use_threads,
            database=glue_database,
            table=glue_table,
            partition_cols=["par"],
        )


@pytest.mark.parametrize("use_threads", [True, False])
def test_mixed_types_column(path, glue_table, glue_database, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [1, 2, "foo"], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    with pytest.raises(pa.ArrowInvalid):
        wr.s3.to_parquet(
            df,
            path,
            dataset=True,
            use_threads=use_threads,
            database=glue_database,
            table=glue_table,
            partition_cols=["par"],
        )


@pytest.mark.parametrize("use_threads", [True, False])
def test_failing_catalog(path, glue_table, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3]})
    try:
        wr.s3.to_parquet(
            df,
            path,
            use_threads=use_threads,
            max_rows_by_file=1,
            dataset=True,
            database="foo",
            table=glue_table,
        )
    except boto3.client("glue").exceptions.EntityNotFoundException:
        pass
    assert len(wr.s3.list_objects(path)) == 0


def test_cast_decimal(path, glue_table, glue_database):
    df = pd.DataFrame(
        {"c0": [100.1], "c1": ["100.1"], "c2": [Decimal((0, (1, 0, 0, 1), -1))], "c3": [Decimal((0, (1, 0, 0, 1), -1))]}
    )
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        dtype={"c0": "decimal(4,1)", "c1": "decimal(4,1)", "c2": "decimal(4,1)", "c3": "string"},
    )
    df2 = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert df2.shape == (1, 4)
    assert df2["c0"].iloc[0] == Decimal((0, (1, 0, 0, 1), -1))
    assert df2["c1"].iloc[0] == Decimal((0, (1, 0, 0, 1), -1))
    assert df2["c2"].iloc[0] == Decimal((0, (1, 0, 0, 1), -1))
    assert df2["c3"].iloc[0] == "100.1"


def test_splits():
    s = "a:struct<id:string,name:string>,b:struct<id:string,name:string>"
    assert list(_split_fields(s)) == ["a:struct<id:string,name:string>", "b:struct<id:string,name:string>"]
    s = "a:struct<a:struct<id:string,name:string>,b:struct<id:string,name:string>>,b:struct<a:struct<id:string,name:string>,b:struct<id:string,name:string>>"  # noqa
    assert list(_split_fields(s)) == [
        "a:struct<a:struct<id:string,name:string>,b:struct<id:string,name:string>>",
        "b:struct<a:struct<id:string,name:string>,b:struct<id:string,name:string>>",
    ]
    s = "a:struct<id:string,name:string>,b:struct<id:string,name:string>,c:struct<id:string,name:string>,d:struct<id:string,name:string>"  # noqa
    assert list(_split_fields(s)) == [
        "a:struct<id:string,name:string>",
        "b:struct<id:string,name:string>",
        "c:struct<id:string,name:string>",
        "d:struct<id:string,name:string>",
    ]


def test_to_parquet_nested_structs(glue_database, glue_table, path):
    df = pd.DataFrame(
        {
            "c0": [1],
            "c1": [[{"a": {"id": "0", "name": "foo", "amount": 1}, "b": {"id": "1", "name": "boo", "amount": 2}}]],
        }
    )
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    df2 = wr.athena.read_sql_query(sql=f"SELECT * FROM {glue_table}", database=glue_database)
    assert df2.shape == (1, 2)
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    df3 = wr.athena.read_sql_query(sql=f"SELECT * FROM {glue_table}", database=glue_database)
    assert df3.shape == (2, 2)


def test_ignore_empty_files(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    bucket, directory = wr._utils.parse_path(path)
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=f"{directory}to_be_ignored")
    df2 = wr.athena.read_sql_query(sql=f"SELECT * FROM {glue_table}", database=glue_database)
    assert df2.shape == df.shape
    df3 = wr.s3.read_parquet_table(database=glue_database, table=glue_table)
    assert df3.shape == df.shape


def test_suffix(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    bucket, directory = wr._utils.parse_path(path)
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    boto3.client("s3").put_object(Body=b"garbage", Bucket=bucket, Key=f"{directory}to_be_ignored")
    df2 = wr.s3.read_parquet_table(database=glue_database, table=glue_table, filename_suffix=".parquet")
    assert df2.shape == df.shape


def test_ignore_suffix(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    bucket, directory = wr._utils.parse_path(path)
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    boto3.client("s3").put_object(Body=b"garbage", Bucket=bucket, Key=f"{directory}to_be_ignored")
    df2 = wr.s3.read_parquet_table(database=glue_database, table=glue_table, filename_ignore_suffix="ignored")
    assert df2.shape == df.shape


def test_athena_timestamp_overflow():
    sql = "SELECT timestamp '2262-04-11 23:47:17.000' AS c0"
    df1 = wr.athena.read_sql_query(sql, "default")

    df_overflow = pd.DataFrame({"c0": [pd.Timestamp("1677-09-21 00:12:43.290448384")]})
    assert df_overflow.c0.values[0] == df1.c0.values[0]

    df2 = wr.athena.read_sql_query(sql, "default", pyarrow_additional_kwargs={"timestamp_as_object": True})

    df_overflow_fix = pd.DataFrame({"c0": [datetime.datetime(2262, 4, 11, 23, 47, 17)]})
    df_overflow_fix.c0.values[0] == df2.c0.values[0]


@pytest.mark.parametrize("file_format", ["ORC", "PARQUET", "AVRO", "JSON", "TEXTFILE"])
@pytest.mark.parametrize("partitioned_by", [None, ["c1", "c2"]])
def test_unload(path, glue_table, glue_database, file_format, partitioned_by):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 1, 2]})
    wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        table=glue_table,
        database=glue_database,
    )
    query_metadata = wr.athena.unload(
        sql=f"SELECT * FROM {glue_database}.{glue_table}",
        path=f"{path}test_{file_format}/",
        database=glue_database,
        file_format=file_format,
        partitioned_by=partitioned_by,
    )
    assert query_metadata is not None


@pytest.mark.parametrize("file_format", [None, "PARQUET"])
def test_read_sql_query_unload(path: str, glue_table: str, glue_database: str, file_format: Optional[str]):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 1, 2]})
    wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        table=glue_table,
        database=glue_database,
    )
    df_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        s3_output=f"{path}unload/",
        ctas_approach=False,
        unload_approach=True,
        unload_parameters={"file_format": file_format},
    )
    assert df.shape == df_out.shape
