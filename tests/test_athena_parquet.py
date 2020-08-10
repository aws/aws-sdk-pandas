import datetime
import logging
import math

import pandas as pd
import pytest

import awswrangler as wr

from ._utils import ensure_data_types, get_df, get_df_cast, get_df_list

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_parquet_catalog(path, path2, glue_table, glue_table2, glue_database):
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_parquet(
            df=pd.DataFrame({"A": [None]}), path=path, dataset=True, database=glue_database, table=glue_table
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
        df=df, path=path, use_threads=True, dataset=True, mode="overwrite", database=glue_database, table=glue_table
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
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table) is True
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table2) is True


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("max_rows_by_file", [None, 0, 40, 250, 1000])
@pytest.mark.parametrize("partition_cols", [None, ["par0"], ["par0", "par1"]])
def test_file_size(path, glue_table, glue_database, use_threads, max_rows_by_file, partition_cols):
    df = get_df_list()
    df = pd.concat([df for _ in range(100)])
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
    wr.s3.wait_objects_exist(paths, use_threads=use_threads)
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
            df=df, path=path, index=False, dataset=True, mode="overwrite", database=glue_database, table=glue_table
        )


def test_parquet_catalog_casting(path, glue_database):
    paths = wr.s3.to_parquet(
        df=get_df_cast(),
        path=path,
        index=False,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table="__test_parquet_catalog_casting",
        dtype={
            "iint8": "tinyint",
            "iint16": "smallint",
            "iint32": "int",
            "iint64": "bigint",
            "float": "float",
            "double": "double",
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
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df = wr.s3.read_parquet(path=path)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)
    df = wr.athena.read_sql_table(table="__test_parquet_catalog_casting", database=glue_database, ctas_approach=True)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)
    df = wr.athena.read_sql_table(table="__test_parquet_catalog_casting", database=glue_database, ctas_approach=False)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)
    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table="__test_parquet_catalog_casting") is True


@pytest.mark.parametrize("compression", [None, "gzip", "snappy"])
def test_parquet_compress(path, glue_table, glue_database, compression):
    paths = wr.s3.to_parquet(
        df=get_df(),
        path=path,
        compression=compression,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    ensure_data_types(df2)
    df2 = wr.s3.read_parquet(path=path)
    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table) is True
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
    wr.s3.delete_objects(path=path)
    values = list(range(5))
    df = pd.DataFrame({"col1": values, "col2": col2})
    paths = wr.s3.to_parquet(
        df,
        path,
        index=False,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["col2"],
        mode="overwrite",
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)

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

    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table) is True


def test_unsigned_parquet(path, glue_database, glue_table):
    wr.s3.delete_objects(path=path)
    df = pd.DataFrame({"c0": [0, 0, (2 ** 8) - 1], "c1": [0, 0, (2 ** 16) - 1], "c2": [0, 0, (2 ** 32) - 1]})
    df["c0"] = df.c0.astype("uint8")
    df["c1"] = df.c1.astype("uint16")
    df["c2"] = df.c2.astype("uint32")
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=glue_database, table=glue_table, mode="overwrite"
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert df.c0.sum() == (2 ** 8) - 1
    assert df.c1.sum() == (2 ** 16) - 1
    assert df.c2.sum() == (2 ** 32) - 1
    schema = wr.s3.read_parquet_metadata(path=path)[0]
    assert schema["c0"] == "smallint"
    assert schema["c1"] == "int"
    assert schema["c2"] == "bigint"
    df = wr.s3.read_parquet(path=path)
    assert df.c0.sum() == (2 ** 8) - 1
    assert df.c1.sum() == (2 ** 16) - 1
    assert df.c2.sum() == (2 ** 32) - 1

    df = pd.DataFrame({"c0": [0, 0, (2 ** 64) - 1]})
    df["c0"] = df.c0.astype("uint64")
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table, mode="overwrite")

    wr.s3.delete_objects(path=path)
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)


def test_parquet_overwrite_partition_cols(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [1, 2, 1, 2], "c1": [1, 2, 1, 2], "c2": [2, 1, 2, 1]})

    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        partition_cols=["c2"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 4
    assert len(df.columns) == 3
    assert df.c0.sum() == 6
    assert df.c1.sum() == 6
    assert df.c2.sum() == 6

    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        partition_cols=["c1", "c2"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 4
    assert len(df.columns) == 3
    assert df.c0.sum() == 6
    assert df.c1.sum() == 6
    assert df.c2.sum() == 6


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["c1", "c2"]])
def test_store_metadata_partitions_dataset(glue_database, glue_table, path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
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
        paths = wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)["paths"]
        wr.s3.wait_objects_exist(paths=paths, use_threads=False)
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


@pytest.mark.parametrize("partition_cols", [None, ["c1"], ["c2"], ["c1", "c2"], ["c2", "c1"]])
def test_to_parquet_reverse_partitions(glue_database, glue_table, path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=glue_database, table=glue_table, partition_cols=partition_cols
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
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
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {glue_table}", database=glue_database)
    assert len(df2.index) == 2
    assert len(df2.columns) == 4
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {glue_table}", database=glue_database)
    assert len(df2.index) == 4
    assert len(df2.columns) == 4


def test_to_parquet_nested_cast(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [[1, 2, 3], [4, 5, 6]], "c1": [[], []], "c2": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        dtype={"c0": "array<double>", "c1": "array<string>", "c2": "struct<a:bigint, b:double>"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = pd.DataFrame({"c0": [[1, 2, 3], [4, 5, 6]], "c1": [["a"], ["b"]], "c2": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c2 FROM {glue_table}", database=glue_database)
    assert len(df2.index) == 4
    assert len(df2.columns) == 2


def test_parquet_catalog_casting_to_string(path, glue_table, glue_database):
    for df in [get_df(), get_df_cast()]:
        paths = wr.s3.to_parquet(
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
                "double": "string",
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
        )["paths"]
        wr.s3.wait_objects_exist(paths=paths)
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
    paths = wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        partition_cols=partition_cols,
        use_threads=use_threads,
        table=glue_table,
        database=glue_database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=use_threads)
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
    sql = "SELECT timestamp '2012-08-08 01:00' AS c0"
    df = wr.athena.read_sql_query(sql, "default", use_threads=use_threads)
    df["c0"] = df["c0"] + pd.DateOffset(months=-2)
    assert df.c0[0].value == 1339117200000000000


def test_glue_number_of_versions_created(path, glue_table, glue_database):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2]})
    for _ in range(5):
        wr.s3.to_parquet(
            df, path, dataset=True, table=glue_table, database=glue_database,
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
