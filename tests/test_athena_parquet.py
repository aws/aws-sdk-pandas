import datetime
import logging
import math

import pandas as pd
import pytest

import awswrangler as wr

from ._utils import ensure_data_types, get_df, get_df_cast, get_df_list

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


def test_to_parquet_modes(glue_database, glue_table, path):

    # Round 1 - Warm up
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        description="c0",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c0": "0"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "0"

    # Round 2 - Overwrite
    df = pd.DataFrame({"c1": [None, 1, None]}, dtype="Int16")
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c1": "1"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 3 - Append
    df = pd.DataFrame({"c1": [None, 2, None]}, dtype="Int8")
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index) * 2)},
        columns_comments={"c1": "1"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df.columns) == len(df2.columns)
    assert len(df.index) * 2 == len(df2.index)
    assert df.c1.sum() + 1 == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 4 - Append + New Column
    df = pd.DataFrame({"c2": ["a", None, "b"], "c1": [None, None, None]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c1+c2",
        parameters={"num_cols": "2", "num_rows": "9"},
        columns_comments={"c1": "1", "c2": "2"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.columns) == 2
    assert len(df2.index) == 9
    assert df2.c1.sum() == 3
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "9"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1+c2"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"
    assert comments["c2"] == "2"

    # Round 5 - Append + New Column + Wrong Types
    df = pd.DataFrame({"c2": [1], "c3": [True], "c1": ["1"]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c1+c2+c3",
        parameters={"num_cols": "3", "num_rows": "10"},
        columns_comments={"c1": "1!", "c2": "2!", "c3": "3"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.columns) == 3
    assert len(df2.index) == 10
    assert df2.c1.sum() == 4
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "3"
    assert parameters["num_rows"] == "10"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1+c2+c3"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1!"
    assert comments["c2"] == "2!"
    assert comments["c3"] == "3"

    # Round 6 - Overwrite Partitioned
    df = pd.DataFrame({"c0": ["foo", None], "c1": [0, 1]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        partition_cols=["c1"],
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "2"},
        columns_comments={"c0": "zero", "c1": "one"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "2"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 7 - Overwrite Partitions
    df = pd.DataFrame({"c0": [None, None], "c1": [0, 2]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        database=glue_database,
        table=glue_table,
        partition_cols=["c1"],
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "3"},
        columns_comments={"c0": "zero", "c1": "one"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.columns) == 2
    assert len(df2.index) == 3
    assert df2.c1.sum() == 3
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "3"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 8 - Overwrite Partitions + New Column + Wrong Type
    df = pd.DataFrame({"c0": [1, 2], "c1": ["1", "3"], "c2": [True, False]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        database=glue_database,
        table=glue_table,
        partition_cols=["c1"],
        description="c0+c1+c2",
        parameters={"num_cols": "3", "num_rows": "4"},
        columns_comments={"c0": "zero", "c1": "one", "c2": "two"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.columns) == 3
    assert len(df2.index) == 4
    assert df2.c1.sum() == 6
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "3"
    assert parameters["num_rows"] == "4"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0+c1+c2"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"
    assert comments["c2"] == "two"


def test_store_parquet_metadata_modes(glue_database, glue_table, path):

    # Round 1 - Warm up
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        description="c0",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c0": "0"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "0"

    # Round 2 - Overwrite
    df = pd.DataFrame({"c1": [None, 1, None]}, dtype="Int16")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c1": "1"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 3 - Append
    df = pd.DataFrame({"c1": [None, 2, None]}, dtype="Int16")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="append")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index) * 2)},
        columns_comments={"c1": "1"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df.columns) == len(df2.columns)
    assert len(df.index) * 2 == len(df2.index)
    assert df.c1.sum() + 1 == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 4 - Append + New Column
    df = pd.DataFrame({"c2": ["a", None, "b"], "c1": [None, 1, None]})
    df["c1"] = df["c1"].astype("Int16")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="append")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c1+c2",
        parameters={"num_cols": "2", "num_rows": "9"},
        columns_comments={"c1": "1", "c2": "2"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.columns) == 2
    assert len(df2.index) == 9
    assert df2.c1.sum() == 4
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "9"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1+c2"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"
    assert comments["c2"] == "2"

    # Round 5 - Overwrite Partitioned
    df = pd.DataFrame({"c0": ["foo", None], "c1": [0, 1]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", partition_cols=["c1"])["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "2"},
        columns_comments={"c0": "zero", "c1": "one"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.astype(int).sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "2"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 6 - Overwrite Partitions
    df = pd.DataFrame({"c0": [None, "boo"], "c1": [0, 2]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite_partitions", partition_cols=["c1"])[
        "paths"
    ]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "3"},
        columns_comments={"c0": "zero", "c1": "one"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.columns) == 2
    assert len(df2.index) == 3
    assert df2.c1.astype(int).sum() == 3
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "3"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 7 - Overwrite Partitions + New Column
    df = pd.DataFrame({"c0": ["bar", None], "c1": [1, 3], "c2": [True, False]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite_partitions", partition_cols=["c1"])[
        "paths"
    ]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c0+c1+c2",
        parameters={"num_cols": "3", "num_rows": "4"},
        columns_comments={"c0": "zero", "c1": "one", "c2": "two"},
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.columns) == 3
    assert len(df2.index) == 4
    assert df2.c1.astype(int).sum() == 6
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "3"
    assert parameters["num_rows"] == "4"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0+c1+c2"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"
    assert comments["c2"] == "two"


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
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df, path=path, use_threads=True, dataset=False, table=glue_table)
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df, path=path, use_threads=True, dataset=True, mode="overwrite", database=glue_database)
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


def test_parquet_catalog_duplicated(path, glue_table, glue_database):
    df = pd.DataFrame({"A": [1], "a": [1]})
    wr.s3.to_parquet(
        df=df, path=path, index=False, dataset=True, mode="overwrite", database=glue_database, table=glue_table
    )
    df = wr.s3.read_parquet(path=path)
    assert df.shape == (1, 1)


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
