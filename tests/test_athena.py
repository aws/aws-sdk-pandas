import datetime
import logging
import math
from unittest.mock import patch

import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import (
    dt,
    ensure_data_types,
    ensure_data_types_category,
    ensure_data_types_csv,
    get_df,
    get_df_cast,
    get_df_category,
    get_df_csv,
    get_df_list,
    get_query_long,
    get_time_str_with_random_suffix,
    ts,
)

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


def test_athena_ctas(path, path2, path3, glue_table, glue_table2, glue_database, kms_key):
    df = get_df_list()
    columns_types, partitions_types = wr.catalog.extract_athena_types(df=df, partition_cols=["par0", "par1"])
    assert len(columns_types) == 17
    assert len(partitions_types) == 2
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.catalog.extract_athena_types(df=df, file_format="avro")
    paths = wr.s3.to_parquet(
        df=get_df_list(),
        path=path,
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        partition_cols=["par0", "par1"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    dirs = wr.s3.list_directories(path=path)
    for d in dirs:
        assert d.startswith(f"{path}par0=")
    df = wr.s3.read_parquet_table(table=glue_table, database=glue_database)
    assert len(df.index) == 3
    ensure_data_types(df=df, has_list=True)
    df = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=True,
        encryption="SSE_KMS",
        kms_key=kms_key,
        s3_output=path2,
        keep_files=False,
    )
    assert len(df.index) == 3
    ensure_data_types(df=df, has_list=True)
    final_destination = f"{path3}{glue_table2}/"

    # keep_files=False
    wr.s3.delete_objects(path=path3)
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table}",
        database=glue_database,
        ctas_approach=True,
        chunksize=1,
        keep_files=False,
        ctas_temp_table_name=glue_table2,
        s3_output=path3,
    )
    assert wr.catalog.does_table_exist(database=glue_database, table=glue_table2) is False
    assert len(wr.s3.list_objects(path=path3)) > 2
    assert len(wr.s3.list_objects(path=final_destination)) > 0
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
    assert len(wr.s3.list_objects(path=path3)) == 0

    # keep_files=True
    wr.s3.delete_objects(path=path3)
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table}",
        database=glue_database,
        ctas_approach=True,
        chunksize=2,
        keep_files=True,
        ctas_temp_table_name=glue_table2,
        s3_output=path3,
    )
    assert wr.catalog.does_table_exist(database=glue_database, table=glue_table2) is False
    assert len(wr.s3.list_objects(path=path3)) > 2
    assert len(wr.s3.list_objects(path=final_destination)) > 0
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
    assert len(wr.s3.list_objects(path=path3)) > 2


def test_athena(path, glue_database, kms_key, workgroup0, workgroup1):
    wr.catalog.delete_table_if_exists(database=glue_database, table="__test_athena")
    paths = wr.s3.to_parquet(
        df=get_df(),
        path=path,
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table="__test_athena",
        partition_cols=["par0", "par1"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    dfs = wr.athena.read_sql_query(
        sql="SELECT * FROM __test_athena",
        database=glue_database,
        ctas_approach=False,
        chunksize=1,
        encryption="SSE_KMS",
        kms_key=kms_key,
        workgroup=workgroup0,
        keep_files=False,
    )
    for df2 in dfs:
        ensure_data_types(df=df2)
    df = wr.athena.read_sql_query(
        sql="SELECT * FROM __test_athena",
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup1,
        keep_files=False,
    )
    assert len(df.index) == 3
    ensure_data_types(df=df)
    wr.athena.repair_table(table="__test_athena", database=glue_database)
    wr.catalog.delete_table_if_exists(database=glue_database, table="__test_athena")


def test_parquet_catalog(bucket, glue_database):
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_parquet(
            df=pd.DataFrame({"A": [None]}),
            path=f"s3://{bucket}/test_parquet_catalog",
            dataset=True,
            database=glue_database,
            table="test_parquet_catalog",
        )
    df = get_df_list()
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/test_parquet_catalog",
            use_threads=True,
            dataset=False,
            mode="overwrite",
            database=glue_database,
            table="test_parquet_catalog",
        )
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/test_parquet_catalog",
            use_threads=True,
            dataset=False,
            table="test_parquet_catalog",
        )
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/test_parquet_catalog",
            use_threads=True,
            dataset=True,
            mode="overwrite",
            database=glue_database,
        )
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/test_parquet_catalog",
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table="test_parquet_catalog",
    )
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/test_parquet_catalog2",
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table="test_parquet_catalog2",
        partition_cols=["iint8", "iint16"],
    )
    columns_types, partitions_types = wr.s3.read_parquet_metadata(
        path=f"s3://{bucket}/test_parquet_catalog2", dataset=True
    )
    assert len(columns_types) == 18
    assert len(partitions_types) == 2
    columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
        path=f"s3://{bucket}/test_parquet_catalog2", database=glue_database, table="test_parquet_catalog2", dataset=True
    )
    assert len(columns_types) == 18
    assert len(partitions_types) == 2
    assert len(partitions_values) == 2
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_catalog/")
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_catalog2/")
    assert wr.catalog.delete_table_if_exists(database=glue_database, table="test_parquet_catalog") is True
    assert wr.catalog.delete_table_if_exists(database=glue_database, table="test_parquet_catalog2") is True


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


def test_catalog(path, glue_database, glue_table):
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    assert wr.catalog.does_table_exist(database=glue_database, table=glue_table) is False
    wr.catalog.create_parquet_table(
        database=glue_database,
        table=glue_table,
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        partitions_types={"y": "int", "m": "int"},
        compression="snappy",
    )
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.catalog.create_parquet_table(
            database=glue_database, table=glue_table, path=path, columns_types={"col0": "string"}, mode="append"
        )
    assert wr.catalog.does_table_exist(database=glue_database, table=glue_table) is True
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table) is True
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table) is False
    wr.catalog.create_parquet_table(
        database=glue_database,
        table=glue_table,
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        partitions_types={"y": "int", "m": "int"},
        compression="snappy",
        description="Foo boo bar",
        parameters={"tag": "test"},
        columns_comments={"col0": "my int", "y": "year"},
        mode="overwrite",
    )
    wr.catalog.add_parquet_partitions(
        database=glue_database,
        table=glue_table,
        partitions_values={f"{path}y=2020/m=1/": ["2020", "1"], f"{path}y=2021/m=2/": ["2021", "2"]},
        compression="snappy",
    )
    assert wr.catalog.get_table_location(database=glue_database, table=glue_table) == path
    # get_parquet_partitions
    parquet_partitions_values = wr.catalog.get_parquet_partitions(database=glue_database, table=glue_table)
    assert len(parquet_partitions_values) == 2
    parquet_partitions_values = wr.catalog.get_parquet_partitions(
        database=glue_database, table=glue_table, catalog_id=account_id, expression="y = 2021 AND m = 2"
    )
    assert len(parquet_partitions_values) == 1
    assert len(set(parquet_partitions_values[f"{path}y=2021/m=2/"]) & {"2021", "2"}) == 2
    # get_partitions
    partitions_values = wr.catalog.get_partitions(database=glue_database, table=glue_table)
    assert len(partitions_values) == 2
    partitions_values = wr.catalog.get_partitions(
        database=glue_database, table=glue_table, catalog_id=account_id, expression="y = 2021 AND m = 2"
    )
    assert len(partitions_values) == 1
    assert len(set(partitions_values[f"{path}y=2021/m=2/"]) & {"2021", "2"}) == 2

    dtypes = wr.catalog.get_table_types(database=glue_database, table=glue_table)
    assert dtypes["col0"] == "int"
    assert dtypes["col1"] == "double"
    assert dtypes["y"] == "int"
    assert dtypes["m"] == "int"
    df_dbs = wr.catalog.databases()
    assert len(wr.catalog.databases(catalog_id=account_id)) == len(df_dbs)
    assert glue_database in df_dbs["Database"].to_list()
    tables = list(wr.catalog.get_tables())
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == glue_table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    tables = list(wr.catalog.get_tables(database=glue_database))
    assert len(tables) > 0
    for tbl in tables:
        assert tbl["DatabaseName"] == glue_database
    # search
    tables = list(wr.catalog.search_tables(text="parquet", catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == glue_table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # prefix
    tables = list(wr.catalog.get_tables(name_prefix=glue_table[:4], catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == glue_table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # suffix
    tables = list(wr.catalog.get_tables(name_suffix=glue_table[-4:], catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == glue_table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # name_contains
    tables = list(wr.catalog.get_tables(name_contains=glue_table[4:-4], catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == glue_table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # prefix & suffix & name_contains
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        list(
            wr.catalog.get_tables(
                name_prefix=glue_table[0],
                name_contains=glue_table[3],
                name_suffix=glue_table[-1],
                catalog_id=account_id,
            )
        )
    # prefix & suffix
    tables = list(wr.catalog.get_tables(name_prefix=glue_table[0], name_suffix=glue_table[-1], catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == glue_table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # DataFrames
    assert len(wr.catalog.databases().index) > 0
    assert len(wr.catalog.tables().index) > 0
    assert (
        len(
            wr.catalog.tables(
                database=glue_database,
                search_text="parquet",
                name_prefix=glue_table[0],
                name_contains=glue_table[3],
                name_suffix=glue_table[-1],
                catalog_id=account_id,
            ).index
        )
        > 0
    )
    assert len(wr.catalog.table(database=glue_database, table=glue_table).index) > 0
    assert len(wr.catalog.table(database=glue_database, table=glue_table, catalog_id=account_id).index) > 0
    with pytest.raises(wr.exceptions.InvalidTable):
        wr.catalog.overwrite_table_parameters({"foo": "boo"}, glue_database, "fake_table")


def test_catalog_get_databases(glue_database):
    dbs = list(wr.catalog.get_databases())
    assert len(dbs) > 0
    for db in dbs:
        if db["Name"] == glue_database:
            assert db["Description"] == "AWS Data Wrangler Test Arena - Glue Database"


def test_athena_query_cancelled(glue_database):
    session = boto3.DEFAULT_SESSION
    query_execution_id = wr.athena.start_query_execution(
        sql=get_query_long(), database=glue_database, boto3_session=session
    )
    wr.athena.stop_query_execution(query_execution_id=query_execution_id, boto3_session=session)
    with pytest.raises(wr.exceptions.QueryCancelled):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)


def test_athena_query_failed(glue_database):
    query_execution_id = wr.athena.start_query_execution(sql="SELECT random(-1)", database=glue_database)
    with pytest.raises(wr.exceptions.QueryFailed):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)


def test_athena_read_list(glue_database):
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.athena.read_sql_query(sql="SELECT ARRAY[1, 2, 3]", database=glue_database, ctas_approach=False)


def test_sanitize_names():
    assert wr.catalog.sanitize_column_name("CamelCase") == "camel_case"
    assert wr.catalog.sanitize_column_name("CamelCase2") == "camel_case2"
    assert wr.catalog.sanitize_column_name("Camel_Case3") == "camel_case3"
    assert wr.catalog.sanitize_column_name("Cámël_Casë4仮") == "camel_case4_"
    assert wr.catalog.sanitize_column_name("Camel__Case5") == "camel__case5"
    assert wr.catalog.sanitize_column_name("Camel{}Case6") == "camel_case6"
    assert wr.catalog.sanitize_column_name("Camel.Case7") == "camel_case7"
    assert wr.catalog.sanitize_column_name("xyz_cd") == "xyz_cd"
    assert wr.catalog.sanitize_column_name("xyz_Cd") == "xyz_cd"
    assert wr.catalog.sanitize_table_name("CamelCase") == "camel_case"
    assert wr.catalog.sanitize_table_name("CamelCase2") == "camel_case2"
    assert wr.catalog.sanitize_table_name("Camel_Case3") == "camel_case3"
    assert wr.catalog.sanitize_table_name("Cámël_Casë4仮") == "camel_case4_"
    assert wr.catalog.sanitize_table_name("Camel__Case5") == "camel__case5"
    assert wr.catalog.sanitize_table_name("Camel{}Case6") == "camel_case6"
    assert wr.catalog.sanitize_table_name("Camel.Case7") == "camel_case7"
    assert wr.catalog.sanitize_table_name("xyz_cd") == "xyz_cd"
    assert wr.catalog.sanitize_table_name("xyz_Cd") == "xyz_cd"


def test_athena_ctas_empty(glue_database):
    sql = """
        WITH dataset AS (
          SELECT 0 AS id
        )
        SELECT id
        FROM dataset
        WHERE id != 0
    """
    assert wr.athena.read_sql_query(sql=sql, database=glue_database).empty is True
    assert len(list(wr.athena.read_sql_query(sql=sql, database=glue_database, chunksize=1))) == 0


def test_athena_struct(glue_database):
    sql = "SELECT CAST(ROW(1, 'foo') AS ROW(id BIGINT, value VARCHAR)) AS col0"
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=False)
    df = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=True)
    assert len(df.index) == 1
    assert len(df.columns) == 1
    assert df["col0"].iloc[0]["id"] == 1
    assert df["col0"].iloc[0]["value"] == "foo"
    sql = "SELECT ROW(1, ROW(2, ROW(3, '4'))) AS col0"
    df = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=True)
    assert len(df.index) == 1
    assert len(df.columns) == 1
    assert df["col0"].iloc[0]["field0"] == 1
    assert df["col0"].iloc[0]["field1"]["field0"] == 2
    assert df["col0"].iloc[0]["field1"]["field1"]["field0"] == 3
    assert df["col0"].iloc[0]["field1"]["field1"]["field1"] == "4"


def test_athena_time_zone(glue_database):
    sql = "SELECT current_timestamp AS value, typeof(current_timestamp) AS type"
    df = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=False)
    assert len(df.index) == 1
    assert len(df.columns) == 2
    assert df["type"][0] == "timestamp with time zone"
    assert df["value"][0].year == datetime.datetime.utcnow().year


def test_category(bucket, glue_database):
    df = get_df_category()
    path = f"s3://{bucket}/test_category/"
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table="test_category",
        mode="overwrite",
        partition_cols=["par0", "par1"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.s3.read_parquet(path=path, dataset=True, categories=[c for c in df.columns if c not in ["par0", "par1"]])
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_query("SELECT * FROM test_category", database=glue_database, categories=list(df.columns))
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_table(table="test_category", database=glue_database, categories=list(df.columns))
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_query(
        "SELECT * FROM test_category", database=glue_database, categories=list(df.columns), ctas_approach=False
    )
    ensure_data_types_category(df2)
    dfs = wr.athena.read_sql_query(
        "SELECT * FROM test_category",
        database=glue_database,
        categories=list(df.columns),
        ctas_approach=False,
        chunksize=1,
    )
    for df2 in dfs:
        ensure_data_types_category(df2)
    dfs = wr.athena.read_sql_query(
        "SELECT * FROM test_category",
        database=glue_database,
        categories=list(df.columns),
        ctas_approach=True,
        chunksize=1,
    )
    for df2 in dfs:
        ensure_data_types_category(df2)
    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table="test_category") is True


def test_csv_dataset(path, glue_database):
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_csv(pd.DataFrame({"A": [None]}), path, dataset=True, database=glue_database, table="test_csv_dataset")
    df = get_df_csv()
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df, path, dataset=False, mode="overwrite", database=glue_database, table="test_csv_dataset")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df, path, dataset=False, table="test_csv_dataset")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df, path, dataset=True, mode="overwrite", database=glue_database)
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, path=path, mode="append")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, path=path, partition_cols=["col2"])
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, path=path, description="foo")
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_csv(df=df, path=path, partition_cols=["col2"], dataset=True, mode="WRONG")
    paths = wr.s3.to_csv(
        df=df,
        path=path,
        sep="|",
        index=False,
        use_threads=True,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.s3.read_csv(path=paths, sep="|", header=None)
    assert len(df2.index) == 3
    assert len(df2.columns) == 8
    assert df2[0].sum() == 6
    wr.s3.delete_objects(path=paths)


def test_csv_catalog(path, glue_table, glue_database):
    df = get_df_csv()
    paths = wr.s3.to_csv(
        df=df,
        path=path,
        sep="\t",
        index=True,
        use_threads=True,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table=glue_table,
        database=glue_database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 11
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)
    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table) is True


def test_csv_catalog_columns(bucket, glue_database):
    path = f"s3://{bucket}/test_csv_catalog_columns /"
    paths = wr.s3.to_csv(
        df=get_df_csv(),
        path=path,
        sep="|",
        columns=["id", "date", "timestamp", "par0", "par1"],
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table="test_csv_catalog_columns",
        database=glue_database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_csv_catalog_columns", glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)

    paths = wr.s3.to_csv(
        df=pd.DataFrame({"id": [4], "date": [None], "timestamp": [None], "par0": [1], "par1": ["a"]}),
        path=path,
        sep="|",
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite_partitions",
        table="test_csv_catalog_columns",
        database=glue_database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_csv_catalog_columns", glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 9
    ensure_data_types_csv(df2)

    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table="test_csv_catalog_columns") is True


def test_athena_types(bucket, glue_database):
    path = f"s3://{bucket}/test_athena_types/"
    df = get_df_csv()
    paths = wr.s3.to_csv(
        df=df,
        path=path,
        sep=",",
        index=False,
        use_threads=True,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    columns_types, partitions_types = wr.catalog.extract_athena_types(
        df=df, index=False, partition_cols=["par0", "par1"], file_format="csv"
    )
    wr.catalog.create_csv_table(
        table="test_athena_types",
        database=glue_database,
        path=path,
        partitions_types=partitions_types,
        columns_types=columns_types,
    )
    wr.catalog.create_csv_table(
        database=glue_database, table="test_athena_types", path=path, columns_types={"col0": "string"}, mode="append"
    )
    wr.athena.repair_table("test_athena_types", glue_database)
    assert len(wr.catalog.get_csv_partitions(glue_database, "test_athena_types")) == 3
    df2 = wr.athena.read_sql_table("test_athena_types", glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 10
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)
    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table="test_athena_types") is True


def test_parquet_catalog_columns(bucket, glue_database):
    path = f"s3://{bucket}/test_parquet_catalog_columns/"
    paths = wr.s3.to_parquet(
        df=get_df_csv()[["id", "date", "timestamp", "par0", "par1"]],
        path=path,
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table="test_parquet_catalog_columns",
        database=glue_database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_parquet_catalog_columns", glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)

    paths = wr.s3.to_parquet(
        df=pd.DataFrame({"id": [4], "date": [None], "timestamp": [None], "par0": [1], "par1": ["a"]}),
        path=path,
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite_partitions",
        table="test_parquet_catalog_columns",
        database=glue_database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_parquet_catalog_columns", glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 9
    ensure_data_types_csv(df2)

    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table="test_parquet_catalog_columns") is True


@pytest.mark.parametrize("compression", [None, "gzip", "snappy"])
def test_parquet_compress(bucket, glue_database, compression):
    path = f"s3://{bucket}/test_parquet_compress_{compression}/"
    paths = wr.s3.to_parquet(
        df=get_df(),
        path=path,
        compression=compression,
        dataset=True,
        database=glue_database,
        table=f"test_parquet_compress_{compression}",
        mode="overwrite",
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(f"test_parquet_compress_{compression}", glue_database)
    ensure_data_types(df2)
    df2 = wr.s3.read_parquet(path=path)
    wr.s3.delete_objects(path=path)
    assert (
        wr.catalog.delete_table_if_exists(database=glue_database, table=f"test_parquet_compress_{compression}") is True
    )
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


@pytest.mark.parametrize("workgroup", [None, 0, 1, 2, 3])
@pytest.mark.parametrize("encryption", [None, "SSE_S3", "SSE_KMS"])
def test_athena_encryption(
    path,
    path2,
    glue_database,
    glue_table,
    glue_table2,
    kms_key,
    encryption,
    workgroup,
    workgroup0,
    workgroup1,
    workgroup2,
    workgroup3,
):
    kms_key = None if (encryption == "SSE_S3") or (encryption is None) else kms_key
    if workgroup == 0:
        workgroup = workgroup0
    elif workgroup == 1:
        workgroup = workgroup1
    elif workgroup == 2:
        workgroup = workgroup2
    elif workgroup == 3:
        workgroup = workgroup3
    df = pd.DataFrame({"a": [1, 2], "b": ["foo", "boo"]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        s3_additional_kwargs=None,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(
        table=glue_table,
        ctas_approach=True,
        database=glue_database,
        encryption=encryption,
        workgroup=workgroup,
        kms_key=kms_key,
        keep_files=True,
        ctas_temp_table_name=glue_table2,
        s3_output=path2,
    )
    assert wr.catalog.does_table_exist(database=glue_database, table=glue_table2) is False
    assert len(df2.index) == 2
    assert len(df2.columns) == 2


def test_athena_nested(path, glue_database, glue_table):
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
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        index=False,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {glue_table}", database=glue_database)
    assert len(df2.index) == 2
    assert len(df2.columns) == 4


def test_catalog_versioning(path, glue_database, glue_table):
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.s3.delete_objects(path=path)

    # Version 0
    df = pd.DataFrame({"c0": [1, 2]})
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=glue_database, table=glue_table, mode="overwrite"
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c0.dtype).startswith("Int")

    # Version 1
    df = pd.DataFrame({"c1": ["foo", "boo"]})
    paths1 = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        catalog_versioning=True,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths1, use_threads=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype) == "string"

    # Version 2
    df = pd.DataFrame({"c1": [1.0, 2.0]})
    paths2 = wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        catalog_versioning=True,
        index=False,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths2, use_threads=False)
    wr.s3.wait_objects_not_exist(paths=paths1, use_threads=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype).startswith("float")

    # Version 3 (removing version 2)
    df = pd.DataFrame({"c1": [True, False]})
    paths3 = wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        catalog_versioning=False,
        index=False,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths3, use_threads=False)
    wr.s3.wait_objects_not_exist(paths=paths2, use_threads=False)
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype).startswith("boolean")


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


def test_catalog_parameters(path, glue_database, glue_table):
    wr.s3.to_parquet(
        df=pd.DataFrame({"c0": [1, 2]}),
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        parameters={"a": "1", "b": "2"},
    )
    pars = wr.catalog.get_table_parameters(database=glue_database, table=glue_table)
    assert pars["a"] == "1"
    assert pars["b"] == "2"
    pars["a"] = "0"
    pars["c"] = "3"
    wr.catalog.upsert_table_parameters(parameters=pars, database=glue_database, table=glue_table)
    pars = wr.catalog.get_table_parameters(database=glue_database, table=glue_table)
    assert pars["a"] == "0"
    assert pars["b"] == "2"
    assert pars["c"] == "3"
    wr.catalog.overwrite_table_parameters(parameters={"d": "4"}, database=glue_database, table=glue_table)
    pars = wr.catalog.get_table_parameters(database=glue_database, table=glue_table)
    assert pars.get("a") is None
    assert pars.get("b") is None
    assert pars.get("c") is None
    assert pars["d"] == "4"
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert df.c0.sum() == 3

    wr.s3.to_parquet(
        df=pd.DataFrame({"c0": [3, 4]}),
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="append",
        parameters={"e": "5"},
    )
    pars = wr.catalog.get_table_parameters(database=glue_database, table=glue_table)
    assert pars.get("a") is None
    assert pars.get("b") is None
    assert pars.get("c") is None
    assert pars["d"] == "4"
    assert pars["e"] == "5"
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 4
    assert len(df.columns) == 1
    assert df.c0.sum() == 10


def test_athena_cache(path, glue_database, glue_table, workgroup1):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, mode="overwrite", database=glue_database, table=glue_table
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)

    df2 = wr.athena.read_sql_table(
        glue_table, glue_database, ctas_approach=False, max_cache_seconds=1, workgroup=workgroup1
    )
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()

    df2 = wr.athena.read_sql_table(
        glue_table, glue_database, ctas_approach=False, max_cache_seconds=900, workgroup=workgroup1
    )
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()

    dfs = wr.athena.read_sql_table(
        glue_table, glue_database, ctas_approach=False, max_cache_seconds=900, workgroup=workgroup1, chunksize=1
    )
    assert len(list(dfs)) == 2


def test_cache_query_ctas_approach_true(path, glue_database, glue_table):
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
    wr.s3.wait_objects_exist(paths=paths)

    with patch(
        "awswrangler.athena._check_for_cached_results", return_value={"has_valid_cache": False}
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_table(glue_table, glue_database, ctas_approach=True, max_cache_seconds=0)
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_table(glue_table, glue_database, ctas_approach=True, max_cache_seconds=900)
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()


def test_cache_query_ctas_approach_false(path, glue_database, glue_table):
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
    wr.s3.wait_objects_exist(paths=paths)

    with patch(
        "awswrangler.athena._check_for_cached_results", return_value={"has_valid_cache": False}
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_table(glue_table, glue_database, ctas_approach=False, max_cache_seconds=0)
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_table(glue_table, glue_database, ctas_approach=False, max_cache_seconds=900)
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()


def test_cache_query_semicolon(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, mode="overwrite", database=glue_database, table=glue_table
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)

    with patch(
        "awswrangler.athena._check_for_cached_results", return_value={"has_valid_cache": False}
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table}", database=glue_database, ctas_approach=True, max_cache_seconds=0
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table};", database=glue_database, ctas_approach=True, max_cache_seconds=900
        )
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()


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


def test_athena_undefined_column(glue_database):
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.read_sql_query("SELECT 1", glue_database)
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.read_sql_query("SELECT NULL AS my_null", glue_database)


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


def test_to_parquet_projection_integer(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 100, 200], "c3": [0, 1, 2]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c1", "c2", "c3"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "integer", "c2": "integer", "c3": "integer"},
        projection_ranges={"c1": "0,2", "c2": "0,200", "c3": "0,2"},
        projection_intervals={"c2": "100"},
        projection_digits={"c3": "1"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.sum()
    assert df.c2.sum() == df2.c2.sum()
    assert df.c3.sum() == df2.c3.sum()


def test_to_parquet_projection_enum(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [1, 2, 3], "c2": ["foo", "boo", "bar"]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "enum", "c2": "enum"},
        projection_values={"c1": "1,2,3", "c2": "foo,boo,bar"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.sum()


def test_to_parquet_projection_date(glue_database, glue_table, path):
    df = pd.DataFrame(
        {
            "c0": [0, 1, 2],
            "c1": [dt("2020-01-01"), dt("2020-01-02"), dt("2020-01-03")],
            "c2": [ts("2020-01-01 01:01:01.0"), ts("2020-01-01 01:01:02.0"), ts("2020-01-01 01:01:03.0")],
        }
    )
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "date", "c2": "date"},
        projection_ranges={"c1": "2020-01-01,2020-01-03", "c2": "2020-01-01 01:01:00,2020-01-01 01:01:03"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    print(df2)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()


def test_to_parquet_projection_injected(glue_database, glue_table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["foo", "boo", "bar"], "c2": ["0", "1", "2"]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "injected", "c2": "injected"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_query(f"SELECT * FROM {glue_table} WHERE c1='foo' AND c2='0'", glue_database)
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 0


def test_glue_database():

    # Round 1 - Create Database
    glue_database_name = f"database_{get_time_str_with_random_suffix()}"
    print(f"Database Name: {glue_database_name}")
    wr.catalog.create_database(name=glue_database_name, description="Database Description")
    databases = wr.catalog.get_databases()
    test_database_name = ""
    test_database_description = ""

    for database in databases:
        if database["Name"] == glue_database_name:
            test_database_name = database["Name"]
            test_database_description = database["Description"]

    assert test_database_name == glue_database_name
    assert test_database_description == "Database Description"

    # Round 2 - Delete Database
    print(f"Glue Database Name: {glue_database_name}")
    wr.catalog.delete_database(name=glue_database_name)
    databases = wr.catalog.get_databases()
    test_database_name = ""
    test_database_description = ""

    for database in databases:
        if database["Name"] == glue_database_name:
            test_database_name = database["Name"]
            test_database_description = database["Description"]

    assert test_database_name == ""
    assert test_database_description == ""


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
