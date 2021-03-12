import logging

import pandas as pd
import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("concurrent_partitioning", [True, False])
def test_routine_0(glue_database, glue_table, path, use_threads, concurrent_partitioning):

    # Round 1 - Warm up
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        description="c0",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c0": "0"},
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
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
    wr.s3.to_parquet(
        df=df,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c1": "1"},
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
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
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index) * 2)},
        columns_comments={"c1": "1"},
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
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
    wr.s3.to_parquet(
        df=df,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c1+c2",
        parameters={"num_cols": "2", "num_rows": "9"},
        columns_comments={"c1": "1", "c2": "2"},
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
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
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        description="c1+c2+c3",
        parameters={"num_cols": "3", "num_rows": "10"},
        columns_comments={"c1": "1!", "c2": "2!", "c3": "3"},
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
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
    wr.s3.to_parquet(
        df=df,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        partition_cols=["c1"],
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "2"},
        columns_comments={"c0": "zero", "c1": "one"},
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
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
    wr.s3.to_parquet(
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
        concurrent_partitioning=concurrent_partitioning,
        use_threads=use_threads,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
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
    wr.s3.to_parquet(
        df=df,
        dataset=True,
        mode="overwrite_partitions",
        database=glue_database,
        table=glue_table,
        partition_cols=["c1"],
        description="c0+c1+c2",
        parameters={"num_cols": "3", "num_rows": "4"},
        columns_comments={"c0": "zero", "c1": "one", "c2": "two"},
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
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


def test_routine_1(glue_database, glue_table, path):

    # Round 1 - Warm up
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")
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
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
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
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")
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
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
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
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="append")
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
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
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
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="append")
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
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
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
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", partition_cols=["c1"])
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
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
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
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite_partitions", partition_cols=["c1"])
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
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
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
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite_partitions", partition_cols=["c1"])
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
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
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
