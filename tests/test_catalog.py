import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import ensure_data_types_csv, get_df_csv


def test_catalog(path: str, glue_database: str, glue_table: str, account_id: str) -> None:
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
    # add & delete column
    wr.catalog.add_column(
        database=glue_database, table=glue_table, column_name="col2", column_type="int", column_comment="comment"
    )
    dtypes = wr.catalog.get_table_types(database=glue_database, table=glue_table)
    assert len(dtypes) == 5
    assert dtypes["col2"] == "int"
    wr.catalog.delete_column(database=glue_database, table=glue_table, column_name="col2")
    dtypes = wr.catalog.get_table_types(database=glue_database, table=glue_table)
    assert len(dtypes) == 4
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
    dbs = [db["Name"] for db in wr.catalog.get_databases()]
    assert len(dbs) > 0
    assert glue_database in dbs


def test_catalog_versioning(path, glue_database, glue_table, glue_table2):
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.s3.delete_objects(path=path)

    # Version 1 - Parquet
    df = pd.DataFrame({"c0": [1, 2]})
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table, mode="overwrite")[
        "paths"
    ]
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c0.dtype).startswith("Int")

    # Version 2 - Parquet
    df = pd.DataFrame({"c1": ["foo", "boo"]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        catalog_versioning=True,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 2
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype) == "string"

    # Version 1 - CSV
    df = pd.DataFrame({"c1": [1.0, 2.0]})
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table2,
        mode="overwrite",
        catalog_versioning=True,
        index=False,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table2, database=glue_database) == 1
    df = wr.athena.read_sql_table(table=glue_table2, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype).startswith("float")

    # Version 1 - CSV
    df = pd.DataFrame({"c1": [True, False]})
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table2,
        mode="overwrite",
        catalog_versioning=False,
        schema_evolution=True,
        index=False,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table2, database=glue_database) == 1
    df = wr.athena.read_sql_table(table=glue_table2, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype).startswith("boolean")

    # Version 2 - CSV
    df = pd.DataFrame({"c1": [True, False]})
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table2,
        mode="overwrite",
        catalog_versioning=True,
        schema_evolution=True,
        index=False,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table2, database=glue_database) == 2
    df = wr.athena.read_sql_table(table=glue_table2, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype).startswith("boolean")


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


def test_catalog_columns(path, glue_table, glue_database):
    wr.s3.to_parquet(
        df=get_df_csv()[["id", "date", "timestamp", "par0", "par1"]],
        path=path,
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table=glue_table,
        database=glue_database,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)

    wr.s3.to_parquet(
        df=pd.DataFrame({"id": [4], "date": [None], "timestamp": [None], "par0": [1], "par1": ["a"]}),
        path=path,
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite_partitions",
        table=glue_table,
        database=glue_database,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 9
    ensure_data_types_csv(df2)

    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table) is True


@pytest.mark.parametrize("use_catalog_id", [False, True])
def test_create_database(random_glue_database: str, account_id: str, use_catalog_id: bool):
    if not use_catalog_id:
        account_id = None
    description = "foo"
    glue_client = boto3.client("glue")

    wr.catalog.create_database(name=random_glue_database, catalog_id=account_id, description=description)
    r = glue_client.get_database(Name=random_glue_database)
    assert r["Database"]["Name"] == random_glue_database
    assert r["Database"]["Description"] == description

    with pytest.raises(wr.exceptions.AlreadyExists):
        wr.catalog.create_database(name=random_glue_database, catalog_id=account_id, description=description)

    description = "bar"
    wr.catalog.create_database(name=random_glue_database, catalog_id=account_id, description=description, exist_ok=True)
    r = glue_client.get_database(Name=random_glue_database)
    assert r["Database"]["Name"] == random_glue_database
    assert r["Database"]["Description"] == description
