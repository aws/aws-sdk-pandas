import datetime
import logging
import string

import boto3
import numpy as np
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import (
    ensure_athena_query_metadata,
    ensure_data_types,
    ensure_data_types_category,
    ensure_data_types_csv,
    get_df,
    get_df_category,
    get_df_csv,
    get_df_list,
    get_df_txt,
    get_query_long,
    get_time_str_with_random_suffix,
)

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_athena_ctas(path, path2, path3, glue_table, glue_table2, glue_database, glue_ctas_database, kms_key):
    df = get_df_list()
    columns_types, partitions_types = wr.catalog.extract_athena_types(df=df, partition_cols=["par0", "par1"])
    assert len(columns_types) == 17
    assert len(partitions_types) == 2
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.catalog.extract_athena_types(df=df, file_format="avro")
    wr.s3.to_parquet(
        df=get_df_list(),
        path=path,
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        partition_cols=["par0", "par1"],
    )
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
    ensure_athena_query_metadata(df=df, ctas_approach=True, encrypted=True)
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
        ensure_athena_query_metadata(df=df, ctas_approach=True, encrypted=False)
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
        ensure_athena_query_metadata(df=df, ctas_approach=True, encrypted=False)
    assert len(wr.s3.list_objects(path=path3)) > 2

    # ctas_database_name
    wr.s3.delete_objects(path=path3)
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table}",
        database=glue_database,
        ctas_approach=True,
        chunksize=1,
        keep_files=False,
        ctas_database_name=glue_ctas_database,
        ctas_temp_table_name=glue_table2,
        s3_output=path3,
    )
    assert wr.catalog.does_table_exist(database=glue_ctas_database, table=glue_table2) is True
    assert len(wr.s3.list_objects(path=path3)) > 2
    assert len(wr.s3.list_objects(path=final_destination)) > 0
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
        ensure_athena_query_metadata(df=df, ctas_approach=True, encrypted=False)
    assert len(wr.s3.list_objects(path=path3)) == 0


def test_athena(path, glue_database, glue_table, kms_key, workgroup0, workgroup1):
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.s3.to_parquet(
        df=get_df(),
        path=path,
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        partition_cols=["par0", "par1"],
    )
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table}",
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
        ensure_athena_query_metadata(df=df2, ctas_approach=False, encrypted=False)
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table}",
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup1,
        keep_files=False,
    )
    assert len(df.index) == 3
    ensure_data_types(df=df)
    ensure_athena_query_metadata(df=df, ctas_approach=False, encrypted=False)
    wr.athena.repair_table(table=glue_table, database=glue_database)
    assert len(wr.athena.describe_table(database=glue_database, table=glue_table).index) > 0
    assert (
        wr.catalog.table(database=glue_database, table=glue_table).to_dict()
        == wr.athena.describe_table(database=glue_database, table=glue_table).to_dict()
    )
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table} WHERE iint8 = :iint8_value;",
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup1,
        keep_files=False,
        params={"iint8_value": 1},
    )
    assert len(df.index) == 1
    ensure_athena_query_metadata(df=df, ctas_approach=False, encrypted=False)
    query = wr.athena.show_create_table(database=glue_database, table=glue_table)
    assert (
        query.split("LOCATION")[0] == f"CREATE EXTERNAL TABLE `{glue_table}`"
        f"( `iint8` tinyint,"
        f" `iint16` smallint,"
        f" `iint32` int,"
        f" `iint64` bigint,"
        f" `float` float,"
        f" `ddouble` double,"
        f" `decimal` decimal(3,2),"
        f" `string_object` string,"
        f" `string` string,"
        f" `date` date,"
        f" `timestamp` timestamp,"
        f" `bool` boolean,"
        f" `binary` binary,"
        f" `category` double,"
        f" `__index_level_0__` bigint) "
        f"PARTITIONED BY ( `par0` bigint, `par1` string) "
        f"ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "
        f"STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "
        f"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' "
    )


def test_catalog(path: str, glue_database: str, glue_table: str) -> None:
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
    df1 = wr.athena.read_sql_query(sql=sql, database=glue_database)
    assert df1.empty is True
    ensure_athena_query_metadata(df=df1, ctas_approach=True, encrypted=False)
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


def test_category(path, glue_table, glue_database):
    df = get_df_category()
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        partition_cols=["par0", "par1"],
    )
    df2 = wr.s3.read_parquet(path=path, dataset=True, categories=[c for c in df.columns if c not in ["par0", "par1"]])
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_query(f"SELECT * FROM {glue_table}", database=glue_database, categories=list(df.columns))
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_table(table=glue_table, database=glue_database, categories=list(df.columns))
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_query(
        f"SELECT * FROM {glue_table}", database=glue_database, categories=list(df.columns), ctas_approach=False
    )
    ensure_data_types_category(df2)
    dfs = wr.athena.read_sql_query(
        f"SELECT * FROM {glue_table}",
        database=glue_database,
        categories=list(df.columns),
        ctas_approach=False,
        chunksize=1,
    )
    for df2 in dfs:
        ensure_data_types_category(df2)
    dfs = wr.athena.read_sql_query(
        f"SELECT * FROM {glue_table}",
        database=glue_database,
        categories=list(df.columns),
        ctas_approach=True,
        chunksize=1,
    )
    for df2 in dfs:
        ensure_data_types_category(df2)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table) is True


@pytest.mark.parametrize("workgroup", [None, 0, 1, 2, 3])
@pytest.mark.parametrize("encryption", [None, "SSE_S3", "SSE_KMS"])
@pytest.mark.parametrize("ctas_approach", [False, True])
def test_athena_encryption(
    path,
    path2,
    glue_database,
    glue_table,
    glue_table2,
    kms_key,
    ctas_approach,
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
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        s3_additional_kwargs=None,
    )
    df2 = wr.athena.read_sql_table(
        table=glue_table,
        ctas_approach=ctas_approach,
        database=glue_database,
        encryption=encryption,
        workgroup=workgroup,
        kms_key=kms_key,
        keep_files=True,
        ctas_temp_table_name=glue_table2,
        s3_output=path2,
    )
    assert wr.catalog.does_table_exist(database=glue_database, table=glue_table2) is False
    assert df2.shape == (2, 2)


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
    wr.s3.to_parquet(
        df=df,
        path=path,
        index=False,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
    )
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {glue_table}", database=glue_database)
    assert len(df2.index) == 2
    assert len(df2.columns) == 4


def test_catalog_versioning(path, glue_database, glue_table):
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.s3.delete_objects(path=path)

    # Version 0
    df = pd.DataFrame({"c0": [1, 2]})
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table, mode="overwrite")[
        "paths"
    ]
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 1
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c0.dtype).startswith("Int")

    # Version 1
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

    # Version 2
    df = pd.DataFrame({"c1": [1.0, 2.0]})
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        catalog_versioning=True,
        index=False,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 3
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype).startswith("float")

    # Version 3 (removing version 2)
    df = pd.DataFrame({"c1": [True, False]})
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="overwrite",
        catalog_versioning=False,
        index=False,
    )
    assert wr.catalog.get_table_number_of_versions(table=glue_table, database=glue_database) == 3
    df = wr.athena.read_sql_table(table=glue_table, database=glue_database)
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


def test_athena_undefined_column(glue_database):
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.read_sql_query("SELECT 1", glue_database)
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.read_sql_query("SELECT NULL AS my_null", glue_database)


def test_glue_database():

    # Round 1 - Create Database
    glue_database_name = f"database_{get_time_str_with_random_suffix()}"
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


def test_read_sql_query_wo_results(path, glue_database, glue_table):
    wr.catalog.create_parquet_table(database=glue_database, table=glue_table, path=path, columns_types={"c0": "int"})
    sql = f"ALTER TABLE {glue_database}.{glue_table} SET LOCATION '{path}dir/'"
    df = wr.athena.read_sql_query(sql, database=glue_database, ctas_approach=False)
    assert df.empty
    ensure_athena_query_metadata(df=df, ctas_approach=False, encrypted=False)


def test_read_sql_query_wo_results_ctas(path, glue_database, glue_table):
    wr.catalog.create_parquet_table(database=glue_database, table=glue_table, path=path, columns_types={"c0": "int"})
    sql = f"ALTER TABLE {glue_database}.{glue_table} SET LOCATION '{path}dir/'"
    with pytest.raises(wr.exceptions.InvalidCtasApproachQuery):
        wr.athena.read_sql_query(sql, database=glue_database, ctas_approach=True)


def test_read_sql_query_duplicated_col_name(glue_database):
    sql = "SELECT 1 AS foo, 2 AS foo"
    df = wr.athena.read_sql_query(sql, database=glue_database, ctas_approach=False)
    assert df.shape == (1, 2)
    assert df.columns.to_list() == ["foo", "foo.1"]


def test_read_sql_query_duplicated_col_name_ctas(glue_database):
    sql = "SELECT 1 AS foo, 2 AS foo"
    with pytest.raises(wr.exceptions.InvalidCtasApproachQuery):
        wr.athena.read_sql_query(sql, database=glue_database, ctas_approach=True)


def test_parse_describe_table():
    df = get_df_txt()
    parsed_df = wr.athena._utils._parse_describe_table(df)
    assert parsed_df["Partition"].to_list() == [False, False, False, True, True]
    assert parsed_df["Column Name"].to_list() == ["iint8", "iint16", "iint32", "par0", "par1"]


def test_describe_table(path, glue_database, glue_table):
    wr.catalog.create_parquet_table(database=glue_database, table=glue_table, path=path, columns_types={"c0": "int"})
    assert wr.athena.describe_table(database=glue_database, table=glue_table).shape == (1, 4)


@pytest.mark.parametrize("data_source", [None, "AwsDataCatalog"])
@pytest.mark.parametrize("ctas_approach", [False, True])
def test_athena_nan_inf(glue_database, ctas_approach, data_source):
    sql = "SELECT nan() AS nan, infinity() as inf, -infinity() as inf_n, 1.2 as regular"
    df = wr.athena.read_sql_query(sql, glue_database, ctas_approach, data_source=data_source)
    assert df.shape == (1, 4)
    assert df.dtypes.to_list() == ["float64", "float64", "float64", "float64"]
    assert np.isnan(df.nan.iloc[0])
    assert df.inf.iloc[0] == np.PINF
    assert df.inf_n.iloc[0] == np.NINF
    assert df.regular.iloc[0] == 1.2


def test_athena_ctas_data_source(glue_database):
    sql = "SELECT nan() AS nan, infinity() as inf, -infinity() as inf_n, 1.2 as regular"
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.athena.read_sql_query(sql, glue_database, True, data_source="foo")


def test_chunked_ctas_false(glue_database):
    sql = "SELECT 1 AS foo, 2 AS boo"
    df_iter = wr.athena.read_sql_query(sql, database=glue_database, ctas_approach=False, chunksize=True)
    assert len(list(df_iter)) == 1


def test_bucketing_catalog_parquet_table(path, glue_database, glue_table):
    nb_of_buckets = 3
    bucket_cols = ["col0"]
    wr.catalog.create_parquet_table(
        database=glue_database,
        table=glue_table,
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        bucketing_info=(bucket_cols, nb_of_buckets),
    )

    table = next(wr.catalog.get_tables(name_contains=glue_table))
    assert table["StorageDescriptor"]["NumberOfBuckets"] == nb_of_buckets
    assert table["StorageDescriptor"]["BucketColumns"] == bucket_cols
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)


@pytest.mark.parametrize("bucketing_data", [[0, 1, 2], [False, True, False], ["b", "c", "d"]])
@pytest.mark.parametrize(
    "dtype",
    [
        "int",
        "int8",
        "Int8",
        "int16",
        "Int16",
        "int32",
        "Int32",
        "int64",
        "Int64",
        "bool",
        "boolean",
        "object",
        "string",
    ],
)
def test_bucketing_parquet_dataset(path, glue_database, glue_table, bucketing_data, dtype):
    # Skip invalid combinations of data and data types
    if type(bucketing_data[0]) == int and "int" not in dtype.lower():
        pytest.skip()
    if type(bucketing_data[0]) == bool and "bool" not in dtype.lower():
        pytest.skip()
    if type(bucketing_data[0]) == str and (dtype != "string" or dtype != "object"):
        pytest.skip()

    nb_of_buckets = 2
    df = pd.DataFrame({"c0": bucketing_data, "c1": ["foo", "bar", "baz"]})
    r = wr.s3.to_parquet(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        bucketing_info=(["c0"], nb_of_buckets),
    )

    assert len(r["paths"]) == 2
    assert r["paths"][0].endswith("bucket-00000.snappy.parquet")
    assert r["paths"][1].endswith("bucket-00001.snappy.parquet")

    dtype = None
    if isinstance(bucketing_data[0], int):
        dtype = pd.Int64Dtype()
    if isinstance(bucketing_data[0], bool):
        dtype = pd.BooleanDtype()
    if isinstance(bucketing_data[0], str):
        dtype = pd.StringDtype()

    first_bucket_df = wr.s3.read_parquet(path=[r["paths"][0]])
    assert len(first_bucket_df) == 2
    assert pd.Series([bucketing_data[0], bucketing_data[2]], dtype=dtype).equals(first_bucket_df["c0"])
    assert pd.Series(["foo", "baz"], dtype=pd.StringDtype()).equals(first_bucket_df["c1"])

    second_bucket_df = wr.s3.read_parquet(path=[r["paths"][1]])
    assert len(second_bucket_df) == 1
    assert pd.Series([bucketing_data[1]], dtype=dtype).equals(second_bucket_df["c0"])
    assert pd.Series(["bar"], dtype=pd.StringDtype()).equals(second_bucket_df["c1"])

    loaded_dfs = [
        wr.s3.read_parquet(path=path),
        wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=False),
    ]

    for loaded_df in loaded_dfs:
        assert len(loaded_df) == 3
        assert all(x in bucketing_data for x in loaded_df["c0"].to_list())


def test_bucketing_catalog_csv_table(path, glue_database, glue_table):
    nb_of_buckets = 3
    bucket_cols = ["col0"]
    wr.catalog.create_csv_table(
        database=glue_database,
        table=glue_table,
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        bucketing_info=(bucket_cols, nb_of_buckets),
    )

    table = next(wr.catalog.get_tables(name_contains=glue_table))
    assert table["StorageDescriptor"]["NumberOfBuckets"] == nb_of_buckets
    assert table["StorageDescriptor"]["BucketColumns"] == bucket_cols
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)


@pytest.mark.parametrize("bucketing_data", [[0, 1, 2], [False, True, False], ["b", "c", "d"]])
@pytest.mark.parametrize(
    "dtype",
    [
        "int",
        "int8",
        "Int8",
        "int16",
        "Int16",
        "int32",
        "Int32",
        "int64",
        "Int64",
        "bool",
        "boolean",
        "object",
        "string",
    ],
)
def test_bucketing_csv_dataset(path, glue_database, glue_table, bucketing_data, dtype):
    # Skip invalid combinations of data and data types
    if type(bucketing_data[0]) == int and "int" not in dtype.lower():
        pytest.skip()
    if type(bucketing_data[0]) == bool and "bool" not in dtype.lower():
        pytest.skip()
    if type(bucketing_data[0]) == str and (dtype != "string" or dtype != "object"):
        pytest.skip()

    nb_of_buckets = 2
    df = pd.DataFrame({"c0": bucketing_data, "c1": ["foo", "bar", "baz"]})
    r = wr.s3.to_csv(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        bucketing_info=(["c0"], nb_of_buckets),
        index=False,
    )

    assert len(r["paths"]) == 2
    assert r["paths"][0].endswith("bucket-00000.csv")
    assert r["paths"][1].endswith("bucket-00001.csv")

    first_bucket_df = wr.s3.read_csv(path=[r["paths"][0]], header=None, names=["c0", "c1"])
    assert len(first_bucket_df) == 2
    assert pd.Series([bucketing_data[0], bucketing_data[2]]).equals(first_bucket_df["c0"])
    assert pd.Series(["foo", "baz"]).equals(first_bucket_df["c1"])

    second_bucket_df = wr.s3.read_csv(path=[r["paths"][1]], header=None, names=["c0", "c1"])
    assert len(second_bucket_df) == 1
    assert pd.Series([bucketing_data[1]]).equals(second_bucket_df["c0"])
    assert pd.Series(["bar"]).equals(second_bucket_df["c1"])

    loaded_dfs = [
        wr.s3.read_csv(path=path, header=None, names=["c0", "c1"]),
        wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=False),
    ]

    for loaded_df in loaded_dfs:
        assert len(loaded_df) == 3
        assert all(x in bucketing_data for x in loaded_df["c0"].to_list())


@pytest.mark.parametrize("bucketing_data", [[0, 1, 2, 3], [False, True, False, True], ["b", "c", "d", "e"]])
def test_combined_bucketing_partitioning_parquet_dataset(path, glue_database, glue_table, bucketing_data):
    nb_of_buckets = 2
    df = pd.DataFrame(
        {"c0": bucketing_data, "c1": ["foo", "bar", "baz", "boo"], "par_col": ["par1", "par1", "par2", "par2"]}
    )
    r = wr.s3.to_parquet(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        partition_cols=["par_col"],
        bucketing_info=(["c0"], nb_of_buckets),
    )

    assert len(r["paths"]) == 4
    assert r["paths"][0].endswith("bucket-00000.snappy.parquet")
    assert r["paths"][1].endswith("bucket-00001.snappy.parquet")
    partitions_values_keys = list(r["partitions_values"].keys())
    assert partitions_values_keys[0] in r["paths"][0]
    assert partitions_values_keys[0] in r["paths"][1]

    assert r["paths"][2].endswith("bucket-00000.snappy.parquet")
    assert r["paths"][3].endswith("bucket-00001.snappy.parquet")
    assert partitions_values_keys[1] in r["paths"][2]
    assert partitions_values_keys[1] in r["paths"][3]

    dtype = None
    if isinstance(bucketing_data[0], int):
        dtype = pd.Int64Dtype()
    if isinstance(bucketing_data[0], bool):
        dtype = pd.BooleanDtype()
    if isinstance(bucketing_data[0], str):
        dtype = pd.StringDtype()

    bucket_df = wr.s3.read_parquet(path=[r["paths"][0]])
    assert len(bucket_df) == 1
    assert pd.Series([bucketing_data[0]], dtype=dtype).equals(bucket_df["c0"])
    assert pd.Series(["foo"], dtype=pd.StringDtype()).equals(bucket_df["c1"])

    bucket_df = wr.s3.read_parquet(path=[r["paths"][1]])
    assert len(bucket_df) == 1
    assert pd.Series([bucketing_data[1]], dtype=dtype).equals(bucket_df["c0"])
    assert pd.Series(["bar"], dtype=pd.StringDtype()).equals(bucket_df["c1"])

    bucket_df = wr.s3.read_parquet(path=[r["paths"][2]])
    assert len(bucket_df) == 1
    assert pd.Series([bucketing_data[2]], dtype=dtype).equals(bucket_df["c0"])
    assert pd.Series(["baz"], dtype=pd.StringDtype()).equals(bucket_df["c1"])

    bucket_df = wr.s3.read_parquet(path=[r["paths"][3]])
    assert len(bucket_df) == 1
    assert pd.Series([bucketing_data[3]], dtype=dtype).equals(bucket_df["c0"])
    assert pd.Series(["boo"], dtype=pd.StringDtype()).equals(bucket_df["c1"])

    loaded_dfs = [
        wr.s3.read_parquet(path=path),
        wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=False),
    ]

    for loaded_df in loaded_dfs:
        assert len(loaded_df) == 4
        assert all(x in bucketing_data for x in loaded_df["c0"].to_list())


@pytest.mark.parametrize("bucketing_data", [[0, 1, 2, 3], [False, True, False, True], ["b", "c", "d", "e"]])
def test_combined_bucketing_partitioning_csv_dataset(path, glue_database, glue_table, bucketing_data):
    nb_of_buckets = 2
    df = pd.DataFrame(
        {"c0": bucketing_data, "c1": ["foo", "bar", "baz", "boo"], "par_col": ["par1", "par1", "par2", "par2"]}
    )
    r = wr.s3.to_csv(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        partition_cols=["par_col"],
        bucketing_info=(["c0"], nb_of_buckets),
        index=False,
    )

    assert len(r["paths"]) == 4
    assert r["paths"][0].endswith("bucket-00000.csv")
    assert r["paths"][1].endswith("bucket-00001.csv")
    partitions_values_keys = list(r["partitions_values"].keys())
    assert partitions_values_keys[0] in r["paths"][0]
    assert partitions_values_keys[0] in r["paths"][1]

    assert r["paths"][2].endswith("bucket-00000.csv")
    assert r["paths"][3].endswith("bucket-00001.csv")
    assert partitions_values_keys[1] in r["paths"][2]
    assert partitions_values_keys[1] in r["paths"][3]

    bucket_df = wr.s3.read_csv(path=[r["paths"][0]], header=None, names=["c0", "c1"])
    assert len(bucket_df) == 1
    assert pd.Series([bucketing_data[0]]).equals(bucket_df["c0"])
    assert pd.Series(["foo"]).equals(bucket_df["c1"])

    bucket_df = wr.s3.read_csv(path=[r["paths"][1]], header=None, names=["c0", "c1"])
    assert len(bucket_df) == 1
    assert pd.Series([bucketing_data[1]]).equals(bucket_df["c0"])
    assert pd.Series(["bar"]).equals(bucket_df["c1"])

    bucket_df = wr.s3.read_csv(path=[r["paths"][2]], header=None, names=["c0", "c1"])
    assert len(bucket_df) == 1
    assert pd.Series([bucketing_data[2]]).equals(bucket_df["c0"])
    assert pd.Series(["baz"]).equals(bucket_df["c1"])

    bucket_df = wr.s3.read_csv(path=[r["paths"][3]], header=None, names=["c0", "c1"])
    assert len(bucket_df) == 1
    assert pd.Series([bucketing_data[3]]).equals(bucket_df["c0"])
    assert pd.Series(["boo"]).equals(bucket_df["c1"])

    loaded_dfs = [
        wr.s3.read_csv(path=path, header=None, names=["c0", "c1"]),
        wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=False),
    ]

    for loaded_df in loaded_dfs:
        assert len(loaded_df) == 4
        assert all(x in bucketing_data for x in loaded_df["c0"].to_list())


def test_multiple_bucketing_columns_parquet_dataset(path, glue_database, glue_table):
    nb_of_buckets = 2
    df = pd.DataFrame({"c0": [0, 1, 2, 3], "c1": [4, 6, 5, 7], "c2": ["foo", "bar", "baz", "boo"]})
    r = wr.s3.to_parquet(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        bucketing_info=(["c0", "c1"], nb_of_buckets),
    )

    assert len(r["paths"]) == 2
    assert r["paths"][0].endswith("bucket-00000.snappy.parquet")
    assert r["paths"][1].endswith("bucket-00001.snappy.parquet")

    first_bucket_df = wr.s3.read_parquet(path=[r["paths"][0]])
    assert len(first_bucket_df) == 2
    assert pd.Series([0, 3], dtype=pd.Int64Dtype()).equals(first_bucket_df["c0"])
    assert pd.Series([4, 7], dtype=pd.Int64Dtype()).equals(first_bucket_df["c1"])
    assert pd.Series(["foo", "boo"], dtype=pd.StringDtype()).equals(first_bucket_df["c2"])

    second_bucket_df = wr.s3.read_parquet(path=[r["paths"][1]])
    assert len(second_bucket_df) == 2
    assert pd.Series([1, 2], dtype=pd.Int64Dtype()).equals(second_bucket_df["c0"])
    assert pd.Series([6, 5], dtype=pd.Int64Dtype()).equals(second_bucket_df["c1"])
    assert pd.Series(["bar", "baz"], dtype=pd.StringDtype()).equals(second_bucket_df["c2"])


@pytest.mark.parametrize("dtype", ["int", "str", "bool"])
def test_bucketing_csv_saving(path, glue_database, glue_table, dtype):
    nb_of_rows = 1_000
    if dtype == "int":
        nb_of_buckets = 10
        saving_factor = 10
        data = np.arange(nb_of_rows)
        query_params = {"c0": 0}
    elif dtype == "str":
        nb_of_buckets = 10
        saving_factor = 9.9
        data = [string.ascii_letters[i % nb_of_buckets] for i in range(nb_of_rows)]
        query_params = {"c0": "'a'"}
    elif dtype == "bool":
        nb_of_buckets = 2
        saving_factor = 2.1
        data = [bool(i % nb_of_buckets) for i in range(nb_of_rows)]
        query_params = {"c0": True}
    else:
        raise ValueError(f"Invalid Argument for dtype: {dtype}")
    query = f"SELECT c0 FROM {glue_table} WHERE c0=:c0;"
    df = pd.DataFrame({"c0": data})

    # Regular
    wr.s3.to_csv(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        index=False,
    )
    df2 = wr.athena.read_sql_query(query, database=glue_database, params=query_params, ctas_approach=False)
    scanned_regular = df2.query_metadata["Statistics"]["DataScannedInBytes"]

    # Bucketed
    wr.s3.to_csv(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        bucketing_info=(["c0"], nb_of_buckets),
        index=False,
    )
    df3 = wr.athena.read_sql_query(query, database=glue_database, params=query_params, ctas_approach=False)
    scanned_bucketed = df3.query_metadata["Statistics"]["DataScannedInBytes"]

    print(scanned_bucketed)
    assert df2.equals(df3)
    assert scanned_regular >= scanned_bucketed * saving_factor


def test_bucketing_combined_csv_saving(path, glue_database, glue_table):
    nb_of_rows = 1_000
    nb_of_buckets = 10
    df = pd.DataFrame(
        {
            "c0": np.arange(nb_of_rows),
            "c1": [string.ascii_letters[i % nb_of_buckets] for i in range(nb_of_rows)],
            "c2": [bool(i % 2) for i in range(nb_of_rows)],
        }
    )
    query = f"SELECT c0 FROM {glue_table} WHERE c0=0 AND c1='a' AND c2=TRUE"

    # Regular
    wr.s3.to_csv(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        index=False,
    )
    df2 = wr.athena.read_sql_query(query, database=glue_database, ctas_approach=False)
    scanned_regular = df2.query_metadata["Statistics"]["DataScannedInBytes"]

    # Bucketed
    wr.s3.to_csv(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        mode="overwrite",
        bucketing_info=(["c0", "c1", "c2"], nb_of_buckets),
        index=False,
    )
    df3 = wr.athena.read_sql_query(query, database=glue_database, ctas_approach=False)
    scanned_bucketed = df3.query_metadata["Statistics"]["DataScannedInBytes"]

    assert df2.equals(df3)
    assert scanned_regular >= scanned_bucketed * nb_of_buckets
