import datetime
import logging
import string
from typing import Any
from unittest.mock import patch

import boto3
import botocore
import numpy as np
import pytest
from pandas import DataFrame as PandasDataFrame

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import (
    assert_pandas_equals,
    ensure_athena_ctas_table,
    ensure_athena_query_metadata,
    ensure_data_types,
    ensure_data_types_category,
    get_df,
    get_df_category,
    get_df_list,
    get_df_txt,
    get_time_str_with_random_suffix,
    pandas_equals,
    ts,
)

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


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
        ctas_parameters=wr.typing.AthenaCTASSettings(
            temp_table_name=glue_table2,
        ),
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
        ctas_parameters=wr.typing.AthenaCTASSettings(
            temp_table_name=glue_table2,
        ),
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
        ctas_parameters=wr.typing.AthenaCTASSettings(
            database=glue_ctas_database,
            temp_table_name=glue_table2,
        ),
        s3_output=path3,
    )
    assert wr.catalog.does_table_exist(database=glue_ctas_database, table=glue_table2) is False
    assert len(wr.s3.list_objects(path=path3)) > 2
    assert len(wr.s3.list_objects(path=final_destination)) > 0
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
        ensure_athena_query_metadata(df=df, ctas_approach=True, encrypted=False)
    assert len(wr.s3.list_objects(path=path3)) == 0


@pytest.mark.modin_index
def test_athena_read_sql_ctas_bucketing(path, path2, glue_table, glue_table2, glue_database, glue_ctas_database):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "bar"]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    df_ctas = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table}",
        ctas_approach=True,
        database=glue_database,
        ctas_parameters=wr.typing.AthenaCTASSettings(
            database=glue_ctas_database,
            temp_table_name=glue_table2,
            bucketing_info=(["c0"], 1),
        ),
        s3_output=path2,
        pyarrow_additional_kwargs={"ignore_metadata": True},
    )
    df_no_ctas = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table}",
        ctas_approach=False,
        database=glue_database,
        s3_output=path2,
        pyarrow_additional_kwargs={"ignore_metadata": True},
    )
    assert df_ctas.equals(df_no_ctas)


def test_athena_create_ctas(path, glue_table, glue_table2, glue_database, glue_ctas_database, kms_key):
    boto3_session = boto3.DEFAULT_SESSION
    wr.s3.to_parquet(
        df=get_df_list(),
        path=path,
        index=False,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        partition_cols=["par0", "par1"],
    )

    # Select *
    ctas_query_info = wr.athena.create_ctas_table(
        sql=f"select * from {glue_table}",
        database=glue_database,
        encryption="SSE_KMS",
        kms_key=kms_key,
        wait=False,
    )
    ensure_athena_ctas_table(ctas_query_info=ctas_query_info, boto3_session=boto3_session)

    # Schema only (i.e. WITH NO DATA)
    ctas_query_info = wr.athena.create_ctas_table(
        sql=f"select * from {glue_table}",
        database=glue_database,
        ctas_table=glue_table2,
        schema_only=True,
        wait=True,
    )
    ensure_athena_ctas_table(ctas_query_info=ctas_query_info, boto3_session=boto3_session)

    # Convert to new data storage and compression
    ctas_query_info = wr.athena.create_ctas_table(
        sql=f"select string, bool from {glue_table}",
        database=glue_database,
        storage_format="avro",
        write_compression="snappy",
        wait=False,
    )
    ensure_athena_ctas_table(ctas_query_info=ctas_query_info, boto3_session=boto3_session)

    # Partition and save to CTAS database
    ctas_query_info = wr.athena.create_ctas_table(
        sql=f"select * from {glue_table}",
        database=glue_database,
        ctas_database=glue_ctas_database,
        partitioning_info=["par0", "par1"],
        wait=True,
    )
    ensure_athena_ctas_table(ctas_query_info=ctas_query_info, boto3_session=boto3_session)


def test_athena(path, glue_database, glue_table, kms_key, workgroup0, workgroup1):
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
        sql=f"SELECT * FROM {glue_table} WHERE iint8 = :iint8_value",
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


def test_athena_orc(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "bar", "foo"], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["c1"] = df["c1"].astype("string")
    df["par"] = df["par"].astype("string")

    wr.s3.to_orc(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        partition_cols=["par"],
    )
    df_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        keep_files=False,
    )
    df_out = df_out.sort_values(by="c0", ascending=True).reset_index(drop=True)

    assert_pandas_equals(df, df_out)


@pytest.mark.parametrize(
    "ctas_approach,unload_approach",
    [
        pytest.param(False, False, id="regular"),
        pytest.param(True, False, id="ctas"),
        pytest.param(False, True, id="unload"),
    ],
)
@pytest.mark.parametrize(
    "col_name,col_value", [("string", "Washington"), ("iint32", "1"), ("date", "DATE '2020-01-01'")]
)
def test_athena_paramstyle_qmark_parameters(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
    workgroup0: str,
    ctas_approach: bool,
    unload_approach: bool,
    col_name: str,
    col_value: Any,
) -> None:
    wr.s3.to_parquet(
        df=get_df(),
        path=path,
        index=False,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        partition_cols=["par0", "par1"],
    )

    df_out = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table} WHERE {col_name} = ?",
        database=glue_database,
        ctas_approach=ctas_approach,
        unload_approach=unload_approach,
        workgroup=workgroup0,
        params=[col_value],
        paramstyle="qmark",
        keep_files=False,
        s3_output=path2,
    )
    ensure_data_types(df=df_out)
    ensure_athena_query_metadata(df=df_out, ctas_approach=ctas_approach, encrypted=False)

    assert len(df_out) == 1


def test_read_sql_query_parameter_formatting_respects_prefixes(path, glue_database, glue_table, workgroup0):
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
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table} WHERE string = :string OR string_object = :string_object",
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup0,
        keep_files=False,
        params={"string": "Seattle", "string_object": "boo"},
    )
    assert len(df) == 2


@pytest.mark.parametrize(
    "col_name,col_value",
    [("string", "Seattle"), ("date", datetime.date(2020, 1, 1)), ("bool", True), ("category", 1.0)],
)
def test_read_sql_query_parameter_formatting(path, glue_database, glue_table, workgroup0, col_name, col_value):
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
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table} WHERE {col_name} = :value",
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup0,
        keep_files=False,
        params={"value": col_value},
    )
    assert len(df.index) == 1


@pytest.mark.parametrize("col_name", [("string"), ("date"), ("bool"), ("category")])
def test_read_sql_query_parameter_formatting_null(path, glue_database, glue_table, workgroup0, col_name):
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
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table} WHERE {col_name} IS :value",
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup0,
        keep_files=False,
        params={"value": None},
    )
    assert len(df.index) == 1


@pytest.mark.xfail(raises=botocore.exceptions.ClientError, reason="QueryId not found.")
def test_athena_query_cancelled(glue_database):
    query_execution_id = wr.athena.start_query_execution(
        sql="SELECT " + "rand(), " * 10000 + "rand()", database=glue_database
    )
    wr.athena.stop_query_execution(query_execution_id=query_execution_id)
    with pytest.raises(wr.exceptions.QueryCancelled):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)


def test_athena_query_failed(glue_database):
    query_execution_id = wr.athena.start_query_execution(sql="SELECT random(-1)", database=glue_database)
    with pytest.raises(wr.exceptions.QueryFailed):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)


def test_athena_read_list(glue_database):
    df = wr.athena.read_sql_query(sql="SELECT ARRAY[1, 2, 3] AS col0", database=glue_database, ctas_approach=False)
    assert len(df) == 1
    assert len(df.index) == 1
    assert len(df.columns) == 1
    assert df["col0"].iloc[0] == "[1, 2, 3]"


def test_sanitize_dataframe_column_names():
    with pytest.warns(UserWarning, match=r"Duplicate*"):
        test_df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        test_df.columns = ["a", "a"]
        assert wr.catalog.sanitize_dataframe_columns_names(df=pd.DataFrame({"A": [1, 2], "a": [3, 4]})).equals(test_df)
    assert wr.catalog.sanitize_dataframe_columns_names(
        df=pd.DataFrame({"A": [1, 2], "a": [3, 4]}), handle_duplicate_columns="drop"
    ).equals(pd.DataFrame({"a": [1, 2]}))
    assert wr.catalog.sanitize_dataframe_columns_names(
        df=pd.DataFrame({"A": [1, 2], "a": [3, 4], "a_1": [5, 6]}), handle_duplicate_columns="rename"
    ).equals(pd.DataFrame({"a": [1, 2], "a_1": [3, 4], "a_1_1": [5, 6]}))


def test_sanitize_names():
    assert wr.catalog.sanitize_column_name("CamelCase") == "camelcase"
    assert wr.catalog.sanitize_column_name("CamelCase2") == "camelcase2"
    assert wr.catalog.sanitize_column_name("Camel_Case3") == "camel_case3"
    assert wr.catalog.sanitize_column_name("Cámël_Casë4仮") == "camel_case4_"
    assert wr.catalog.sanitize_column_name("Camel__Case5") == "camel__case5"
    assert wr.catalog.sanitize_column_name("Camel{}Case6") == "camel_case6"
    assert wr.catalog.sanitize_column_name("Camel.Case7") == "camel_case7"
    assert wr.catalog.sanitize_column_name("xyz_cd") == "xyz_cd"
    assert wr.catalog.sanitize_column_name("xyz_Cd") == "xyz_cd"
    assert wr.catalog.sanitize_table_name("CamelCase") == "camelcase"
    assert wr.catalog.sanitize_table_name("CamelCase2") == "camelcase2"
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
    assert len(list(wr.athena.read_sql_query(sql=sql, database=glue_database, chunksize=1))) == 1


def test_athena_struct_simple(path, glue_database):
    sql = "SELECT CAST(ROW(1, 'foo') AS ROW(id BIGINT, value VARCHAR)) AS col0"
    # Regular approach
    df = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=False)
    assert len(df) == 1
    assert len(df.index) == 1
    assert len(df.columns) == 1
    assert df["col0"].iloc[0] == "{id=1, value=foo}"
    # CTAS and UNLOAD
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=True, unload_approach=True)
    # CTAS approach
    df_ctas = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=True)
    assert len(df_ctas.index) == 1
    assert len(df_ctas.columns) == 1
    assert df_ctas["col0"].iloc[0]["id"] == 1
    assert df_ctas["col0"].iloc[0]["value"] == "foo"
    # UNLOAD approach
    df_unload = wr.athena.read_sql_query(
        sql=sql, database=glue_database, ctas_approach=False, unload_approach=True, s3_output=path
    )
    assert df_unload.equals(df_ctas)


def test_athena_struct_nested(path, glue_database):
    sql = (
        "SELECT CAST("
        "    ROW(1, ROW(2, ROW(3, '4'))) AS"
        "    ROW(field0 BIGINT, field1 ROW(field2 BIGINT, field3 ROW(field4 BIGINT, field5 VARCHAR)))"
        ") AS col0"
    )
    # CTAS approach
    df_ctas = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=True)
    assert len(df_ctas.index) == 1
    assert len(df_ctas.columns) == 1
    assert df_ctas["col0"].iloc[0]["field0"] == 1
    assert df_ctas["col0"].iloc[0]["field1"]["field2"] == 2
    assert df_ctas["col0"].iloc[0]["field1"]["field3"]["field4"] == 3
    assert df_ctas["col0"].iloc[0]["field1"]["field3"]["field5"] == "4"
    # UNLOAD approach
    df_unload = wr.athena.read_sql_query(
        sql=sql, database=glue_database, ctas_approach=False, unload_approach=True, s3_output=path
    )
    assert df_unload.equals(df_ctas)


def test_athena_time_zone(glue_database):
    sql = "SELECT current_timestamp AS value, typeof(current_timestamp) AS type"
    df = wr.athena.read_sql_query(sql=sql, database=glue_database, ctas_approach=False)
    assert len(df.index) == 1
    assert len(df.columns) == 2
    assert df["type"][0] == "timestamp(3) with time zone"
    assert df["value"][0].year == datetime.datetime.utcnow().year


@pytest.mark.xfail(raises=NotImplementedError, reason="Unable to create pandas categorical from pyarrow table")
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
    df2 = wr.s3.read_parquet(
        path=path,
        dataset=True,
        pyarrow_additional_kwargs={
            "categories": [c for c in df.columns if c not in ["par0", "par1"]],
            "strings_to_categorical": True,
        },
    )
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
        ctas_parameters=wr.typing.AthenaCTASSettings(
            temp_table_name=glue_table2,
        ),
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


def test_athena_get_query_column_types(path, glue_database, glue_table):
    df = get_df()
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
    query_execution_id = wr.athena.start_query_execution(sql=f"SELECT * FROM {glue_table}", database=glue_database)
    wr.athena.wait_query(query_execution_id=query_execution_id)
    column_types = wr.athena.get_query_columns_types(query_execution_id=query_execution_id)
    assert len(column_types) == len(df.columns)
    assert set(column_types.keys()) == set(df.columns)


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


def test_read_sql_query_wo_results(path, glue_database, glue_table):
    wr.catalog.create_parquet_table(database=glue_database, table=glue_table, path=path, columns_types={"c0": "int"})
    sql = f"ALTER TABLE {glue_database}.{glue_table} SET LOCATION '{path}dir/'"
    df = wr.athena.read_sql_query(sql, database=glue_database, ctas_approach=False)
    assert df.empty
    ensure_athena_query_metadata(df=df, ctas_approach=False, encrypted=False)


@pytest.mark.parametrize("ctas_approach", [False, True])
def test_read_sql_query_wo_results_chunked(path, glue_database, glue_table, ctas_approach):
    wr.catalog.create_parquet_table(database=glue_database, table=glue_table, path=path, columns_types={"c0": "int"})
    sql = f"SELECT * FROM {glue_database}.{glue_table}"

    counter = 0
    for df in wr.athena.read_sql_query(sql, database=glue_database, ctas_approach=ctas_approach, chunksize=100):
        assert df.empty
        counter += 1

    assert counter == 1


@pytest.mark.xfail(raises=botocore.exceptions.ClientError)
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


@pytest.mark.modin_index
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

    first_bucket_df = wr.s3.read_parquet(path=[r["paths"][0]], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert len(first_bucket_df) == 2
    assert pandas_equals(pd.Series([bucketing_data[0], bucketing_data[2]], dtype=dtype), first_bucket_df["c0"])
    assert pandas_equals(pd.Series(["foo", "baz"], dtype=pd.StringDtype()), first_bucket_df["c1"])

    second_bucket_df = wr.s3.read_parquet(path=[r["paths"][1]], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert len(second_bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[1]], dtype=dtype), second_bucket_df["c0"])
    assert pandas_equals(pd.Series(["bar"], dtype=pd.StringDtype()), second_bucket_df["c1"])

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


@pytest.mark.modin_index
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

    first_bucket_df = wr.s3.read_csv(path=[r["paths"][0]], header=None, names=["c0", "c1"]).reset_index(drop=True)
    assert len(first_bucket_df) == 2
    assert pandas_equals(pd.Series([bucketing_data[0], bucketing_data[2]]), first_bucket_df["c0"])
    assert pandas_equals(pd.Series(["foo", "baz"]), first_bucket_df["c1"])

    second_bucket_df = wr.s3.read_csv(path=[r["paths"][1]], header=None, names=["c0", "c1"]).reset_index(drop=True)
    assert len(second_bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[1]]), second_bucket_df["c0"])
    assert pandas_equals(pd.Series(["bar"]), second_bucket_df["c1"])

    loaded_dfs = [
        wr.s3.read_csv(path=path, header=None, names=["c0", "c1"]),
        wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=False),
    ]

    for loaded_df in loaded_dfs:
        assert len(loaded_df) == 3
        assert all(x in bucketing_data for x in loaded_df["c0"].to_list())


@pytest.mark.modin_index
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

    bucket_df = wr.s3.read_parquet(path=[r["paths"][0]], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert len(bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[0]], dtype=dtype), bucket_df["c0"])
    assert pandas_equals(pd.Series(["foo"], dtype=pd.StringDtype()), bucket_df["c1"])

    bucket_df = wr.s3.read_parquet(path=[r["paths"][1]], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert len(bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[1]], dtype=dtype), bucket_df["c0"])
    assert pandas_equals(pd.Series(["bar"], dtype=pd.StringDtype()), bucket_df["c1"])

    bucket_df = wr.s3.read_parquet(path=[r["paths"][2]], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert len(bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[2]], dtype=dtype), bucket_df["c0"])
    assert pandas_equals(pd.Series(["baz"], dtype=pd.StringDtype()), bucket_df["c1"])

    bucket_df = wr.s3.read_parquet(path=[r["paths"][3]], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert len(bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[3]], dtype=dtype), bucket_df["c0"])
    assert pandas_equals(pd.Series(["boo"], dtype=pd.StringDtype()), bucket_df["c1"])

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
    assert pandas_equals(pd.Series([bucketing_data[0]]), bucket_df["c0"])
    assert pandas_equals(pd.Series(["foo"]), bucket_df["c1"])

    bucket_df = wr.s3.read_csv(path=[r["paths"][1]], header=None, names=["c0", "c1"])
    assert len(bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[1]]), bucket_df["c0"])
    assert pandas_equals(pd.Series(["bar"]), bucket_df["c1"])

    bucket_df = wr.s3.read_csv(path=[r["paths"][2]], header=None, names=["c0", "c1"])
    assert len(bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[2]]), bucket_df["c0"])
    assert pandas_equals(pd.Series(["baz"]), bucket_df["c1"])

    bucket_df = wr.s3.read_csv(path=[r["paths"][3]], header=None, names=["c0", "c1"])
    assert len(bucket_df) == 1
    assert pandas_equals(pd.Series([bucketing_data[3]]), bucket_df["c0"])
    assert pandas_equals(pd.Series(["boo"]), bucket_df["c1"])

    loaded_dfs = [
        wr.s3.read_csv(path=path, header=None, names=["c0", "c1"]),
        wr.athena.read_sql_table(table=glue_table, database=glue_database, ctas_approach=False),
    ]

    for loaded_df in loaded_dfs:
        assert len(loaded_df) == 4
        assert all(x in bucketing_data for x in loaded_df["c0"].to_list())


@pytest.mark.modin_index
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

    first_bucket_df = wr.s3.read_parquet(path=[r["paths"][0]], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert len(first_bucket_df) == 2
    assert pandas_equals(pd.Series([0, 3], dtype=pd.Int64Dtype()), first_bucket_df["c0"])
    assert pandas_equals(pd.Series([4, 7], dtype=pd.Int64Dtype()), first_bucket_df["c1"])
    assert pandas_equals(pd.Series(["foo", "boo"], dtype=pd.StringDtype()), first_bucket_df["c2"])

    second_bucket_df = wr.s3.read_parquet(path=[r["paths"][1]], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert len(second_bucket_df) == 2
    assert pandas_equals(pd.Series([1, 2], dtype=pd.Int64Dtype()), second_bucket_df["c0"])
    assert pandas_equals(pd.Series([6, 5], dtype=pd.Int64Dtype()), second_bucket_df["c1"])
    assert pandas_equals(pd.Series(["bar", "baz"], dtype=pd.StringDtype()), second_bucket_df["c2"])


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
        query_params = {"c0": "a"}
    elif dtype == "bool":
        nb_of_buckets = 2
        saving_factor = 2.1
        data = [bool(i % nb_of_buckets) for i in range(nb_of_rows)]
        query_params = {"c0": True}
    else:
        raise ValueError(f"Invalid Argument for dtype: {dtype}")
    query = f"SELECT c0 FROM {glue_table} WHERE c0=:c0"
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


def test_start_query_execution_wait(path, glue_database, glue_table):
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

    sql = f"SELECT * FROM {glue_table}"
    query_id = wr.athena.start_query_execution(sql=sql, database=glue_database, wait=False)

    query_execution_result = wr.athena.start_query_execution(sql=sql, database=glue_database, wait=True)

    assert isinstance(query_id, str)
    assert isinstance(query_execution_result, dict)
    assert query_execution_result["Query"] == sql
    assert query_execution_result["StatementType"] == "DML"
    assert query_execution_result["QueryExecutionContext"]["Database"] == glue_database


def test_get_query_results(path, glue_table, glue_database):
    sql = (
        "SELECT CAST("
        "    ROW(1, ROW(2, ROW(3, '4'))) AS"
        "    ROW(field0 BIGINT, field1 ROW(field2 BIGINT, field3 ROW(field4 BIGINT, field5 VARCHAR)))"
        ") AS col0"
    )

    df_ctas: pd.DataFrame = wr.athena.read_sql_query(
        sql=sql, database=glue_database, ctas_approach=True, unload_approach=False
    )
    query_id_ctas = df_ctas.query_metadata["QueryExecutionId"]
    df_get_query_results_ctas = wr.athena.get_query_results(query_execution_id=query_id_ctas)
    pandas_equals(df_get_query_results_ctas, df_ctas)

    df_unload: pd.DataFrame = wr.athena.read_sql_query(
        sql=sql, database=glue_database, ctas_approach=False, unload_approach=True, s3_output=path
    )
    query_id_unload = df_unload.query_metadata["QueryExecutionId"]
    df_get_query_results_df_unload = wr.athena.get_query_results(query_execution_id=query_id_unload)
    pandas_equals(df_get_query_results_df_unload, df_unload)

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

    reg_sql = f"SELECT * FROM {glue_table}"

    df_regular: pd.DataFrame = wr.athena.read_sql_query(
        sql=reg_sql, database=glue_database, ctas_approach=False, unload_approach=False
    )
    query_id_regular = df_regular.query_metadata["QueryExecutionId"]
    df_get_query_results_df_regular = wr.athena.get_query_results(query_execution_id=query_id_regular)
    assert pandas_equals(df_get_query_results_df_regular, df_regular)


def test_athena_generate_create_query(path, glue_database, glue_table):
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.catalog.create_parquet_table(database=glue_database, table=glue_table, path=path, columns_types={"c0": "int"})
    query: str = wr.athena.generate_create_query(database=glue_database, table=glue_table)
    create_query_no_partition: str = "\n".join(
        [
            f"CREATE EXTERNAL TABLE `{glue_table}`(",
            "  `c0` int)",
            "ROW FORMAT SERDE ",
            "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' ",
            "STORED AS INPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' ",
            "OUTPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'",
            "LOCATION",
            f"  '{path}'",
            "TBLPROPERTIES (",
            "  'compressionType'='none', ",
            "  'classification'='parquet', ",
            "  'projection.enabled'='false', ",
            "  'typeOfData'='file')",
        ]
    )
    assert query == create_query_no_partition
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.catalog.create_parquet_table(
        database=glue_database,
        table=glue_table,
        path=path,
        columns_types={"c0": "int"},
        partitions_types={"col2": "date"},
    )
    query: str = wr.athena.generate_create_query(database=glue_database, table=glue_table)
    create_query_partition: str = "\n".join(
        [
            f"CREATE EXTERNAL TABLE `{glue_table}`(",
            "  `c0` int)",
            "PARTITIONED BY ( ",
            "  `col2` date)",
            "ROW FORMAT SERDE ",
            "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' ",
            "STORED AS INPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' ",
            "OUTPUTFORMAT ",
            "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'",
            "LOCATION",
            f"  '{path}'",
            "TBLPROPERTIES (",
            "  'compressionType'='none', ",
            "  'classification'='parquet', ",
            "  'projection.enabled'='false', ",
            "  'typeOfData'='file')",
        ]
    )

    assert query == create_query_partition

    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    create_view: str = "\n".join(
        [
            f"""CREATE OR REPLACE VIEW "{glue_table}" AS """,
            (
                "SELECT CAST(ROW (1, ROW (2, ROW (3, '4'))) AS "
                "ROW(field0 bigint, field1 ROW(field2 bigint, field3 ROW(field4 bigint, field5 varchar)))) col0\n\n"
            ),
        ]
    )
    wr.athena.start_query_execution(sql=create_view, database=glue_database, wait=True)
    query: str = wr.athena.generate_create_query(database=glue_database, table=glue_table)
    assert query == create_view


def test_get_query_execution(workgroup0, workgroup1):
    query_execution_ids = wr.athena.list_query_executions(workgroup=workgroup0) + wr.athena.list_query_executions(
        workgroup=workgroup1
    )
    assert query_execution_ids
    query_execution_detail = wr.athena.get_query_execution(query_execution_id=query_execution_ids[0])
    query_executions_df = wr.athena.get_query_executions(query_execution_ids)
    assert isinstance(query_executions_df, PandasDataFrame)
    assert isinstance(query_execution_detail, dict)
    assert set(query_execution_ids).intersection(set(query_executions_df["QueryExecutionId"].values.tolist()))
    query_execution_ids1 = query_execution_ids + ["aaa", "bbb"]
    query_executions_df, unprocessed_query_executions_df = wr.athena.get_query_executions(
        query_execution_ids1, return_unprocessed=True
    )
    assert isinstance(unprocessed_query_executions_df, PandasDataFrame)
    assert set(query_execution_ids).intersection(set(query_executions_df["QueryExecutionId"].values.tolist()))
    assert {"aaa", "bbb"}.intersection(set(unprocessed_query_executions_df["QueryExecutionId"].values.tolist()))


@pytest.mark.parametrize("compression", [None, "snappy", "gzip"])
def test_read_sql_query_ctas_write_compression(path, glue_database, glue_table, compression):
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

    with patch(
        "awswrangler.athena._read.create_ctas_table", wraps=wr.athena.create_ctas_table
    ) as mock_create_ctas_table:
        wr.athena.read_sql_query(
            sql=f"SELECT * FROM {glue_table}",
            database=glue_database,
            ctas_approach=True,
            ctas_parameters={
                "compression": compression,
            },
        )

        mock_create_ctas_table.assert_called_once()

        if compression:
            assert mock_create_ctas_table.call_args[1]["write_compression"] == compression


def test_athena_date_recovery(path, glue_database, glue_table):
    df = pd.DataFrame(
        {
            # Valid datetime64 values
            "date1": [datetime.date(2020, 1, 3), datetime.date(2020, 1, 4), pd.NaT],
            # Object column because of None
            "date2": [datetime.date(2021, 1, 3), datetime.date(2021, 1, 4), None],
            # Object column because dates are out of bounds for pandas datetime types
            "date3": [datetime.date(3099, 1, 3), datetime.date(3099, 1, 4), datetime.date(4080, 1, 5)],
        }
    )
    df["date1"] = df["date1"].astype("datetime64[ns]")
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    df2 = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {glue_table}",
        database=glue_database,
        ctas_approach=False,
    )
    assert pandas_equals(df, df2)


@pytest.mark.parametrize("partition_cols", [None, ["name"], ["name", "day(ts)"]])
@pytest.mark.parametrize(
    "additional_table_properties",
    [None, {"write_target_data_file_size_bytes": 536870912, "optimize_rewrite_delete_file_threshold": 10}],
)
def test_athena_to_iceberg(path, path2, glue_database, glue_table, partition_cols, additional_table_properties):
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "ts": [ts("2020-01-01 00:00:00.0"), ts("2020-01-02 00:00:01.0"), ts("2020-01-03 00:00:00.0")],
        }
    )
    df["id"] = df["id"].astype("Int64")  # Cast as nullable int64 type
    df["name"] = df["name"].astype("string")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        partition_cols=partition_cols,
        additional_table_properties=additional_table_properties,
    )

    df_out = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table}" ORDER BY id',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert df.equals(df_out)


def test_athena_to_iceberg_schema_evolution_add_columns(
    path: str, path2: str, glue_database: str, glue_table: str
) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    df["c2"] = [6, 7, 8]
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    column_types = wr.catalog.get_table_types(glue_database, glue_table)
    assert len(column_types) == len(df.columns)

    df_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )
    assert len(df_out) == len(df) * 2

    df["c3"] = [9, 10, 11]
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.to_iceberg(
            df=df,
            database=glue_database,
            table=glue_table,
            table_location=path,
            temp_path=path2,
            keep_files=False,
            schema_evolution=False,
        )


def test_athena_to_iceberg_schema_evolution_modify_columns(
    path: str, path2: str, glue_database: str, glue_table: str
) -> None:
    # Version 1
    df = pd.DataFrame({"c1": pd.Series([1.0, 2.0], dtype="float32"), "c2": pd.Series([-1, -2], dtype="int32")})

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    df_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert len(df_out) == 2
    assert len(df_out.columns) == 2
    assert str(df_out["c1"].dtype).startswith("float32")
    assert str(df_out["c2"].dtype).startswith("Int32")

    # Version 2
    df2 = pd.DataFrame({"c1": pd.Series([3.0, 4.0], dtype="float64"), "c2": pd.Series([-3, -4], dtype="int64")})

    wr.athena.to_iceberg(
        df=df2,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    df2_out = wr.athena.read_sql_table(
        table=glue_table,
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert len(df2_out) == 4
    assert len(df2_out.columns) == 2
    assert str(df2_out["c1"].dtype).startswith("float64")
    assert str(df2_out["c2"].dtype).startswith("Int64")


def test_athena_to_iceberg_schema_evolution_remove_columns_error(
    path: str, path2: str, glue_database: str, glue_table: str
) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        schema_evolution=True,
    )

    df = pd.DataFrame({"c0": [6, 7, 8]})

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.athena.to_iceberg(
            df=df,
            database=glue_database,
            table=glue_table,
            table_location=path,
            temp_path=path2,
            keep_files=False,
            schema_evolution=True,
        )


def test_to_iceberg_cast(path, path2, glue_table, glue_database):
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
                datetime.date(1970, 1, 1),
                datetime.date(2000, 1, 1),
                datetime.date(2020, 1, 1),
                datetime.date(2020, 1, 1),
                datetime.date(4000, 1, 1),
                None,
                None,
                None,
                None,
                None,
            ]
        }
    )
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        dtype={"c0": "date"},
    )
    df2 = wr.athena.read_sql_table(database=glue_database, table=glue_table, ctas_approach=False)
    assert pandas_equals(df_expected, df2.sort_values("c0").reset_index(drop=True))


def test_athena_to_iceberg_with_hyphenated_table_name(
    path: str, path2: str, glue_database: str, glue_table_with_hyphenated_name: str
):
    df = pd.DataFrame({"c0": [1, 2, 3, 4], "c1": ["foo", "bar", "baz", "boo"]})
    df["c0"] = df["c0"].astype("int")
    df["c1"] = df["c1"].astype("string")

    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table_with_hyphenated_name,
        table_location=path,
        temp_path=path2,
        keep_files=False,
    )

    df_out = wr.athena.read_sql_query(
        sql=f'SELECT * FROM "{glue_table_with_hyphenated_name}"',
        database=glue_database,
        ctas_approach=False,
        unload_approach=False,
    )

    assert len(df) == len(df_out)
    assert len(df.columns) == len(df_out.columns)


def test_athena_to_iceberg_column_comments(path: str, path2: str, glue_database: str, glue_table: str) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    column_comments = {
        "c0": "comment 0",
        "c1": "comment 1",
    }
    wr.athena.to_iceberg(
        df=df,
        database=glue_database,
        table=glue_table,
        table_location=path,
        temp_path=path2,
        keep_files=False,
        glue_table_settings={
            "columns_comments": column_comments,
        },
    )

    column_comments_actual = wr.catalog.get_columns_comments(glue_database, glue_table)

    assert column_comments_actual == column_comments
