import logging

import pytest
from botocore.exceptions import ClientError

import awswrangler as wr

from .._utils import (
    ensure_athena_query_metadata,
    ensure_data_types,
    get_df,
    get_time_str_with_random_suffix,
)

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.fixture(scope="function")
def statement(workgroup0: str) -> str:
    name = f"prepared_statement_{get_time_str_with_random_suffix()}"
    yield name
    try:
        wr.athena.deallocate_prepared_statement(statement_name=name, workgroup=workgroup0)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise e


def test_update_prepared_statement(workgroup0: str, statement: str) -> None:
    wr.athena.prepare_statement(
        sql="SELECT 1 AS col0",
        statement_name=statement,
        workgroup=workgroup0,
    )

    wr.athena.prepare_statement(
        sql="SELECT 1 AS col0, 2 AS col1",
        statement_name=statement,
        workgroup=workgroup0,
    )


def test_update_prepared_statement_error(workgroup0: str, statement: str) -> None:
    wr.athena.prepare_statement(
        sql="SELECT 1 AS col0",
        statement_name=statement,
        workgroup=workgroup0,
    )

    with pytest.raises(wr.exceptions.AlreadyExists):
        wr.athena.prepare_statement(
            sql="SELECT 1 AS col0, 2 AS col1",
            statement_name=statement,
            workgroup=workgroup0,
            mode="error",
        )


def test_athena_deallocate_prepared_statement(workgroup0: str, statement: str) -> None:
    wr.athena.prepare_statement(
        sql="SELECT 1 as col0",
        statement_name=statement,
        workgroup=workgroup0,
    )

    wr.athena.deallocate_prepared_statement(
        statement_name=statement,
        workgroup=workgroup0,
    )


def test_list_prepared_statements(workgroup1: str, statement: str) -> None:
    wr.athena.prepare_statement(
        sql="SELECT 1 as col0",
        statement_name=statement,
        workgroup=workgroup1,
    )

    statement_list = wr.athena.list_prepared_statements(workgroup1)

    assert len(statement_list) == 1
    assert statement_list[0]["StatementName"] == statement

    wr.athena.deallocate_prepared_statement(statement, workgroup=workgroup1)

    statement_list = wr.athena.list_prepared_statements(workgroup1)
    assert len(statement_list) == 0


def test_athena_execute_prepared_statement(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
    workgroup0: str,
    statement: str,
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

    wr.athena.prepare_statement(
        sql=f"SELECT * FROM {glue_table} WHERE string = ?",
        statement_name=statement,
        workgroup=workgroup0,
    )

    df_out1 = wr.athena.read_sql_query(
        sql=f"EXECUTE \"{statement}\" USING 'Washington'",
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup0,
        keep_files=False,
        s3_output=path2,
    )
    df_out2 = wr.athena.read_sql_query(
        sql=f"EXECUTE \"{statement}\" USING 'Seattle'",
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup0,
        keep_files=False,
        s3_output=path2,
    )

    ensure_data_types(df=df_out1)
    ensure_data_types(df=df_out2)

    ensure_athena_query_metadata(df=df_out1, ctas_approach=False, encrypted=False)
    ensure_athena_query_metadata(df=df_out2, ctas_approach=False, encrypted=False)

    assert len(df_out1) == 1
    assert len(df_out2) == 1


def test_athena_execute_prepared_statement_with_params(
    path: str,
    path2: str,
    glue_database: str,
    glue_table: str,
    workgroup0: str,
    statement: str,
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

    wr.athena.prepare_statement(
        sql=f"SELECT * FROM {glue_table} WHERE string = ?",
        statement_name=statement,
        workgroup=workgroup0,
    )

    df_out1 = wr.athena.read_sql_query(
        sql=f'EXECUTE "{statement}"',
        database=glue_database,
        ctas_approach=False,
        workgroup=workgroup0,
        execution_params=["Washington"],
        keep_files=False,
        s3_output=path2,
    )

    ensure_data_types(df=df_out1)
    ensure_athena_query_metadata(df=df_out1, ctas_approach=False, encrypted=False)

    assert len(df_out1) == 1
