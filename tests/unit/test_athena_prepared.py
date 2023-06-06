import logging

import pytest

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
    except wr.exceptions.QueryFailed as e:
        if not str(e).startswith(f"PreparedStatement {name} was not found"):
            raise e


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
        database=glue_database,
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
