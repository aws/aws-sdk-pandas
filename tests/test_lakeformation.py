import logging

import pytest

import awswrangler as wr

from ._utils import get_df_csv

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("use_threads", [True, False])
def test_lakeformation(path, glue_database, glue_table, use_threads):
    table = f"__{glue_table}"
    wr.catalog.delete_table_if_exists(database=glue_database, table=table)
    wr.s3.to_parquet(
        df=get_df_csv(),
        path=path,
        index=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        table=table,
        database=glue_database,
        partition_cols=["par0", "par1"],
        mode="overwrite",
    )
    df = wr.lakeformation.read_sql_query(
        sql=f"SELECT * FROM {table} WHERE id = :id;",
        database=glue_database,
        use_threads=use_threads,
        params={"id": 1},
    )
    assert len(df.index) == 1
    wr.catalog.delete_table_if_exists(database=glue_database, table=table)
