import calendar
import logging
import time

import pandas as pd

import awswrangler as wr

from ._utils import ensure_data_types, ensure_data_types_csv, get_df, get_df_csv

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_lakeformation(path, path2, glue_database, glue_table, glue_table2, use_threads=False):
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table2)

    wr.s3.to_parquet(
        df=get_df(governed=True),
        path=path,
        index=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table=glue_table,
        table_type="GOVERNED",
        database=glue_database,
    )

    df = wr.lakeformation.read_sql_table(
        table=glue_table,
        database=glue_database,
        use_threads=use_threads,
    )
    assert len(df.index) == 3
    assert len(df.columns) == 14
    assert df["iint32"].sum() == 3
    ensure_data_types(df=df)

    # Filter query
    df2 = wr.lakeformation.read_sql_query(
        sql=f"SELECT * FROM {glue_table} WHERE iint16 = :iint16;",
        database=glue_database,
        params={"iint16": 1},
    )
    assert len(df2.index) == 1

    wr.s3.to_csv(
        df=get_df_csv(),
        path=path2,
        index=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="append",
        table=glue_table2,
        table_type="GOVERNED",
        database=glue_database,
    )
    # Read within a transaction
    transaction_id = wr.lakeformation.start_transaction(read_only=True)
    df3 = wr.lakeformation.read_sql_table(
        table=glue_table2,
        database=glue_database,
        transaction_id=transaction_id,
        use_threads=use_threads,
    )
    assert df3["int"].sum() == 3
    ensure_data_types_csv(df3)

    # Read within a query as of time
    query_as_of_time = calendar.timegm(time.gmtime())
    df4 = wr.lakeformation.read_sql_table(
        table=glue_table2,
        database=glue_database,
        query_as_of_time=query_as_of_time,
        use_threads=use_threads,
    )
    assert len(df4.index) == 3

    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table2)


def test_lakeformation_multi_transaction(path, path2, glue_database, glue_table, glue_table2, use_threads=True):
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table2)

    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    transaction_id = wr.lakeformation.start_transaction(read_only=False)
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        table_type="GOVERNED",
        transaction_id=transaction_id,
        description="c0",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c0": "0"},
        use_threads=use_threads,
    )

    df2 = pd.DataFrame({"c1": [None, 1, None]}, dtype="Int16")
    wr.s3.to_parquet(
        df=df2,
        path=path2,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table2,
        table_type="GOVERNED",
        transaction_id=transaction_id,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c1": "1"},
        use_threads=use_threads,
    )
    wr.lakeformation.commit_transaction(transaction_id=transaction_id)

    df3 = wr.lakeformation.read_sql_table(
        table=glue_table,
        database=glue_database,
        use_threads=use_threads,
    )
    df4 = wr.lakeformation.read_sql_table(
        table=glue_table2,
        database=glue_database,
        use_threads=use_threads,
    )

    assert df.shape == df3.shape
    assert df.c0.sum() == df3.c0.sum()

    assert df2.shape == df4.shape
    assert df2.c1.sum() == df4.c1.sum()

    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
    wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table2)
