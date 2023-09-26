import logging
from unittest.mock import patch

import pytest

import awswrangler.pandas as pd

from .._utils import ensure_athena_query_metadata

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


def test_athena_cache(wr, path, glue_database, glue_table, workgroup1):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database=glue_database, table=glue_table)

    df2 = wr.athena.read_sql_table(
        glue_table,
        glue_database,
        ctas_approach=False,
        workgroup=workgroup1,
        athena_cache_settings={"max_cache_seconds": 1},
    )
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()

    df2 = wr.athena.read_sql_table(
        glue_table,
        glue_database,
        ctas_approach=False,
        athena_cache_settings={"max_cache_seconds": 900},
        workgroup=workgroup1,
    )
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()

    dfs = wr.athena.read_sql_table(
        glue_table,
        glue_database,
        ctas_approach=False,
        athena_cache_settings={"max_cache_seconds": 900},
        workgroup=workgroup1,
        chunksize=1,
    )
    assert len(list(dfs)) == 2


@pytest.mark.parametrize("data_source", [None, "AwsDataCatalog"])
def test_cache_query_ctas_approach_true(wr, path, glue_database, glue_table, data_source):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        glue_table_settings=wr.typing.GlueTableSettings(
            description="c0",
            parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
            columns_comments={"c0": "0"},
        ),
    )

    with patch(
        "awswrangler.athena._read._check_for_cached_results",
        return_value=wr.athena._read._CacheInfo(has_valid_cache=False),
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_table(
            glue_table,
            glue_database,
            ctas_approach=True,
            athena_cache_settings={"max_cache_seconds": 0},
            data_source=data_source,
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._read._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_table(
            glue_table,
            glue_database,
            ctas_approach=True,
            athena_cache_settings={"max_cache_seconds": 900},
            data_source=data_source,
        )
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()
        ensure_athena_query_metadata(df=df3, ctas_approach=True, encrypted=False)


@pytest.mark.parametrize("data_source", [None, "AwsDataCatalog"])
def test_cache_query_ctas_approach_false(wr, path, glue_database, glue_table, data_source):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        glue_table_settings=wr.typing.GlueTableSettings(
            description="c0",
            parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
            columns_comments={"c0": "0"},
        ),
    )

    with patch(
        "awswrangler.athena._read._check_for_cached_results",
        return_value=wr.athena._read._CacheInfo(has_valid_cache=False),
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_table(
            glue_table,
            glue_database,
            ctas_approach=False,
            athena_cache_settings={"max_cache_seconds": 0},
            data_source=data_source,
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._read._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_table(
            glue_table,
            glue_database,
            ctas_approach=False,
            athena_cache_settings={"max_cache_seconds": 900},
            data_source=data_source,
        )
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()
        ensure_athena_query_metadata(df=df3, ctas_approach=False, encrypted=False)


def test_cache_query_semicolon(wr, path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database=glue_database, table=glue_table)

    with patch(
        "awswrangler.athena._read._check_for_cached_results",
        return_value=wr.athena._read._CacheInfo(has_valid_cache=False),
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table}",
            database=glue_database,
            ctas_approach=True,
            athena_cache_settings={"max_cache_seconds": 0},
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._read._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table};",
            database=glue_database,
            ctas_approach=True,
            athena_cache_settings={"max_cache_seconds": 900},
        )
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()


def test_local_cache(wr, path, glue_database, glue_table):
    wr.config.max_local_cache_entries = 1

    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database=glue_database, table=glue_table)

    # Set max cache size because it is supposed to be set in the patched method below
    wr.athena._cache._cache_manager.max_cache_size = 1

    with patch(
        "awswrangler.athena._read._check_for_cached_results",
        return_value=wr.athena._read._CacheInfo(has_valid_cache=False),
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table}",
            database=glue_database,
            ctas_approach=True,
            athena_cache_settings={"max_cache_seconds": 0},
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()
        first_query_id = df2.query_metadata["QueryExecutionId"]
        assert first_query_id in wr.athena._cache._cache_manager

        df3 = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table}",
            database=glue_database,
            ctas_approach=True,
            athena_cache_settings={"max_cache_seconds": 0},
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()
        second_query_id = df3.query_metadata["QueryExecutionId"]

        assert first_query_id not in wr.athena._cache._cache_manager
        assert second_query_id in wr.athena._cache._cache_manager


def test_paginated_remote_cache(wr, path, glue_database, glue_table, workgroup1):
    wr.config.max_remote_cache_entries = 100
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database=glue_database, table=glue_table)

    df2 = wr.athena.read_sql_table(
        glue_table,
        glue_database,
        ctas_approach=False,
        athena_cache_settings={"max_cache_seconds": 1},
        workgroup=workgroup1,
    )
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()


@pytest.mark.parametrize("data_source", [None, "AwsDataCatalog"])
def test_cache_start_query(wr, path, glue_database, glue_table, data_source):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        glue_table_settings=wr.typing.GlueTableSettings(
            description="c0",
            parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
            columns_comments={"c0": "0"},
        ),
    )

    with patch(
        "awswrangler.athena._executions._check_for_cached_results",
        return_value=wr.athena._read._CacheInfo(has_valid_cache=False),
    ) as mocked_cache_attempt:
        query_id = wr.athena.start_query_execution(
            sql=f"SELECT * FROM {glue_table}", database=glue_database, data_source=data_source
        )
        mocked_cache_attempt.assert_called()

    # Wait for query to finish in order to successfully check cache
    wr.athena.wait_query(query_execution_id=query_id)

    with patch("awswrangler.athena._executions._start_query_execution") as internal_start_query:
        query_id_2 = wr.athena.start_query_execution(
            sql=f"SELECT * FROM {glue_table}",
            database=glue_database,
            data_source=data_source,
            athena_cache_settings={"max_cache_seconds": 900},
        )
        internal_start_query.assert_not_called()
        assert query_id == query_id_2


def test_start_query_client_request_token(wr, path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
    )

    client_request_token = f"token-{glue_database}-{glue_table}-1"
    query_id_1 = wr.athena.start_query_execution(
        sql=f"SELECT * FROM {glue_table}",
        database=glue_database,
        client_request_token=client_request_token,
    )
    query_id_2 = wr.athena.start_query_execution(
        sql=f"SELECT * FROM {glue_table}",
        database=glue_database,
        client_request_token=client_request_token,
    )

    assert query_id_1 == query_id_2
