import logging
from unittest.mock import patch

import pandas as pd
import pytest

import awswrangler as wr

from ._utils import ensure_athena_query_metadata

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_athena_cache(path, glue_database, glue_table, workgroup1):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database=glue_database, table=glue_table)

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


@pytest.mark.parametrize("data_source", [None, "AwsDataCatalog"])
def test_cache_query_ctas_approach_true(path, glue_database, glue_table, data_source):
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
    )

    with patch(
        "awswrangler.athena._read._check_for_cached_results",
        return_value=wr.athena._read._CacheInfo(has_valid_cache=False),
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_table(
            glue_table, glue_database, ctas_approach=True, max_cache_seconds=0, data_source=data_source
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._read._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_table(
            glue_table, glue_database, ctas_approach=True, max_cache_seconds=900, data_source=data_source
        )
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()
        ensure_athena_query_metadata(df=df3, ctas_approach=True, encrypted=False)


@pytest.mark.parametrize("data_source", [None, "AwsDataCatalog"])
def test_cache_query_ctas_approach_false(path, glue_database, glue_table, data_source):
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
    )

    with patch(
        "awswrangler.athena._read._check_for_cached_results",
        return_value=wr.athena._read._CacheInfo(has_valid_cache=False),
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_table(
            glue_table, glue_database, ctas_approach=False, max_cache_seconds=0, data_source=data_source
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._read._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_table(
            glue_table, glue_database, ctas_approach=False, max_cache_seconds=900, data_source=data_source
        )
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()
        ensure_athena_query_metadata(df=df3, ctas_approach=False, encrypted=False)


def test_cache_query_semicolon(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database=glue_database, table=glue_table)

    with patch(
        "awswrangler.athena._read._check_for_cached_results",
        return_value=wr.athena._read._CacheInfo(has_valid_cache=False),
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table}", database=glue_database, ctas_approach=True, max_cache_seconds=0
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._read._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_query(
            f"SELECT * FROM {glue_table};", database=glue_database, ctas_approach=True, max_cache_seconds=900
        )
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()
