import datetime
import logging

import pandas as pd
import pytest

import awswrangler as wr
from awswrangler.exceptions import FailedQualityCheck
from awswrangler.s3._merge_upsert_table import _is_data_quality_sufficient, merge_upsert_table

logger = logging.getLogger("awswrangler")
logger.setLevel(logging.DEBUG)


def test_is_data_quality_sufficient_check_column_names():
    # Check both table have the same columns
    existing_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col_a", "col_b", "col_c"])
    delta_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col_a", "col_b", "col_c"])
    primary_key = ["col_a", "col_b"]
    assert _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key)


def test_is_data_quality_sufficient_mistmatch_column_names():
    # Check both dataframe have the same columns.
    # In this case they are different thus it should fail
    existing_df = pd.DataFrame({"c0": [1, 2, 1, 2], "c1": [1, 2, 1, 2], "c2": [2, 1, 2, 1]})
    delta_df = pd.DataFrame({"d0": [1, 2, 1, 2], "d1": [1, 2, 1, 2], "c2": [2, 1, 2, 1]})
    primary_key = ["c0", "c1"]
    with pytest.raises(FailedQualityCheck):
        _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key)


def test_is_data_quality_sufficient_same_column_names_different_row_count():
    # Check both table have the same columns and
    existing_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]], columns=["col_a", "col_b", "col_c"])
    delta_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col_a", "col_b", "col_c"])
    primary_key = ["col_a", "col_b"]
    assert _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key)


def test_is_data_quality_sufficient_missing_primary_key():
    # Check both tables have the same primary key
    existing_df = pd.DataFrame({"c0": [1, 2, 1], "c1": [1, 2, 1], "c2": [2, 1, 1]})
    delta_df = pd.DataFrame({"c0": [1, 2, 1, 2]})
    primary_key = ["c0", "c1"]
    with pytest.raises(FailedQualityCheck):
        _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key)


def test_is_data_quality_sufficient_fail_for_duplicate_data():
    # Check for duplicate data inside the dataframe
    existing_df = pd.DataFrame([[1, 2, 3], [1, 2, 3], [7, 8, 9], [10, 11, 12]], columns=["col_a", "col_b", "col_c"])
    delta_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col_a", "col_b", "col_c"])
    primary_key = ["col_a", "col_b"]
    with pytest.raises(FailedQualityCheck):
        _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key)


def test_table_does_not_exist(glue_database, glue_table):
    # Fail as table does not exist
    delta_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col_a", "col_b", "col_c"])
    primary_key = ["col_a", "col_b"]
    with pytest.raises(AttributeError):
        merge_upsert_table(delta_df=delta_df, database=glue_database, table=glue_table, primary_key=primary_key)


def test_success_case(glue_database, glue_table, path):
    df = pd.DataFrame(
        {"id": [1, 2], "cchar": ["foo", "boo"], "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2)]}
    )
    # Create the table
    wr.s3.to_parquet(df=df, path=path, index=False, dataset=True, database=glue_database, table=glue_table)
    delta_df = pd.DataFrame({"id": [1], "cchar": ["foo"], "date": [datetime.date(2021, 1, 1)]})
    primary_key = ["id", "cchar"]
    merge_upsert_table(delta_df=delta_df, database=glue_database, table=glue_table, primary_key=primary_key)
    merged_df = wr.s3.read_parquet_table(database=glue_database, table=glue_table)
    # Row count should still be 2 rows
    assert merged_df.shape == (2, 3)


def test_success_case2(glue_database, glue_table, path):
    df = pd.DataFrame(
        {"id": [1, 2], "cchar": ["foo", "boo"], "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2)]}
    )
    # Create the table
    wr.s3.to_parquet(df=df, path=path, index=False, dataset=True, database=glue_database, table=glue_table)
    delta_df = pd.DataFrame(
        {"id": [1, 2], "cchar": ["foo", "boo"], "date": [datetime.date(2021, 1, 1), datetime.date(2021, 1, 2)]}
    )
    primary_key = ["id", "cchar"]
    merge_upsert_table(delta_df=delta_df, database=glue_database, table=glue_table, primary_key=primary_key)
    merged_df = wr.s3.read_parquet_table(database=glue_database, table=glue_table)
    # Row count should still be 2 rows
    assert merged_df.shape == (2, 3)
