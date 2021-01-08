import logging

import pandas as pd

from awswrangler.s3._merge_upsert_table import _is_data_quality_sufficient

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
    assert _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key) is False


def test_is_data_quality_sufficient_same_column_names_different_row_count():
    # Check both table have the same columns and
    existing_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]], columns=["col_a", "col_b", "col_c"])
    delta_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col_a", "col_b", "col_c"])
    primary_key = ["col_a", "col_b"]
    assert _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key) is True


def test_is_data_quality_sufficient_missing_primary_key():
    # Check both tables have the same primary key
    existing_df = pd.DataFrame({"c0": [1, 2, 1], "c1": [1, 2, 1], "c2": [2, 1, 1]})
    delta_df = pd.DataFrame({"c0": [1, 2, 1, 2]})
    primary_key = ["c0", "c1"]
    assert _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key) is False


def test_is_data_quality_sufficient_fail_for_duplicate_data():
    # Check for duplicate data inside the dataframe
    existing_df = pd.DataFrame([[1, 2, 3], [1, 2, 3], [7, 8, 9], [10, 11, 12]], columns=["col_a", "col_b", "col_c"])
    delta_df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["col_a", "col_b", "col_c"])
    primary_key = ["col_a", "col_b"]
    assert _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key) is False
