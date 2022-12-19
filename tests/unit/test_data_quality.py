import logging

import pandas as pd
import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture()
def df(path: str, glue_database: str, glue_table: str) -> pd.DataFrame:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_parquet(df, path, dataset=True, database=glue_database, table=glue_table)
    return df


def test_ruleset_df(df, path, glue_database, glue_table, glue_ruleset, glue_data_quality_role):
    df_rules = pd.DataFrame(
        {
            "rule_type": ["RowCount", "IsComplete", "Uniqueness", "ColumnValues"],
            "parameter": [None, "c0", "c0", "c1"],
            "expression": ["between 1 and 6", None, "> 0.95", "in [0, 1, 2]"],
        }
    )
    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        df_rules=df_rules,
    )
    df_ruleset = wr.data_quality.get_ruleset(name=glue_ruleset)
    assert df_rules.equals(df_ruleset)

    df_results = wr.data_quality.evaluate_ruleset(
        name=glue_ruleset,
        iam_role_arn=glue_data_quality_role,
        number_of_workers=2,
    )
    assert df_results.shape == (4, 4)
    assert df_results["Result"].eq("PASS").all()


def test_ruleset_dqdl(df, path, glue_database, glue_table, glue_ruleset, glue_data_quality_role):
    dqdl_rules = (
        "Rules = ["
        "RowCount between 1 and 6,"
        'IsComplete "c0",'
        'Uniqueness "c0" > 0.95,'
        'ColumnValues "c0" <= 2,'
        'IsComplete "c1",'
        'Uniqueness "c1" > 0.95,'
        'ColumnValues "c1" <= 2,'
        'IsComplete "c2",'
        'ColumnValues "c2" <= 1'
        "]"
    )
    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        dqdl_rules=dqdl_rules,
    )
    df_results = wr.data_quality.evaluate_ruleset(
        name=glue_ruleset,
        iam_role_arn=glue_data_quality_role,
        number_of_workers=2,
    )
    assert df_results["Result"].eq("PASS").all()


def test_recommendation_ruleset(df, path, glue_database, glue_table, glue_ruleset, glue_data_quality_role):
    df_recommended_ruleset = wr.data_quality.create_recommendation_ruleset(
        database=glue_database,
        table=glue_table,
        iam_role_arn=glue_data_quality_role,
        number_of_workers=2,
    )
    df_rules = df_recommended_ruleset.append(
        {"rule_type": "ColumnValues", "parameter": "c2", "expression": "in [0, 1, 2]"}, ignore_index=True
    )
    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        df_rules=df_rules,
    )
    df_results = wr.data_quality.evaluate_ruleset(
        name=glue_ruleset,
        iam_role_arn=glue_data_quality_role,
        number_of_workers=2,
    )
    assert df_results["Result"].eq("PASS").all()


def test_ruleset_fail(df, path, glue_database, glue_table, glue_ruleset, glue_data_quality_role):
    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        dqdl_rules="Rules = [ RowCount between 1 and 3 ]",  # Exclusive
    )
    df_results = wr.data_quality.evaluate_ruleset(
        name=glue_ruleset,
        iam_role_arn=glue_data_quality_role,
        number_of_workers=2,
    )
    assert df_results["Result"][0] == "FAIL"


def test_ruleset_pushdown_predicate(path, glue_database, glue_table, glue_ruleset, glue_data_quality_role):
    df = pd.DataFrame({"c0": [0, 1, 2, 3], "c1": [0, 1, 2, 3], "c2": [0, 0, 1, 1]})
    wr.s3.to_parquet(df, path, dataset=True, database=glue_database, table=glue_table, partition_cols=["c2"])
    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
    )
    df_results = wr.data_quality.evaluate_ruleset(
        name=glue_ruleset,
        iam_role_arn=glue_data_quality_role,
        number_of_workers=2,
        additional_options={
            "pushDownPredicate": "(c2 == '0')",
        },
    )
    assert df_results["Result"].eq("PASS").all()


def test_create_ruleset_already_exists(
    df: pd.DataFrame,
    glue_database: str,
    glue_table: str,
    glue_ruleset: str,
) -> None:
    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
    )

    with pytest.raises(wr.exceptions.AlreadyExists):
        wr.data_quality.create_ruleset(
            name=glue_ruleset,
            database=glue_database,
            table=glue_table,
            dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
        )


def test_update_ruleset(df: pd.DataFrame, glue_database: str, glue_table: str, glue_ruleset: str) -> None:
    df_rules = pd.DataFrame(
        {
            "rule_type": ["RowCount", "IsComplete", "Uniqueness", "ColumnValues"],
            "parameter": [None, "c0", "c0", "c1"],
            "expression": ["between 1 and 6", None, "> 0.95", "in [0, 1, 2]"],
        }
    )
    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        df_rules=df_rules,
    )

    df_rules = df_rules.append(
        {"rule_type": "ColumnValues", "parameter": "c2", "expression": "in [0, 1, 2]"}, ignore_index=True
    )

    new_glue_ruleset_name = f"{glue_ruleset} 2.0"
    wr.data_quality.update_ruleset(
        name=glue_ruleset,
        updated_name=new_glue_ruleset_name,
        df_rules=df_rules,
    )

    df_ruleset = wr.data_quality.get_ruleset(name=new_glue_ruleset_name)

    assert df_rules.equals(df_ruleset)


def test_update_ruleset_does_not_exists(df: pd.DataFrame, glue_ruleset: str) -> None:
    df_rules = pd.DataFrame(
        {
            "rule_type": ["RowCount"],
            "parameter": [None],
            "expression": ["between 1 and 6"],
        }
    )

    with pytest.raises(wr.exceptions.ResourceDoesNotExist):
        wr.data_quality.update_ruleset(
            name=glue_ruleset,
            updated_name=f"{glue_ruleset} 2.0",
            df_rules=df_rules,
        )


def test_two_evaluations_at_once(
    df: pd.DataFrame, glue_database: str, glue_table: str, glue_ruleset: str, glue_data_quality_role: str
) -> None:
    df_rules1 = pd.DataFrame(
        [
            {
                "rule_type": "RowCount",
                "parameter": None,
                "expression": "between 1 and 6",
            }
        ]
    )
    df_rules2 = pd.DataFrame(
        [
            {
                "rule_type": "IsComplete",
                "parameter": "c0",
                "expression": None,
            }
        ]
    )

    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        df_rules=df_rules1,
    )
    wr.data_quality.create_ruleset(
        name=f"{glue_ruleset}2",
        database=glue_database,
        table=glue_table,
        df_rules=df_rules2,
    )

    df_results = wr.data_quality.evaluate_ruleset(
        name=[glue_ruleset, f"{glue_ruleset}2"],
        iam_role_arn=glue_data_quality_role,
        number_of_workers=2,
    )
    assert df_results["Result"].eq("PASS").all()
