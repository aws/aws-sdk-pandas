import logging

import pandas as pd

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_ruleset_df(path, glue_database, glue_table, glue_ruleset, glue_data_quality_role):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_parquet(df, path, dataset=True, database=glue_database, table=glue_table)
    df_rules = pd.DataFrame(
        {
            "rule_type": ["RowCount", "IsComplete", "Uniqueness"],
            "parameter": [None, "c0", "c0"],
            "expression": ["between 1 and 6", None, "> 0.95"],
        }
    )
    wr.data_quality.create_ruleset(
        name=glue_ruleset,
        database=glue_database,
        table=glue_table,
        df_rules=df_rules,
    )
    wr.data_quality.evaluate_ruleset(
        name=glue_ruleset,
        iam_role_arn=glue_data_quality_role,
    )


def test_ruleset_dqdl(path, glue_database, glue_table, glue_ruleset, glue_data_quality_role):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_parquet(df, path, dataset=True, database=glue_database, table=glue_table)
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
    wr.data_quality.evaluate_ruleset(
        name=glue_ruleset,
        iam_role_arn=glue_data_quality_role,
    )
