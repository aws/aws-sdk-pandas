---
id: 037-glue-data-quality
title: "Glue Data Quality"
sidebar_position: 37
sidebar_label: "37 - Glue Data Quality"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/037%20-%20Glue%20Data%20Quality.ipynb
---

# Glue Data Quality

> This page is generated from [`tutorials/037 - Glue Data Quality.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/037%20-%20Glue%20Data%20Quality.ipynb). Open it in Jupyter to run it yourself.
AWS Glue Data Quality helps you evaluate and monitor the quality of your data.

## Create test data

First, let's start by creating test data, writing it to S3, and registering it in the Glue Data Catalog.

```python
import pandas as pd

import awswrangler as wr

glue_database = "aws_sdk_pandas"
glue_table = "my_glue_table"
path = "s3://BUCKET_NAME/my_glue_table/"

df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 0]})
wr.s3.to_parquet(df, path, dataset=True, database=glue_database, table=glue_table, partition_cols=["c2"])
```

## Start with recommended data quality rules

AWS Glue Data Quality can recommend a set of data quality rules so you can get started quickly.

Note: Running Glue Data Quality recommendation and evaluation tasks requires an IAM role. This role must trust the Glue principal and allow permissions to various resources including the Glue table and the S3 bucket where your data is stored. Moreover, data quality IAM actions must be granted. To find out more, check [Authorization](https://docs.aws.amazon.com/glue/latest/dg/data-quality-authorization.html).

```python
first_ruleset = "ruleset_1"
iam_role_arn = "arn:aws:iam::..."  # IAM role assumed by the Glue Data Quality job to access resources

df_recommended_ruleset = wr.data_quality.create_recommendation_ruleset(  # Creates a recommended ruleset
    name=first_ruleset,
    database=glue_database,
    table=glue_table,
    iam_role_arn=iam_role_arn,
    number_of_workers=2,
)

df_recommended_ruleset
```

```text
      rule_type parameter       expression
0      RowCount      None  between 1 and 6
1    IsComplete      "c0"             None
2    Uniqueness      "c0"           > 0.95
3  ColumnValues      "c0"             <= 2
4    IsComplete      "c1"             None
5    Uniqueness      "c1"           > 0.95
6  ColumnValues      "c1"             <= 2
7    IsComplete      "c2"             None
8  ColumnValues      "c2"         in ["0"]
```

## Update the recommended rules

Recommended rulesets are not perfect and you are likely to modify them or create your own.

```python
# Append and update rules
df_updated_ruleset = df_recommended_ruleset.append(
    {"rule_type": "Uniqueness", "parameter": '"c2"', "expression": "> 0.95"}, ignore_index=True
)

df_updated_ruleset.at[8, "expression"] = "in [0, 1, 2]"

# Update the existing ruleset (upsert)
wr.data_quality.update_ruleset(
    name=first_ruleset,
    df_rules=df_updated_ruleset,
    mode="upsert",  # update existing or insert new rules to the ruleset
)

wr.data_quality.get_ruleset(name=first_ruleset)
```

```text
      rule_type parameter       expression
0      RowCount      None  between 1 and 6
1    IsComplete      "c0"             None
2    Uniqueness      "c0"           > 0.95
3  ColumnValues      "c0"             <= 2
4    IsComplete      "c1"             None
5    Uniqueness      "c1"           > 0.95
6  ColumnValues      "c1"             <= 2
7    IsComplete      "c2"             None
8  ColumnValues      "c2"     in [0, 1, 2]
9    Uniqueness      "c2"           > 0.95
```

## Run a data quality task

The ruleset can now be evaluated against the data. A cluster with 2 workers is used for the run. It returns a report with `PASS`/`FAIL` results for each rule.

```python
wr.data_quality.evaluate_ruleset(
    name=first_ruleset,
    iam_role_arn=iam_role_arn,
    number_of_workers=2,
)
```

```text
      Name                   Description Result  \
0   Rule_1      RowCount between 1 and 6   PASS   
1   Rule_2               IsComplete "c0"   PASS   
2   Rule_3        Uniqueness "c0" > 0.95   PASS   
3   Rule_4        ColumnValues "c0" <= 2   PASS   
4   Rule_5               IsComplete "c1"   PASS   
5   Rule_6        Uniqueness "c1" > 0.95   PASS   
6   Rule_7        ColumnValues "c1" <= 2   PASS   
7   Rule_8               IsComplete "c2"   PASS   
8   Rule_9  ColumnValues "c2" in [0,1,2]   PASS   
9  Rule_10        Uniqueness "c2" > 0.95   FAIL   

                                            ResultId  \
0  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
1  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
2  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
3  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
4  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
5  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
6  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
7  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
8  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   
9  dqresult-be413b527c0e5520ad843323fecd9cf2e2edbdd5   

                                   EvaluationMessage  
0                                                NaN  
1                                                NaN  
2                                                NaN  
3                                                NaN  
4                                                NaN  
5                                                NaN  
6                                                NaN  
7                                                NaN  
8                                                NaN  
9  Value: 0.0 does not meet the constraint requir...
```

## Create ruleset from Data Quality Definition Language definition

The Data Quality Definition Language (DQDL) is a domain specific language that you can use to define Data Quality rules. For the full syntax reference, see [DQDL](https://docs.aws.amazon.com/glue/latest/dg/dqdl.html).

```python
second_ruleset = "ruleset_2"

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
    name=second_ruleset,
    database=glue_database,
    table=glue_table,
    dqdl_rules=dqdl_rules,
)
```

## Create or update a ruleset from a data frame

AWS SDK for pandas also enables you to create or update a ruleset from a pandas data frame.

```python
third_ruleset = "ruleset_3"

df_rules = pd.DataFrame(
    {
        "rule_type": ["RowCount", "ColumnCorrelation", "Uniqueness"],
        "parameter": [None, '"c0" "c1"', '"c0"'],
        "expression": ["between 2 and 8", "> 0.8", "> 0.95"],
    }
)

wr.data_quality.create_ruleset(
    name=third_ruleset,
    df_rules=df_rules,
    database=glue_database,
    table=glue_table,
)

wr.data_quality.get_ruleset(name=third_ruleset)
```

```text
           rule_type  parameter       expression
0           RowCount       None  between 2 and 8
1  ColumnCorrelation  "c0" "c1"            > 0.8
2         Uniqueness       "c0"           > 0.95
```

## Get multiple rulesets

```python
wr.data_quality.get_ruleset(name=[first_ruleset, second_ruleset, third_ruleset])
```

```text
           rule_type  parameter       expression    ruleset
0           RowCount       None  between 1 and 6  ruleset_1
1         IsComplete       "c0"             None  ruleset_1
2         Uniqueness       "c0"           > 0.95  ruleset_1
3       ColumnValues       "c0"             <= 2  ruleset_1
4         IsComplete       "c1"             None  ruleset_1
5         Uniqueness       "c1"           > 0.95  ruleset_1
6       ColumnValues       "c1"             <= 2  ruleset_1
7         IsComplete       "c2"             None  ruleset_1
8       ColumnValues       "c2"     in [0, 1, 2]  ruleset_1
9         Uniqueness       "c2"           > 0.95  ruleset_1
0           RowCount       None  between 1 and 6  ruleset_2
1         IsComplete       "c0"             None  ruleset_2
2         Uniqueness       "c0"           > 0.95  ruleset_2
3       ColumnValues       "c0"             <= 2  ruleset_2
4         IsComplete       "c1"             None  ruleset_2
5         Uniqueness       "c1"           > 0.95  ruleset_2
6       ColumnValues       "c1"             <= 2  ruleset_2
7         IsComplete       "c2"             None  ruleset_2
8       ColumnValues       "c2"             <= 1  ruleset_2
0           RowCount       None  between 2 and 8  ruleset_3
1  ColumnCorrelation  "c0" "c1"            > 0.8  ruleset_3
2         Uniqueness       "c0"           > 0.95  ruleset_3
```

## Evaluate Data Quality for a given partition

A data quality evaluation run can be limited to specific partition(s) by leveraging the `pushDownPredicate` expression in the `additional_options` argument

```python
df = pd.DataFrame({"c0": [2, 0, 1], "c1": [1, 0, 2], "c2": [1, 1, 1]})
wr.s3.to_parquet(df, path, dataset=True, database=glue_database, table=glue_table, partition_cols=["c2"])

wr.data_quality.evaluate_ruleset(
    name=third_ruleset,
    iam_role_arn=iam_role_arn,
    number_of_workers=2,
    additional_options={
        "pushDownPredicate": "(c2 == '1')",
    },
)
```

```text
     Name                        Description Result  \
0  Rule_1           RowCount between 2 and 8   PASS   
1  Rule_2  ColumnCorrelation "c0" "c1" > 0.8   FAIL   
2  Rule_3             Uniqueness "c0" > 0.95   PASS   

                                            ResultId  \
0  dqresult-f676cfe0345aa93f492e3e3c3d6cf1ad99b84dc6   
1  dqresult-f676cfe0345aa93f492e3e3c3d6cf1ad99b84dc6   
2  dqresult-f676cfe0345aa93f492e3e3c3d6cf1ad99b84dc6   

                                   EvaluationMessage  
0                                                NaN  
1  Value: 0.5 does not meet the constraint requir...  
2                                                NaN
```
