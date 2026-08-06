---
id: aws-glue-data-quality
title: "AWS Glue Data Quality"
sidebar_position: 9
---

# AWS Glue Data Quality

Module: `wr.data_quality`

### create_recommendation_ruleset

```python
wr.data_quality.create_recommendation_ruleset(
    database: 'str',
    table: 'str',
    iam_role_arn: 'str',
    name: 'str | None' = None,
    catalog_id: 'str | None' = None,
    connection_name: 'str | None' = None,
    additional_options: 'dict[str, Any] | None' = None,
    number_of_workers: 'int' = 5,
    timeout: 'int' = 2880,
    client_token: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Create recommendation Data Quality ruleset.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Glue database name.
- **`table`** — Glue table name.
- **`iam_role_arn`** — IAM Role ARN.
- **`name`** — Ruleset name.
- **`catalog_id`** — Glue Catalog id.
- **`connection_name`** — Glue connection name.
- **`additional_options`** — Additional options for the table. Supported keys: - `pushDownPredicate`: to filter on partitions without having to list and read all the files in your dataset. - `catalogPartitionPredicate`: to use server-side partition pruning using partition indexes in the Glue Data Catalog.
- **`number_of_workers`** — The number of G.1X workers to be used in the run. The default is 5.
- **`timeout`** — The timeout for a run in minutes. The default is 2880 (48 hours).
- **`client_token`** — Random id used for idempotency. Is automatically generated if not provided.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data frame with recommended ruleset details.

**Examples**

```python
>>> import awswrangler as wr
>>> df_recommended_ruleset = wr.data_quality.create_recommendation_ruleset(
...     database="database",
...     table="table",
...     iam_role_arn="arn:...",
... )
```

---

### create_ruleset

```python
wr.data_quality.create_ruleset(
    name: 'str',
    database: 'str',
    table: 'str',
    df_rules: 'pd.DataFrame | None' = None,
    dqdl_rules: 'str | None' = None,
    description: 'str' = '',
    client_token: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Create Data Quality ruleset.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`name`** — Ruleset name.
- **`database`** — Glue database name.
- **`table`** — Glue table name.
- **`df_rules`** — Data frame with `rule_type`, `parameter`, and `expression` columns.
- **`dqdl_rules`** — Data Quality Definition Language definition.
- **`description`** — Ruleset description.
- **`client_token`** — Random id used for idempotency. Is automatically generated if not provided.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>>
>>> df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
>>> wr.s3.to_parquet(df, path, dataset=True, database="database", table="table")
>>> wr.data_quality.create_ruleset(
...     name="ruleset",
...     database="database",
...     table="table",
...     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
... )
```

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>>
>>> df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
>>> df_rules = pd.DataFrame({
...        "rule_type": ["RowCount", "IsComplete", "Uniqueness"],
...        "parameter": [None, '"c0"', '"c0"'],
...        "expression": ["between 1 and 6", None, "> 0.95"],
... })
>>> wr.s3.to_parquet(df, path, dataset=True, database="database", table="table")
>>> wr.data_quality.create_ruleset(
...     name="ruleset",
...     database="database",
...     table="table",
...     df_rules=df_rules,
>>> )
```

---

### evaluate_ruleset

```python
wr.data_quality.evaluate_ruleset(
    name: 'str | list[str]',
    iam_role_arn: 'str',
    number_of_workers: 'int' = 5,
    timeout: 'int' = 2880,
    database: 'str | None' = None,
    table: 'str | None' = None,
    catalog_id: 'str | None' = None,
    connection_name: 'str | None' = None,
    additional_options: 'dict[str, str] | None' = None,
    additional_run_options: 'dict[str, str | bool] | None' = None,
    client_token: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Evaluate Data Quality ruleset.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`name`** — Ruleset name or list of names.
- **`iam_role_arn`** — IAM Role ARN.
- **`number_of_workers`** — The number of G.1X workers to be used in the run. The default is 5.
- **`timeout`** — The timeout for a run in minutes. The default is 2880 (48 hours).
- **`database`** — Glue database name. Database associated with the ruleset will be used if not provided.
- **`table`** — Glue table name. Table associated with the ruleset will be used if not provided.
- **`catalog_id`** — Glue Catalog id.
- **`connection_name`** — Glue connection name.
- **`additional_options`** — Additional options for the table. Supported keys: `pushDownPredicate`: to filter on partitions without having to list and read all the files in your dataset. `catalogPartitionPredicate`: to use server-side partition pruning using partition indexes in the Glue Data Catalog.
- **`additional_run_options`** — Additional run options. Supported keys: - `CloudWatchMetricsEnabled`: whether to enable CloudWatch metrics. - `ResultsS3Prefix`: prefix for Amazon S3 to store results.
- **`client_token`** — Random id used for idempotency. Will be automatically generated if not provided.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data frame with ruleset evaluation results.

**Examples**

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>>
>>> df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
>>> wr.s3.to_parquet(df, path, dataset=True, database="database", table="table")
>>> wr.data_quality.create_ruleset(
...     name="ruleset",
...     database="database",
...     table="table",
...     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
... )
>>> df_ruleset_results = wr.data_quality.evaluate_ruleset(
...     name="ruleset",
...     iam_role_arn=glue_data_quality_role,
... )
```

---

### get_ruleset

```python
wr.data_quality.get_ruleset(
    name: 'str | list[str]',
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Get a Data Quality ruleset.

**Parameters**

- **`name`** — Ruleset name or list of names.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data frame with ruleset(s) details.

**Examples**

Get single ruleset
```python
>>> import awswrangler as wr
>>> df_ruleset = wr.data_quality.get_ruleset(name="my_ruleset")
```

Get multiple rulesets. A column with the ruleset name is added to the data frame
```python
>>> import awswrangler as wr
>>> df_rulesets = wr.data_quality.get_ruleset(name=["ruleset_1", "ruleset_2"])
```

---

### update_ruleset

```python
wr.data_quality.update_ruleset(
    name: 'str',
    mode: "Literal['overwrite', 'upsert']" = 'overwrite',
    df_rules: 'pd.DataFrame | None' = None,
    dqdl_rules: 'str | None' = None,
    description: 'str' = '',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Update Data Quality ruleset.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`name`** — Ruleset name.
- **`mode`** — overwrite (default) or upsert.
- **`df_rules`** — Data frame with `rule_type`, `parameter`, and `expression` columns.
- **`dqdl_rules`** — Data Quality Definition Language definition.
- **`description`** — Ruleset description.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

Overwrite rules in the existing ruleset.
```python
>>> wr.data_quality.update_ruleset(
...     name="ruleset",
...     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
... )
```

Update or insert rules in the existing ruleset.
```python
>>> wr.data_quality.update_ruleset(
...     name="ruleset",
...     mode="insert",
...     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
... )
```

---
