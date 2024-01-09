"""AWS Glue Data Quality Create module."""

from __future__ import annotations

import logging
import pprint
import uuid
from typing import Any, List, Literal, cast

import boto3

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.data_quality._get import get_ruleset
from awswrangler.data_quality._utils import (
    _create_datasource,
    _get_data_quality_results,
    _rules_to_df,
    _start_ruleset_evaluation_run,
    _wait_ruleset_run,
)

_logger: logging.Logger = logging.getLogger(__name__)


def _create_dqdl(
    df_rules: pd.DataFrame,
) -> str:
    """Create DQDL from pandas data frame."""
    rules = []
    for rule_type, parameter, expression in df_rules.itertuples(index=False):
        parameter_str = f" {parameter} " if parameter else " "
        expression_str = expression if expression else ""
        rules.append(f"{rule_type}{parameter_str}{expression_str}")
    return "Rules = [ " + ", ".join(rules) + " ]"


@apply_configs
def create_ruleset(
    name: str,
    database: str,
    table: str,
    df_rules: pd.DataFrame | None = None,
    dqdl_rules: str | None = None,
    description: str = "",
    client_token: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Create Data Quality ruleset.

    Parameters
    ----------
    name : str
        Ruleset name.
    database : str
        Glue database name.
    table : str
        Glue table name.
    df_rules : str, optional
        Data frame with `rule_type`, `parameter`, and `expression` columns.
    dqdl_rules : str, optional
        Data Quality Definition Language definition.
    description : str
        Ruleset description.
    client_token : str, optional
        Random id used for idempotency. Is automatically generated if not provided.
    boto3_session : boto3.Session, optional
        Boto3 Session. If none, the default boto3 session is used.

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>>
    >>> df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    >>> wr.s3.to_parquet(df, path, dataset=True, database="database", table="table")
    >>> wr.data_quality.create_ruleset(
    >>>     name="ruleset",
    >>>     database="database",
    >>>     table="table",
    >>>     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
    >>>)

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>>
    >>> df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    >>> df_rules = pd.DataFrame({
    >>>        "rule_type": ["RowCount", "IsComplete", "Uniqueness"],
    >>>        "parameter": [None, '"c0"', '"c0"'],
    >>>        "expression": ["between 1 and 6", None, "> 0.95"],
    >>> })
    >>> wr.s3.to_parquet(df, path, dataset=True, database="database", table="table")
    >>> wr.data_quality.create_ruleset(
    >>>     name="ruleset",
    >>>     database="database",
    >>>     table="table",
    >>>     df_rules=df_rules,
    >>>)
    """
    if (df_rules is not None and dqdl_rules) or (df_rules is None and not dqdl_rules):
        raise exceptions.InvalidArgumentCombination("You must pass either ruleset `df_rules` or `dqdl_rules`.")

    client_glue = _utils.client(service_name="glue", session=boto3_session)
    dqdl_rules = _create_dqdl(df_rules) if df_rules is not None else dqdl_rules

    try:
        client_glue.create_data_quality_ruleset(
            Name=name,
            Description=description,
            Ruleset=cast(str, dqdl_rules),
            TargetTable={
                "TableName": table,
                "DatabaseName": database,
            },
            ClientToken=client_token if client_token else uuid.uuid4().hex,
        )
    except client_glue.exceptions.AlreadyExistsException as not_found:
        raise exceptions.AlreadyExists(f"Ruleset {name} already exists.") from not_found


@apply_configs
def update_ruleset(
    name: str,
    mode: Literal["overwrite", "upsert"] = "overwrite",
    df_rules: pd.DataFrame | None = None,
    dqdl_rules: str | None = None,
    description: str = "",
    boto3_session: boto3.Session | None = None,
) -> None:
    """Update Data Quality ruleset.

    Parameters
    ----------
    name : str
        Ruleset name.
    mode : str
        overwrite (default) or upsert.
    df_rules : str, optional
        Data frame with `rule_type`, `parameter`, and `expression` columns.
    dqdl_rules : str, optional
        Data Quality Definition Language definition.
    description : str
        Ruleset description.
    boto3_session : boto3.Session, optional
        Boto3 Session. If none, the default boto3 session is used.

    Examples
    --------
    Overwrite rules in the existing ruleset.
    >>> wr.data_quality.update_ruleset(
    >>>     name="ruleset",
    >>>     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
    >>>)

    Update or insert rules in the existing ruleset.
    >>> wr.data_quality.update_ruleset(
    >>>     name="ruleset",
    >>>     mode="insert",
    >>>     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
    >>>)
    """
    if (df_rules is not None and dqdl_rules) or (df_rules is None and not dqdl_rules):
        raise exceptions.InvalidArgumentCombination("You must pass either ruleset `df_rules` or `dqdl_rules`.")
    if mode not in ["overwrite", "upsert"]:
        raise exceptions.InvalidArgumentValue("`mode` must be one of 'overwrite' or 'upsert'.")

    if mode == "upsert":
        df_existing = get_ruleset(name=name, boto3_session=boto3_session)
        df_existing = df_existing.set_index(keys=["rule_type", "parameter"], drop=False, verify_integrity=True)
        df_updated = _rules_to_df(dqdl_rules) if dqdl_rules is not None else df_rules
        df_updated = df_updated.set_index(keys=["rule_type", "parameter"], drop=False, verify_integrity=True)
        merged_df = pd.concat([df_existing[~df_existing.index.isin(df_updated.index)], df_updated])
        dqdl_rules = _create_dqdl(merged_df.reset_index(drop=True))
    else:
        dqdl_rules = _create_dqdl(df_rules) if df_rules is not None else dqdl_rules

    args = {
        "Name": name,
        "Description": description,
        "Ruleset": dqdl_rules,
    }

    client_glue = _utils.client(service_name="glue", session=boto3_session)
    try:
        client_glue.update_data_quality_ruleset(**args)  # type: ignore[arg-type]
    except client_glue.exceptions.EntityNotFoundException as not_found:
        raise exceptions.ResourceDoesNotExist(f"Ruleset {name} does not exist.") from not_found


@apply_configs
def create_recommendation_ruleset(
    database: str,
    table: str,
    iam_role_arn: str,
    name: str | None = None,
    catalog_id: str | None = None,
    connection_name: str | None = None,
    additional_options: dict[str, Any] | None = None,
    number_of_workers: int = 5,
    timeout: int = 2880,
    client_token: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """Create recommendation Data Quality ruleset.

    Parameters
    ----------
    database : str
        Glue database name.
    table : str
        Glue table name.
    iam_role_arn : str
        IAM Role ARN.
    name : str, optional
        Ruleset name.
    catalog_id : str, optional
        Glue Catalog id.
    connection_name : str, optional
        Glue connection name.
    additional_options : dict, optional
        Additional options for the table. Supported keys:
        `pushDownPredicate`: to filter on partitions without having to list and read all the files in your dataset.
        `catalogPartitionPredicate`: to use server-side partition pruning using partition indexes in the
        Glue Data Catalog.
    number_of_workers: int, optional
        The number of G.1X workers to be used in the run. The default is 5.
    timeout: int, optional
        The timeout for a run in minutes. The default is 2880 (48 hours).
    client_token : str, optional
        Random id used for idempotency. Is automatically generated if not provided.
    boto3_session : boto3.Session, optional
        Boto3 Session. If none, the default boto3 session is used.

    Returns
    -------
    pd.DataFrame
        Data frame with recommended ruleset details.

    Examples
    --------
    >>> import awswrangler as wr

    >>> df_recommended_ruleset = wr.data_quality.create_recommendation_ruleset(
    >>>     database="database",
    >>>     table="table",
    >>>     iam_role_arn="arn:...",
    >>>)
    """
    client_glue = _utils.client(service_name="glue", session=boto3_session)

    args: dict[str, Any] = {
        "DataSource": _create_datasource(
            database=database,
            table=table,
            catalog_id=catalog_id,
            connection_name=connection_name,
            additional_options=additional_options,
        ),
        "Role": iam_role_arn,
        "NumberOfWorkers": number_of_workers,
        "Timeout": timeout,
        "ClientToken": client_token if client_token else uuid.uuid4().hex,
    }
    if name:
        args["CreatedRulesetName"] = name
    _logger.debug("args: \n%s", pprint.pformat(args))
    run_id: str = client_glue.start_data_quality_rule_recommendation_run(**args)["RunId"]

    _logger.debug("run_id: %s", run_id)
    dqdl_recommended_rules: str = cast(
        str,
        _wait_ruleset_run(
            run_id=run_id,
            run_type="recommendation",
            boto3_session=boto3_session,
        )["RecommendedRuleset"],
    )
    return _rules_to_df(rules=dqdl_recommended_rules)


@apply_configs
def evaluate_ruleset(
    name: str | list[str],
    iam_role_arn: str,
    number_of_workers: int = 5,
    timeout: int = 2880,
    database: str | None = None,
    table: str | None = None,
    catalog_id: str | None = None,
    connection_name: str | None = None,
    additional_options: dict[str, str] | None = None,
    additional_run_options: dict[str, str | bool] | None = None,
    client_token: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """Evaluate Data Quality ruleset.

    Parameters
    ----------
    name : str or list[str]
        Ruleset name or list of names.
    iam_role_arn : str
        IAM Role ARN.
    number_of_workers: int, optional
        The number of G.1X workers to be used in the run. The default is 5.
    timeout: int, optional
        The timeout for a run in minutes. The default is 2880 (48 hours).
    database : str, optional
        Glue database name. Database associated with the ruleset will be used if not provided.
    table : str, optional
        Glue table name. Table associated with the ruleset will be used if not provided.
    catalog_id : str, optional
        Glue Catalog id.
    connection_name : str, optional
        Glue connection name.
    additional_options : dict, optional
        Additional options for the table. Supported keys:
        `pushDownPredicate`: to filter on partitions without having to list and read all the files in your dataset.
        `catalogPartitionPredicate`: to use server-side partition pruning using partition indexes in the
        Glue Data Catalog.
    additional_run_options : Dict[str, Union[str, bool]], optional
        Additional run options. Supported keys:
        `CloudWatchMetricsEnabled`: whether to enable CloudWatch metrics.
        `ResultsS3Prefix`: prefix for Amazon S3 to store results.
    client_token : str, optional
        Random id used for idempotency. Will be automatically generated if not provided.
    boto3_session : boto3.Session, optional
        Boto3 Session. If none, the default boto3 session is used.

    Returns
    -------
    pd.DataFrame
        Data frame with ruleset evaluation results.

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>>
    >>> df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    >>> wr.s3.to_parquet(df, path, dataset=True, database="database", table="table")
    >>> wr.data_quality.create_ruleset(
    >>>     name="ruleset",
    >>>     database="database",
    >>>     table="table",
    >>>     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
    >>>)
    >>> df_ruleset_results = wr.data_quality.evaluate_ruleset(
    >>>     name="ruleset",
    >>>     iam_role_arn=glue_data_quality_role,
    >>> )
    """
    run_id: str = _start_ruleset_evaluation_run(
        ruleset_names=[name] if isinstance(name, str) else name,
        iam_role_arn=iam_role_arn,
        number_of_workers=number_of_workers,
        timeout=timeout,
        database=database,
        table=table,
        catalog_id=catalog_id,
        connection_name=connection_name,
        additional_options=additional_options,
        additional_run_options=additional_run_options,
        client_token=client_token if client_token else uuid.uuid4().hex,
        boto3_session=boto3_session,
    )
    _logger.debug("run_id: %s", run_id)
    result_ids: list[str] = cast(
        List[str],
        _wait_ruleset_run(
            run_id=run_id,
            run_type="evaluation",
            boto3_session=boto3_session,
        )["ResultIds"],
    )
    return _get_data_quality_results(result_ids=result_ids, boto3_session=boto3_session)
