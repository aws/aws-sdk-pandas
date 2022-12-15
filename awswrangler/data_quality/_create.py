"""AWS Glue Data Quality Create module."""

import logging
import pprint
import uuid
from typing import Any, Dict, List, Optional, Union, cast

import boto3
import pandas as pd

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
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
        parameter_str = f' "{parameter}" ' if parameter else " "
        expression_str = expression if expression else ""
        rules.append(f"{rule_type}{parameter_str}{expression_str}")
    return "Rules = [ " + ", ".join(rules) + " ]"


@apply_configs
def create_ruleset(
    name: str,
    database: str,
    table: str,
    df_rules: Optional[pd.DataFrame] = None,
    dqdl_rules: Optional[str] = None,
    description: str = "",
    client_token: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
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
    >>>        "parameter": [None, "c0", "c0"],
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

    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    dqdl_rules = _create_dqdl(df_rules) if df_rules is not None else dqdl_rules

    try:
        client_glue.create_data_quality_ruleset(
            Name=name,
            Description=description,
            Ruleset=dqdl_rules,
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
    updated_name: Optional[str] = None,
    df_rules: Optional[pd.DataFrame] = None,
    dqdl_rules: Optional[str] = None,
    description: str = "",
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Update Data Quality ruleset.

    Parameters
    ----------
    name : str
        Ruleset name.
    updated_name : str
        New ruleset name if renaming an existing ruleset.
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
    >>> wr.data_quality.update_ruleset(
    >>>     name="ruleset",
    >>>     new_name="my_ruleset",
    >>>     dqdl_rules="Rules = [ RowCount between 1 and 3 ]",
    >>>)
    """
    if (df_rules is not None and dqdl_rules) or (df_rules is None and not dqdl_rules):
        raise exceptions.InvalidArgumentCombination("You must pass either ruleset `df_rules` or `dqdl_rules`.")

    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    dqdl_rules = _create_dqdl(df_rules) if df_rules is not None else dqdl_rules

    try:
        client_glue.update_data_quality_ruleset(
            Name=name,
            UpdatedName=updated_name,
            Description=description,
            Ruleset=dqdl_rules,
        )
    except client_glue.exceptions.EntityNotFoundException as not_found:
        raise exceptions.ResourceDoesNotExist(f"Ruleset {name} does not exist.") from not_found


@apply_configs
def create_recommendation_ruleset(
    database: str,
    table: str,
    iam_role_arn: str,
    name: Optional[str] = None,
    catalog_id: Optional[str] = None,
    connection_name: Optional[str] = None,
    additional_options: Optional[Dict[str, Any]] = None,
    number_of_workers: int = 5,
    timeout: int = 2880,
    client_token: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
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
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    args: Dict[str, Any] = {
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
    run_id: str = cast(str, client_glue.start_data_quality_rule_recommendation_run(**args)["RunId"])

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
    name: Union[str, List[str]],
    iam_role_arn: str,
    number_of_workers: int = 5,
    timeout: int = 2880,
    database: Optional[str] = None,
    table: Optional[str] = None,
    catalog_id: Optional[str] = None,
    connection_name: Optional[str] = None,
    additional_options: Optional[Dict[str, str]] = None,
    additional_run_options: Optional[Dict[str, str]] = None,
    client_token: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
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
    additional_run_options : Dict[str, str], optional
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
    >>>     name=["ruleset1", "rulseset2"],
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
    result_ids: List[str] = cast(
        List[str],
        _wait_ruleset_run(
            run_id=run_id,
            run_type="evaluation",
            boto3_session=boto3_session,
        )["ResultIds"],
    )
    return _get_data_quality_results(result_ids=result_ids, boto3_session=boto3_session)
