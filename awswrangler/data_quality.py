"""Data Quality module."""

import logging
import pprint
import time
import uuid
from typing import Any, Dict, List, Optional, Union, cast

import boto3
import botocore
import pandas as pd

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_RULESET_EVALUATION_FINAL_STATUSES: List[str] = ["STOPPED", "SUCCEEDED", "FAILED"]
_RULESET_EVALUATION_WAIT_POLLING_DELAY: float = 0.25  # SECONDS


def _start_ruleset_evaluation_run(
    ruleset_names: Union[str, List[str]],
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    boto3_session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    args: Dict[str, Any] = {
        "RulesetNames": ruleset_names if isinstance(ruleset_names, list) else [ruleset_names],
        "DataSource": data_source,
    }

    _logger.debug("args: \n%s", pprint.pformat(args))
    response: Dict[str, Any] = client_glue.start_data_quality_ruleset_evaluation_run(
        **args,
    )
    return cast(str, response["RunId"])


def _get_ruleset_evaluation_run(
    run_id: str,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    boto3_session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    response: Dict[str, Any] = client_glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
    return cast(Dict[str, Any], response)


def _wait_ruleset_evaluation_run(
    run_id: str,
    boto3_session: Optional[boto3.Session] = None,
) -> List[str]:
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    response: Dict[str, Any] = _get_ruleset_evaluation_run(run_id=run_id, boto3_session=session)
    status: str = response["Status"]
    while status not in _RULESET_EVALUATION_FINAL_STATUSES:
        time.sleep(_RULESET_EVALUATION_WAIT_POLLING_DELAY)
        response = _get_ruleset_evaluation_run(run_id=run_id, boto3_session=session)
        status = response["Status"]
    _logger.debug("status: %s", status)
    if status == "FAILED":
        raise exceptions.QueryFailed(response.get("ErrorString"))
    if status == "STOPPED":
        raise exceptions.QueryCancelled("Ruleset execution stopped")
    return response["ResultIds"]


def _get_ruleset(
    ruleset_name: str,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    boto3_session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    response: Dict[str, Any] = client_glue.get_data_quality_ruleset(Name=ruleset_name)
    return cast(Dict[str, Any], response)


def _get_data_quality_result(
    result_id: str,
    boto3_session: Optional[boto3.Session] = None,
):
    boto3_session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    rule_results: Dict[str, Any] = client_glue.get_data_quality_result(
        ResultId=result_id,
    )["RuleResults"]


def _evaluate_ruleset(
    ruleset_names: Union[str, List[str]],
    boto3_session: Optional[boto3.Session] = None,
):
    run_id: str = _start_ruleset_evaluation_run(
        ruleset_names=ruleset_names,
        boto3_session=boto3_session,
    )
    _logger.debug("query_id: %s", run_id)

    result_ids: List[str] = _wait_ruleset_evaluation_run(
        run_id=run_id,
        boto3_session=boto3_session,
    )


def _create_dqdl(
    df_rules: pd.DataFrame,
):
    """Create DQDL from pandas data frame."""
    rules = []
    for rule_type, parameter, expression in df_rules.itertuples(index=False):
        parameter_str = f' "{parameter}" ' if parameter else " "
        expression_str = expression if expression else ""
        rules.append(f"{rule_type}{parameter_str}{expression_str}")

    rules_str = "Rules = [ " + ", ".join(rules) + " ]"
    return rules_str


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
        Random id used for idempotency. Will be automatically generated if not provided.
    boto3_session : boto3.Session, optional
        Ruleset description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.data_quality.create_ruleset(
    >>>     name="ruleset",
    >>>     database="db",
    >>>     table="table",
    >>>     dqdl_rules=dqdl_rules,
    >>>)
    """
    if df_rules is not None and dqdl_rules:
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
    except client_glue.exceptions.AlreadyExistsException:
        raise exceptions.AlreadyExists(f"Ruleset {name} already exists.")
