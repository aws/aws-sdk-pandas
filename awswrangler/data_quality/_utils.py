"""AWS Glue Data Quality Utils module."""

import ast
import logging
import pprint
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import boto3
import botocore.exceptions
import pandas as pd

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_RULESET_EVALUATION_FINAL_STATUSES: List[str] = ["STOPPED", "SUCCEEDED", "FAILED", "TIMEOUT"]
_RULESET_EVALUATION_WAIT_POLLING_DELAY: float = 0.25  # SECONDS


def _parse_rules(rules: List[str]) -> List[Tuple[str, Optional[str], Optional[str]]]:
    parsed_rules: List[Tuple[str, Optional[str], Optional[str]]] = []
    for rule in rules:
        rule_type, remainder = tuple(rule.split(maxsplit=1))
        if remainder.startswith('"'):
            expression_regex = r"\s+(?:[=><]|between\s+.+\s+and\s+|in\s+\[.+\]|matches\s+).*"
            expression_matches = re.findall(expression_regex, remainder)
            expression = None if len(expression_matches) == 0 else expression_matches[0].strip()
            parameter = remainder.split(expression)[0].strip() if expression else remainder
        else:
            expression = remainder
            parameter = None
        parsed_rules.append((rule_type, parameter, expression))
    return parsed_rules


def _rules_to_df(rules: str) -> pd.DataFrame:
    rules = re.sub(r"^\s*Rules\s*=\s*\[\s*", "", rules)  # remove Rules = [\n
    rules = re.sub(r"\s*\]\s*$", "", rules)  # remove \n]
    rules = re.sub(r"\s*,\s*(?![^[]*])", "', '", rules)
    list_rules = ast.literal_eval(f"['{rules}']")
    return pd.DataFrame(_parse_rules(list_rules), columns=["rule_type", "parameter", "expression"])


def _create_datasource(
    database: str,
    table: str,
    catalog_id: Optional[str] = None,
    connection_name: Optional[str] = None,
    additional_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    datasource: Dict[str, Dict[str, Any]] = {
        "GlueTable": {
            "DatabaseName": database,
            "TableName": table,
        }
    }
    if catalog_id:
        datasource["GlueTable"]["CatalogId"] = catalog_id
    if connection_name:
        datasource["GlueTable"]["ConnectionName"] = connection_name
    if additional_options:
        datasource["GlueTable"]["AdditionalOptions"] = additional_options
    return datasource


def _start_ruleset_evaluation_run(
    ruleset_names: Union[str, List[str]],
    iam_role_arn: str,
    number_of_workers: int = 5,
    timeout: int = 2880,
    database: Optional[str] = None,
    table: Optional[str] = None,
    catalog_id: Optional[str] = None,
    connection_name: Optional[str] = None,
    additional_options: Optional[Dict[str, str]] = None,
    additional_run_options: Optional[Dict[str, Union[str, bool]]] = None,
    client_token: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    if not database or not table:
        ruleset: Dict[str, Dict[str, str]] = _get_ruleset(ruleset_name=ruleset_names[0], boto3_session=boto3_session)
        database = ruleset["TargetTable"]["DatabaseName"]
        table = ruleset["TargetTable"]["TableName"]
    datasource: Dict[str, Dict[str, str]] = _create_datasource(
        database=database,
        table=table,
        catalog_id=catalog_id,
        connection_name=connection_name,
        additional_options=additional_options,
    )
    args: Dict[str, Any] = {
        "RulesetNames": ruleset_names if isinstance(ruleset_names, list) else [ruleset_names],
        "DataSource": datasource,
        "Role": iam_role_arn,
        "NumberOfWorkers": number_of_workers,
        "Timeout": timeout,
        "ClientToken": client_token,
    }
    if additional_run_options:
        args["AdditionalRunOptions"] = additional_run_options
    _logger.debug("args: \n%s", pprint.pformat(args))
    response: Dict[str, Any] = client_glue.start_data_quality_ruleset_evaluation_run(
        **args,
    )
    return cast(str, response["RunId"])


def _get_ruleset_run(
    run_id: str,
    run_type: str,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    f = (
        client_glue.get_data_quality_rule_recommendation_run
        if run_type == "recommendation"
        else client_glue.get_data_quality_ruleset_evaluation_run
    )
    response = _utils.try_it(
        f=f,
        ex=botocore.exceptions.ClientError,
        ex_code="ThrottlingException",
        max_num_tries=5,
        RunId=run_id,
    )
    return cast(Dict[str, Any], response)


def _wait_ruleset_run(
    run_id: str,
    run_type: str,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    response: Dict[str, Any] = _get_ruleset_run(run_id=run_id, run_type=run_type, boto3_session=boto3_session)
    status: str = response["Status"]
    while status not in _RULESET_EVALUATION_FINAL_STATUSES:
        time.sleep(_RULESET_EVALUATION_WAIT_POLLING_DELAY)
        response = _get_ruleset_run(run_id=run_id, run_type=run_type, boto3_session=boto3_session)
        status = response["Status"]
    _logger.debug("status: %s", status)
    if status == "FAILED":
        raise exceptions.QueryFailed(response.get("ErrorString"))
    if status == "STOPPED":
        raise exceptions.QueryCancelled("Ruleset execution stopped")
    return response


def _get_ruleset(
    ruleset_name: str,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    response = _utils.try_it(
        f=client_glue.get_data_quality_ruleset,
        ex=botocore.exceptions.ClientError,
        ex_code="ThrottlingException",
        max_num_tries=5,
        Name=ruleset_name,
    )
    return cast(Dict[str, Any], response)


def _get_data_quality_results(
    result_ids: List[str],
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    results: List[Dict[str, Any]] = client_glue.batch_get_data_quality_result(
        ResultIds=result_ids,
    )["Results"]
    rule_results: List[Dict[str, Any]] = []
    for result in results:
        rules: List[Dict[str, str]] = result["RuleResults"]
        for rule in rules:
            rule["ResultId"] = result["ResultId"]
        rule_results.extend(rules)
    return cast(pd.DataFrame, pd.json_normalize(rule_results))
