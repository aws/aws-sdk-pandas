"""AWS Glue Data Quality Utils module."""

from __future__ import annotations

import ast
import logging
import pprint
import re
import time
from typing import Any, Dict, List, cast

import boto3
import botocore.exceptions

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_RULESET_EVALUATION_FINAL_STATUSES: list[str] = ["STOPPED", "SUCCEEDED", "FAILED", "TIMEOUT"]
_RULESET_EVALUATION_WAIT_POLLING_DELAY: float = 1.0  # SECONDS


def _parse_rules(rules: list[str]) -> list[tuple[str, str | None, str | None]]:
    parsed_rules: list[tuple[str, str | None, str | None]] = []
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
    catalog_id: str | None = None,
    connection_name: str | None = None,
    additional_options: dict[str, str] | None = None,
) -> dict[str, dict[str, str]]:
    datasource: dict[str, dict[str, Any]] = {
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
    ruleset_names: str | list[str],
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
) -> str:
    client_glue = _utils.client(service_name="glue", session=boto3_session)

    if not database or not table:
        ruleset: dict[str, dict[str, str]] = _get_ruleset(ruleset_name=ruleset_names[0], boto3_session=boto3_session)
        database = ruleset["TargetTable"]["DatabaseName"]
        table = ruleset["TargetTable"]["TableName"]
    datasource: dict[str, dict[str, str]] = _create_datasource(
        database=database,
        table=table,
        catalog_id=catalog_id,
        connection_name=connection_name,
        additional_options=additional_options,
    )
    args: dict[str, Any] = {
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
    response = client_glue.start_data_quality_ruleset_evaluation_run(
        **args,
    )
    return response["RunId"]


def _get_ruleset_run(
    run_id: str,
    run_type: str,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    client_glue = _utils.client(service_name="glue", session=boto3_session)
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
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    response: dict[str, Any] = _get_ruleset_run(run_id=run_id, run_type=run_type, boto3_session=boto3_session)
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
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    client_glue = _utils.client(service_name="glue", session=boto3_session)
    response = _utils.try_it(
        f=client_glue.get_data_quality_ruleset,
        ex=botocore.exceptions.ClientError,
        ex_code="ThrottlingException",
        max_num_tries=5,
        Name=ruleset_name,
    )
    return cast(Dict[str, Any], response)


def _get_data_quality_results(
    result_ids: list[str],
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    client_glue = _utils.client(service_name="glue", session=boto3_session)

    results = client_glue.batch_get_data_quality_result(
        ResultIds=result_ids,
    )["Results"]
    rule_results: list[dict[str, Any]] = []
    for result in results:
        rule_results.extend(
            cast(
                List[Dict[str, Any]],
                [
                    dict(
                        ((k, d[k]) for k in ("Name", "Description", "Result") if k in d),  # type: ignore[literal-required]
                        **{"ResultId": result["ResultId"]},
                    )
                    for d in result["RuleResults"]
                ],
            )
        )
    return pd.json_normalize(rule_results)
