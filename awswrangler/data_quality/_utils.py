"""AWS Glue Data Quality Utils module."""

import logging
import pprint
import time
from typing import Any, Dict, List, Optional, Union, cast

import boto3

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_RULESET_EVALUATION_FINAL_STATUSES: List[str] = ["STOPPED", "SUCCEEDED", "FAILED"]
_RULESET_EVALUATION_WAIT_POLLING_DELAY: float = 0.25  # SECONDS


def _create_datasource(
    database: str,
    table: str,
    catalog_id: Optional[str] = None,
    connection: Optional[str] = None,
    additional_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    datasource: Dict[str, Dict[str, str]] = {
        "GlueTable": {
            "DatabaseName": database,
            "TableName": table,
        }
    }
    if catalog_id:
        datasource["GlueTable"]["CatalogId"] = catalog_id
    if connection:
        datasource["GlueTable"]["ConnectionName"] = connection
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
    connection: Optional[str] = None,
    additional_options: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    boto3_session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    if not database or not table:
        ruleset: Dict[str, Dict[str, str]] = _get_ruleset(ruleset_name=ruleset_names[0], boto3_session=boto3_session)
        database: str = ruleset["TargetTable"]["DatabaseName"]
        table: str = ruleset["TargetTable"]["TableName"]
    datasource: Dict[str, Dict[str, str]] = _create_datasource(
        database=database,
        table=table,
        catalog_id=catalog_id,
        connection=connection,
        additional_options=additional_options,
    )
    args: Dict[str, Any] = {
        "RulesetNames": ruleset_names if isinstance(ruleset_names, list) else [ruleset_names],
        "DataSource": datasource,
        "Role": iam_role_arn,
        "NumberOfWorkers": number_of_workers,
        "Timeout": timeout,
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
