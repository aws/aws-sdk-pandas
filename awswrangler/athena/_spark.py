"""Apache Spark on Amazon Athena Module."""
# pylint: disable=too-many-lines
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

import boto3

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mypy_boto3_athena.type_defs import (
        EngineConfigurationTypeDef,
        GetCalculationExecutionResponseTypeDef,
        GetCalculationExecutionStatusResponseTypeDef,
        GetSessionStatusResponseTypeDef,
    )

_SESSION_FINAL_STATES: List[str] = ["IDLE", "TERMINATED", "DEGRADED", "FAILED"]
_CALCULATION_EXECUTION_FINAL_STATES: List[str] = ["COMPLETED", "FAILED", "CANCELED"]
_SESSION_WAIT_POLLING_DELAY: float = 5.0  # SECONDS
_CALCULATION_EXECUTION_WAIT_POLLING_DELAY: float = 5.0  # SECONDS


def _wait_session(
    session_id: str,
    boto3_session: Optional[boto3.Session] = None,
    athena_session_wait_polling_delay: float = _SESSION_WAIT_POLLING_DELAY,
) -> "GetSessionStatusResponseTypeDef":
    client_athena = _utils.client(service_name="athena", session=boto3_session)

    response: "GetSessionStatusResponseTypeDef" = client_athena.get_session_status(SessionId=session_id)
    state: str = response["Status"]["State"]

    while state not in _SESSION_FINAL_STATES:
        time.sleep(athena_session_wait_polling_delay)
        response = client_athena.get_session_status(SessionId=session_id)
        state = response["Status"]["State"]
    _logger.debug("Session state: %s", state)
    _logger.debug("Session state change reason: %s", response["Status"].get("StateChangeReason"))
    if state in ["FAILED", "DEGRADED", "TERMINATED"]:
        raise exceptions.SessionFailed(response["Status"].get("StateChangeReason"))
    return response


# TODO: refactor copy-pasted waiters
def _wait_calculation_execution(
    calculation_execution_id: str,
    boto3_session: Optional[boto3.Session] = None,
    athena_calculation_execution_wait_polling_delay: float = _CALCULATION_EXECUTION_WAIT_POLLING_DELAY,
) -> "GetCalculationExecutionStatusResponseTypeDef":
    client_athena = _utils.client(service_name="athena", session=boto3_session)

    response: "GetCalculationExecutionStatusResponseTypeDef" = client_athena.get_calculation_execution_status(
        CalculationExecutionId=calculation_execution_id
    )
    state: str = response["Status"]["State"]

    while state not in _CALCULATION_EXECUTION_FINAL_STATES:
        time.sleep(athena_calculation_execution_wait_polling_delay)
        response = client_athena.get_calculation_execution_status(CalculationExecutionId=calculation_execution_id)
        state = response["Status"]["State"]
    _logger.debug("Calculation execution state: %s", state)
    _logger.debug("Calculation execution state change reason: %s", response["Status"].get("StateChangeReason"))
    if state in ["CANCELED", "FAILED"]:
        raise exceptions.CalculationFailed(response["Status"].get("StateChangeReason"))
    return response


def _get_calculation_execution_results(
    calculation_execution_id: str,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    client_athena = _utils.client(service_name="athena", session=boto3_session)

    _wait_calculation_execution(
        calculation_execution_id=calculation_execution_id,
        boto3_session=boto3_session,
    )

    response: "GetCalculationExecutionResponseTypeDef" = client_athena.get_calculation_execution(
        CalculationExecutionId=calculation_execution_id,
    )
    # TODO: process results by content-types
    # result_type = response["Result"]["ResultType"]
    # result_s3_uri = response["Result"]["ResultS3Uri"]
    # if result_type == "application/vnd.aws.athena.v1+json":
    #    return s3.read_json(path=result_s3_uri)
    return cast(Dict[str, Any], response)


def create_spark_session(
    workgroup: str,
    coordinator_dpu_size: int = 1,
    max_concurrent_dpus: int = 5,
    default_executor_dpu_size: int = 1,
    additional_configs: Optional[Dict[str, Any]] = None,
    idle_timeout: int = 15,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """
    Create session and wait until ready to accept calculations.

    Parameters
    ----------
    workgroup : str
    coordinator_dpu_size : int, optional
    max_concurrent_dpus : int, optional
    default_executor_dpu_size: int, optional
    additional_configs : Dict[str, Any], optional
    idle_timeout : int, optional
    boto3_session : boto3.Session(), optional

    Returns
    -------
        Session id
    """
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    engine_configuration: "EngineConfigurationTypeDef" = {
        "CoordinatorDpuSize": coordinator_dpu_size,
        "MaxConcurrentDpus": max_concurrent_dpus,
        "DefaultExecutorDpuSize": default_executor_dpu_size,
    }
    if additional_configs:
        engine_configuration["AdditionalConfigs"] = additional_configs
    response = client_athena.start_session(
        WorkGroup=workgroup,
        EngineConfiguration=engine_configuration,
        SessionIdleTimeoutInMinutes=idle_timeout,
    )
    _logger.info("Session info:\n%s", response)
    session_id: str = response["SessionId"]
    # Wait for the session to reach IDLE state to be able to accept calculations
    _wait_session(
        session_id=session_id,
        boto3_session=boto3_session,
    )
    return session_id


def run_spark_calculation(
    code: str,
    workgroup: str,
    session_id: Optional[str] = None,
    coordinator_dpu_size: int = 1,
    max_concurrent_dpus: int = 5,
    default_executor_dpu_size: int = 1,
    additional_configs: Optional[Dict[str, Any]] = None,
    idle_timeout: int = 15,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """
    Execute Spark Calculation and wait for completion.

    Parameters
    ----------
    code : str
    workgroup : str
    session_id : str, optional
    coordinator_dpu_size : int, optional
    max_concurrent_dpus : int, optional
    default_executor_dpu_size: int, optional
    additional_configs : Dict[str, Any], optional
    idle_timeout : int, optional
    boto3_session : boto3.Session(), optional

    Returns
    -------
        Calculation response
    """
    client_athena = _utils.client(service_name="athena", session=boto3_session)

    session_id = (
        create_spark_session(
            workgroup=workgroup,
            coordinator_dpu_size=coordinator_dpu_size,
            max_concurrent_dpus=max_concurrent_dpus,
            default_executor_dpu_size=default_executor_dpu_size,
            additional_configs=additional_configs,
            idle_timeout=idle_timeout,
            boto3_session=boto3_session,
        )
        if not session_id
        else session_id
    )

    response = client_athena.start_calculation_execution(
        SessionId=session_id,
        CodeBlock=code,
    )
    _logger.info("Calculation execution info:\n%s", response)

    return _get_calculation_execution_results(
        calculation_execution_id=response["CalculationExecutionId"],
        boto3_session=boto3_session,
    )
