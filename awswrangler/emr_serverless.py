"""EMR Serverless module."""

import logging
import pprint
import time
from typing import Any, Dict, List, Literal, Optional, Union

import boto3

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_EMR_SERVERLESS_JOB_WAIT_POLLING_DELAY: float = 5  # SECONDS
_EMR_SERVERLESS_JOB_FINAL_STATES: List[str] = ["SUCCESS", "FAILED", "CANCELLED"]


def create_application(
    name: str,
    release_label: str,
    application_type: Literal["Spark", "Hive"] = "Spark",
    initial_capacity: Optional[Dict[str, str]] = None,
    maximum_capacity: Optional[Dict[str, str]] = None,
    tags: Optional[Dict[str, str]] = None,
    autostart: bool = True,
    autostop: bool = True,
    idle_timeout: int = 15,
    network_configuration: Optional[Dict[str, str]] = None,
    architecture: Literal["ARM64", "X86_64"] = "X86_64",
    image_uri: Optional[str] = None,
    worker_type_specifications: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """
    Create an EMR Serverless application.

    https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html

    Parameters
    ----------
    name
    release_label
    application_type
    initial_capacity
    maximum_capacity
    tags
    autostart
    autostop
    idle_timeout
    network_configuration
    architecture
    image_uri
    worker_type_specifications
    boto3_session

    Returns
    -------
    str
        Application ID.

    """
    emr_serverless = _utils.client(service_name="emr-serverless", session=boto3_session)
    application_args: Dict[str, Any] = {
        "name": name,
        "releaseLabel": release_label,
        "type": application_type,
        "autoStartConfiguration": {
            "enabled": autostart,
        },
        "autoStopConfiguration": {
            "enabled": autostop,
            "idleTimeoutMinutes": idle_timeout,
        },
        "architecture": architecture,
    }
    if initial_capacity:
        application_args["initialCapacity"] = initial_capacity
    if maximum_capacity:
        application_args["maximumCapacity"] = maximum_capacity
    if tags:
        application_args["tags"] = tags
    if network_configuration:
        application_args["networkConfiguration"] = network_configuration
    if worker_type_specifications:
        application_args["workerTypeSpecifications"] = worker_type_specifications
    if image_uri:
        application_args["imageConfiguration"] = {
            "imageUri": image_uri,
        }
    response: Dict[str, str] = emr_serverless.create_application(**application_args)
    _logger.debug("response: \n%s", pprint.pformat(response))
    return response["applicationId"]


def run_job(
    application_id: str,
    execution_role_arn: str,
    job_driver_args: Dict[str, Any],
    job_type: Literal["Spark", "Hive"] = "Spark",
    wait: bool = True,
    configuration_overrides: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
    execution_timeout: Optional[int] = None,
    name: Optional[str] = None,
    emr_serverless_job_wait_polling_delay: float = _EMR_SERVERLESS_JOB_WAIT_POLLING_DELAY,
    boto3_session: Optional[boto3.Session] = None,
) -> Union[str, Dict[str, Any]]:
    emr_serverless = _utils.client(service_name="emr-serverless", session=boto3_session)
    job_args: Dict[str, Any] = {
        "applicationId": application_id,
        "executionRoleArn": execution_role_arn,
    }
    if job_type == "Spark":
        job_args["jobDriver"] = {
            "sparkSubmit": job_driver_args,
        }
    elif job_type == "Hive":
        job_args["jobDriver"] = {
            "hive": job_driver_args,
        }
    else:
        raise ValueError(f"Unknown job type `{job_type}`")

    if configuration_overrides:
        job_args["configurationOverrides"] = configuration_overrides
    if tags:
        job_args["tags"] = tags
    if execution_timeout:
        job_args["executionTimeoutMinutes"] = execution_timeout
    if name:
        job_args["name"] = name
    response = emr_serverless.start_job_run(**job_args)
    _logger.debug("Job run response: %s", response)
    job_run_id: str = response["jobRunId"]
    if wait:
        return wait_job(
            application_id=application_id,
            job_run_id=job_run_id,
            emr_serverless_job_wait_polling_delay=emr_serverless_job_wait_polling_delay,
        )
    return job_run_id


def wait_job(
    application_id: str,
    job_run_id: str,
    emr_serverless_job_wait_polling_delay: float = _EMR_SERVERLESS_JOB_WAIT_POLLING_DELAY,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    emr_serverless = _utils.client(service_name="emr-serverless", session=boto3_session)
    response = emr_serverless.get_job_run(
        applicationId=application_id,
        jobRunId=job_run_id,
    )
    state = response["jobRun"]["state"]
    while state not in _EMR_SERVERLESS_JOB_FINAL_STATES:
        time.sleep(emr_serverless_job_wait_polling_delay)
        response = emr_serverless.get_job_run(
            applicationId=application_id,
            jobRunId=job_run_id,
        )
        state = response["jobRun"]["state"]
    _logger.debug("Job state: %s", state)
    if state != "SUCCESS":
        _logger.debug("Job run response: %s", response)
        raise exceptions.EMRServerlessJobError(response.get("jobRun", {}).get("stateDetails"))
    return response  # type: ignore[return-value]
