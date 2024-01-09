"""EMR Serverless module."""

from __future__ import annotations

import logging
import pprint
import time
from typing import Any, Literal, TypedDict

import boto3
from typing_extensions import NotRequired, Required

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.annotations import Experimental

_logger: logging.Logger = logging.getLogger(__name__)

_EMR_SERVERLESS_JOB_WAIT_POLLING_DELAY: float = 5  # SECONDS
_EMR_SERVERLESS_JOB_FINAL_STATES: list[str] = ["SUCCESS", "FAILED", "CANCELLED"]


class SparkSubmitJobArgs(TypedDict):
    """Typed dictionary defining the Spark submit job arguments."""

    entryPoint: Required[str]
    """The entry point for the Spark submit job run."""
    entryPointArguments: NotRequired[list[str]]
    """The arguments for the Spark submit job run."""
    sparkSubmitParameters: NotRequired[str]
    """The parameters for the Spark submit job run."""


class HiveRunJobArgs(TypedDict):
    """Typed dictionary defining the Hive job run arguments."""

    query: Required[str]
    """The S3 location of the query file for the Hive job run."""
    initQueryFile: NotRequired[str]
    """The S3 location of the query file for the Hive job run."""
    parameters: NotRequired[str]
    """The parameters for the Hive job run."""


@Experimental
def create_application(
    name: str,
    release_label: str,
    application_type: Literal["Spark", "Hive"] = "Spark",
    initial_capacity: dict[str, str] | None = None,
    maximum_capacity: dict[str, str] | None = None,
    tags: dict[str, str] | None = None,
    autostart: bool = True,
    autostop: bool = True,
    idle_timeout: int = 15,
    network_configuration: dict[str, str] | None = None,
    architecture: Literal["ARM64", "X86_64"] = "X86_64",
    image_uri: str | None = None,
    worker_type_specifications: dict[str, str] | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    """
    Create an EMR Serverless application.

    https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html

    Parameters
    ----------
    name : str
        Name of EMR Serverless appliation
    release_label : str
        Release label e.g. `emr-6.10.0`
    application_type : str, optional
        Application type: "Spark" or "Hive". Defaults to "Spark".
    initial_capacity : Dict[str, str], optional
        The capacity to initialize when the application is created.
    maximum_capacity : Dict[str, str], optional
        The maximum capacity to allocate when the application is created.
        This is cumulative across all workers at any given point in time,
        not just when an application is created. No new resources will
        be created once any one of the defined limits is hit.
    tags : Dict[str, str], optional
        Key/Value collection to put tags on the application.
        e.g. {"foo": "boo", "bar": "xoo"})
    autostart : bool, optional
        Enables the application to automatically start on job submission. Defaults to true.
    autostop : bool, optional
        Enables the application to automatically stop after a certain amount of time being idle. Defaults to true.
    idle_timeout : int, optional
        The amount of idle time in minutes after which your application will automatically stop. Defaults to 15 minutes.
    network_configuration : Dict[str, str], optional
        The network configuration for customer VPC connectivity.
    architecture : str, optional
        The CPU architecture of an application: "ARM64" or "X86_64". Defaults to "X86_64".
    image_uri : str, optional
        The URI of an image in the Amazon ECR registry.
    worker_type_specifications : Dict[str, str], optional
        The key-value pairs that specify worker type.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Application Id.
    """
    emr_serverless = _utils.client(service_name="emr-serverless", session=boto3_session)
    application_args: dict[str, Any] = {
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
    response: dict[str, str] = emr_serverless.create_application(**application_args)  # type: ignore[assignment]
    _logger.debug("response: \n%s", pprint.pformat(response))
    return response["applicationId"]


@Experimental
@apply_configs
def run_job(
    application_id: str,
    execution_role_arn: str,
    job_driver_args: dict[str, Any] | SparkSubmitJobArgs | HiveRunJobArgs,
    job_type: Literal["Spark", "Hive"] = "Spark",
    wait: bool = True,
    configuration_overrides: dict[str, Any] | None = None,
    tags: dict[str, str] | None = None,
    execution_timeout: int | None = None,
    name: str | None = None,
    emr_serverless_job_wait_polling_delay: float = _EMR_SERVERLESS_JOB_WAIT_POLLING_DELAY,
    boto3_session: boto3.Session | None = None,
) -> str | dict[str, Any]:
    """
    Run an EMR serverless job.

    https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html

    Parameters
    ----------
    application_id : str
        The id of the application on which to run the job.
    execution_role_arn : str
        The execution role ARN for the job run.
    job_driver_args : Union[Dict[str, str], SparkSubmitJobArgs, HiveRunJobArgs]
        The job driver arguments for the job run.
    job_type : str, optional
        Type of the job: "Spark" or "Hive". Defaults to "Spark".
    wait : bool, optional
        Whether to wait for the job completion or not. Defaults to true.
    configuration_overrides : Dict[str, str], optional
        The configuration overrides for the job run.
    tags : Dict[str, str], optional
        Key/Value collection to put tags on the application.
        e.g. {"foo": "boo", "bar": "xoo"})
    execution_timeout : int, optional
        The maximum duration for the job run to run. If the job run runs beyond this duration,
        it will be automatically cancelled.
    name : str, optional
        Name of the job.
    emr_serverless_job_wait_polling_delay : int, optional
        Time to wait between polling attempts.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Union[str, Dict[str, Any]]
        Job Id if wait=False, or job run details.
    """
    emr_serverless = _utils.client(service_name="emr-serverless", session=boto3_session)
    job_args: dict[str, Any] = {
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
        raise exceptions.InvalidArgumentValue(f"Unsupported job type `{job_type}`")

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


@Experimental
@apply_configs
def wait_job(
    application_id: str,
    job_run_id: str,
    emr_serverless_job_wait_polling_delay: float = _EMR_SERVERLESS_JOB_WAIT_POLLING_DELAY,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """
    Wait for the EMR Serverless job to finish.

    https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html

    Parameters
    ----------
    application_id : str
        The id of the application on which the job is running.
    job_run_id : str
        The id of the job.
    emr_serverless_job_wait_polling_delay : int, optional
        Time to wait between polling attempts.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Job run details.
    """
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
