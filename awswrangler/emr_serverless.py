"""EMR Serverless module."""

import logging
import pprint
from typing import Any, Dict, Literal, Optional

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


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
