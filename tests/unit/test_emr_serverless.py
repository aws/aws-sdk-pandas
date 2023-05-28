import logging

import boto3
import pytest

import awswrangler as wr

from .._utils import _get_unique_suffix

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("application_type", ["Spark", "Hive"])
@pytest.mark.parametrize("release_label", ["emr-6.10.0"])
def test_application(application_type, release_label):
    application_name: str = f"test_app_{_get_unique_suffix()}"
    try:
        application_id: str = wr.emr_serverless.create_application(
            name=application_name,
            application_type=application_type,
            release_label=release_label,
        )
    finally:
        emr_serverless = boto3.client(service_name="emr-serverless")
        emr_serverless.delete_application(applicationId=application_id)
