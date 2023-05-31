import logging
import time
from typing import Any, Dict, Literal

import boto3
import pytest

import awswrangler as wr

from .._utils import _get_unique_suffix

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def _get_emr_serverless_application(
    application_type: Literal["Spark", "Hive"],
    release_label: str = "emr-6.10.0",
):
    """Create and tear down an EMR Serverless application."""
    application_name: str = f"test_app_{_get_unique_suffix()}"
    try:
        application_id: str = wr.emr_serverless.create_application(
            name=application_name,
            application_type=application_type,
            release_label=release_label,
        )
        yield application_id
    finally:
        emr_serverless = boto3.client(service_name="emr-serverless")
        emr_serverless.stop_application(applicationId=application_id)
        time.sleep(30)
        emr_serverless.delete_application(applicationId=application_id)


@pytest.fixture(scope="session")
def emr_serverless_spark_application_id() -> str:
    yield from _get_emr_serverless_application(application_type="Spark")


@pytest.fixture(scope="session")
def emr_serverless_hive_application_id() -> str:
    yield from _get_emr_serverless_application(application_type="Hive")


@pytest.mark.parametrize("application_type", ["Spark", "Hive"])
@pytest.mark.parametrize("release_label", ["emr-6.9.0", "emr-6.10.0"])
def test_create_application(application_type, release_label):
    application_id: str = _get_emr_serverless_application(
        application_type=application_type, release_label=release_label
    )

    assert application_id


@pytest.mark.parametrize(
    "job_type,job_driver_args",
    [
        (
            "Spark",
            {
                "entryPoint": "/usr/lib/spark/examples/jars/spark-examples.jar",
                "entryPointArguments": ["1"],
                "sparkSubmitParameters": "--class org.apache.spark.examples.SparkPi --conf spark.executor.cores=4 --conf spark.executor.memory=20g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=1",
            },
        ),
        (
            "Hive",
            {
                "query": "$PATHhive-query.ql",
                "parameters": "--hiveconf hive.exec.scratchdir=$PATHscratch --hiveconf hive.metastore.warehouse.dir=$PATHwarehouse",
            },
        ),
    ],
)
def test_run_job(
    emr_serverless_spark_application_id,
    emr_serverless_hive_application_id,
    emr_serverless_execution_role_arn,
    job_type,
    job_driver_args,
    path,
):
    if job_type == "Hive":
        # Replace path with temporary path from a fixture
        job_driver_args["query"] = job_driver_args["query"].replace("$PATH", path)
        job_driver_args["parameters"] = job_driver_args["parameters"].replace("$PATH", path)
        # Put a file containing SQL query to run into the temp path
        client_s3 = boto3.client("s3")
        bucket, key = wr._utils.parse_path(job_driver_args["query"])
        client_s3.put_object(Body="SELECT 1", Bucket=bucket, Key=key)

    job_run: Dict[str, Any] = wr.emr_serverless.run_job(
        application_id=emr_serverless_spark_application_id
        if job_type == "Spark"
        else emr_serverless_hive_application_id,
        execution_role_arn=emr_serverless_execution_role_arn,
        job_driver_args=job_driver_args,
        job_type=job_type,
        wait=True,
    )

    assert job_run["jobRun"]["state"] == "SUCCESS"
