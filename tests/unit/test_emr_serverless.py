import logging
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
        emr_serverless.delete_application(applicationId=application_id)


@pytest.fixture(scope="session")
def emr_serverless_spark_application_id() -> str:
    yield from _get_emr_serverless_application(application_type="Spark")


@pytest.fixture(scope="session")
def emr_serverless_hive_application_id() -> str:
    yield from _get_emr_serverless_application(application_type="Hive")


@pytest.mark.parametrize("application_type", ["Spark", "Hive"])
@pytest.mark.parametrize("release_label", ["emr-6.10.0"])
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
                "query": "s3://DOC-EXAMPLE-BUCKET/emr-serverless-hive/query/hive-query.ql",
                "parameters": "--hiveconf hive.exec.scratchdir=$PATH/scratch --hiveconf hive.metastore.warehouse.dir=$PATH/warehouse",
            }
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
        job_driver_args["parameters"] = job_driver_args["parameters"].replace("$PATH", path)

    job_run: Dict[str, Any] = wr.emr_serverless.run_job(
        application_id=emr_serverless_spark_application_id
        if job_type == "Spark"
        else emr_serverless_hive_application_id,
        execution_role_arn=emr_serverless_execution_role_arn,
        job_driver_args=job_driver_args,
        job_type=job_type,
    )

    assert job_run["jobRun"]["state"] == "SUCCESS"
