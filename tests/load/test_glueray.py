import time
import uuid
from typing import Dict, Iterable

import boto3
import pytest


@pytest.fixture(scope="session")
def wrangler_zip_location(cloudformation_outputs: Dict[str, str]) -> str:
    return cloudformation_outputs["AWSSDKforpandasZIPLocation"]


@pytest.fixture(scope="session")
def glue_job_role_arn(cloudformation_outputs: Dict[str, str]) -> str:
    return cloudformation_outputs["GlueJobRoleArn"]


@pytest.fixture(scope="session")
def glue_ray_athena_workgroup_name(cloudformation_outputs: Dict[str, str]) -> str:
    return cloudformation_outputs["GlueRayAthenaWorkgroupName"]


@pytest.fixture(scope="function")
def glue_job1(
    path: str,
    wrangler_zip_location: str,
    glue_job_role_arn: str,
    glue_ray_athena_workgroup_name: str,
) -> Iterable[str]:
    session = boto3.session.Session()

    s3_client = session.client("s3")
    glue_client = session.client("glue")

    script_path = f"{path}script.py"

    bucket, key = tuple(script_path[len("s3://") :].split("/", 1))
    s3_client.upload_file(
        "test_infra/glue_scripts/glue_example_1.py",
        bucket,
        key,
    )

    glue_job_name = f"GlueJob1_{uuid.uuid4()}"
    glue_client.create_job(
        Name=glue_job_name,
        Role=glue_job_role_arn,
        Command={
            "Name": "glueray",
            "PythonVersion": "3.9",
            "ScriptLocation": script_path,
        },
        DefaultArguments={
            "--additional-python-modules": wrangler_zip_location,
            "--auto-scaling-ray-min-workers": "5",
            "--athena-workgroup": glue_ray_athena_workgroup_name,
        },
        GlueVersion="4.0",
        WorkerType="Z.2X",
        NumberOfWorkers=5,
    )

    yield glue_job_name

    glue_client.delete_job(JobName=glue_job_name)


def run_glue_job_get_status(job_name: str, arguments: Dict[str, str] = {}) -> str:
    session = boto3.session.Session()
    glue_client = session.client("glue")
    job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)

    while True:
        status_detail = glue_client.get_job_run(JobName=job_name, RunId=job_run_id.get("JobRunId"))
        job_run = status_detail.get("JobRun")

        status: str = job_run.get("JobRunState")
        if "CompletedOn" in job_run:
            return status

        time.sleep(5)


@pytest.mark.skip(reason="Skipping until we make the required change to CodeBuild")
@pytest.mark.timeout(300)
def test_glue_job_1(path: str, glue_table: str, glue_database: str, glue_job1: str) -> None:
    state = run_glue_job_get_status(
        job_name=glue_job1,
        arguments={
            "--output-path": path,
            "--glue-database": glue_database,
            "--glue-table": glue_table,
        },
    )
    assert state == "SUCCEEDED"
