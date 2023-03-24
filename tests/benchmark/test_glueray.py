import time
import uuid
from typing import Any, Dict, Iterable

import boto3
import pytest

from .._utils import ExecutionTimer


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
def glue_job(
    request: Any,
    path: str,
    wrangler_zip_location: str,
    glue_job_role_arn: str,
) -> Iterable[str]:
    glue_script_name = request.param
    session = boto3.session.Session()

    s3_client = session.client("s3")
    glue_client = session.client("glue")

    script_path = f"{path}script.py"

    bucket, key = tuple(script_path[len("s3://") :].split("/", 1))
    s3_client.upload_file(
        f"tests/glue_scripts/{glue_script_name}.py",
        bucket,
        key,
    )

    glue_job_name = f"{glue_script_name}_{uuid.uuid4()}"
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
            "--auto-scaling-ray-min-workers": "2",
        },
        GlueVersion="4.0",
        WorkerType="Z.2X",
        NumberOfWorkers=5,
    )

    yield glue_job_name

    glue_client.delete_job(JobName=glue_job_name)


def run_glue_job_get_status(job_name: str, arguments: Dict[str, str] = {}, num_workers: int = 2) -> str:
    session = boto3.session.Session()
    glue_client = session.client("glue")
    job_run_id = glue_client.start_job_run(
        JobName=job_name,
        Arguments=arguments,
        NumberOfWorkers=num_workers,
        WorkerType="Z.2X",
    )

    while True:
        status_detail = glue_client.get_job_run(JobName=job_name, RunId=job_run_id.get("JobRunId"))
        job_run = status_detail.get("JobRun")

        status: str = job_run.get("JobRunState")
        if "CompletedOn" in job_run:
            return status

        time.sleep(5)


@pytest.mark.timeout(300)
@pytest.mark.parametrize("glue_job", ["wrangler_blog_simple"], indirect=True)
def test_blog_simple(
    path: str,
    glue_table: str,
    glue_database: str,
    glue_ray_athena_workgroup_name: str,
    glue_job: str,
) -> None:
    state = run_glue_job_get_status(
        job_name=glue_job,
        arguments={
            "--output-path": path,
            "--glue-database": glue_database,
            "--glue-table": glue_table,
            "--athena-workgroup": glue_ray_athena_workgroup_name,
        },
    )
    assert state == "SUCCEEDED"


@pytest.mark.parametrize("glue_job", ["ray_read_parquet", "wrangler_read_parquet"], indirect=True)
def test_read_benchmark(data_gen_bucket: str, glue_job: str, request: pytest.FixtureRequest) -> None:
    with ExecutionTimer(request):
        state = run_glue_job_get_status(
            job_name=glue_job,
            arguments={
                "--data-gen-bucket": data_gen_bucket,
                "--auto-scaling-ray-min-workers": "10",
            },
            num_workers=10,
        )
    assert state == "SUCCEEDED"
