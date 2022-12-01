import pytest
from typing import Dict

import boto3


@pytest.fixture(scope="session")
def glue_job1(cloudformation_outputs: Dict[str, str]) -> str:
    return cloudformation_outputs["GlueJob1Name"]


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


def test_glue_job_1(path: str, glue_table: str, glue_database: str, glue_job1: str) -> None:
    state = run_glue_job_get_status(
        job_name=glue_job1,
        arguments={
            "--output-path": path,
            "--glue-database": glue_database,
            "--glue-table": glue_table,
        }
    )
    assert state == "SUCCEEDED"
