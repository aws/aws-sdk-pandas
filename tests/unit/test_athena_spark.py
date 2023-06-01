import pytest

import awswrangler as wr
from tests._utils import create_workgroup


@pytest.fixture(scope="session")
def athena_spark_execution_role_arn(cloudformation_outputs):
    return cloudformation_outputs["AthenaSparkExecutionRole"]


@pytest.fixture(scope="session")
def workgroup_spark(bucket, kms_key, athena_spark_execution_role_arn):
    return create_workgroup(
        wkg_name="aws_sdk_pandas_spark",
        config={
            "EngineVersion": {
                "SelectedEngineVersion": "PySpark engine version 3",
            },
            "ExecutionRole": athena_spark_execution_role_arn,
            "ResultConfiguration": {"OutputLocation": f"s3://{bucket}/athena_workgroup_spark/"},
        },
    )


@pytest.mark.parametrize(
    "code",
    ["print(spark)"],
)
def test_athena_spark_calculation(code, path, workgroup_spark):
    result = wr.athena.run_spark_calculation(
        code=code,
        workgroup=workgroup_spark,
    )

    assert result["Status"]["State"] == "COMPLETED"
