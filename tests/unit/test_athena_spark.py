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
    [
        "print(spark)",
        """
input_path = "s3://athena-examples-us-east-1/notebooks/yellow_tripdata_2016-01.parquet"
output_path = "$PATH"

taxi_df = spark.read.format("parquet").load(input_path)

taxi_passenger_counts = taxi_df.groupBy("VendorID", "passenger_count").count()
taxi_passenger_counts.coalesce(1).write.mode('overwrite').csv(output_path)
        """,
    ],
)
def test_athena_spark_calculation(code, path, workgroup_spark):
    code = code.replace("$PATH", path)

    result = wr.athena.run_spark_calculation(
        code=code,
        workgroup=workgroup_spark,
    )

    assert result["Status"]["State"] == "COMPLETED"
