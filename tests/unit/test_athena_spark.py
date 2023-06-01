import pandas as pd
import pytest

import awswrangler as wr

from tests._utils import create_workgroup


@pytest.fixture(scope="session")
def workgroup_spark(bucket, kms_key):
    """
    return create_workgroup(
        wkg_name="aws_sdk_pandas_spark",
        config={
            "EngineVersion": {
                "SelectedEngineVersion": "PySpark engine version 3",
            },
        },
    )
    """
    return "dummy-spark"


@pytest.mark.parametrize(
    "code",
    [
        "print(spark)",
        (
            "taxi_df = spark.read.format('parquet')"
            ".option('header', 'true')"
            ".option('inferSchema', 'true')"
            ".load('s3://athena-examples-us-east-1/notebooks/yellow_tripdata_2016-01.parquet')\n"
            "taxi_df.coalesce(1)"
            ".write.mode('overwrite')"
            ".csv('s3://<REDACTED>/select_taxi')"
        ),
    ],
)
def test_athena_spark_calculation(code, path, workgroup_spark):
    result = wr.athena.run_spark_calculation(
        code=code,
        workgroup=workgroup_spark,
    )
    assert type(result) in [str, dict, pd.DataFrame]
