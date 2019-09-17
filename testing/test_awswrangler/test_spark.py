import logging

import pytest
import boto3
from pyspark.sql import SparkSession

from awswrangler import Session

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(
        StackName="aws-data-wrangler-test-arena")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    yield Session(spark_session=SparkSession.builder.appName(
        "AWS Wrangler Test").getOrCreate())


@pytest.fixture(scope="module")
def bucket(session, cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs.get("BucketName")
        session.s3.delete_objects(path=f"s3://{bucket}/")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield bucket
    session.s3.delete_objects(path=f"s3://{bucket}/")


@pytest.mark.parametrize(
    "sample_name",
    ["nano", "micro", "small"],
)
def test_read_csv(session, bucket, sample_name):
    path = f"data_samples/{sample_name}.csv"
    if sample_name == "micro":
        schema = "id SMALLINT, name STRING, value FLOAT, date TIMESTAMP"
        timestamp_format = "yyyy-MM-dd"
    elif sample_name == "small":
        schema = "id BIGINT, name STRING, date DATE"
        timestamp_format = "dd-MM-yy"
    elif sample_name == "nano":
        schema = "id INTEGER, name STRING, value DOUBLE, date TIMESTAMP, time TIMESTAMP"
        timestamp_format = "yyyy-MM-dd"
    dataframe = session.spark.read_csv(path=path,
                                       schema=schema,
                                       timestampFormat=timestamp_format,
                                       dateFormat=timestamp_format,
                                       header=True)

    boto3.client("s3").upload_file(path, bucket, path)
    path2 = f"s3://{bucket}/{path}"
    dataframe2 = session.spark.read_csv(path=path2,
                                        schema=schema,
                                        timestampFormat=timestamp_format,
                                        dateFormat=timestamp_format,
                                        header=True)
    assert dataframe.count() == dataframe2.count()
    assert len(list(dataframe.columns)) == len(list(dataframe2.columns))
