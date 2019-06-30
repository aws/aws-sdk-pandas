import logging

import pytest
import boto3
from pyspark.sql import SparkSession

from awswrangler import Session


logging.basicConfig(level=logging.INFO)
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(
        StackName="aws-data-wrangler-test-arena"
    )
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    yield Session(
        spark_session=SparkSession.builder.appName("AWS Wrangler Test").getOrCreate()
    )


@pytest.fixture(scope="module")
def bucket(session, cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs.get("BucketName")
        session.s3.delete_objects(path=f"s3://{bucket}/")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield bucket
    session.s3.delete_objects(path=f"s3://{bucket}/")


def test_read_csv(session, bucket):
    boto3.client("s3").upload_file(
        "data_samples/small.csv", bucket, "data_samples/small.csv"
    )
    path = f"s3://{bucket}/data_samples/small.csv"
    dataframe = session.spark.read_csv(path=path)
    assert dataframe.count() == 100
