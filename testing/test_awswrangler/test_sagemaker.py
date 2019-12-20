import os
import pickle
import logging
import tarfile

import boto3
import pytest

from awswrangler import Session
from sklearn.linear_model import LinearRegression

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def session():
    yield Session()


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test-arena")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def bucket(session, cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
        session.s3.delete_objects(path=f"s3://{bucket}/")
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield bucket
    session.s3.delete_objects(path=f"s3://{bucket}/")


@pytest.fixture(scope="module")
def model(bucket):
    model_path = "output/model.tar.gz"

    lr = LinearRegression()
    with open("model.pkl", "wb") as fp:
        pickle.dump(lr, fp, pickle.HIGHEST_PROTOCOL)

    with tarfile.open("model.tar.gz", "w:gz") as tar:
        tar.add("model.pkl")

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).upload_file("model.tar.gz", model_path)

    yield f"s3://{bucket}/{model_path}"

    os.remove("model.pkl")
    os.remove("model.tar.gz")


def test_get_job_outputs_by_path(session, model):
    outputs = session.sagemaker.get_job_outputs(path=model)
    assert type(outputs[0]) == LinearRegression


def test_get_job_outputs_by_job_id(session, bucket):
    pass


def test_get_job_outputs_empty(session, bucket):
    pass
