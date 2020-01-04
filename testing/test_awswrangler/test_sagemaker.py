import os
import pickle
import logging
import tarfile

import boto3
import pytest

import awswrangler as wr
from awswrangler import Session
from awswrangler.exceptions import InvalidSagemakerOutput
from sklearn.linear_model import LinearRegression

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def session():
    yield Session()


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test")
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
    with open("model", "wb") as fp:
        pickle.dump(lr, fp, pickle.HIGHEST_PROTOCOL)

    with tarfile.open("model.tar.gz", "w:gz") as tar:
        tar.add("model")

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).upload_file("model.tar.gz", model_path)

    try:
        os.remove("model")
    except OSError:
        pass
    try:
        os.remove("model.tar.gz")
    except OSError:
        pass

    yield f"s3://{bucket}/{model_path}"


@pytest.fixture(scope="module")
def model_empty(bucket):
    model_path = "output_empty/model.tar.gz"

    with tarfile.open("model.tar.gz", "w:gz"):
        pass

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).upload_file("model.tar.gz", model_path)

    try:
        os.remove("model.tar.gz")
    except OSError:
        pass

    yield f"s3://{bucket}/{model_path}"


@pytest.fixture(scope="module")
def model_double(bucket):
    model_path = "output_double/model.tar.gz"

    lr = LinearRegression()
    with open("model.pkl", "wb") as fp:
        pickle.dump(lr, fp, pickle.HIGHEST_PROTOCOL)

    with open("model2.pkl", "wb") as fp:
        pickle.dump(lr, fp, pickle.HIGHEST_PROTOCOL)

    with tarfile.open("model.tar.gz", "w:gz") as tar:
        tar.add("model.pkl")
        tar.add("model2.pkl")

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).upload_file("model.tar.gz", model_path)

    try:
        os.remove("model.pkl")
    except OSError:
        pass
    try:
        os.remove("model2.pkl")
    except OSError:
        pass
    try:
        os.remove("model.tar.gz")
    except OSError:
        pass

    yield f"s3://{bucket}/{model_path}"


@pytest.fixture(scope="module")
def model_dirty(bucket):
    model_path = "output/model.tar.gz"

    lr = LinearRegression()
    with open("model.pkl", "wb") as fp:
        pickle.dump(lr, fp, pickle.HIGHEST_PROTOCOL)
    with open("model", "wb") as fp:
        pickle.dump(lr, fp, pickle.HIGHEST_PROTOCOL)
    with open("test.txt", "w") as fp:
        fp.write("foo-boo-bar")
    with tarfile.open("model.tar.gz", "w:gz") as tar:
        tar.add("model.pkl")
        tar.add("model")
        tar.add("test.txt")

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).upload_file("model.tar.gz", model_path)

    try:
        os.remove("model.pkl")
    except OSError:
        pass
    try:
        os.remove("model")
    except OSError:
        pass
    try:
        os.remove("test.txt")
    except OSError:
        pass
    try:
        os.remove("model.tar.gz")
    except OSError:
        pass

    yield f"s3://{bucket}/{model_path}"


def test_get_job_outputs_by_path(session, model):
    outputs = session.sagemaker.get_job_outputs(path=model)
    assert type(list(outputs.values())[0]) == tarfile.ExFileObject


def test_get_model_empty(model_empty):
    with pytest.raises(InvalidSagemakerOutput):
        wr.sagemaker.get_job_outputs(path=model_empty)


def test_get_model_double(session, model_double):
    objs = session.sagemaker.get_job_outputs(path=model_double)
    assert type(objs["model.pkl"]) == tarfile.ExFileObject
    assert type(objs["model2.pkl"]) == tarfile.ExFileObject


def test_get_model_by_path(session, model):
    objs = session.sagemaker.get_job_outputs(path=model)
    print(objs)
    assert type(pickle.load(objs["model"])) == LinearRegression


def test_get_job_outputs_model_dirty(model_dirty):
    outputs = wr.sagemaker.get_job_outputs(path=model_dirty)
    assert type(pickle.load(outputs["model.pkl"])) == LinearRegression
    assert type(pickle.load(outputs["model"])) == LinearRegression
    assert outputs["test.txt"].read().decode("utf-8") == "foo-boo-bar"
