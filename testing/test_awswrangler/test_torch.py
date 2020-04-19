import logging

import boto3
import pandas as pd
import pytest
import torch

import awswrangler as wr

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket


@pytest.fixture(scope="module")
def parameters(cloudformation_outputs):
    parameters = dict(postgresql={}, mysql={}, redshift={})
    parameters["postgresql"]["host"] = cloudformation_outputs["PostgresqlAddress"]
    parameters["postgresql"]["port"] = 3306
    parameters["postgresql"]["schema"] = "public"
    parameters["postgresql"]["database"] = "postgres"
    parameters["mysql"]["host"] = cloudformation_outputs["MysqlAddress"]
    parameters["mysql"]["port"] = 3306
    parameters["mysql"]["schema"] = "test"
    parameters["mysql"]["database"] = "test"
    parameters["redshift"]["host"] = cloudformation_outputs["RedshiftAddress"]
    parameters["redshift"]["port"] = cloudformation_outputs["RedshiftPort"]
    parameters["redshift"]["identifier"] = cloudformation_outputs["RedshiftIdentifier"]
    parameters["redshift"]["schema"] = "public"
    parameters["redshift"]["database"] = "test"
    parameters["redshift"]["role"] = cloudformation_outputs["RedshiftRole"]
    parameters["password"] = cloudformation_outputs["DatabasesPassword"]
    parameters["user"] = "test"
    yield parameters


@pytest.mark.parametrize("db_type, chunksize", [
    ("mysql", None),
    ("redshift", None),
    ("postgresql", None),
    ("mysql", 1),
    ("redshift", 1),
    ("postgresql", 1),
])
def test_torch_sql(parameters, db_type, chunksize):
    schema = parameters[db_type]["schema"]
    table = "test_torch_sql"
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-{db_type}")
    wr.db.to_sql(
        df=pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]}),
        con=engine,
        name=table,
        schema=schema,
        if_exists="replace",
        index=False,
        index_label=None,
        chunksize=None,
        method=None
    )
    ds = list(wr.torch.SQLDataset(f"SELECT * FROM {schema}.{table}", con=engine, chunksize=chunksize))
    assert torch.all(ds[0].eq(torch.tensor([1.0, 4.0])))
    assert torch.all(ds[1].eq(torch.tensor([2.0, 5.0])))
    assert torch.all(ds[2].eq(torch.tensor([3.0, 6.0])))


def test_torch_image_s3(bucket):
    s3 = boto3.client('s3')
    ref_label = 0
    s3.put_object(
        Body=open("../../docs/source/_static/logo.png"),
        Bucket=bucket,
        Key=f'class={ref_label}/logo.png',
    )
    ds = wr.torch.ImageS3Dataset()
    for image, label in ds:
        assert image.shape == torch.Size([1, 28, 28])
        assert label == torch.int(ref_label)
        break


# def test_torch_audio_s3(bucket):
#     ds = wr.torch.AudioS3Dataset()
#     for image, label in ds:
#         assert image.shape == torch.Size([1, 28, 28])
#         break