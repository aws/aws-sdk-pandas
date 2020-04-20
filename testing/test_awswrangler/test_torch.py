import logging

import re
import boto3
import numpy as np
import pandas as pd
import pytest
import torch

from PIL import Image
from torch.utils.data import DataLoader
from torchvision.transforms.functional import to_tensor

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


@pytest.mark.parametrize("chunksize", [None, 1, 10])
@pytest.mark.parametrize("db_type", ["mysql", "redshift", "postgresql"])
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
        method=None,
    )
    ds = list(wr.torch.SQLDataset(f"SELECT * FROM {schema}.{table}", con=engine, chunksize=chunksize))
    assert torch.all(ds[0].eq(torch.tensor([1.0, 4.0])))
    assert torch.all(ds[1].eq(torch.tensor([2.0, 5.0])))
    assert torch.all(ds[2].eq(torch.tensor([3.0, 6.0])))


@pytest.mark.parametrize("chunksize", [None, 1, 10])
@pytest.mark.parametrize("db_type", ["mysql", "redshift", "postgresql"])
def test_torch_sql_label(parameters, db_type, chunksize):
    schema = parameters[db_type]["schema"]
    table = "test_torch_sql_label"
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-{db_type}")
    wr.db.to_sql(
        df=pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0], "c": [7, 8, 9]}),
        con=engine,
        name=table,
        schema=schema,
        if_exists="replace",
        index=False,
        index_label=None,
        chunksize=None,
        method=None,
    )
    ts = list(wr.torch.SQLDataset(f"SELECT * FROM {schema}.{table}", con=engine, chunksize=chunksize, label_col=2))
    assert torch.all(ts[0][0].eq(torch.tensor([1.0, 4.0])))
    assert torch.all(ts[0][1].eq(torch.tensor([7], dtype=torch.long)))
    assert torch.all(ts[1][0].eq(torch.tensor([2.0, 5.0])))
    assert torch.all(ts[1][1].eq(torch.tensor([8], dtype=torch.long)))
    assert torch.all(ts[2][0].eq(torch.tensor([3.0, 6.0])))
    assert torch.all(ts[2][1].eq(torch.tensor([9], dtype=torch.long)))


def test_torch_image_s3(bucket):
    wr.s3.delete_objects(path=bucket, boto3_session=boto3.Session())

    s3 = boto3.client("s3")
    ref_label = 0
    s3.put_object(
        Body=open("../../docs/source/_static/logo.png", "rb").read(),
        Bucket=bucket,
        Key=f"class={ref_label}/logo.png",
        ContentType="image/png",
    )
    ds = wr.torch.ImageS3Dataset(path=bucket, suffix="png", boto3_session=boto3.Session())
    image, label = ds[0]
    assert image.shape == torch.Size([4, 494, 1636])
    assert label == torch.tensor(ref_label, dtype=torch.int)

    wr.s3.delete_objects(path=bucket, boto3_session=boto3.Session())


def test_torch_image_s3_dataloader(bucket):
    wr.s3.delete_objects(path=bucket, boto3_session=boto3.Session())

    s3 = boto3.client("s3")
    labels = np.random.randint(0, 4, size=(8,))
    for i, label in enumerate(labels):
        s3.put_object(
            Body=open("../../docs/source/_static/logo.png", "rb").read(),
            Bucket=bucket,
            Key=f"class={label}/logo{i}.png",
            ContentType="image/png",
        )
    ds = wr.torch.ImageS3Dataset(path=bucket, suffix="png", boto3_session=boto3.Session())
    batch_size = 2
    num_train = len(ds)
    indices = list(range(num_train))
    loader = DataLoader(
        ds, batch_size=batch_size, num_workers=4, sampler=torch.utils.data.sampler.RandomSampler(indices)
    )
    for i, (image, label) in enumerate(loader):
        assert image.shape == torch.Size([batch_size, 4, 494, 1636])
        assert label.dtype == torch.int64

    wr.s3.delete_objects(path=bucket, boto3_session=boto3.Session())


def test_torch_lambda_s3(bucket):
    wr.s3.delete_objects(path=bucket, boto3_session=boto3.Session())

    s3 = boto3.client("s3")
    ref_label = 0
    s3.put_object(
        Body=open("../../docs/source/_static/logo.png", "rb").read(),
        Bucket=bucket,
        Key=f"class={ref_label}/logo.png",
        ContentType="image/png",
    )
    ds = wr.torch.LambdaS3Dataset(
        path=bucket,
        suffix="png",
        boto3_session=boto3.Session(),
        data_fn=lambda x: to_tensor(Image.open(x)),
        label_fn=lambda x: int(re.findall(r'/class=(.*?)/', x)[-1]),
    )
    image, label = ds[0]
    assert image.shape == torch.Size([4, 494, 1636])
    assert label == torch.tensor(ref_label, dtype=torch.int)

    wr.s3.delete_objects(path=bucket, boto3_session=boto3.Session())

# def test_torch_audio_s3(bucket):
#     ds = wr.torch.AudioS3Dataset()
#     for image, label in ds:
#         assert image.shape == torch.Size([1, 28, 28])
#         break
