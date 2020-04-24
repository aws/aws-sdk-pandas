import io
import logging
import re

import boto3
import numpy as np
import pandas as pd
import pytest
import torch
import torchaudio
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
    table = f"test_torch_sql_{db_type}_{str(chunksize).lower()}"
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
    table = f"test_torch_sql_label_{db_type}_{str(chunksize).lower()}"
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
    folder = "test_torch_image_s3"
    path = f"s3://{bucket}/{folder}/"
    wr.s3.delete_objects(path=path, boto3_session=boto3.Session())
    s3 = boto3.client("s3")
    ref_label = 0
    s3.put_object(
        Body=open("docs/source/_static/logo.png", "rb").read(),
        Bucket=bucket,
        Key=f"{folder}/class={ref_label}/logo.png",
        ContentType="image/png",
    )
    ds = wr.torch.ImageS3Dataset(path=path, suffix="png", boto3_session=boto3.Session())
    image, label = ds[0]
    assert image.shape == torch.Size([4, 494, 1636])
    assert label == torch.tensor(ref_label, dtype=torch.int)
    wr.s3.delete_objects(path=path)


@pytest.mark.parametrize("drop_last", [True, False])
def test_torch_image_s3_loader(bucket, drop_last):
    folder = f"test_torch_image_s3_loader_{str(drop_last).lower()}"
    path = f"s3://{bucket}/{folder}/"
    wr.s3.delete_objects(path=path)
    client_s3 = boto3.client("s3")
    labels = np.random.randint(0, 4, size=(8,))
    for i, label in enumerate(labels):
        client_s3.put_object(
            Body=open("./docs/source/_static/logo.png", "rb").read(),
            Bucket=bucket,
            Key=f"{folder}/class={label}/logo{i}.png",
            ContentType="image/png",
        )
    ds = wr.torch.ImageS3Dataset(path=path, suffix="png", boto3_session=boto3.Session())
    batch_size = 2
    num_train = len(ds)
    indices = list(range(num_train))
    loader = DataLoader(
        ds,
        batch_size=batch_size,
        num_workers=4,
        sampler=torch.utils.data.sampler.RandomSampler(indices),
        drop_last=drop_last,
    )
    for i, (image, label) in enumerate(loader):
        assert image.shape == torch.Size([batch_size, 4, 494, 1636])
        assert label.dtype == torch.int64
    wr.s3.delete_objects(path=path)


def test_torch_lambda_s3(bucket):
    path = f"s3://{bucket}/test_torch_lambda_s3/"
    wr.s3.delete_objects(path=path)
    s3 = boto3.client("s3")
    ref_label = 0
    s3.put_object(
        Body=open("./docs/source/_static/logo.png", "rb").read(),
        Bucket=bucket,
        Key=f"test_torch_lambda_s3/class={ref_label}/logo.png",
        ContentType="image/png",
    )
    ds = wr.torch.LambdaS3Dataset(
        path=path,
        suffix="png",
        boto3_session=boto3.Session(),
        data_fn=lambda x: to_tensor(Image.open(x)),
        label_fn=lambda x: int(re.findall(r"/class=(.*?)/", x)[-1]),
    )
    image, label = ds[0]
    assert image.shape == torch.Size([4, 494, 1636])
    assert label == torch.tensor(ref_label, dtype=torch.int)
    wr.s3.delete_objects(path=path)


def test_torch_audio_s3(bucket):
    size = (1, 8_000 * 5)
    audio = torch.randint(low=-25, high=25, size=size) / 100.0
    audio_file = "/tmp/amazing_sound.wav"
    torchaudio.save(audio_file, audio, 8_000)
    folder = "test_torch_audio_s3"
    path = f"s3://{bucket}/{folder}/"
    wr.s3.delete_objects(path=path)
    s3 = boto3.client("s3")
    ref_label = 0
    s3.put_object(
        Body=open(audio_file, "rb").read(),
        Bucket=bucket,
        Key=f"{folder}/class={ref_label}/amazing_sound.wav",
        ContentType="audio/wav",
    )
    s3_audio_file = f"{bucket}/test_torch_audio_s3/class={ref_label}/amazing_sound.wav"
    ds = wr.torch.AudioS3Dataset(path=s3_audio_file, suffix="wav")
    loader = DataLoader(ds, batch_size=1)
    for (audio, rate), label in loader:
        assert audio.shape == torch.Size((1, *size))
    wr.s3.delete_objects(path=path)


# def test_torch_s3_file_dataset(bucket):
#     cifar10 = "s3://fast-ai-imageclas/cifar10.tgz"
#     batch_size = 64
#     for image, label in DataLoader(
#         wr.torch.S3FilesDataset(cifar10),
#         batch_size=batch_size,
#     ):
#         assert image.shape == torch.Size([batch_size, 3, 32, 32])
#         assert label.dtype == torch.int64
#         break


@pytest.mark.parametrize("drop_last", [True, False])
def test_torch_s3_iterable(bucket, drop_last):
    folder = f"test_torch_s3_iterable_{str(drop_last).lower()}"
    path = f"s3://{bucket}/{folder}/"
    wr.s3.delete_objects(path=path)
    batch_size = 32
    client_s3 = boto3.client("s3")
    for i in range(3):
        batch = torch.randn(100, 3, 32, 32)
        buff = io.BytesIO()
        torch.save(batch, buff)
        buff.seek(0)
        client_s3.put_object(Body=buff.read(), Bucket=bucket, Key=f"{folder}/file{i}.pt")

    for image in DataLoader(
        wr.torch.S3IterableDataset(path=f"s3://{bucket}/{folder}/file"), batch_size=batch_size, drop_last=drop_last
    ):
        if drop_last:
            assert image.shape == torch.Size([batch_size, 3, 32, 32])
        else:
            assert image[0].shape == torch.Size([3, 32, 32])

    wr.s3.delete_objects(path=path)


@pytest.mark.parametrize("drop_last", [True, False])
def test_torch_s3_iterable_with_labels(bucket, drop_last):
    folder = f"test_torch_s3_iterable_with_labels_{str(drop_last).lower()}"
    path = f"s3://{bucket}/{folder}/"
    wr.s3.delete_objects(path=path)
    batch_size = 32
    client_s3 = boto3.client("s3")
    for i in range(3):
        batch = (torch.randn(100, 3, 32, 32), torch.randint(2, size=(100,)))
        buff = io.BytesIO()
        torch.save(batch, buff)
        buff.seek(0)
        client_s3.put_object(Body=buff.read(), Bucket=bucket, Key=f"{folder}/file{i}.pt")

    for images, labels in DataLoader(
        wr.torch.S3IterableDataset(path=f"s3://{bucket}/{folder}/file"), batch_size=batch_size, drop_last=drop_last
    ):
        if drop_last:
            assert images.shape == torch.Size([batch_size, 3, 32, 32])
            assert labels.dtype == torch.int64
            assert labels.shape == torch.Size([batch_size])

        else:
            assert images[0].shape == torch.Size([3, 32, 32])
            assert labels[0].dtype == torch.int64
            assert labels[0].shape == torch.Size([])

    wr.s3.delete_objects(path=path)
