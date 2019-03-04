import os
import pytest
import pandas as pd
import awswrangler
from .utils import write_fake_objects


@pytest.fixture(scope="module")
def bucket():
    if "AWSWRANGLER_TEST_BUCKET" in os.environ:
        bucket = os.environ.get("AWSWRANGLER_TEST_BUCKET")
    else:
        raise Exception("You must provide AWSWRANGLER_TEST_BUCKET environment variable")
    yield bucket


@pytest.fixture(scope="module")
def database():
    if "AWSWRANGLER_TEST_DATABASE" in os.environ:
        database = os.environ.get("AWSWRANGLER_TEST_DATABASE")
    else:
        raise Exception(
            "You must provide AWSWRANGLER_TEST_DATABASE environment variable"
        )
    yield database


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
def test_s3_write(bucket, database, file_format):
    df = pd.read_csv("data_samples/micro.csv")
    awswrangler.s3.write(
        df=df,
        database=database,
        path="s3://{}/test/".format(bucket),
        file_format=file_format,
        preserve_index=True,
        mode="overwrite",
    )
    df2 = awswrangler.athena.read(database, "select * from test")
    assert len(df.index) == len(df2.index)


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
def test_s3_write_single(bucket, database, file_format):
    df = pd.read_csv("data_samples/micro.csv")
    awswrangler.s3.write(
        df=df,
        database=database,
        path="s3://{}/test/".format(bucket),
        file_format=file_format,
        preserve_index=False,
        mode="overwrite",
        num_procs=1,
    )
    df2 = awswrangler.athena.read(database, "select * from test")
    assert len(df.index) == len(df2.index)


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
def test_s3_write_partitioned(bucket, database, file_format):
    df = pd.read_csv("data_samples/micro.csv")
    awswrangler.s3.write(
        df=df,
        database=database,
        path="s3://{}/test/".format(bucket),
        file_format=file_format,
        preserve_index=True,
        partition_cols=["date"],
        mode="overwrite",
    )
    df2 = awswrangler.athena.read(database, "select * from test")
    assert len(df.index) == len(df2.index)


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
def test_s3_write_partitioned_single(bucket, database, file_format):
    df = pd.read_csv("data_samples/micro.csv")
    awswrangler.s3.write(
        df=df,
        database=database,
        path="s3://{}/test/".format(bucket),
        file_format=file_format,
        preserve_index=False,
        partition_cols=["date"],
        mode="overwrite",
        num_procs=1,
    )
    df2 = awswrangler.athena.read(database, "select * from test")
    assert len(df.index) == len(df2.index)


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
def test_s3_write_multi_partitioned(bucket, database, file_format):
    df = pd.read_csv("data_samples/micro.csv")
    awswrangler.s3.write(
        df=df,
        database=database,
        path="s3://{}/test/".format(bucket),
        file_format=file_format,
        preserve_index=True,
        partition_cols=["name", "date"],
        mode="overwrite",
    )
    df2 = awswrangler.athena.read(database, "select * from test")
    assert len(df.index) == len(df2.index)


@pytest.mark.parametrize("file_format", ["parquet", "csv"])
def test_s3_write_append(bucket, database, file_format):
    df = pd.read_csv("data_samples/micro.csv")
    awswrangler.s3.write(
        df=df,
        database=database,
        table="test",
        path="s3://{}/test/".format(bucket),
        file_format=file_format,
        partition_cols=["name", "date"],
        mode="overwrite",
    )
    awswrangler.s3.write(
        df=df,
        database=database,
        table="test",
        path="s3://{}/test/".format(bucket),
        file_format=file_format,
        partition_cols=["name", "date"],
        mode="overwrite_partitions",
    )
    awswrangler.s3.write(
        df=df,
        database=database,
        table="test",
        path="s3://{}/test/".format(bucket),
        file_format=file_format,
        partition_cols=["name", "date"],
        mode="append",
    )
    df2 = awswrangler.athena.read(
        database, "select * from test", "s3://{}/athena/".format(bucket)
    )
    assert 2 * len(df.index) == len(df2.index)


def test_delete_objects(bucket):
    write_fake_objects(bucket, "objs/", 3)
    awswrangler.s3.utils.delete_objects("s3://" + bucket + "/objs/", batch_size=2)


def test_delete_listed_objects(bucket):
    write_fake_objects(bucket, "objs/", 3)
    keys = awswrangler.s3.utils.list_objects("s3://" + bucket + "/objs/", batch_size=2)
    assert len(keys) == 3
    awswrangler.s3.utils.delete_listed_objects(bucket, keys, batch_size=2)
    keys = awswrangler.s3.utils.list_objects("s3://" + bucket + "/objs/", batch_size=2)
    assert len(keys) == 0


def test_get_session():
    session = awswrangler.common.get_session()
    session.client("sts").get_caller_identity().get("Account")
    profile = session.profile_name
    key = session.get_credentials().access_key
    secret = session.get_credentials().secret_key
    region = session.region_name
    session = awswrangler.common.get_session(profile=profile)
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(region=region)
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(key=key, secret=secret)
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(
        session_primitives=awswrangler.common.SessionPrimitives()
    )
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(
        session_primitives=awswrangler.common.SessionPrimitives(profile=profile)
    )
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(
        session_primitives=awswrangler.common.SessionPrimitives(region=region)
    )
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(
        session_primitives=awswrangler.common.SessionPrimitives(key=key, secret=secret)
    )
    session.client("sts").get_caller_identity().get("Account")
