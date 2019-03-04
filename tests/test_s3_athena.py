import os
import pytest
import pandas as pd
import awswrangler


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
