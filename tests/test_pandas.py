import os

import pytest
import boto3
import pandas

from awswrangler import Session


@pytest.fixture(scope="module")
def session():
    yield Session()


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


def test_read_csv(session, bucket):
    boto3.client("s3").upload_file(
        "data_samples/small.csv", bucket, "data_samples/small.csv"
    )
    dataframe = session.pandas.read_csv(path=f"s3://{bucket}/data_samples/small.csv")
    assert len(dataframe.index) == 100


@pytest.mark.parametrize(
    "mode, file_format, preserve_index, partition_cols, num_procs, num_files, factor",
    [
        ("overwrite", "csv", True, [], 1, 1, 1),
        ("append", "csv", True, [], 1, 1, 2),
        ("append", "csv", True, [], 16, 16, 3),
        ("overwrite", "csv", False, ["date"], 4, 4, 1),
        ("overwrite", "csv", True, ["name", "date"], 4, 4, 1),
        ("overwrite_partitions", "csv", True, ["name", "date"], 4, 4, 1),
        ("append", "csv", True, ["name", "date"], 10, 10, 2),
        ("overwrite", "parquet", True, [], 1, 1, 1),
        ("append", "parquet", True, [], 1, 1, 2),
        ("append", "parquet", True, [], 16, 16, 3),
        ("overwrite", "parquet", False, ["date"], 4, 4, 1),
        ("overwrite", "parquet", True, ["name", "date"], 4, 4, 1),
        ("overwrite_partitions", "parquet", True, ["name", "date"], 4, 4, 1),
        ("append", "parquet", True, ["name", "date"], 10, 10, 2),
    ],
)
def test_to_s3_overwrite(
    session,
    bucket,
    database,
    mode,
    file_format,
    preserve_index,
    partition_cols,
    num_procs,
    num_files,
    factor,
):
    dataframe = pandas.read_csv("data_samples/micro.csv")
    func = session.pandas.to_csv if file_format == "csv" else session.pandas.to_parquet
    func(
        dataframe=dataframe,
        database=database,
        path=f"s3://{bucket}/test/",
        preserve_index=preserve_index,
        mode=mode,
        partition_cols=partition_cols,
        num_procs=num_procs,
        num_files=num_files,
    )
    dataframe2 = session.pandas.read_sql_athena(
        sql="select * from test", database=database
    )
    assert factor * len(dataframe.index) == len(dataframe2.index)
