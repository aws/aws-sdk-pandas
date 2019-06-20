import os
from time import sleep

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
    "mode, file_format, preserve_index, partition_cols, procs_cpu_bound, factor",
    [
        ("overwrite", "csv", False, [], 1, 1),
        ("append", "csv", False, [], 1, 2),
        ("overwrite_partitions", "csv", False, [], 1, 1),
        ("overwrite", "csv", True, [], 1, 1),
        ("append", "csv", True, [], 1, 2),
        ("overwrite_partitions", "csv", True, [], 1, 1),
        ("overwrite", "csv", False, [], 10, 1),
        ("append", "csv", False, [], 10, 2),
        ("overwrite_partitions", "csv", False, [], 10, 1),
        ("overwrite", "csv", True, [], 10, 1),
        ("append", "csv", True, [], 10, 2),
        ("overwrite_partitions", "csv", True, [], 10, 1),
        ("overwrite", "csv", False, ["date"], 1, 1),
        ("append", "csv", False, ["date"], 1, 2),
        ("overwrite_partitions", "csv", False, ["date"], 1, 1),
        ("overwrite", "csv", True, ["date"], 1, 1),
        ("append", "csv", True, ["date"], 1, 2),
        ("overwrite_partitions", "csv", True, ["date"], 1, 1),
        ("overwrite", "csv", False, ["date"], 10, 1),
        ("append", "csv", False, ["date"], 10, 2),
        ("overwrite_partitions", "csv", False, ["date"], 10, 1),
        ("overwrite", "csv", True, ["date"], 10, 1),
        ("append", "csv", True, ["date"], 10, 2),
        ("overwrite_partitions", "csv", True, ["date"], 10, 1),
        ("overwrite", "csv", False, ["name", "date"], 1, 1),
        ("append", "csv", False, ["name", "date"], 1, 2),
        ("overwrite_partitions", "csv", False, ["name", "date"], 1, 1),
        ("overwrite", "csv", True, ["name", "date"], 1, 1),
        ("append", "csv", True, ["name", "date"], 1, 2),
        ("overwrite_partitions", "csv", True, ["name", "date"], 1, 1),
        ("overwrite", "csv", False, ["name", "date"], 10, 1),
        ("append", "csv", False, ["name", "date"], 10, 2),
        ("overwrite_partitions", "csv", False, ["name", "date"], 10, 1),
        ("overwrite", "csv", True, ["name", "date"], 10, 1),
        ("append", "csv", True, ["name", "date"], 10, 2),
        ("overwrite_partitions", "csv", True, ["name", "date"], 2, 1),
        ("overwrite", "parquet", False, [], 1, 1),
        ("append", "parquet", False, [], 1, 2),
        ("overwrite_partitions", "parquet", False, [], 1, 1),
        ("overwrite", "parquet", True, [], 1, 1),
        ("append", "parquet", True, [], 1, 2),
        ("overwrite_partitions", "parquet", True, [], 1, 1),
        ("overwrite", "parquet", False, [], 10, 1),
        ("append", "parquet", False, [], 10, 2),
        ("overwrite_partitions", "parquet", False, [], 10, 1),
        ("overwrite", "parquet", True, [], 10, 1),
        ("append", "parquet", True, [], 10, 2),
        ("overwrite_partitions", "parquet", True, [], 10, 1),
        ("overwrite", "parquet", False, ["date"], 1, 1),
        ("append", "parquet", False, ["date"], 1, 2),
        ("overwrite_partitions", "parquet", False, ["date"], 1, 1),
        ("overwrite", "parquet", True, ["date"], 1, 1),
        ("append", "parquet", True, ["date"], 1, 2),
        ("overwrite_partitions", "parquet", True, ["date"], 1, 1),
        ("overwrite", "parquet", False, ["date"], 10, 1),
        ("append", "parquet", False, ["date"], 10, 2),
        ("overwrite_partitions", "parquet", False, ["date"], 10, 1),
        ("overwrite", "parquet", True, ["date"], 10, 1),
        ("append", "parquet", True, ["date"], 10, 2),
        ("overwrite_partitions", "parquet", True, ["date"], 10, 1),
        ("overwrite", "parquet", False, ["name", "date"], 1, 1),
        ("append", "parquet", False, ["name", "date"], 1, 2),
        ("overwrite_partitions", "parquet", False, ["name", "date"], 1, 1),
        ("overwrite", "parquet", True, ["name", "date"], 1, 1),
        ("append", "parquet", True, ["name", "date"], 1, 2),
        ("overwrite_partitions", "parquet", True, ["name", "date"], 1, 1),
        ("overwrite", "parquet", False, ["name", "date"], 10, 1),
        ("append", "parquet", False, ["name", "date"], 10, 2),
        ("overwrite_partitions", "parquet", False, ["name", "date"], 10, 1),
        ("overwrite", "parquet", True, ["name", "date"], 10, 1),
        ("append", "parquet", True, ["name", "date"], 10, 2),
        ("overwrite_partitions", "parquet", True, ["name", "date"], 10, 1),
    ],
)
def test_to_s3(
    session,
    bucket,
    database,
    mode,
    file_format,
    preserve_index,
    partition_cols,
    procs_cpu_bound,
    factor,
):
    dataframe = pandas.read_csv("data_samples/micro.csv")
    func = session.pandas.to_csv if file_format == "csv" else session.pandas.to_parquet
    objects_paths = func(
        dataframe=dataframe,
        database=database,
        path=f"s3://{bucket}/test/",
        preserve_index=preserve_index,
        mode=mode,
        partition_cols=partition_cols,
        procs_cpu_bound=procs_cpu_bound,
    )
    num_partitions = (
        len([keys for keys in dataframe.groupby(partition_cols)])
        if partition_cols
        else 1
    )
    assert len(objects_paths) >= num_partitions
    dataframe2 = None
    for counter in range(10):
        dataframe2 = session.pandas.read_sql_athena(
            sql="select * from test", database=database
        )
        if factor * len(dataframe.index) == len(dataframe2.index):
            break
        sleep(1)
    assert factor * len(dataframe.index) == len(dataframe2.index)
