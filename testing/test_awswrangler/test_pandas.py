from time import sleep
import logging

import pytest
import boto3
import pandas

from awswrangler import Session

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(
        StackName="aws-data-wrangler-test-arena")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    yield Session()


@pytest.fixture(scope="module")
def bucket(session, cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs.get("BucketName")
        session.s3.delete_objects(path=f"s3://{bucket}/")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield bucket
    session.s3.delete_objects(path=f"s3://{bucket}/")


@pytest.fixture(scope="module")
def database(cloudformation_outputs):
    if "GlueDatabaseName" in cloudformation_outputs:
        database = cloudformation_outputs.get("GlueDatabaseName")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield database


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30),
                                             ("data_samples/small.csv", 100)])
def test_read_csv(session, bucket, sample, row_num):
    boto3.client("s3").upload_file(sample, bucket, sample)
    path = f"s3://{bucket}/{sample}"
    dataframe = session.pandas.read_csv(path=path)
    session.s3.delete_objects(path=path)
    assert len(dataframe.index) == row_num


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30),
                                             ("data_samples/small.csv", 100)])
def test_read_csv_iterator(session, bucket, sample, row_num):
    boto3.client("s3").upload_file(sample, bucket, sample)
    path = f"s3://{bucket}/{sample}"
    dataframe_iter = session.pandas.read_csv(path=path, max_result_size=200)
    total_count = 0
    for dataframe in dataframe_iter:
        total_count += len(dataframe.index)
    session.s3.delete_objects(path=path)
    assert total_count == row_num


@pytest.mark.parametrize(
    "mode, file_format, preserve_index, partition_cols, procs_cpu_bound, factor",
    [
        ("overwrite", "csv", False, [], 1, 1),
        ("append", "csv", False, [], 1, 2),
        ("overwrite_partitions", "csv", False, [], 1, 1),
        ("overwrite", "csv", True, [], 1, 1),
        ("append", "csv", True, [], 1, 2),
        ("overwrite_partitions", "csv", True, [], 1, 1),
        ("overwrite", "csv", False, [], 5, 1),
        ("append", "csv", False, [], 5, 2),
        ("overwrite_partitions", "csv", False, [], 5, 1),
        ("overwrite", "csv", True, [], 5, 1),
        ("append", "csv", True, [], 5, 2),
        ("overwrite_partitions", "csv", True, [], 5, 1),
        ("overwrite", "csv", False, ["date"], 1, 1),
        ("append", "csv", False, ["date"], 1, 2),
        ("overwrite_partitions", "csv", False, ["date"], 1, 1),
        ("overwrite", "csv", True, ["date"], 1, 1),
        ("append", "csv", True, ["date"], 1, 2),
        ("overwrite_partitions", "csv", True, ["date"], 1, 1),
        ("overwrite", "csv", False, ["date"], 5, 1),
        ("append", "csv", False, ["date"], 5, 2),
        ("overwrite_partitions", "csv", False, ["date"], 5, 1),
        ("overwrite", "csv", True, ["date"], 5, 1),
        ("append", "csv", True, ["date"], 5, 2),
        ("overwrite_partitions", "csv", True, ["date"], 5, 1),
        ("overwrite", "csv", False, ["name", "date"], 1, 1),
        ("append", "csv", False, ["name", "date"], 1, 2),
        ("overwrite_partitions", "csv", False, ["name", "date"], 1, 1),
        ("overwrite", "csv", True, ["name", "date"], 1, 1),
        ("append", "csv", True, ["name", "date"], 1, 2),
        ("overwrite_partitions", "csv", True, ["name", "date"], 1, 1),
        ("overwrite", "csv", False, ["name", "date"], 5, 1),
        ("append", "csv", False, ["name", "date"], 5, 2),
        ("overwrite_partitions", "csv", False, ["name", "date"], 5, 1),
        ("overwrite", "csv", True, ["name", "date"], 5, 1),
        ("append", "csv", True, ["name", "date"], 5, 2),
        ("overwrite_partitions", "csv", True, ["name", "date"], 2, 1),
        ("overwrite", "parquet", False, [], 1, 1),
        ("append", "parquet", False, [], 1, 2),
        ("overwrite_partitions", "parquet", False, [], 1, 1),
        ("overwrite", "parquet", True, [], 1, 1),
        ("append", "parquet", True, [], 1, 2),
        ("overwrite_partitions", "parquet", True, [], 1, 1),
        ("overwrite", "parquet", False, [], 5, 1),
        ("append", "parquet", False, [], 5, 2),
        ("overwrite_partitions", "parquet", False, [], 5, 1),
        ("overwrite", "parquet", True, [], 5, 1),
        ("append", "parquet", True, [], 5, 2),
        ("overwrite_partitions", "parquet", True, [], 5, 1),
        ("overwrite", "parquet", False, ["date"], 1, 1),
        ("append", "parquet", False, ["date"], 1, 2),
        ("overwrite_partitions", "parquet", False, ["date"], 1, 1),
        ("overwrite", "parquet", True, ["date"], 1, 1),
        ("append", "parquet", True, ["date"], 1, 2),
        ("overwrite_partitions", "parquet", True, ["date"], 1, 1),
        ("overwrite", "parquet", False, ["date"], 5, 1),
        ("append", "parquet", False, ["date"], 5, 2),
        ("overwrite_partitions", "parquet", False, ["date"], 5, 1),
        ("overwrite", "parquet", True, ["date"], 5, 1),
        ("append", "parquet", True, ["date"], 5, 2),
        ("overwrite_partitions", "parquet", True, ["date"], 5, 1),
        ("overwrite", "parquet", False, ["name", "date"], 1, 1),
        ("append", "parquet", False, ["name", "date"], 1, 2),
        ("overwrite_partitions", "parquet", False, ["name", "date"], 1, 1),
        ("overwrite", "parquet", True, ["name", "date"], 1, 1),
        ("append", "parquet", True, ["name", "date"], 1, 2),
        ("overwrite_partitions", "parquet", True, ["name", "date"], 1, 1),
        ("overwrite", "parquet", False, ["name", "date"], 5, 1),
        ("append", "parquet", False, ["name", "date"], 5, 2),
        ("overwrite_partitions", "parquet", False, ["name", "date"], 5, 1),
        ("overwrite", "parquet", True, ["name", "date"], 5, 1),
        ("append", "parquet", True, ["name", "date"], 5, 2),
        ("overwrite_partitions", "parquet", True, ["name", "date"], 5, 1),
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
    num_partitions = (len([keys for keys in dataframe.groupby(partition_cols)])
                      if partition_cols else 1)
    assert len(objects_paths) >= num_partitions
    dataframe2 = None
    for counter in range(10):
        dataframe2 = session.pandas.read_sql_athena(sql="select * from test",
                                                    database=database)
        if factor * len(dataframe.index) == len(dataframe2.index):
            break
        sleep(1)
    assert factor * len(dataframe.index) == len(dataframe2.index)


@pytest.mark.parametrize("sample, row_num, max_result_size",
                         [("data_samples/micro.csv", 30, 100),
                          ("data_samples/small.csv", 100, 100),
                          ("data_samples/micro.csv", 30, 500),
                          ("data_samples/small.csv", 100, 500),
                          ("data_samples/micro.csv", 30, 3000),
                          ("data_samples/small.csv", 100, 3000)])
def test_read_sql_athena_iterator(session, bucket, database, sample, row_num,
                                  max_result_size):
    dataframe_sample = pandas.read_csv(sample)
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=dataframe_sample,
                              database=database,
                              path=path,
                              preserve_index=False,
                              mode="overwrite")
    total_count = 0
    for counter in range(10):
        dataframe_iter = session.pandas.read_sql_athena(
            sql="select * from test",
            database=database,
            max_result_size=max_result_size)
        total_count = 0
        for dataframe in dataframe_iter:
            total_count += len(dataframe.index)
        if total_count == row_num:
            break
        sleep(1)
    session.s3.delete_objects(path=path)
    assert total_count == row_num
