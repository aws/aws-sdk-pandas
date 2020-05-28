import logging
import time

import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import CFN_VALID_STATUS, get_time_str_with_random_suffix

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler")
    stack = [x for x in response.get("Stacks") if x["StackStatus"] in CFN_VALID_STATUS][0]
    outputs = {}
    for output in stack.get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    yield cloudformation_outputs["BucketName"]


@pytest.fixture(scope="function")
def path(bucket):
    s3_path = f"s3://{bucket}/{get_time_str_with_random_suffix()}/"
    print(f"S3 Path: {s3_path}")
    time.sleep(1)
    objs = wr.s3.list_objects(s3_path)
    wr.s3.delete_objects(path=objs)
    wr.s3.wait_objects_not_exist(objs)
    yield s3_path
    time.sleep(1)
    objs = wr.s3.list_objects(s3_path)
    wr.s3.delete_objects(path=objs)
    wr.s3.wait_objects_not_exist(objs)


def test_read_parquet_filter_partitions(path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    paths = wr.s3.to_parquet(df, path, dataset=True, partition_cols=["c1", "c2"])["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.s3.read_parquet(path, dataset=True, filters=[("c1", "==", "0")])
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 0
    assert df2.c1.iloc[0] == 0
    assert df2.c2.iloc[0] == 0
    df2 = wr.s3.read_parquet(path, dataset=True, filters=[("c1", "==", "1"), ("c2", "==", "0")])
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 1
    assert df2.c1.iloc[0] == 1
    assert df2.c2.iloc[0] == 0
    df2 = wr.s3.read_parquet(path, dataset=True, filters=[("c2", "==", "0")])
    assert df2.shape == (2, 3)
    assert df2.c0.astype(int).sum() == 1
    assert df2.c1.astype(int).sum() == 1
    assert df2.c2.astype(int).sum() == 0
