import concurrent.futures
import logging
import time

import boto3
import pandas as pd
import pytest

import awswrangler as wr
from awswrangler import _utils  # noqa

EVENTUAL_CONSISTENCY_SLEEP: float = 20.0  # seconds

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def wrt_fake_objs_batch(bucket, path, chunk, size=10):
    s3 = boto3.resource("s3")
    for obj_id in chunk:
        s3.Object(bucket, f"{path}{obj_id}").put(Body=str(obj_id).zfill(size).encode())


def write_fake_objects(bucket, path, num, obj_size=5):
    cpus = _utils.ensure_cpu_count(use_threads=True) * 4
    chunks = _utils.chunkify(list(range(num)), cpus)
    args = []
    for chunk in chunks:
        args.append((bucket, path, chunk, obj_size))
    with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
        executor.map(lambda x: wrt_fake_objs_batch(*x), args)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def region(cloudformation_outputs):
    if "Region" in cloudformation_outputs:
        region = cloudformation_outputs["Region"]
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)!")
    yield region


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
        wr.s3.delete_objects(f"s3://{bucket}/")
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket
    wr.s3.delete_objects(f"s3://{bucket}/")


def test_get_bucket_region(bucket, region):
    assert wr.s3.get_bucket_region(bucket=bucket) == region
    assert wr.s3.get_bucket_region(bucket=bucket, boto3_session=boto3.Session()) == region


def test_objects(bucket):
    num = 1_001
    prefix = f"test_objects/"
    path = f"s3://{bucket}/{prefix}"
    assert len(wr.s3.size_objects(path=path, use_threads=False)) == 0
    print("Starting writes...")
    write_fake_objects(bucket, prefix, num, obj_size=5)
    print("Getting sizes with wait_time...")
    sizes = wr.s3.size_objects(path=path, wait_time=EVENTUAL_CONSISTENCY_SLEEP, use_threads=True)
    for _, size in sizes.items():
        assert size == 5
    print("Waiting eventual consistency...")
    time.sleep(EVENTUAL_CONSISTENCY_SLEEP)
    print("checking existence...")
    assert wr.s3.does_object_exists(path=f"{path}0") is True
    assert wr.s3.does_object_exists(path=f"{path}0_wrong") is False
    assert wr.s3.does_object_exists(path=f"{path}0", boto3_session=boto3.Session()) is True
    assert wr.s3.does_object_exists(path=f"s3://{path}0_wrong", boto3_session=boto3.Session()) is False
    print("Listing...")
    paths = wr.s3.list_objects(path)
    assert len(paths) == num
    print("Getting sizes...")
    sizes = wr.s3.size_objects(path=path, use_threads=True)
    assert len(sizes) == num
    for _, size in sizes.items():
        assert size == 5
    print("Deleting...")
    wr.s3.delete_objects(path=path, use_threads=True)
    print("Waiting eventual consistency...")
    time.sleep(EVENTUAL_CONSISTENCY_SLEEP)
    assert len(wr.s3.list_objects(path)) == 0
    wr.s3.delete_objects(path=[], use_threads=True)
    key = f"test_objects.txt"
    boto3.resource("s3").Object(bucket, key).put(Body=key.encode())
    assert len(wr.s3.size_objects(path=[f"s3://{bucket}/{key}"], use_threads=False)) == 1
    wr.s3.delete_objects(path=[f"s3://{bucket}/{key}"], use_threads=False)
    with pytest.raises(wr.exceptions.InvalidArgumentType):
        wr.s3.delete_objects(path=1, use_threads=True)


def test_csv(bucket):
    df = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"s3://{bucket}/test_csv0.csv"
    path1 = f"s3://{bucket}/test_csv1.csv"
    path2 = f"s3://{bucket}/test_csv2.csv"
    wr.s3.to_csv(df=df, path=path0, index=False)
    wr.s3.to_csv(df=df, path=path1, index=False, boto3_session=None)
    wr.s3.to_csv(df=df, path=path2, index=False, boto3_session=boto3.Session())
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=False))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=True))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=False, boto3_session=boto3.Session()))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=True, boto3_session=boto3.Session()))
    paths = [path0, path1, path2]
    df2 = pd.concat(objs=[df, df, df], sort=False, ignore_index=True)
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=False))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=True))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=False, boto3_session=boto3.Session()))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=True, boto3_session=boto3.Session()))
    with pytest.raises(wr.exceptions.InvalidArgumentType):
        wr.s3.read_csv(path=1)


def test_parquet(bucket):
    df_file = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"s3://{bucket}/test_to_parquet_file.parquet"
    df_dataset = pd.DataFrame({"id": [1, 2, 3], "partition": ["A", "A", "B"]})
    df_dataset["partition"] = df_dataset["partition"].astype("category")
    path_dataset = f"s3://{bucket}/test_to_parquet_dataset"
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df_file, path=path_file, mode="append")
    with pytest.raises(wr.exceptions.InvalidCompression):
        wr.s3.to_parquet(df=df_file, path=path_file, compression="WRONG")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df_dataset, path=path_dataset, partition_cols=["col2"])
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_parquet(df=df_dataset, path=path_dataset, partition_cols=["col2"], dataset=True, mode="WRONG")
    wr.s3.to_parquet(df=df_file, path=path_file)
    time.sleep(EVENTUAL_CONSISTENCY_SLEEP)
    assert df_file.equals(wr.s3.read_parquet(path=path_file, use_threads=True, boto3_session=None))
    assert df_file.equals(wr.s3.read_parquet(path=[path_file], use_threads=False, boto3_session=boto3.Session()))
    wr.s3.to_parquet(df=df_dataset, path=path_dataset, dataset=True)
    time.sleep(EVENTUAL_CONSISTENCY_SLEEP)
    assert df_dataset.equals(wr.s3.read_parquet(path=path_dataset, use_threads=True, boto3_session=boto3.Session()))
    dataset_paths = wr.s3.to_parquet(
        df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite"
    )
    time.sleep(EVENTUAL_CONSISTENCY_SLEEP)
    assert df_file.equals(wr.s3.read_parquet(path=path_dataset, use_threads=True, boto3_session=None))
    assert df_file.equals(wr.s3.read_parquet(path=dataset_paths, use_threads=True))
    assert df_dataset.equals(wr.s3.read_parquet(path=path_dataset, dataset=True, use_threads=True).sort_values("id"))
    wr.s3.to_parquet(df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite")
    wr.s3.to_parquet(
        df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="partitions_upsert"
    )
