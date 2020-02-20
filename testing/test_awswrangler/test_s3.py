import logging
import multiprocessing as mp
from time import sleep

import boto3
import pytest

import awswrangler as wr

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def wrt_fake_objs_batch(bucket, path, chunk, size=10):
    s3 = boto3.resource("s3")
    for obj_id in chunk:
        s3.Object(bucket, f"{path}{obj_id}").put(Body=str(obj_id).zfill(size))


def wrt_fake_objs_batch_wrapper(args):
    return wrt_fake_objs_batch(*args)


def write_fake_objects(bucket, path, num, obj_size=3):
    cpus = mp.cpu_count()
    chunks = wr.utils.chunkify(list(range(num)), cpus)
    args = []
    for chunk in chunks:
        args.append((bucket, path, chunk, obj_size))
    pool = mp.Pool(processes=cpus)
    print("Starting parallel writes...")
    pool.map(wrt_fake_objs_batch_wrapper, args)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(
        StackName="aws-data-wrangler-test")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def region(cloudformation_outputs):
    if "Region" in cloudformation_outputs:
        region = cloudformation_outputs["Region"]
    else:
        raise Exception(
            "You must deploy/update the test infrastructure (CloudFormation)!")
    yield region


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
    else:
        raise Exception(
            "You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket


def test_does_object_exists(bucket):
    key = "test_does_object_exists"
    boto3.resource("s3").Object(bucket, key).put(Body=key)
    assert wr.s3.does_object_exists(path=f"s3://{bucket}/{key}") is True
    assert wr.s3.does_object_exists(path=f"s3://{bucket}/{key}_wrong") is False
    session = wr.Session()
    assert session.s3.does_object_exists(path=f"s3://{bucket}/{key}") is True
    assert session.s3.does_object_exists(
        path=f"s3://{bucket}/{key}_wrong") is False


def test_wait_object_exists(bucket):
    key = "test_wait_object_exists"
    boto3.resource("s3").Object(bucket, key).put(Body=key)
    assert wr.s3.wait_object_exists(path=f"s3://{bucket}/{key}") is None
    assert wr.s3.wait_object_exists(
        path=f"s3://{bucket}/{key}", polling_sleep=0.05, timeout=10.0) is None
    with pytest.raises(wr.exceptions.S3WaitObjectTimeout):
        wr.s3.wait_object_exists(path=f"s3://{bucket}/{key}_wrong", timeout=2.0)


def test_parse_path():
    assert wr.s3.parse_path("s3://bucket/key") == ("bucket", "key")
    assert wr.s3.parse_path("s3://bucket/dir/dir2/filename") == (
        "bucket", "dir/dir2/filename")
    assert wr.s3.parse_path("s3://bucket/dir/dir2/filename/") == (
        "bucket", "dir/dir2/filename/")
    assert wr.s3.parse_path("s3://bucket/") == ("bucket", "")
    assert wr.s3.parse_path("s3://bucket") == ("bucket", "")


def test_get_bucket_region(bucket, region):
    assert wr.s3.get_bucket_region(bucket) == region


@pytest.mark.parametrize("parallel", [True, False])
def test_delete_objects_prefix(bucket, parallel):
    num = 1_001
    prefix = f"test_list_objects_{parallel}/"
    path = f"s3://{bucket}/{prefix}"
    print("Starting writes...")
    write_fake_objects(bucket, prefix, num)
    print("Waiting eventual consistency...")
    sleep(15)
    print("Listing...")
    paths = wr.s3.list_objects(path)
    assert len(paths) == num
    print("Deleting...")
    wr.s3.delete_objects_prefix(path=path, parallel=parallel)
    print("Waiting eventual consistency...")
    sleep(15)
    assert len(wr.s3.list_objects(path)) == 0
