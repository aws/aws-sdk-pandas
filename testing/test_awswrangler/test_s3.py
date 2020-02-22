import logging
import multiprocessing as mp
from datetime import datetime
from decimal import Decimal
from time import sleep

import boto3
import pandas as pd
import pytest

import awswrangler as wr

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
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
        wr.s3.delete_objects_prefix(f"s3://{bucket}/")
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket
    wr.s3.delete_objects_prefix(f"s3://{bucket}/")


def df():
    ts = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")  # noqa
    dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()  # noqa
    df = pd.DataFrame({
        "int_full": [1, 2, 3, 4, 5],
        "int_none": [None, 2, None, 4, None],
        "int_na": [pd.NA, 2, pd.NA, 4, pd.NA],
        "double_full": [1.1, 2.2, 3.3, 4.4, 5.5],
        "double_none": [None, 2.2, None, 4.4, None],
        "double_na": [pd.NA, 2.2, pd.NA, 4.4, pd.NA],
        "decimal_full": [
            Decimal((0, (1, 1), -1)),
            Decimal((0, (2, 2), -1)),
            Decimal((0, (3, 3), -1)),
            Decimal((0, (4, 4), -1)),
            Decimal((0, (5, 5), -1))
        ],
        "decimal_none": [None, Decimal((0, (2, 2), -1)), None,
                         Decimal((0, (4, 4), -1)), None],
        "decimal_na": [pd.NA, Decimal((0, (2, 2), -1)), pd.NA,
                       Decimal((0, (4, 4), -1)), pd.NA],
        "bool_full": [True, False, True, False, True],
        "bool_none": [True, None, None, False, None],
        "bool_na": [True, pd.NA, pd.NA, False, pd.NA],
        "string_full": ["øåæøå", "汉字", "foo", "1.1", "True"],
        "string_none": ["øåæøå", None, None, "foo", None],
        "string_na": ["øåæøå", pd.NA, pd.NA, "foo", pd.NA],
        "timestamp_full": [
            ts("0001-01-10 10:34:55.863"),
            ts("2500-01-10 10:34:55.863"),
            ts("2020-01-10 10:34:55.863"),
            ts("2021-01-10 10:34:55.863"),
            ts("2023-01-10 10:34:55.863")
        ],
        "timestamp_none": [ts("0001-01-10 10:34:55.863"), None, None, None,
                           ts("2023-01-10 10:34:55.863")],
        "timestamp_na": [ts("0001-01-10 10:34:55.863"), pd.NA, pd.NA, pd.NA,
                         ts("2023-01-10 10:34:55.863")],
        "date_full": [dt("0001-01-10"),
                      dt("2500-01-10"),
                      dt("2020-01-10"),
                      dt("2021-01-10"),
                      dt("2023-01-10")],
        "date_none": [dt("0001-01-10"), None, None, None, dt("2023-01-10")],
        "date_na": [dt("0001-01-10"), pd.NA, pd.NA, pd.NA, dt("2023-01-10")],
        "category_full": ["a", "a", "b", "b", "a"],
        "category_none": ["a", None, None, "b", None],
    })
    df["category_full"] = df["category_full"].astype("category")
    df["category_none"] = df["category_none"].astype("category")
    df["int_full_new"] = df["int_full"].astype("Int64")
    df["int_none_new"] = df["int_none"].astype("Int64")
    df["int_na_new"] = df["int_na"].astype("Int64")
    df["string_full_new"] = df["string_full"].astype("string")
    df["string_none_new"] = df["string_none"].astype("string")
    df["string_na_new"] = df["string_na"].astype("string")
    df["bool_full_new"] = df["bool_full"].astype("boolean")
    df["bool_none_new"] = df["bool_none"].astype("boolean")
    df["bool_na_new"] = df["bool_na"].astype("boolean")
    return df


def test_does_object_exists(bucket):
    key = "test_does_object_exists"
    boto3.resource("s3").Object(bucket, key).put(Body=key)
    assert wr.s3.does_object_exists(path=f"s3://{bucket}/{key}") is True
    assert wr.s3.does_object_exists(path=f"s3://{bucket}/{key}_wrong") is False
    session = wr.Session()
    assert session.s3.does_object_exists(path=f"s3://{bucket}/{key}") is True
    assert session.s3.does_object_exists(path=f"s3://{bucket}/{key}_wrong") is False


def test_wait_object_exists(bucket):
    key = "test_wait_object_exists"
    boto3.resource("s3").Object(bucket, key).put(Body=key)
    assert wr.s3.wait_object_exists(path=f"s3://{bucket}/{key}") is None
    assert wr.s3.wait_object_exists(path=f"s3://{bucket}/{key}", polling_sleep=0.05, timeout=10.0) is None
    with pytest.raises(wr.exceptions.S3WaitObjectTimeout):
        wr.s3.wait_object_exists(path=f"s3://{bucket}/{key}_wrong", timeout=2.0)


def test_parse_path():
    assert wr.s3.parse_path("s3://bucket/key") == ("bucket", "key")
    assert wr.s3.parse_path("s3://bucket/dir/dir2/filename") == ("bucket", "dir/dir2/filename")
    assert wr.s3.parse_path("s3://bucket/dir/dir2/filename/") == ("bucket", "dir/dir2/filename/")
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
    wr.s3.delete_objects_list(paths=[])


@pytest.mark.parametrize("filename", [None, "filename.csv"])
def test_to_csv_filename(bucket, filename):
    paths = wr.s3.to_csv(df=df(), path=f"s3://{bucket}/test_to_csv_filename/", filename=filename)
    wr.s3.wait_object_exists(path=paths[0])


def test_to_csv_overwrite(bucket):
    path = f"s3://{bucket}/test_to_csv_overwrite/"
    wr.s3.to_csv(df=df(), path=path, mode="overwrite")
    wr.s3.to_csv(df=df(), path=path, mode="overwrite")
    print("Waiting eventual consistency...")
    sleep(15)
    print("Listing...")
    assert len(wr.s3.list_objects(path)) == 1


@pytest.mark.parametrize("partition_cols", [
    'int_full', 'double_full', 'decimal_full', 'bool_full', 'string_full', 'timestamp_full', 'date_full',
    'category_full', 'int_full_new', 'string_full_new', 'bool_full_new'
])
def test_to_csv_partitioned(bucket, partition_cols):
    paths = wr.s3.to_csv(df=df(), path=f"s3://{bucket}/test_to_csv_partitioned/", partition_cols=partition_cols)
    for p in paths:
        wr.s3.wait_object_exists(path=p)


def test_to_csv_partition_upsert(bucket):
    path = f"s3://{bucket}/test_to_csv_partition_upsert/"
    wr.s3.to_csv(df=df(), path=path, mode="overwrite", partition_cols=["int_full_new"])
    print("Waiting eventual consistency...")
    sleep(15)
    wr.s3.to_csv(df=df(), path=path, mode="partition_upsert", partition_cols=["int_full_new"])
    print("Waiting eventual consistency...")
    sleep(15)
    print("Listing...")
    assert len(wr.s3.list_objects(path)) == 5
