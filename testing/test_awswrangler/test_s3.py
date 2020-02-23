import logging
import multiprocessing as mp
from datetime import datetime
from decimal import Decimal
from time import sleep

import boto3
import pandas as pd
import pytest

import awswrangler as wr

EVENTUAL_CONSISTENCY_SLEEP: float = 5.0  # seconds

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


def get_df():
    ts = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")  # noqa
    dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()  # noqa
    df = pd.DataFrame({
        "int_full": [1, 2, 3, 4, 5],
        "int_none": [None, 2, None, 4, None],
        "double_full": [1.1, 2.2, 3.3, 4.4, 5.5],
        "double_none": [None, 2.2, None, 4.4, None],
        "decimal_full": [
            Decimal((0, (1, 1), -1)),
            Decimal((0, (2, 2), -1)),
            Decimal((0, (3, 3), -1)),
            Decimal((0, (4, 4), -1)),
            Decimal((0, (5, 5), -1))
        ],
        "decimal_none": [None, Decimal((0, (2, 2), -1)), None,
                         Decimal((0, (4, 4), -1)), None],
        "bool_full": [True, False, True, False, True],
        "bool_none": [True, None, None, False, None],
        "string_full": ["øåæøå", "汉字", "foo", "1.1", "True"],
        "string_none": ["øåæøå", None, None, "foo", None],
        "timestamp_full": [
            ts("0001-01-10 10:34:55.863"),
            ts("2500-01-10 10:34:55.863"),
            ts("2020-01-10 10:34:55.863"),
            ts("2021-01-10 10:34:55.863"),
            ts("2023-01-10 10:34:55.863")
        ],
        "timestamp_none": [ts("0001-01-10 10:34:55.863"), None, None, None,
                           ts("2023-01-10 10:34:55.863")],
        "date_full": [dt("0001-01-10"),
                      dt("2500-01-10"),
                      dt("2020-01-10"),
                      dt("2021-01-10"),
                      dt("2023-01-10")],
        "date_none": [dt("0001-01-10"), None, None, None, dt("2023-01-10")],
        "category_full": ["a", "a", "b", "b", "a"],
        "category_none": ["a", None, None, "b", None],
    })
    df["category_full"] = df["category_full"].astype("category")
    df["category_none"] = df["category_none"].astype("category")
    df["int_full_new"] = df["int_full"].astype("Int64")
    df["int_none_new"] = df["int_none"].astype("Int64")
    df["string_full_new"] = df["string_full"].astype("string")
    df["string_none_new"] = df["string_none"].astype("string")
    df["bool_full_new"] = df["bool_full"].astype("boolean")
    df["bool_none_new"] = df["bool_none"].astype("boolean")
    return df


def test_does_object_exists(bucket):
    key = "test_does_object_exists"
    boto3.resource("s3").Object(bucket, key).put(Body=key)
    print("Waiting eventual consistency...")
    sleep(EVENTUAL_CONSISTENCY_SLEEP)
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
    num = 20
    prefix = f"test_list_objects_{parallel}/"
    path = f"s3://{bucket}/{prefix}"
    print("Starting writes...")
    write_fake_objects(bucket, prefix, num)
    print("Waiting eventual consistency...")
    sleep(EVENTUAL_CONSISTENCY_SLEEP)
    print("Listing...")
    paths = wr.s3.list_objects(path)
    assert len(paths) == num
    print("Deleting...")
    wr.s3.delete_objects_prefix(path=path, parallel=parallel)
    print("Waiting eventual consistency...")
    sleep(EVENTUAL_CONSISTENCY_SLEEP)
    assert len(wr.s3.list_objects(path)) == 0
    wr.s3.delete_objects_list(paths=[])


@pytest.mark.parametrize("filename, self_destruct", [(None, True), ("filename.csv", False)])
def test_to_csv_filename(bucket, filename, self_destruct):
    paths = wr.s3.to_csv(df=get_df(),
                         path=f"s3://{bucket}/test_to_csv_filename/",
                         filename=filename,
                         self_destruct=self_destruct)
    wr.s3.wait_object_exists(path=paths[0])


def test_to_csv_overwrite(bucket):
    path = f"s3://{bucket}/test_to_csv_overwrite/"
    path1 = wr.s3.to_csv(df=get_df(), path=path, mode="overwrite")[0]
    path2 = wr.s3.to_csv(df=get_df(), path=path, mode="overwrite")[0]
    assert wr.s3.does_object_exists(path1) is False
    assert wr.s3.does_object_exists(path2) is True


@pytest.mark.parametrize("partition_cols, self_destruct", [('int_full', True), ('double_full', False),
                                                           ('decimal_full', True), ('bool_full', False),
                                                           ('string_full', True), ('timestamp_full', False),
                                                           ('date_full', True), ('category_full', False),
                                                           ('int_full_new', True), ('string_full_new', False),
                                                           ('bool_full_new', True)])
def test_to_csv_partitioned(bucket, partition_cols, self_destruct):
    paths = wr.s3.to_csv(df=get_df(),
                         path=f"s3://{bucket}/test_to_csv_partitioned/",
                         partition_cols=partition_cols,
                         self_destruct=self_destruct)
    for p in paths:
        wr.s3.wait_object_exists(path=p)


@pytest.mark.parametrize("compression", [None, "gzip"])
def test_to_csv_compressed(bucket, compression):
    paths = wr.s3.to_csv(df=get_df(),
                         path=f"s3://{bucket}/test_to_csv_compressed/",
                         compression=compression,
                         num_files=2)
    assert len(paths) == 2
    wr.s3.wait_object_exists(path=paths[0])
    wr.s3.wait_object_exists(path=paths[1])


def test_to_csv_partition_upsert(bucket):
    path = f"s3://{bucket}/test_to_csv_partition_upsert/"
    paths1 = wr.s3.to_csv(df=get_df(), path=path, mode="overwrite", partition_cols=["int_full_new"])
    print("Waiting eventual consistency...")
    sleep(EVENTUAL_CONSISTENCY_SLEEP)
    paths2 = wr.s3.to_csv(df=get_df(), path=path, mode="partition_upsert", partition_cols=["int_full_new"])
    for p in paths1:
        assert wr.s3.does_object_exists(p) is False
    for p in paths2:
        assert wr.s3.does_object_exists(p) is True


@pytest.mark.parametrize("filename, self_destruct", [(None, True), ("filename.csv", False)])
def test_to_parquet_filename(bucket, filename, self_destruct):
    paths = wr.s3.to_parquet(df=get_df(),
                             path=f"s3://{bucket}/test_to_parquet_filename/",
                             filename=filename,
                             parallel=False)
    wr.s3.wait_object_exists(path=paths[0])


@pytest.mark.parametrize("partition_cols, self_destruct", [('int_full', True), ('double_full', False),
                                                           ('decimal_full', True), ('bool_full', False),
                                                           ('string_full', True), ('timestamp_full', False),
                                                           ('date_full', True), ('category_full', False),
                                                           ('int_full_new', True), ('string_full_new', False),
                                                           ('bool_full_new', True)])
def test_to_parquet_partitioned(bucket, partition_cols, self_destruct):
    paths = wr.s3.to_parquet(df=get_df(),
                             path=f"s3://{bucket}/test_to_parquet_partitioned/",
                             partition_cols=partition_cols,
                             self_destruct=self_destruct)
    for p in paths:
        wr.s3.wait_object_exists(path=p)


@pytest.mark.parametrize("compression", [None, "snappy", "gzip"])
def test_to_parquet_compressed(bucket, compression):
    paths = wr.s3.to_parquet(df=get_df(),
                             path=f"s3://{bucket}/test_to_parquet_compressed/",
                             compression=compression,
                             num_files=2)
    assert len(paths) == 2
    wr.s3.wait_object_exists(path=paths[0])
    wr.s3.wait_object_exists(path=paths[1])


@pytest.mark.parametrize("parallel", [True, False])
def test_read_csv(bucket, parallel):
    path = f"s3://{bucket}/test_read_csv/"
    df = get_df()
    paths = wr.s3.to_csv(df=df, path=path, parallel=parallel)
    df2 = wr.s3.read_csv(path=paths[0], parallel=parallel)
    assert df[["int_full"]].equals(df2[["int_full"]])
    paths = wr.s3.to_csv(df=df, path=path, parallel=parallel, compression="gzip")
    df2 = wr.s3.read_csv(path=paths[0], parallel=parallel, compression="gzip")
    assert df[["int_full"]].equals(df2[["int_full"]])


@pytest.mark.parametrize("parallel", [True, False])
def test_read_parquet(bucket, parallel):
    path = f"s3://{bucket}/test_read_parquet/"
    df = get_df()
    paths = wr.s3.to_parquet(df=df, path=path, parallel=parallel)
    df2 = wr.s3.read_parquet(path=paths[0], parallel=parallel)
    assert df[["int_full"]].equals(df2[["int_full"]])
    paths = wr.s3.to_parquet(df=df, path=path, parallel=parallel, compression=None)
    df2 = wr.s3.read_parquet(path=paths[0], parallel=parallel)
    assert df[["int_full"]].equals(df2[["int_full"]])
    paths = wr.s3.to_parquet(df=df, path=path, parallel=parallel, compression="gzip")
    df2 = wr.s3.read_parquet(path=paths[0], parallel=parallel)
    assert df[["int_full"]].equals(df2[["int_full"]])
