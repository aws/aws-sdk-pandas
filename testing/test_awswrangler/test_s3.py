import multiprocessing as mp
from time import sleep
import logging

import pytest
import boto3
import pandas

from awswrangler import Session

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def calc_bounders(num, cpus):
    cpus = num if num < cpus else cpus
    size = int(num / cpus)
    rest = num % cpus
    bounders = []
    end = -1
    for _ in range(cpus):
        start = end + 1
        end += size
        if rest:
            end += 1
            rest -= 1
        bounders.append((start, end))
    return bounders


def wrt_fake_objs_batch_wrapper(args):
    return wrt_fake_objs_batch(*args)


def wrt_fake_objs_batch(bucket, path, start, end):
    s3 = boto3.resource("s3")
    for obj_id in range(start, end + 1):
        s3.Object(bucket, f"{path}{obj_id}").put(Body=str(obj_id).zfill(10))


def write_fake_objects(bucket, path, num):
    if path[-1] != "/":
        path += "/"
    if num < 10:
        wrt_fake_objs_batch(bucket, path, 0, num - 1)
        return
    cpus = mp.cpu_count()
    bounders = calc_bounders(num, cpus)
    args = []
    for item in bounders:
        args.append((bucket, path, item[0], item[1]))
    pool = mp.Pool(processes=cpus)
    print("Starting parallel writes...")
    pool.map(wrt_fake_objs_batch_wrapper, args)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test-arena")
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


@pytest.mark.parametrize("objects_num", [1, 10, 1001])
def test_delete_objects(session, bucket, objects_num):
    print("Starting writes...")
    write_fake_objects(bucket, f"objs-{objects_num}/", objects_num)
    print("Starting deletes...")
    session.s3.delete_objects(path=f"s3://{bucket}/objs-{objects_num}/")


@pytest.mark.parametrize("objects_num", [1, 10, 1001])
def test_delete_listed_objects(session, bucket, objects_num):
    path = f"s3://{bucket}/objs-listed-{objects_num}/"
    print("Starting deletes...")
    session.s3.delete_objects(path=path)
    print("Starting writes...")
    write_fake_objects(bucket, f"objs-listed-{objects_num}/", objects_num)
    sleep(3)  # Waiting for eventual consistency
    print("Starting list...")
    objects_paths = session.s3.list_objects(path=path)
    assert len(objects_paths) == objects_num
    print("Starting listed deletes...")
    session.s3.delete_listed_objects(objects_paths=objects_paths)
    print("Starting list...")
    objects_paths = session.s3.list_objects(path=path)
    assert len(objects_paths) == 0


def check_list_with_retry(session, path, length):
    for counter in range(10):
        if len(session.s3.list_objects(path=path)) == length:
            return True
        sleep(1)
    return False


@pytest.mark.parametrize("objects_num", [1, 10, 1001])
def test_delete_not_listed_objects(session, bucket, objects_num):
    path = f"s3://{bucket}/objs-not-listed-{objects_num}/"
    print("Starting deletes...")
    session.s3.delete_objects(path=path)
    print("Starting writes...")
    write_fake_objects(bucket, f"objs-not-listed-{objects_num}/", objects_num)
    sleep(3)  # Waiting for eventual consistency
    print("Starting not listed deletes...")
    session.s3.delete_not_listed_objects(objects_paths=[f"{path}0"])
    print("Starting checks...")
    assert check_list_with_retry(session=session, path=path, length=1)
    print("Starting deletes...")
    session.s3.delete_objects(path=path)


@pytest.mark.parametrize("objects_num", [1, 10, 1001])
def test_get_objects_sizes(session, bucket, objects_num):
    path = f"s3://{bucket}/objs-get-objects-sizes-{objects_num}/"
    print("Starting deletes...")
    session.s3.delete_objects(path=path)
    print("Starting writes...")
    write_fake_objects(bucket, f"objs-get-objects-sizes-{objects_num}/", objects_num)
    objects_paths = [f"s3://{bucket}/objs-get-objects-sizes-{objects_num}/{i}" for i in range(objects_num)]
    print("Starting gets...")
    objects_sizes = session.s3.get_objects_sizes(objects_paths=objects_paths)
    print("Starting deletes...")
    session.s3.delete_objects(path=path)
    for _, object_size in objects_sizes.items():
        assert object_size == 10


@pytest.mark.parametrize("mode, procs_io_bound", [
    ("append", 1),
    ("overwrite", 1),
    ("overwrite_partitions", 1),
    ("append", 8),
    ("overwrite", 8),
    ("overwrite_partitions", 8),
])
def test_copy_listed_objects(session, bucket, database, mode, procs_io_bound):
    path0 = f"s3://{bucket}/test_move_objects_0/"
    path1 = f"s3://{bucket}/test_move_objects_1/"
    print("Starting deletes...")
    session.s3.delete_objects(path=path0)
    session.s3.delete_objects(path=path1)
    dataframe = pandas.read_csv("data_samples/micro.csv")
    print("Starting writing path0...")
    session.pandas.to_parquet(
        dataframe=dataframe,
        database=database,
        path=path0,
        preserve_index=False,
        mode="overwrite",
        partition_cols=["name", "date"],
    )
    print("Starting writing path0...")
    objects_paths = session.pandas.to_parquet(
        dataframe=dataframe,
        path=path1,
        preserve_index=False,
        mode="overwrite",
        partition_cols=["name", "date"],
    )
    print("Starting move...")
    session.s3.copy_listed_objects(
        objects_paths=objects_paths,
        source_path=path1,
        target_path=path0,
        mode=mode,
        procs_io_bound=procs_io_bound,
    )
    print("Asserting...")
    sleep(1)
    dataframe2 = session.pandas.read_sql_athena(sql="select * from test_move_objects_0", database=database)
    if mode == "append":
        assert 2 * len(dataframe.index) == len(dataframe2.index)
    else:
        assert len(dataframe.index) == len(dataframe2.index)
