import os
import multiprocessing as mp
from time import sleep

import pytest
import boto3

from awswrangler import Session


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
    cpus = mp.cpu_count() * 4
    bounders = calc_bounders(num, cpus)
    args = []
    for item in bounders:
        args.append((bucket, path, item[0], item[1]))
    pool = mp.Pool(cpus)
    pool.map(wrt_fake_objs_batch_wrapper, args)


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


@pytest.mark.parametrize("objects_num", [1, 10, 1001])
def test_delete_objects(session, bucket, objects_num):
    write_fake_objects(bucket, f"objs-{objects_num}/", objects_num)
    session.s3.delete_objects(path=f"s3://{bucket}/objs-{objects_num}/")


@pytest.mark.parametrize("objects_num", [1, 10, 1001])
def test_delete_listed_objects(session, bucket, objects_num):
    path = f"s3://{bucket}/objs-listed-{objects_num}/"
    session.s3.delete_objects(path=path)
    write_fake_objects(bucket, f"objs-listed-{objects_num}/", objects_num)
    objects_paths = session.s3.list_objects(path=path)
    assert len(objects_paths) == objects_num
    session.s3.delete_listed_objects(objects_paths=objects_paths)
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
    write_fake_objects(bucket, f"objs-not-listed-{objects_num}/", objects_num)
    session.s3.delete_not_listed_objects(objects_paths=[f"{path}0"])
    assert check_list_with_retry(session=session, path=path, length=1)
    session.s3.delete_objects(path=path)


@pytest.mark.parametrize("objects_num", [1, 10, 1001])
def test_get_objects_sizes(session, bucket, objects_num):
    path = f"s3://{bucket}/objs-get-objects-sizes-{objects_num}/"
    session.s3.delete_objects(path=path)
    write_fake_objects(bucket, f"objs-get-objects-sizes-{objects_num}/", objects_num)
    objects_paths = [
        f"s3://{bucket}/objs-get-objects-sizes-{objects_num}/{i}"
        for i in range(objects_num)
    ]
    objects_sizes = session.s3.get_objects_sizes(objects_paths=objects_paths)
    session.s3.delete_objects(path=path)
    for _, object_size in objects_sizes.items():
        assert object_size == 10
