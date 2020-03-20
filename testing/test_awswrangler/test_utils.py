import logging

from awswrangler._utils import chunkify, ensure_cpu_count, parse_path  # noqa

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


def test_parse_path():
    assert parse_path("s3://bucket/key") == ("bucket", "key")
    assert parse_path("s3://bucket/dir/dir2/filename") == ("bucket", "dir/dir2/filename")
    assert parse_path("s3://bucket/dir/dir2/filename/") == ("bucket", "dir/dir2/filename/")
    assert parse_path("s3://bucket/") == ("bucket", "")
    assert parse_path("s3://bucket") == ("bucket", "")


def test_get_cpu_count():
    assert ensure_cpu_count(use_threads=True) > 1
    assert ensure_cpu_count(use_threads=False) == 1


def test_chunkify():
    assert chunkify(list(range(13)), num_chunks=3) == [[0, 1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
    assert chunkify(list(range(13)), max_length=4) == [[0, 1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]
