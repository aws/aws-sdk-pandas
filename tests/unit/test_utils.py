import logging
import os

import pytest

from awswrangler._utils import ensure_cpu_count, get_even_chunks_sizes

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize(
    "total_size,chunk_size,upper_bound,result",
    [
        (10, 4, True, (4, 3, 3)),
        (2, 3, True, (2,)),
        (1, 1, True, (1,)),
        (2, 1, True, (1, 1)),
        (11, 4, True, (4, 4, 3)),
        (1_001, 500, True, (334, 334, 333)),
        (1_002, 500, True, (334, 334, 334)),
        (10, 4, False, (5, 5)),
        (1, 1, False, (1,)),
        (2, 1, False, (1, 1)),
        (11, 4, False, (6, 5)),
        (1_001, 500, False, (501, 500)),
        (1_002, 500, False, (501, 501)),
    ],
)
def test_get_even_chunks_sizes(total_size, chunk_size, upper_bound, result):
    assert get_even_chunks_sizes(total_size, chunk_size, upper_bound) == result


@pytest.mark.parametrize("use_threads,result", [(True, os.cpu_count()), (False, 1), (-1, 1), (1, 1), (5, 5)])
def test_ensure_cpu_count(use_threads, result):
    assert ensure_cpu_count(use_threads=use_threads) == result


@pytest.mark.parametrize(
    "path_root,path,expected",
    [
        ("s3://bucket/prefix/", "s3://bucket/prefix/year=2023/month=05/file.parquet", {"year": "2023", "month": "05"}),
        ("s3://bucket/prefix", "s3://bucket/prefix/year=2023/month=05/file.parquet", {"year": "2023", "month": "05"}),
        ("s3://bucket/env=dev/table/", "s3://bucket/env=dev/table/year=2023/file.parquet", {"year": "2023"}),
        ("s3://bucket/env=dev/table", "s3://bucket/env=dev/table/year=2023/file.parquet", {"year": "2023"}),
        ("s3://bucket/env=dev/table/", "s3://bucket/env=dev/table/file.parquet", {}),
    ],
)
def test_extract_partitions_from_path(path_root, path, expected):
    from awswrangler._arrow import _extract_partitions_from_path

    assert _extract_partitions_from_path(path_root=path_root, path=path) == expected
