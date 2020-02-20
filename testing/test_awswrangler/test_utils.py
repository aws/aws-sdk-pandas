import logging

import awswrangler as wr

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_chunkify():
    assert wr.utils.chunkify(list(range(13)), num_chunks=3) == [[0, 1, 2, 3, 4],
                                                                [5, 6, 7, 8],
                                                                [9, 10, 11, 12]]
    assert wr.utils.chunkify(list(range(13)), max_length=4) == [[0, 1, 2, 3],
                                                                [4, 5, 6],
                                                                [7, 8, 9],
                                                                [10, 11, 12]]


def test_get_cpu_count():
    assert wr.utils.get_cpu_count(parallel=True) > 1
    assert wr.utils.get_cpu_count(parallel=False) == 1
