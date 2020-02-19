import logging

import awswrangler as wr

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_chunkify():
    assert wr.utils.chunkify(list(range(13)), 3) == [[0, 1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
