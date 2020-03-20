import logging

import awswrangler as wr

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


def test_metadata():
    assert wr.__version__ == "1.0.0"
    assert wr.__title__ == "awswrangler"
    assert wr.__description__ == "Pandas on AWS."
    assert wr.__license__ == "Apache License 2.0"
