import logging

import pytest

import awswrangler

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("in1,in2,out", [(2, 3, 6), (65, 10, 130), (743, 321, 238503)])
def test_lcm(in1, in2, out):
    assert out == awswrangler.utils.lcm(in1, in2)
