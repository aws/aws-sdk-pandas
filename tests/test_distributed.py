import logging

import pytest

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.distributed
def test_placeholder():
    pass
