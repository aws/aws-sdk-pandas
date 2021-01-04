import logging

import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_chime_bad_input():
    with pytest.raises(ValueError):
        result = wr.chime.post_message(message=None, webhook=None)
        assert result is None
