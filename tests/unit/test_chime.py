import logging

import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_chime_bad_input():
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.chime.post_message(message=None, webhook=None)


@pytest.mark.parametrize("webhook", ["http://hooks.chime.aws/incomingwebhooks/x", "file:///etc/passwd", "not-a-url"])
def test_chime_non_https_webhook(webhook):
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.chime.post_message(message="test", webhook=webhook)
