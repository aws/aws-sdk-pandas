"""Chime Message/Notification module."""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_logger: logging.Logger = logging.getLogger(__name__)


def post_message(webhook: str, message: str) -> Any | None:
    """Send message on an existing Chime Chat rooms.

    Parameters
    ----------
    webhook
        Contains all the authentication information to send the message
    message
        The actual message which needs to be posted on Slack channel

    Returns
    -------
        The response from Chime
    """
    response = None
    chime_message = {"Content": f"Message: {message}"}
    req = Request(webhook, json.dumps(chime_message).encode("utf-8"))
    try:
        response = urlopen(req)
        _logger.info("Message posted on Chime. Got respone as %s", response.read())
    except HTTPError as e:
        _logger.exception("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        _logger.exception("Server connection failed: %s", e.reason)
    return response
