"""Chime Message/Notification module."""


import json
import logging
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_logger: logging.Logger = logging.getLogger(__name__)


def post_message(webhook: str, message: str) -> Optional[Any]:
    """Sends message on an existing Chime Chat rooms.

    Parameters
    ----------
    :param webhook : webhook
        Webhook: This contains all the authentication information to send the message
    :param message : message
        The actual message which needs to be posted on Slack channel

    Returns
    -------
    dict
        Represents the response from Chime

    Examples
    --------
    """
    response = None
    chime_message = {"Content": "Message: %s" % (message)}
    req = Request(webhook, json.dumps(chime_message).encode("utf-8"))
    try:
        response = urlopen(req)
        _logger.info(f"Message posted on Chime. Got response as {response.read()}")
    except HTTPError as e:
        _logger.exception(f"Request failed: {e.code} {e.reason}")
    except URLError as e:
        _logger.exception(f"Server connection failed: {e.reason}")
    return response
