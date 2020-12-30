"""Slack Messaging module."""


import json
import logging
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


_logger: logging.Logger = logging.getLogger(__name__)


def post_message(channel_name: str, webhook: str, message: str) -> dict:
    """Sends message on an existing slack channel.
    Documentation on how to setup Slack webhook https://api.slack.com/messaging/webhooks

    Parameters
    ----------
    :param channel_name: str:
        Specifies the channel name in Slack.
        You can specify exact name as it appears on Slack UI
    :param webhook : webhook
        Webhook: This contains all the authentication information to send the message
    :param message : message
        The actual message which needs to be posted on Slack channel

    Returns
    -------
    dict
        Represents the response from Slack

    Examples
    --------
    """
    response = None
    slack_message = {
        'Content': "Message: %s" % (message)
    }
    req = Request(webhook, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        _logger.info(f"Message posted to {channel_name}")
    except HTTPError as e:
        _logger.error(f"Request failed with error code {e.code} and reason {e.reason}")
    except URLError as e:
        _logger.error(f"Server connection failed: {e.reason}")
    return response


'''
import awswrangler as wr
channel_name= "channel_name"
webhook= "webhook"
message = "test message"
value = wr.slack.post_message(channel_name=channel_name, webhook = webhook, message= message)
'''