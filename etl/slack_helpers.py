"""Tools to assist with Slack interactions."""

import json
from typing import Optional

from slack_sdk import WebClient
from slack_sdk.web.slack_response import SlackResponse

from etl import config

slack_client = WebClient(token=config.SLACK_API_TOKEN)


def send_slack_message(channel: str, message: str, **kwargs) -> SlackResponse:
    """Send `message` to Slack channel `channel`."""

    response = slack_client.chat_postMessage(channel=channel, text=message, **kwargs)
    return response


def send_slack_image(channel: str, message: str, image_url, **kwargs) -> SlackResponse:
    """Send `message` to Slack channel `channel`."""

    response = slack_client.chat_postMessage(
        channel=channel,
        text=message,
        attachments=[
            {
                "fallback": "Image preview not available",
                "image_url": image_url,  # Image URL
                "alt_text": "Image description",
            }
        ],
        **kwargs,
    )

    return response


def format_slack_message(method, url, status_code, req_body, res_body):
    try:
        res_body = json.dumps(json.loads(res_body), indent=2)
    except json.decoder.JSONDecodeError:
        pass

    try:
        req_body = json.dumps(json.loads(req_body), indent=2)
    except json.decoder.JSONDecodeError:
        pass

    if status_code == 200:
        emoji = ":information_source:"
    else:
        emoji = ":warning:"

    message = f"{emoji} *{method}* {url}\n"

    if req_body:
        message += f"Request\n```\n{req_body}\n```\n"

    message += f"Response\n```\n{res_body}\n```\n"

    return message


def get_channels():
    response = slack_client.conversations_list()
    channels = response["channels"]
    return channels


def channels_mapping():
    channels = get_channels()
    mapping = {}
    for channel in channels:
        mapping[channel["name"]] = channel["id"]
    return mapping
