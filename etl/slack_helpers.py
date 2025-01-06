"""Tools to assist with Slack interactions."""

import json

from slack_sdk import WebClient

from etl import config

slack_client = WebClient(token=config.SLACK_API_TOKEN)


def send_slack_message(channel: str, message: str) -> None:
    """Send `message` to Slack channel `channel`."""
    if config.SLACK_API_TOKEN:
        slack_client.chat_postMessage(channel=channel, text=message)


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
