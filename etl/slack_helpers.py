"""Tools to assist with Slack interactions."""

import json
import tempfile
import time
from typing import Optional, Union

import requests
import slack_sdk.errors as e
from slack_sdk import WebClient
from slack_sdk.web.slack_response import SlackResponse
from structlog import get_logger

from etl import config

slack_client = WebClient(token=config.SLACK_API_TOKEN)
SECONDS_UPLOAD_WAIT = 10
log = get_logger()


def send_slack_message(
    channel: str, message: str, image_url: Optional[str] = None, image_path: Optional[str] = None, **kwargs
) -> SlackResponse:
    """Send `message` to Slack channel `channel`.

    If there is an error with the image upload, the message will be sent without the image.
    """
    # Send message + image
    if image_url or image_path:
        img_response = upload_image(image_url=image_url, image_path=image_path)
        if img_response and (img_response.get("file", {}).get("url_private")):
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message,
                    },
                },
                {
                    "type": "image",
                    "image_url": img_response["file"]["url_private"],
                    "alt_text": "Uploaded image",
                },
            ]

            assert "blocks" not in kwargs, "Blocks cannot be provided when uploading an image"

            try:
                response = slack_client.chat_postMessage(
                    channel=channel,
                    text=message,
                    blocks=blocks,
                    **kwargs,
                )
            except e.SlackApiError:
                response = slack_client.chat_postMessage(
                    channel=channel,
                    text=message,
                    **kwargs,
                )
        else:
            response = slack_client.chat_postMessage(
                channel=channel,
                text=message,
                **kwargs,
            )

    # Send regular message
    else:
        response = slack_client.chat_postMessage(
            channel=channel,
            text=message,
            **kwargs,
        )

    return response


def upload_image(
    image_url: Optional[str] = None, image_path: Optional[str] = None, seconds_wait: int = SECONDS_UPLOAD_WAIT, **kwargs
) -> Optional[SlackResponse]:
    """Upload image to Slack.

    This way we obtain a Slack URL that we can add to future messageds (in blocks).
    """
    assert (image_url is not None) ^ (image_path is not None), "Either image_url or image_path must be provided"

    if image_url is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
            # Download the image
            response = requests.get(image_url, stream=True)
            if response.status_code == 200:
                for chunk in response.iter_content(1024):  # Download in chunks
                    temp_file.write(chunk)
                log.info(f"File downloaded successfully to {temp_file.name}")
            else:
                log.info(f"Failed to download file. Status code: {response.status_code}")
                return

            # Step 2: Upload the file to Slack
        upload_response = slack_client.files_upload_v2(
            file=temp_file.name,
            **kwargs,
        )
    else:
        upload_response = slack_client.files_upload_v2(
            file=image_path,
            **kwargs,
        )

    if not upload_response["ok"]:
        log.info("Error uploading file:", upload_response["error"])
        return

    # Ensure everything is uploaded before continuing
    time.sleep(seconds_wait)

    return None


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


"""
upload_response = slack_client.files_upload_v2(
    file="/home/lucas/repos/etl/image.png",
    # channel=m["lucas-playground"],
)

fid = upload_response["file"]["id"]

url = upload_response["file"]["url_private"]

share_response = slack_client.chat_postMessage(
    channel="#lucas-playground",
    text="Here is the uploaded image:",
    blocks=[
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Here is the uploaded image:"
            }
        },
        {
            "type": "image",
            "image_url": f"{url}",
            "alt_text": "Uploaded image"
        }
    ]
)



"""
