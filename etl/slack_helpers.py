"""Tools to assist with Slack interactions."""

import json
import tempfile
import time
from datetime import datetime
from typing import Optional

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
    # If no Slack token is configured, just print the message
    if config.SLACK_API_TOKEN is None:
        print(f"[SLACK {channel}] {message}")
        return None  # type: ignore

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

    return upload_response


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


def get_channels() -> list[dict]:
    """Get all Slack channels (with pagination).

    Returns:
        List of channel dicts with 'id', 'name', and other Slack metadata.
    """
    channels = []
    cursor = None
    while True:
        response = slack_client.conversations_list(
            types="public_channel,private_channel",
            limit=200,
            cursor=cursor,
        )
        channels.extend(response["channels"])
        cursor = response.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    return channels


def channels_mapping() -> dict[str, str]:
    """Get mapping of channel names to IDs.

    Returns:
        Dict mapping channel name -> channel ID.
    """
    return {c["name"]: c["id"] for c in get_channels()}


def get_messages(
    channel: str,
    date_min: Optional[datetime] = None,
    date_max: Optional[datetime] = None,
    limit: int = 1000,
) -> list[dict]:
    """Get messages from a Slack channel within a date range.

    Args:
        channel: Channel ID or name (e.g., "#general" or "C1234567890")
        date_min: Start of date range (inclusive). If None, fetches from beginning.
        date_max: End of date range (inclusive). If None, fetches until now.
        limit: Maximum number of messages to fetch (default 1000).

    Returns:
        List of dicts with 'text', 'ts' (timestamp), and 'user' fields,
        sorted by timestamp ascending.
    """
    if config.SLACK_API_TOKEN is None:
        log.warning("No Slack token configured, returning empty list")
        return []

    # Convert datetimes to Unix timestamps
    oldest = str(date_min.timestamp()) if date_min else None
    latest = str(date_max.timestamp()) if date_max else None

    messages = []
    cursor = None

    while len(messages) < limit:
        kwargs = {
            "channel": channel,
            "limit": min(200, limit - len(messages)),  # API max is 200 per request
        }
        if oldest:
            kwargs["oldest"] = oldest
        if latest:
            kwargs["latest"] = latest
        if cursor:
            kwargs["cursor"] = cursor

        response = slack_client.conversations_history(**kwargs)

        if not response["ok"]:
            log.error(f"Error fetching messages: {response.get('error')}")
            break

        for msg in response.get("messages", []):
            # Skip thread replies and bot messages if needed
            if "subtype" not in msg:  # Regular user messages
                messages.append(
                    {
                        "text": msg.get("text", ""),
                        "ts": msg.get("ts"),
                        "user": msg.get("user"),
                    }
                )

        # Check for pagination
        if response.get("has_more") and response.get("response_metadata", {}).get("next_cursor"):
            cursor = response["response_metadata"]["next_cursor"]
        else:
            break

    # Sort by timestamp ascending (API returns newest first)
    messages.sort(key=lambda x: float(x["ts"]))

    return messages


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
