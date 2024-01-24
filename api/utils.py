import json
from typing import Any, Dict

from slack_sdk import WebClient

from etl import config

slack_client = WebClient(token=config.SLACK_API_TOKEN)


def prune_none(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def send_slack_message(channel: str, message: str) -> None:
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

    return f"""
{emoji} *{method}* {url}
Request
```
{req_body}
```
Response
```
{res_body}
```
    """
