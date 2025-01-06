from typing import Any, Dict

from slack_sdk import WebClient

from etl import config

slack_client = WebClient(token=config.SLACK_API_TOKEN)


def prune_none(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}
