"""Keep things in OWID catalog clean by regularly checking and reviewing content."""

import click
from rich_click import RichCommand

from apps.housekeeper.charts import send_slack_chart_review

# TODO: Add more review types
REVIEW_TYPES = [
    "chart",
    # "dataset",
]

# Config
CHANNEL_NAME = "#lucas-playground"
SLACK_USERNAME = "housekeeper"
ICON_EMOJI = "sus-blue"


@click.command("housekeeper", cls=RichCommand, help=__doc__)
@click.option("--review-type", "-t", type=click.Choice(REVIEW_TYPES, case_sensitive=False))
def main(review_type: str):
    # Review charts
    if review_type == "chart":
        send_slack_chart_review(
            channel_name=CHANNEL_NAME,
            slack_username=SLACK_USERNAME,
            icon_emoji=ICON_EMOJI,
        )
