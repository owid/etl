"""Keep things in OWID catalog clean by regularly checking and reviewing content."""

from typing import Optional

import rich_click as click

from apps.housekeeper.charts import send_slack_chart_reviews

# TODO: Add more review types
REVIEW_TYPES = [
    "chart",
    # "dataset",
]

# Config
CHANNEL_NAME = "#chart-reviews"


@click.command("housekeeper", help=__doc__)
@click.option(
    "--review-type",
    "-t",
    type=click.Choice(REVIEW_TYPES, case_sensitive=False),
    help="Type of the review",
)
@click.option(
    "--channel",
    "-c",
    type=str,
    help=f"Name of the slack channel to send the message to. If None, {CHANNEL_NAME} will be used",
)
@click.option(
    "--dev",
    is_flag=True,
    default=False,
    help="Dev mode: replaces Slack mentions with code-formatted names so nobody gets pinged. Requires --channel.",
)
def main(review_type: str, channel: Optional[str] = None, dev: bool = False):
    if dev and channel is None:
        raise click.UsageError("--dev requires --channel to avoid posting to the main channel")

    channel_name = CHANNEL_NAME if channel is None else channel
    # Review charts
    if review_type == "chart":
        send_slack_chart_reviews(
            channel_name=channel_name,
            include_draft=True,
            dev=dev,
        )
