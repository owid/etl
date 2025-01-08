from datetime import datetime

import pandas as pd
from structlog import get_logger

from apps.housekeeper.utils import add_reviews, get_chart_summary, get_reviews_id
from apps.wizard.app_pages.similar_charts.data import get_raw_charts
from etl.config import OWID_ENV, SLACK_API_TOKEN
from etl.slack_helpers import send_slack_message

CHANNEL_NAME = "#lucas-playground"
SLACK_USERNAME = "housekeeper"
log = get_logger()


def get_charts_to_review():
    df = get_raw_charts()

    # Keep only older-than-a-year charts
    TODAY = datetime.today()
    YEAR_AGO = TODAY.replace(year=TODAY.year - 1)
    df = df.loc[df["created_at"] < YEAR_AGO]

    # Discard charts already presented in the chat
    reviews_id = get_reviews_id(object_type="chart")
    df = df.loc[~df["chart_id"].isin(reviews_id)]

    return df


def select_chart(df: pd.DataFrame):
    # Sort by views
    df = df.sort_values(["views_365d", "views_14d", "views_7d"])

    # Select oldest chart
    chart = df.iloc[0]

    return chart


def send_slack_chart_review(channel_name: str, slack_username: str, icon_emoji: str):
    # Get charts
    log.info("Getting charts to review")
    df = get_charts_to_review()

    # Select chart
    log.info("Select chart...")
    chart = select_chart(df)

    # Prepare message
    log.info("Preparing main message...")
    DATE = datetime.today().date().strftime("%d %b, %Y")
    message = (
        f"{DATE}: *Daily chart to review is...*\n"
        f"<{OWID_ENV.chart_site(chart['slug'])}|{chart['title']}> ({chart['views_365d']} views in the last year)\n"
        f"Go to <{OWID_ENV.chart_admin_site(chart['chart_id'])}|edit :writing_hand:>\n"
    )

    # Send message
    if SLACK_API_TOKEN:
        # Main message
        log.info("Sending main message, with image...")
        image_url = OWID_ENV.thumb_url(chart["slug"])
        response = send_slack_message(
            message=message,
            channel=channel_name,
            image_url=image_url,
            icon_emoji=icon_emoji,
            username=slack_username,
        )

        # More context in the thread
        kwargs = {
            "channel": channel_name,
            "icon_emoji": icon_emoji,
            "username": slack_username,
            "thread_ts": response["ts"],
        }
        ## 1/ Similar charts
        similar_messages = (
            f"🕵️ <{OWID_ENV.wizard_url}similar_charts?chart_search_text={chart['slug']}| → Explore similar charts>"
        )

        ## 2/ AI: Chart description, chart edit timeline, suggestion
        log.info("Getting AI summary...")
        ai_summary = get_chart_summary(chart=chart)

        ## 3/ Send extra info
        ### Similar charts
        log.info("Sending similar charts link...")
        send_slack_message(
            message=similar_messages,
            **kwargs,
        )
        ### AI Summary
        log.info("Sending AI summary...")
        if ai_summary:
            send_slack_message(
                message=ai_summary,
                **kwargs,
            )

        # Add chart to reviewed charts
        add_reviews(object_type="chart", object_id=chart["chart_id"])
