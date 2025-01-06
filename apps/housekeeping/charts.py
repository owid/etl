from datetime import datetime

import pandas as pd

from apps.housekeeping.utils import add_reviews, get_reviews_id
from apps.wizard.app_pages.similar_charts import data
from etl.config import OWID_ENV
from etl.slack_helpers import send_slack_message


def get_charts_to_review():
    df = data.get_raw_charts()

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


def ask_for_review():
    # Get charts
    df = get_charts_to_review()

    # Select chart
    chart = select_chart(df)

    # Prepare message
    DATE = datetime.today().date().strftime("%d %b, %Y")

    message = (
        f"{DATE}: *Daily chart to review is...*\n"
        f"<{OWID_ENV.chart_site(chart['slug'])}|{chart['title']}> ({chart['views_365d']} views in the last year)\n"
        f"Go to <{OWID_ENV.chart_admin_site(chart['chart_id'])}|edit :writing_hand:>\n"
    )

    # Send message
    send_slack_message(
        channel="#lucas-playground",
        message=message,
        icon_emoji="sus-blue",
        username="housekeeper2",
    )

    # Add chart to reviewed charts
    add_reviews(object_type="chart", object_id=123)  # chart["chart_id"])
