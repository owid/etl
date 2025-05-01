"""Module for helper functions related to analytics (e.g. chart views).

TODO: We currently have many functions reading analytics (from MySQL and GBQ) in different places. We should gather those functions in this module.
"""

from datetime import datetime

from typing import List, Optional
import pandas as pd
from structlog import get_logger
import pandas as pd
import urllib.parse

########################################################################################################################
########################################################################################################################

# Initialize logger.
log = get_logger()

# First day when we started collecting chart render views.
# TODO: Find out this exact date.
DATE_MIN = "2025-01-01"
# Current date.
DATE_MAX = str(datetime.today().date())
# Base url for Datasette csv queries.
ANALYTICS_CSV_URL = "http://analytics.owid.io/analytics.csv"


def read_datasette(sql: str, datasette_csv_url: str = ANALYTICS_CSV_URL) -> pd.DataFrame:
    """
    Execute a query in the Datasette semantic layer.

    NOTE: This function will attempt to fetch all rows at once, and, if it exceeds the number of rows, it raises a warning. If that happens, you may need to simplify your query and combine results. I don't know if there's any simple workaround for this function.
    """
    # Prepare a query to use in the URL of datasette.
    query = urllib.parse.urlencode({
        "sql": sql,
        "_size": "max"
    })
    full_url = f"{datasette_csv_url}?{query}"
    # Read csv results returned by datasette, and create a dataframe.
    df = pd.read_csv(full_url)

    if len(df) == 10000:
        log.warning("Datasette cannot return more than 10,000 rows in one query. Simplify the query.")

    return df


def get_chart_events_by_chart_id(
    chart_ids: Optional[List[int]] = None,
    min_date: str = DATE_MIN,
    max_date: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch chart view events from Datasette, optionally filtered by chart IDs and minimum date.

    TODO: This function may not be necessary. Consider deleting.
    """
    where_clauses = []
    if min_date:
        where_clauses.append(f"v.day > '{min_date}'")
    if max_date:
        where_clauses.append(f"v.day <= '{max_date}'")
    if chart_ids:
        id_list = ', '.join(str(cid) for cid in chart_ids)
        where_clauses.append(f"c.chart_id IN ({id_list})")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
    SELECT
        c.chart_id,
        c.url,
        c.published_at,
        v.day,
        v.events
    FROM charts c
    JOIN grapher_views_detailed v ON c.url = v.grapher
    {where_sql}
    ORDER BY v.events DESC;
    """
    df = read_datasette(query)

    return df


def get_chart_views_by_chart_id(
    chart_ids: Optional[List[int]] = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch number of chart views (renders) per chart from Datasette, optionally filtered by chart IDs and date range.

    """
    where_clauses = []
    if date_min:
        where_clauses.append(f"v.day >= '{date_min}'")
    if date_max:
        where_clauses.append(f"v.day <= '{date_max}'")
    if chart_ids:
        id_list = ', '.join(str(cid) for cid in chart_ids)
        where_clauses.append(f"c.chart_id IN ({id_list})")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
    SELECT
        c.chart_id,
        c.url,
        c.published_at,
        SUM(v.events) AS views
    FROM charts c
    JOIN grapher_views_detailed v ON c.url = v.grapher
    {where_sql}
    GROUP BY c.chart_id, c.url, c.published_at
    ORDER BY views DESC
    """

    df_views = read_datasette(query)

    # To calculate the average daily views, we need to figure out the number of days for which we are counting views.
    # This will be either the first date since we have analytics (DATE_MIN) or the publication date of the chart (if more recent than that).
    published_at = pd.to_datetime(df_views["published_at"])
    start_date = published_at.where(published_at > pd.to_datetime(date_min), pd.to_datetime(date_min))

    # Add a column with the number of days that the views are referring to.
    df_views["n_days"] = (pd.Timestamp.today().normalize() - start_date).dt.days

    # Add a column for the average number of daily views.
    df_views["views_daily"] = df_views["views"] / df_views["n_days"]

    # Fix infs (for charts that were published in the last day).
    df_views.loc[df_views["views_daily"] == float("inf"), "views_daily"] = 0

    return df_views

def get_chart_views_last_n_days(
    chart_ids: Optional[List[int]] = None,
    n_days: int = 30,
) -> pd.DataFrame:
    """
    Fetch number of chart views per chart for the last n_days.

    """
    # Calculate date range.
    date_max = str(datetime.today().date())
    date_min = str((datetime.today() - pd.Timedelta(days=n_days)).date())

    # Get views.
    df_views = get_chart_views_by_chart_id(
        chart_ids=chart_ids,
        date_min=date_min,
        date_max=date_max
    )

    return df_views
