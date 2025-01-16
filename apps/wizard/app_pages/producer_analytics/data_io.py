"""This module handles interactions with sources like DBs or file systems.

In particular, it gets analytics from Big Query, and from the version tracker (DAG).
"""

from datetime import timedelta
from typing import Optional, cast

import owid.catalog.processing as pr
import pandas as pd
import streamlit as st

from apps.utils.google import read_gbq
from apps.wizard.app_pages.producer_analytics.utils import GRAPHERS_BASE_URL, MIN_DATE, TODAY
from etl.snapshot import Snapshot
from etl.version_tracker import VersionTracker


@st.cache_data(show_spinner=False)
def get_analytics(min_date, max_date, excluded_steps):
    # 1/ Relate charts with producers of their data
    df = get_producers_per_chart(excluded_steps=excluded_steps)

    # 2/ Get chart analytics (chart views)
    df_charts = get_chart_views(min_date=min_date, max_date=max_date)

    # 3/ Combine, to have one table with chart analytics, together with producer of the chart data.
    df = df.merge(df_charts, on="chart_url", how="left").drop(columns=["all_chart_slugs"])

    return df


@st.cache_data(show_spinner=False)
def get_chart_views(min_date: str, max_date: str) -> pd.DataFrame:
    """Get chart views analytics for different date ranges.

    - Last 30 days
    - Last 365 days
    - Custom date range (given by the user)

    NOTE: We do several BQ calls to get the data for each range. Alternatively, we could get all data at once and then filter it in Python. However, this would be substantially slower since we'd need to download a long table from the DB.
    """
    # List ranges of dates to fetch views.
    TODAY_STR = TODAY.strftime("%Y-%m-%d")
    _30D_STR = (TODAY - timedelta(days=30)).strftime("%Y-%m-%d")
    _365D_STR = (TODAY - timedelta(days=365)).strftime("%Y-%m-%d")
    date_ranges = {
        "views_365d": (_365D_STR, TODAY_STR),
        "views_30d": (_30D_STR, TODAY_STR),
        "views_custom": (min_date, max_date),  # Use user-defined date range.
    }

    # Get analytics for those ranges, for all Grapher URLs.
    dfs = [
        get_chart_views_from_bq(
            date_start=date_start,
            date_end=date_end,
            grapher_urls=None,
            groupby=["grapher"],
        ).rename(columns={"renders": column_name})
        for column_name, (date_start, date_end) in date_ranges.items()
    ]

    # Merge all data frames.
    df = pr.multi_merge(
        dfs,  # type: ignore
        on="grapher",
        how="outer",
    )
    df = df.rename(columns={"grapher": "chart_url"})

    return df


@st.cache_data(show_spinner=False)
def get_chart_views_from_bq(
    date_start: str = MIN_DATE.strftime("%Y-%m-%d"),
    date_end: str = TODAY.strftime("%Y-%m-%d"),
    groupby: Optional[list[str]] = None,
    grapher_urls: Optional[list[str]] = None,
) -> pd.DataFrame:
    grapher_filter = ""
    if grapher_urls:
        # If a list of grapher URLs is given, consider only those.
        grapher_urls_formatted = ", ".join(f"'{url}'" for url in grapher_urls)
        grapher_filter = f"AND grapher IN ({grapher_urls_formatted})"
    else:
        # If no list is given, consider all grapher URLs.
        grapher_filter = f"AND grapher LIKE '{GRAPHERS_BASE_URL}%'"

    if not groupby:
        # If a groupby list is not given, assume the simplest case, which gives total views for each grapher.
        groupby = ["grapher"]

    # Prepare the query.
    groupby_clause = ", ".join(groupby)
    select_clause = f"{groupby_clause}, SUM(events) AS renders"
    query = f"""
        SELECT
            {select_clause}
        FROM prod_google_analytics4.grapher_views_by_day_page_grapher_device_country_iframe
        WHERE
            day >= '{date_start}'
            AND day <= '{date_end}'
            {grapher_filter}
        GROUP BY {groupby_clause}
        ORDER BY {groupby_clause}
    """

    # Execute the query.
    df_views = read_gbq(query, project_id="owid-analytics")

    return cast(pd.DataFrame, df_views)


@st.cache_data(show_spinner=False)
def get_producers_per_chart(excluded_steps) -> pd.DataFrame:
    # Load steps dataframe.
    df = VersionTracker(exclude_steps=excluded_steps).steps_df

    # Select only active snapshots.
    df = df[(df["channel"] == "snapshot") & (df["state"] == "active")].reset_index(drop=True)

    # Select only relevant columns.
    df = df[["step", "all_chart_slugs"]]

    # Load snapshot from step (raw) URI
    df["snap"] = df["step"].apply(lambda step: Snapshot.from_raw_uri(step))

    # Obtain producer, if applicable, from each snapshot
    def _obtain_producer(snap, filter_old_sources=True):
        origin = snap.metadata.origin
        if (origin is not None) and (snap.metadata.namespace not in ["dummy"]):
            return snap.metadata.origin.producer  # type: ignore

        # NOTE: Prior to 'origin', we used 'source' to obtain the producer. Set 'filter_old_sources' to False to use this method.
        if not filter_old_sources:
            source = snap.metadata.source
            if source is not None:
                return snap.metadata.source.name  # type: ignore

    df["producer"] = df["snap"].apply(_obtain_producer)

    # Select only relevant columns.
    df = df[["all_chart_slugs", "producer"]]

    # Remove rows with no producer.
    # NOTE: We are ignoring here all snapshots that use SOURCE instead of ORIGIN
    df = df.dropna(subset=["producer"]).reset_index(drop=True)

    # Ignore the chart id, and keep only the slug.
    df["all_chart_slugs"] = df["all_chart_slugs"].apply(lambda id_slug: set(slug for _, slug in id_slug))

    # Create a row for each producer-slug pair. Fill with "" (in cases where the producer has no charts).
    df = df.explode("all_chart_slugs")

    # Remove duplicates.
    # NOTE: This happens because df contains one row per snapshot. Some grapher datasets come from a combination of multiple snapshots (often from the same producer). We want to count producer-chart pairs only once.
    df = df.drop_duplicates(subset=["producer", "all_chart_slugs"]).reset_index(drop=True)

    # Add a column for grapher URL.
    df["chart_url"] = GRAPHERS_BASE_URL + df["all_chart_slugs"]

    return df
