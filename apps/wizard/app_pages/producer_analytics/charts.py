"""Code to generate the chart analytics bit of the app."""

from datetime import timedelta
from typing import Optional, cast

import owid.catalog.processing as pr
import pandas as pd
import plotly.express as px
import streamlit as st
from st_aggrid import AgGrid, JsCode

from apps.utils.google import read_gbq
from apps.wizard.app_pages.producer_analytics.utils import (
    GRAPHERS_BASE_URL,
    MIN_DATE,
    TODAY,
    columns_producer,
    make_grid,
)
from etl.snapshot import Snapshot
from etl.version_tracker import VersionTracker


@st.cache_data(show_spinner=False)
def get_producer_analytics_per_chart(df_expanded):
    # Create an expanded table with number of views per chart.
    df_renders_per_chart = df_expanded.dropna(subset=["grapher"]).fillna(0).reset_index(drop=True)
    df_renders_per_chart = df_renders_per_chart.sort_values("renders_custom", ascending=False).reset_index(drop=True)

    return df_renders_per_chart


@st.cache_data(show_spinner=False)
def get_producer_charts_analytics(min_date, max_date, excluded_steps):
    # Get chart renders using user-defined date range for "renders_custom".
    # st.toast("⌛ Getting analytics on chart renders...")
    df_renders = get_chart_renders(min_date=min_date, max_date=max_date)

    # Load the steps dataframe with producer data.
    df_expanded = load_steps_df_with_producer_data(excluded_steps=excluded_steps)

    # Add columns with the numbers of chart renders.
    df_expanded = df_expanded.merge(df_renders, on="grapher", how="left").drop(columns=["all_chart_slugs"])

    return df_expanded


@st.cache_data(show_spinner=False)
def load_steps_df_with_producer_data(excluded_steps) -> pd.DataFrame:
    # Load steps dataframe.
    # st.toast("⌛ Loading data from VersionTracker...")
    steps_df = load_steps_df(excluded_steps=excluded_steps)

    # st.toast("⌛ Processing VersionTracker data...")
    # Select only active snapshots.
    df = steps_df[(steps_df["channel"] == "snapshot") & (steps_df["state"] == "active")].reset_index(drop=True)

    # Select only relevant columns.
    df = df[["step", "all_chart_slugs"]]

    # Add a column of producer to steps df (where possible).
    for i, row in df.iterrows():
        snap_uri = row["step"].split("snapshot://" if "snapshot://" in row["step"] else "snapshot-private://")[1]
        snap = Snapshot(snap_uri)
        origin = snap.metadata.origin
        if (origin is not None) and (snap.metadata.namespace not in ["dummy"]):
            producer = snap.metadata.origin.producer  # type: ignore
            df.loc[i, "producer"] = producer

    # Select only relevant columns.
    df = df[["all_chart_slugs", "producer"]]

    # Remove rows with no producer.
    df = df.dropna(subset=["producer"]).reset_index(drop=True)

    # Ignore the chart id, and keep only the slug.
    df["all_chart_slugs"] = [sorted(set([slug for _, slug in id_slug])) for id_slug in df["all_chart_slugs"]]

    # Create a row for each producer-slug pair. Fill with "" (in cases where the producer has no charts).
    df_expanded = df.explode("all_chart_slugs")

    # Remove duplicates.
    # NOTE: This happens because df contains one row per snapshot. Some grapher datasets come from a combination of multiple snapshots (often from the same producer). We want to count producer-chart pairs only once.
    df_expanded = df_expanded.drop_duplicates(subset=["producer", "all_chart_slugs"]).reset_index(drop=True)

    # Add a column for grapher URL.
    df_expanded["grapher"] = GRAPHERS_BASE_URL + df_expanded["all_chart_slugs"]

    return df_expanded


@st.cache_data(show_spinner=False)
def load_steps_df(excluded_steps) -> pd.DataFrame:
    # Load steps dataframe.
    steps_df = VersionTracker(exclude_steps=excluded_steps).steps_df

    return steps_df


@st.cache_data(show_spinner=False)
def get_chart_renders(min_date: str, max_date: str) -> pd.DataFrame:
    # List ranges of dates to fetch views.
    date_ranges = {
        "renders_365d": ((TODAY - timedelta(days=365)).strftime("%Y-%m-%d"), TODAY.strftime("%Y-%m-%d")),
        "renders_30d": ((TODAY - timedelta(days=30)).strftime("%Y-%m-%d"), TODAY.strftime("%Y-%m-%d")),
        "renders_custom": (min_date, max_date),  # Use user-defined date range.
    }

    # Get analytics for those ranges, for all grapher URLs.
    list_renders = [
        get_grapher_views(date_start=date_start, date_end=date_end, grapher_urls=None, groupby=["grapher"]).rename(
            columns={"renders": column_name}
        )
        for column_name, (date_start, date_end) in date_ranges.items()
    ]

    # Merge all dataframes.
    df_renders = pr.multi_merge(list_renders, on="grapher", how="outer")  # type: ignore

    return df_renders


def plot_chart_analytics(df, min_date, max_date):
    """Show chart with analytics on producer's charts."""
    # Get total daily views of selected producers.
    grapher_urls_selected = df["grapher"].unique().tolist()  # type: ignore
    df_total_daily_views = get_grapher_views(
        date_start=min_date, date_end=max_date, groupby=["day"], grapher_urls=grapher_urls_selected
    )

    # Get daily views of the top 10 charts.
    grapher_urls_top_10 = (
        df.sort_values("renders_custom", ascending=False)["grapher"].unique().tolist()[0:10]  # type: ignore
    )
    df_top_10_daily_views = get_grapher_views(
        date_start=min_date, date_end=max_date, groupby=["day", "grapher"], grapher_urls=grapher_urls_top_10
    )

    # Get total number of views and average daily views.
    total_views = df_total_daily_views["renders"].sum()
    average_daily_views = df_total_daily_views["renders"].mean()
    # Get total views of the top 10 charts in the selected date range.
    df_top_10_total_views = df_top_10_daily_views.groupby("grapher", as_index=False).agg({"renders": "sum"})

    # Create a line chart.
    df_plot = pd.concat([df_total_daily_views.assign(**{"grapher": "Total"}), df_top_10_daily_views]).rename(
        columns={"grapher": "Chart slug"}
    )
    df_plot["Chart slug"] = df_plot["Chart slug"].apply(lambda x: x.split("/")[-1])
    df_plot["day"] = pd.to_datetime(df_plot["day"]).dt.strftime("%a. %Y-%m-%d")
    fig = px.line(
        df_plot,
        x="day",
        y="renders",
        color="Chart slug",
        title="Total daily views and views of top 10 charts",
    ).update_layout(xaxis_title=None, yaxis_title=None)

    # Display the chart.
    st.plotly_chart(fig, use_container_width=True)

    return total_views, average_daily_views, df_top_10_total_views


@st.cache_data(show_spinner=False)
def get_grapher_views(
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


def show_producer_charts_grid(df, min_date, max_date):
    """Show table with analytics on producer's charts."""
    # Configure and display the second table.
    # Create a JavaScript renderer for clickable slugs.
    grapher_slug_jscode = JsCode(
        r"""
        class UrlCellRenderer {
        init(params) {
            this.eGui = document.createElement('a');
            if (params.value) {
                // Extract the slug from the full URL.
                const url = new URL(params.value);
                const slug = url.pathname.split('/').pop();  // Get the last part of the path as the slug.
                this.eGui.innerText = slug;
                this.eGui.setAttribute('href', params.value);
            } else {
                this.eGui.innerText = '';
            }
            this.eGui.setAttribute('style', "text-decoration:none; color:blue");
            this.eGui.setAttribute('target', "_blank");
        }
        getGui() {
            return this.eGui;
        }
        }
        """
    )

    # Define columns to be shown, including the cell renderer for "grapher".
    COLUMNS_PRODUCERS = columns_producer(min_date, max_date)
    COLUMNS_PRODUCER_CHARTS = {
        column: (
            {
                "headerName": "Chart URL",
                "headerTooltip": "URL of the chart in the grapher.",
                "cellRenderer": grapher_slug_jscode,
            }
            if column == "grapher"
            else COLUMNS_PRODUCERS[column]
        )
        for column in ["renders_custom", "producer", "renders_365d", "renders_30d", "grapher"]
    }

    grid_options = make_grid(df, COLUMNS_PRODUCER_CHARTS, selection=False)

    # Display the grid.
    AgGrid(
        data=df,
        gridOptions=grid_options,
        height=500,
        width="100%",
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        theme="streamlit",
        # excel_export_mode=ExcelExportMode.MANUAL,  # Doesn't work?
    )
