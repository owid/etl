from datetime import datetime, timedelta
from typing import cast

import owid.catalog.processing as pr
import pandas as pd
import streamlit as st
from pandas_gbq import read_gbq
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from structlog import get_logger

from etl.snapshot import Snapshot
from etl.version_tracker import VersionTracker

# Initialize log.
log = get_logger()

# Define constants.
TODAY = datetime.today()
GRAPHERS_BASE_URL = "https://ourworldindata.org/grapher/"

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Producer analytics",
    page_icon="🪄",
)


########################################################################################################################
# FUNCTIONS
########################################################################################################################


def get_chart_renders_query(date_start, date_end) -> pd.DataFrame:
    df_views = read_gbq(
        f"""
        SELECT grapher, SUM(events) AS renders
        FROM prod_google_analytics4.grapher_views_by_day_page_grapher_device_country_iframe
        WHERE
            day >= '{date_start}'
            AND day <= '{date_end}'
            AND grapher LIKE '{GRAPHERS_BASE_URL}%'
        GROUP BY grapher
        """,
        project_id="owid-analytics",
    )
    return cast(pd.DataFrame, df_views)


@st.cache_data
def get_chart_renders() -> pd.DataFrame:
    # List ranges of dates to fetch views.
    date_ranges = {
        "renders_365d": ((TODAY - timedelta(days=365)).strftime("%Y-%m-%d"), TODAY.strftime("%Y-%m-%d")),
        "renders_30d": ((TODAY - timedelta(days=30)).strftime("%Y-%m-%d"), TODAY.strftime("%Y-%m-%d")),
        "renders_all": ("2000-01-01", TODAY.strftime("%Y-%m-%d")),
    }

    # Get analytics for those ranges, for all grapher URLs.
    list_renders = [
        get_chart_renders_query(date_start, date_end).rename(columns={"renders": column_name})
        for column_name, (date_start, date_end) in date_ranges.items()
    ]

    # Merge all dataframes.
    df_renders = pr.multi_merge(list_renders, on="grapher", how="outer")  # type: ignore

    return df_renders


@st.cache_data
def load_steps_df() -> pd.DataFrame:
    # Load steps dataframe.
    steps_df = VersionTracker().steps_df

    return steps_df


@st.cache_data
def load_steps_df_with_producer_data() -> pd.DataFrame:
    # Load steps dataframe.
    steps_df = load_steps_df()

    # Select only active snapshots.
    df = steps_df[(steps_df["channel"] == "snapshot") & (steps_df["state"] == "active")].reset_index(drop=True)

    # Select only relevant columns.
    df = df[["step", "all_chart_slugs"]]

    # TODO: Currently, the analytics are heavily affected by a few producers, namely those that are involved in the population and income groups datasets. Ideally, we should be able to ignore them (as they are used mostly as auxiliary data). Note that FAOSTAT is also loaded as an auxiliary dataset (specifically faostat_rl, I think to get the surface area of each country). I was considering here to have a list of snapshot/walden/github steps that should be removed from df. But a better approach would be to let VersionTracker ingest a DAG as an argument. This way, we could load the original data, and manually remove the dependencies of the garden population and income groups steps. Then, pass this DAG to VersionTracker, and see the resulting analytics. With this approach, we would be able to properly account for any un_wpp or faostat use that are not related to population.

    # Add a column of producer to steps df (where possible).
    for i, row in df.iterrows():
        snap_uri = row["step"].split("snapshot://" if "snapshot://" in row["step"] else "snapshot-private://")[1]
        snap = Snapshot(snap_uri)
        origin = snap.metadata.origin
        if (origin is not None) and (snap.metadata.namespace not in ["dummy"]):
            producer = snap.metadata.origin.producer  # type: ignore
            df.loc[i, "producer"] = producer

    # Select only relevant columns.
    df = df[["producer", "all_chart_slugs"]]

    # Remove rows with no producer.
    df = df.dropna(subset=["producer"]).reset_index(drop=True)

    # Ignore the chart id, and keep only the slug.
    df["all_chart_slugs"] = [sorted(set([slug for _, slug in id_slug])) for id_slug in df["all_chart_slugs"]]

    # Create a row for each producer-slug pair. Fill with "" (in cases where the producer has no charts).
    df_expanded = df.explode("all_chart_slugs")

    # Remove duplicates.
    # NOTE: This happens because df contains one row per snapshot. Some grapher datasets come from a combination of multiple snapshots (often from the same producer). We want to count producer-chart pairs only once.
    # TODO: Consider also counting how many grapher datasets come from each producer. We cannot simply count unique steps, because that would be counting several versions of the same step. We could count identifiers. We can then estimate the number of DB datasets by counting how many identifiers have the grapher channel.
    df_expanded = df_expanded.drop_duplicates(subset=["producer", "all_chart_slugs"]).reset_index(drop=True)

    # Add a column for grapher URL.
    df_expanded["grapher"] = GRAPHERS_BASE_URL + df_expanded["all_chart_slugs"]

    return df_expanded


@st.cache_data
def get_producer_charts_analytics():
    # Get the number of renders for each chart.
    df_renders = get_chart_renders()

    # Load the steps dataframe with producer data.
    df_expanded = load_steps_df_with_producer_data()

    # Add columns with the numbers of chart renders.
    df_expanded = df_expanded.merge(df_renders, on="grapher", how="left").drop(columns=["all_chart_slugs"])

    return df_expanded


@st.cache_data
def get_producer_analytics_per_chart():
    # Load the steps dataframe with producer data and analytics.
    df_expanded = get_producer_charts_analytics()

    # Create an expanded table with number of views per chart.
    df_renders_per_chart = df_expanded.dropna(subset=["grapher"]).fillna(0).reset_index(drop=True)
    df_renders_per_chart.sort_values("renders_all")
    df_renders_per_chart[df_renders_per_chart["producer"] == "Global Carbon Project"]["renders_all"].sum()

    return df_renders_per_chart


@st.cache_data
def get_producer_analytics_per_producer():
    # Load the steps dataframe with producer data and analytics.
    df_expanded = get_producer_charts_analytics()

    # Group by producer and get the full list of chart slugs for each producer.
    df_grouped = df_expanded.groupby("producer", observed=True, as_index=False).agg(
        {
            "grapher": lambda x: [item for item in x if pd.notna(item)],  # Filter out NaN values
            "renders_365d": "sum",
            "renders_30d": "sum",
            "renders_all": "sum",
        }
    )
    df_grouped["n_charts"] = df_grouped["grapher"].apply(len)

    # Check if lists are unique. If not, make them unique in the previous line.
    error = "Duplicated chart slugs found for a given producer."
    assert df_grouped["grapher"].apply(lambda x: len(x) == len(set(x))).all(), error

    # Sort conveniently.
    df_grouped = df_grouped.sort_values(["renders_all"], ascending=False).reset_index(drop=True)

    # Create a separate dataframe with the top producers, just for curiosity.
    # df_check = df_grouped[["producer", "renders_30d", "n_charts"]].sort_values(["renders_30d"], ascending=False).reset_index(drop=True).head(20)
    # df_check["percentage"] = (df_check["renders_30d"] / df_check["renders_30d"].sum() * 100).round(1)

    return df_grouped


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/search: Producer analytics")


########################################
# Display TABLE
########################################
# Load table content and select only columns to be shown.
steps_df_display = get_producer_analytics_per_producer()

# Define the options of the main grid table with pagination.
gb = GridOptionsBuilder.from_dataframe(steps_df_display)
gb.configure_grid_options(domLayout="autoHeight", enableCellTextSelection=True)
gb.configure_selection(
    selection_mode="multiple",
    use_checkbox=True,
    rowMultiSelectWithClick=True,
    suppressRowDeselection=False,
    groupSelectsChildren=True,
    groupSelectsFiltered=True,
)
gb.configure_default_column(editable=False, groupable=True, sortable=True, filterable=True, resizable=True)
gb.configure_column(
    "n_charts", headerName="N. charts", width=120, headerTooltip="Number of charts that use data from the step."
)
# gb.configure_column(
#     "db_dataset_name_and_url",  # This will be the displayed column
#     headerName="Grapher dataset",
#     cellRenderer=grapher_dataset_jscode,
#     headerTooltip="Name of the grapher dataset (if any), linked to its corresponding dataset admin page.",
# )
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
grid_options = gb.build()

# Display the grid table with pagination.
grid_response = AgGrid(
    data=steps_df_display,
    gridOptions=grid_options,
    height=1000,
    width="100%",
    update_mode=GridUpdateMode.MODEL_CHANGED,
    fit_columns_on_grid_load=False,
    allow_unsafe_jscode=True,
    theme="streamlit",
    # The following ensures that the pagination controls are not cropped.
    custom_css={
        "#gridToolBar": {
            "padding-bottom": "0px !important",
        }
    },
)
