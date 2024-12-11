from datetime import datetime, timedelta
from typing import Optional, cast

import owid.catalog.processing as pr
import pandas as pd
import streamlit as st
from pandas_gbq import read_gbq
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from structlog import get_logger

from apps.wizard.utils.components import st_horizontal
from etl.snapshot import Snapshot
from etl.version_tracker import VersionTracker

# Initialize log.
log = get_logger()

# Define constants.
TODAY = datetime.today()
# Date when the new views metric started to be recorded.
MIN_DATE = datetime.strptime("2024-11-01", "%Y-%m-%d")
GRAPHERS_BASE_URL = "https://ourworldindata.org/grapher/"

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Producer analytics",
    layout="wide",
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


@st.cache_data
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
    df_expanded = df_expanded.drop_duplicates(subset=["producer", "all_chart_slugs"]).reset_index(drop=True)

    # Add a column for grapher URL.
    df_expanded["grapher"] = GRAPHERS_BASE_URL + df_expanded["all_chart_slugs"]

    return df_expanded


@st.cache_data
def get_producer_charts_analytics(min_date, max_date):
    # Get chart renders using user-defined date range for "renders_custom".
    df_renders = get_chart_renders(min_date=min_date, max_date=max_date)

    # Load the steps dataframe with producer data.
    df_expanded = load_steps_df_with_producer_data()

    # Add columns with the numbers of chart renders.
    df_expanded = df_expanded.merge(df_renders, on="grapher", how="left").drop(columns=["all_chart_slugs"])

    return df_expanded


@st.cache_data
def get_producer_analytics_per_chart(min_date, max_date):
    # Load the steps dataframe with producer data and analytics.
    df_expanded = get_producer_charts_analytics(min_date=min_date, max_date=max_date)

    # Create an expanded table with number of views per chart.
    df_renders_per_chart = df_expanded.dropna(subset=["grapher"]).fillna(0).reset_index(drop=True)
    df_renders_per_chart = df_renders_per_chart.sort_values("renders_custom", ascending=False).reset_index(drop=True)

    return df_renders_per_chart


@st.cache_data
def get_producer_analytics_per_producer(min_date, max_date):
    # Load the steps dataframe with producer data and analytics.
    df_expanded = get_producer_charts_analytics(min_date=min_date, max_date=max_date)

    # Group by producer and get the full list of chart slugs for each producer.
    df_grouped = df_expanded.groupby("producer", observed=True, as_index=False).agg(
        {
            "grapher": lambda x: [item for item in x if pd.notna(item)],  # Filter out NaN values
            "renders_365d": "sum",
            "renders_30d": "sum",
            "renders_custom": "sum",
        }
    )
    df_grouped["n_charts"] = df_grouped["grapher"].apply(len)

    # Check if lists are unique. If not, make them unique in the previous line.
    error = "Duplicated chart slugs found for a given producer."
    assert df_grouped["grapher"].apply(lambda x: len(x) == len(set(x))).all(), error

    # Drop unnecessary columns.
    df_grouped = df_grouped.drop(columns=["grapher"])

    # Sort conveniently.
    df_grouped = df_grouped.sort_values(["renders_custom"], ascending=False).reset_index(drop=True)

    # Create a separate dataframe with the top producers, just for curiosity.
    # df_check = df_grouped[["producer", "renders_30d", "n_charts"]].sort_values(["renders_30d"], ascending=False).reset_index(drop=True).head(20)
    # df_check["percentage"] = (df_check["renders_30d"] / df_check["renders_30d"].sum() * 100).round(1)

    return df_grouped


def prepare_summary(df_producer_charts_filtered, producers_selected, min_date, max_date) -> str:
    # Prepare the total number of views.
    df_total_str = f"{df_producer_charts_filtered['renders_custom'].sum():9,}".replace(",", " ")
    # Prepare a summary of the top 10 charts to be copy-pasted.
    if len(producers_selected) == 0:
        producers_selected_str = "all producers"
    elif len(producers_selected) == 1:
        producers_selected_str = producers_selected[0]
    else:
        producers_selected_str = ", ".join(producers_selected[:-1]) + " and " + producers_selected[-1]
    # NOTE: I tried .to_string() and .to_markdown() and couldn't find a way to keep a meaningful format.
    df_summary_str = ""
    for i, row in df_producer_charts_filtered.head(10).iterrows():
        df_summary_str += f"{row['renders_custom']:9,}".replace(",", " ") + " - " + row["grapher"] + "\n"

    # Define the content to copy.
    summary = f"""\
Analytics for {producers_selected_str} between {min_date} and {max_date}:

{df_summary_str}

Total number of views of all charts: {df_total_str}

    """
    return summary


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title("📊 Producer analytics")

st.markdown(
    f"""\
Explore analytics of data producers.

Select the minimum and maximum dates for the custom date range. Note that this metric started to be recorded on {MIN_DATE.strftime("%Y-%m-%d")}.
"""
)

with st_horizontal():
    # Create input fields for minimum and maximum dates.
    min_date = st.date_input("Select minimum date", value=MIN_DATE, key="min_date", format="YYYY-MM-DD").strftime(  # type: ignore
        "%Y-%m-%d"
    )
    max_date = st.date_input("Select maximum date", value=TODAY, key="max_date", format="YYYY-MM-DD").strftime(  # type: ignore
        "%Y-%m-%d"
    )

st.markdown(
    """## Producers table

Total number of charts and chart views for each producer. Producers selected in this table will be used to filter the producer-charts table below.
"""
)

########################################
# Display main table with producer analytics.
########################################

# Load table content and select only columns to be shown.
df_producers = get_producer_analytics_per_producer(min_date=min_date, max_date=max_date)

# Define the options of the main grid table with pagination.
gb = GridOptionsBuilder.from_dataframe(df_producers)
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

# Enable column auto-sizing for the grid.
gb.configure_grid_options(suppressSizeToFit=False)  # Allows dynamic resizing to fit.
gb.configure_default_column(autoSizeColumns=True)  # Ensures all columns can auto-size.

# Define columns to be shown.
COLUMNS_PRODUCERS = {
    "producer": {
        "headerName": "Producer",
        "headerTooltip": "Name of the producer.",
    },
    "n_charts": {
        "headerName": "Charts",
        "headerTooltip": "Number of charts using data from a producer.",
    },
    "renders_custom": {
        "headerName": "Views in custom range",
        "headerTooltip": f"Number of renders betweeen {min_date} and {max_date}.",
    },
    "renders_365d": {
        "headerName": "Views 365 days",
        "headerTooltip": "Number of renders in the last 365 days.",
    },
    "renders_30d": {
        "headerName": "Views 30 days",
        "headerTooltip": "Number of renders in the last 30 days.",
    },
}

# Configure individual columns with specific settings.
for column in COLUMNS_PRODUCERS:
    gb.configure_column(column, **COLUMNS_PRODUCERS[column])
# Configure pagination with dynamic page size.
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
# Build the grid options.
grid_options = gb.build()
# Custom CSS to ensure the table stretches across the page.
custom_css = {
    ".ag-theme-streamlit": {
        "max-width": "100% !important",
        "width": "100% !important",
        "margin": "0 auto !important",  # Centers the grid horizontally.
    },
}
# Display the grid table with the updated grid options.
grid_response = AgGrid(
    data=df_producers,
    gridOptions=grid_options,
    height=1000,
    width="100%",
    update_mode=GridUpdateMode.MODEL_CHANGED,
    fit_columns_on_grid_load=True,  # Automatically adjust columns when the grid loads.
    allow_unsafe_jscode=True,
    theme="streamlit",
    custom_css=custom_css,
)

########################################
# Display main table with producer analytics.
########################################

st.markdown(
    """## Producer-charts table

Number of chart views for each chart that uses data of the selected producers.
"""
)

# Load detailed analytics per producer-chart.
df_producer_charts = get_producer_analytics_per_chart(min_date=min_date, max_date=max_date)

# Get the selected producers from the first table.
producers_selected = [row["producer"] for row in grid_response["selected_rows"]]
if len(producers_selected) == 0:
    # If no producers are selected, show all producer-charts.
    df_producer_charts_filtered = df_producer_charts
else:
    # Filter producer-charts by selected producers.
    df_producer_charts_filtered = df_producer_charts[df_producer_charts["producer"].isin(producers_selected)]

# Configure and display the second table.
gb2 = GridOptionsBuilder.from_dataframe(df_producer_charts_filtered)
gb2.configure_grid_options(domLayout="autoHeight", enableCellTextSelection=True)
gb2.configure_default_column(editable=False, groupable=True, sortable=True, filterable=True, resizable=True)

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
    for column in ["producer", "renders_custom", "renders_365d", "renders_30d", "grapher"]
}
# Configure and display the second table.
gb2 = GridOptionsBuilder.from_dataframe(df_producer_charts_filtered)
gb2.configure_grid_options(domLayout="autoHeight", enableCellTextSelection=True)
gb2.configure_default_column(editable=False, groupable=True, sortable=True, filterable=True, resizable=True)

# Apply column configurations directly from the dictionary.
for column, config in COLUMNS_PRODUCER_CHARTS.items():
    gb2.configure_column(column, **config)

# Configure pagination with dynamic page size.
gb2.configure_pagination(paginationAutoPageSize=False, paginationPageSize=30)
grid_options2 = gb2.build()

# Display the grid.
AgGrid(
    data=df_producer_charts_filtered,
    gridOptions=grid_options2,
    height=500,
    width="100%",
    fit_columns_on_grid_load=True,
    allow_unsafe_jscode=True,
    theme="streamlit",
)

# Prepare the summary to be copy-pasted.
summary = prepare_summary(
    df_producer_charts_filtered=df_producer_charts_filtered,
    producers_selected=producers_selected,
    min_date=min_date,
    max_date=max_date,
)

# Display the content.
st.markdown(
    """## Summary for data producers

You can copy the following summary (click on the upper right of the box) and paste it in an email.


If they want more details, you can right-click on the table above and export as a CSV or Excel file.
"""
)
st.code(summary, language="text")

# TODO:
# * It would be good to have a toggle button to ignore auxiliary datasets (namely population and income groups). Currently, the analytics are heavily affected by a few producers, namely those that are involved in the population and income groups datasets. Ideally, we should be able to ignore them (as they are used mostly as auxiliary data). Note that FAOSTAT is also loaded as an auxiliary dataset (specifically faostat_rl, I think to get the surface area of each country). I was considering here to have a list of snapshot/walden/github steps that should be removed from df. But a better approach would be to let VersionTracker ingest a DAG as an argument. This way, we could load the original data, and manually remove the dependencies of the garden population and income groups steps. Then, pass this DAG to VersionTracker, and see the resulting analytics. With this approach, we would be able to properly account for any un_wpp or faostat use that are not related to population.
# * Consider also counting how many grapher datasets come from each producer. We cannot simply count unique steps, because that would be counting several versions of the same step. We could count identifiers. We can then estimate the number of DB datasets by counting how many identifiers have the grapher channel.
# * It would be good to have a chart showing the evolution of daily views in the custom date range.
# * We could add the average number of daily views. But keep in mind that analytics have some delay (it seems maybe one or two days). We could fix max_date to be the last day with at last one view in any chart.
