from datetime import datetime, timedelta
from typing import Optional, cast

import owid.catalog.processing as pr
import pandas as pd
import pandas_gbq
import plotly.express as px
import streamlit as st
from google.oauth2 import service_account
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from structlog import get_logger

from apps.wizard.utils.components import st_horizontal
from etl.config import GOOGLE_APPLICATION_CREDENTIALS
from etl.snapshot import Snapshot
from etl.version_tracker import VersionTracker

# Initialize log.
log = get_logger()

# Define constants.
TODAY = datetime.today()
# Date when the new views metric started to be recorded.
MIN_DATE = datetime.strptime("2024-11-01", "%Y-%m-%d")
GRAPHERS_BASE_URL = "https://ourworldindata.org/grapher/"
# List of auxiliary steps to be (optionally) excluded from the DAG.
# It may be convenient to ignore these steps because the analytics are heavily affected by a few producers (e.g. those that are involved in the population and income groups datasets).
AUXILIARY_STEPS = [
    "data://garden/demography/.*/population",
    # Primary energy consumption is loaded by GCB.
    "data://garden/energy/.*/primary_energy_consumption",
    "data://garden/ggdc/.*/maddison_project_database",
    "data://garden/wb/.*/income_groups",
]

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Producer analytics",
    layout="wide",
    page_icon="🪄",
)


########################################################################################################################
# FUNCTIONS & GLOBAL VARS
########################################################################################################################
def columns_producer(min_date, max_date):
    # Define columns to be shown.
    cols_prod = {
        "producer": {
            "headerName": "Producer",
            "headerTooltip": "Name of the producer. This is NOT the name of the dataset.",
        },
        "n_charts": {
            "headerName": "Charts",
            "headerTooltip": "Number of charts using data from a producer.",
        },
        "renders_custom": {
            "headerName": "Views in custom range",
            "headerTooltip": f"Number of renders between {min_date} and {max_date}.",
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
    return cols_prod


def read_gbq(*args, **kwargs) -> pd.DataFrame:
    if GOOGLE_APPLICATION_CREDENTIALS:
        # Use service account
        credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)
        return pandas_gbq.read_gbq(*args, **kwargs, credentials=credentials)  # type: ignore
    else:
        # Use browser authentication.
        return pandas_gbq.read_gbq(*args, **kwargs)  # type: ignore


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


@st.cache_data(show_spinner=False)
def load_steps_df(excluded_steps) -> pd.DataFrame:
    # Load steps dataframe.
    steps_df = VersionTracker(exclude_steps=excluded_steps).steps_df

    return steps_df


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
def get_producer_analytics_per_chart(min_date, max_date, excluded_steps):
    # Load the steps dataframe with producer data and analytics.
    df_expanded = get_producer_charts_analytics(min_date=min_date, max_date=max_date, excluded_steps=excluded_steps)

    # Create an expanded table with number of views per chart.
    df_renders_per_chart = df_expanded.dropna(subset=["grapher"]).fillna(0).reset_index(drop=True)
    df_renders_per_chart = df_renders_per_chart.sort_values("renders_custom", ascending=False).reset_index(drop=True)

    return df_renders_per_chart


@st.cache_data(show_spinner=False)
def get_producer_analytics_per_producer(min_date, max_date, excluded_steps):
    # Load the steps dataframe with producer data and analytics.
    df_expanded = get_producer_charts_analytics(min_date=min_date, max_date=max_date, excluded_steps=excluded_steps)

    # st.toast("⌛ Adapting the data for presentation...")
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

    return df_grouped


def show_producers_grid(df_producers, min_date, max_date):
    """Show table with producers analytics."""
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

    # Configure individual columns with specific settings.
    COLUMNS_PRODUCERS = columns_producer(min_date, max_date)
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
        # excel_export_mode=ExcelExportMode.MANUAL,  # Doesn't work?
    )

    # Get the selected producers from the first table.
    producers_selected = [row["producer"] for row in grid_response["selected_rows"]]

    return producers_selected


def plot_chart_analytics(df):
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


def show_producer_charts_grid(df):
    """Show table with analytics on producer's charts."""
    # Configure and display the second table.
    gb2 = GridOptionsBuilder.from_dataframe(df)
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
        for column in ["producer", "renders_custom", "renders_365d", "renders_30d", "grapher"]
    }
    # Configure and display the second table.
    gb2 = GridOptionsBuilder.from_dataframe(df)
    gb2.configure_grid_options(domLayout="autoHeight", enableCellTextSelection=True)
    gb2.configure_default_column(editable=False, groupable=True, sortable=True, filterable=True, resizable=True)

    # Apply column configurations directly from the dictionary.
    for column, config in COLUMNS_PRODUCER_CHARTS.items():
        gb2.configure_column(column, **config)

    # Configure pagination with dynamic page size.
    gb2.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
    grid_options2 = gb2.build()

    # Display the grid.
    AgGrid(
        data=df,
        gridOptions=grid_options2,
        height=500,
        width="100%",
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        theme="streamlit",
        # excel_export_mode=ExcelExportMode.MANUAL,  # Doesn't work?
    )


def prepare_summary(
    df_top_10_total_views, producers_selected, total_views, average_daily_views, min_date, max_date
) -> str:
    """Prepare summary at the end of the app."""
    # Prepare the total number of views.
    total_views_str = f"{total_views:9,}"
    # Prepare the average daily views.
    average_views_str = f"{round(average_daily_views):9,}"
    # Prepare a summary of the top 10 charts to be copy-pasted.
    if len(producers_selected) == 0:
        producers_selected_str = "all producers"
    elif len(producers_selected) == 1:
        producers_selected_str = producers_selected[0]
    else:
        producers_selected_str = ", ".join(producers_selected[:-1]) + " and " + producers_selected[-1]
    # NOTE: I tried .to_string() and .to_markdown() and couldn't find a way to keep a meaningful format.
    df_summary_str = ""
    for _, row in df_top_10_total_views.sort_values("renders", ascending=False).iterrows():
        df_summary_str += f"{row['renders']:9,}" + " - " + row["grapher"] + "\n"

    # Define the content to copy.
    summary = f"""\
Analytics of charts using data by {producers_selected_str} between {min_date} and {max_date}:
- Total number of chart views: {total_views_str}
- Average daily chart views: {average_views_str}
- Views of top performing charts:
{df_summary_str}

    """
    return summary


########################################################################################################################
# RENDER
########################################################################################################################

# Streamlit app layout.
st.title(":material/analytics: Producer analytics")
st.markdown("Explore analytics of data producers.")

# SEARCH BOX
with st.container(border=True):
    st.markdown(
        f"Select a custom date range (note that this metric started to be recorded on {MIN_DATE.strftime('%Y-%m-%d')})."
    )

    with st_horizontal(vertical_alignment="center"):
        # Create input fields for minimum and maximum dates.
        min_date = st.date_input(
            "Select minimum date",
            value=MIN_DATE,
            key="min_date",
            format="YYYY-MM-DD",
        ).strftime(  # type: ignore
            "%Y-%m-%d"
        )
        max_date = st.date_input(
            "Select maximum date",
            value=TODAY,
            key="max_date",
            format="YYYY-MM-DD",
        ).strftime(  # type: ignore
            "%Y-%m-%d"
        )
        exclude_auxiliary_steps = st.checkbox(
            "Exclude auxiliary steps (e.g. population)",
            False,
            help="Exclude steps that are commonly used as auxiliary data, so they do not skew the analytics in favor of a few producers. But note that this will exclude all uses of these steps, even when they are the main datasets (not auxiliary). Auxiliary steps are:\n- "
            + "\n- ".join(sorted(f"`{s}`" for s in AUXILIARY_STEPS)),
        )

if exclude_auxiliary_steps:
    # If the user wants to exclude auxiliary steps, take the default list of excluded steps.
    excluded_steps = AUXILIARY_STEPS
else:
    # Otherwise, do not exclude any steps.
    excluded_steps = []

########################################################################################################################
# 1/ PRODUCER ANALYTICS: Display main table, with analytics per producer.
# Allow the user to select a subset of producers.
########################################################################################################################
st.header("Analytics by producer")
st.markdown(
    "Total number of charts and chart views for each producer. Producers selected in this table will be used to filter the producer-charts table below."
)

# Load table content and select only columns to be shown.
with st.spinner("Loading producer data. We are accessing various databases. This can take few seconds..."):
    df_producers = get_producer_analytics_per_producer(
        min_date=min_date, max_date=max_date, excluded_steps=excluded_steps
    )

# Prepare and display the grid table with producer analytics.
producers_selected = show_producers_grid(
    df_producers=df_producers,
    min_date=min_date,
    max_date=max_date,
)

########################################################################################################################
# 2/ CHART ANALYTICS: Display a chart with the total number of daily views, and the daily views of the top performing charts.
########################################################################################################################
st.header("Analytics by chart")
st.markdown("Number of views for each chart that uses data by the selected producers.")

# Load detailed analytics per producer-chart.
with st.spinner("Loading chart data. This can take few seconds..."):
    df_producer_charts = get_producer_analytics_per_chart(
        min_date=min_date, max_date=max_date, excluded_steps=excluded_steps
    )

# Get the selected producers from the first table.
if len(producers_selected) == 0:
    # If no producers are selected, show all producer-charts.
    df_producer_charts_filtered = df_producer_charts
else:
    # Filter producer-charts by selected producers.
    df_producer_charts_filtered = df_producer_charts[df_producer_charts["producer"].isin(producers_selected)]

# Show chart with chart analytics, and get some summary data.
total_views, average_daily_views, df_top_10_total_views = plot_chart_analytics(df_producer_charts_filtered)

# Show table
show_producer_charts_grid(df_producer_charts_filtered)

########################################################################################################################
# 3/ SUMMARY: Display a summary to be shared with the data producer.
########################################################################################################################

# Prepare the summary to be copy-pasted.
summary = prepare_summary(
    df_top_10_total_views=df_top_10_total_views,
    producers_selected=producers_selected,
    total_views=total_views,
    average_daily_views=average_daily_views,
    min_date=min_date,
    max_date=max_date,
)

# Display the content.
st.markdown(
    """## Summary for data producers

For now, to share analytics with a data producer you can so any of the following:
- **Table export**: Right-click on a cell in the above's table and export as a CSV or Excel file.
- **Chart export**: Click on the camera icon on the top right of the chart to download the chart as a PNG.
- **Copy summary**: Click on the upper right corner of the box below to copy the summary to the clipboard.
"""
)
st.code(summary, language="text")
