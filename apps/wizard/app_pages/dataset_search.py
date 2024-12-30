"""Show indicators in datasets from database.

The idea is to quickly prototype a better way to show indicators in datasets.

TODO: only works for ETL-based datasets.
"""

from collections import defaultdict
from typing import Any, Dict

import pandas as pd
import streamlit as st

from apps.wizard.utils.components import Pagination, grapher_chart, st_horizontal, st_tag
from etl.config import OWID_ENV
from etl.db import read_sql
from etl.grapher.io import load_variables_in_dataset
from etl.grapher.model import Dataset
from etl.indicator_upgrade.indicator_update import find_charts_from_variable_ids

ICONS_DIMENSIONS = {
    "age": ":material/cake:",
    "sex": ":material/wc:",
}


# TODO: move it elsewhere
@st.cache_data
def get_charts_view():
    query = """SELECT SUBSTRING_INDEX(url, '/', -1) AS slug, views_7d, views_365d FROM analytics_pageviews WHERE url LIKE %s;"""
    param = ("%ourworldindata.org/grapher/%",)

    df = read_sql(
        query,
        OWID_ENV.engine,
        params=param,
    )
    return df.set_index("slug").to_dict(orient="index")


# @st.cache_data
def get_datasets() -> Dict[int, Dict[str, Any]]:
    df = Dataset.load_all_datasets()
    df = df.dropna(subset="catalogPath")

    # Set display name
    df["display_name"] = df["name"] + " --- " + df["catalogPath"] + " [" + df["version"] + "]"
    df = df.set_index("id", verify_integrity=True).sort_index(ascending=False)

    # Build dictionary
    dix = df.to_dict(orient="index")
    return dix  # type: ignore


class IndicatorArray:
    def __init__(self, indicators, key):
        self.indicators = indicators
        self.key = key
        self.dimensions = None

    @property
    def is_mdim(self):
        return self.dimensions is not None


class IndicatorSingleDimension(IndicatorArray):
    def __init__(self, indicator):
        """Object with a single indicator."""
        super().__init__([indicator], indicator.catalogPath)

    def get_dimension(self):
        return self.indicators[0]

    def get_default(self):
        raise self.get_dimension()


class IndicatorWithDimensions(IndicatorArray):
    def __init__(self, indicators):
        """Object with all indicator-dimensions.

        indicators: List of Variable objects. They should all belong to the same indicator.
        """
        super().__init__(indicators, self.check_and_extract_key(indicators))
        self.df = self.create_df()
        self.dimensions = self.get_dimensions()

    def get_default(self):
        dimensions = (d[0] for d in self.dimensions.values())
        return self.get_dimension(dimensions)

    def check_and_extract_key(self, indicators):
        short_name = None
        table = None
        for indicator in indicators:
            # Extract short_name
            short_name_ = indicator.dimensions["originalShortName"]
            assert isinstance(indicator.catalogPath, str), f"`catalogPath` is empty for variable {indicator.id}"
            # Extract table URI
            table_ = indicator.catalogPath.split("#")[0]

            if short_name is None:
                short_name = short_name_
            if table is None:
                table = table_

            # Checks
            assert short_name == short_name_, f"Short name mismatch: {short_name} != {short_name_}"
            assert table == table_, f"Table mismatch: {table} != {table_}"

        assert isinstance(table, str), "Table is empty"

        key = f"{table.replace('grapher/', '')}#{short_name}"
        return key

    def create_df(self):
        data = []
        for indicator in self.indicators:
            # Prepare data
            data_ = {
                "variable": indicator,
            }
            # Add dimensions, if any
            if "filters" in indicator.dimensions:
                for f in indicator.dimensions["filters"]:
                    data_[f["name"]] = f["value"]

            # Append to main list
            data.append(data_)

        # Build dataframe
        df = pd.DataFrame.from_records(data)
        columns_index = [col for col in df.columns if col != "variable"]
        df = df.set_index(columns_index)

        return df

    def get_dimensions(self):
        dimensions = {
            level: sorted(self.df.index.get_level_values(level).unique().tolist()) for level in self.df.index.names
        }
        return dimensions

    def get_dimension(self, dim_values):
        return self.df.loc[dim_values, "variable"]


class IndicatorsInCharts:
    def __init__(self, indicator_ids):
        # Get affected charts
        charts = find_charts_from_variable_ids(indicator_ids)
        # Map chart ID to chart object (1:1)
        self.chart_id_to_chart = {c.id: c for c in charts}
        # Map indicator ID to chart IDs (1:m)
        self.indicator_id_to_chart_ids = self._get_charts_by_indicator_id(charts)

    @classmethod
    def from_indicators(cls, indicators):
        indicator_ids = {v.id for v in indicators}
        return IndicatorsInCharts(indicator_ids)

    def _get_charts_by_indicator_id(self, charts):
        indicators_to_chart_ids = defaultdict(list)
        for c in charts:
            for dim in c.config["dimensions"]:
                indicators_to_chart_ids[dim["variableId"]].append(c.id)
        return indicators_to_chart_ids

    def get_charts(self, indicator_id):
        chart_ids = self.indicator_id_to_chart_ids[indicator_id]
        chart = [self.chart_id_to_chart[c] for c in chart_ids]
        return chart


def parse_indicators(indicators_raw):
    indicators = []

    # Group indicators with dimensions by short_name (add them to indicators_with_dim)
    # and those without dimensions (add them to indicators_no_dim)
    indicators_with_dim = defaultdict(list)
    for indicator in indicators_raw:
        # Add dimensions, if any
        if indicator.dimensions is not None:
            short_name = indicator.dimensions["originalShortName"]
            assert isinstance(indicator.catalogPath, str), f"`catalogPath` is empty for variable {indicator.id}"
            table = indicator.catalogPath.split("#")[0]
            key = f"{table}#{short_name}"
            indicators_with_dim[key].append(indicator)
        # Does not have dimensions
        else:
            assert isinstance(indicator.catalogPath, str), f"`catalogPath` is empty for variable {indicator.id}"
            key = indicator.catalogPath
            indicators.append(IndicatorSingleDimension(indicator))

    # Prepare objects with indicator-collection
    for key, vars in indicators_with_dim.items():
        indicators.append(IndicatorWithDimensions(vars))

    return indicators


def filter_sort_indicators(indicators):
    indicators.sort(key=lambda x: (x.key is None, x.key))
    return indicators


def _get_average_daily_views(chart_slug):
    num_views = CHART_VIEWS.get(chart_slug)

    if num_views is not None:
        num_views = num_views["views_365d"]
        return round(num_views / 365, 2)


def _get_title_chart(chart):
    if chart.publishedAt is None:
        return f"(DRAFT) {chart.config['title']}"
    return chart.config["title"]


def prompt_dataset_options():
    # Update query params if dataset is selected
    if "dataset_select" in st.session_state:
        st.query_params["datasetId"] = str(st.session_state["dataset_select"])

    # Collect Query params
    dataset_id = st.query_params.get("datasetId")

    # Correct dataset id
    if dataset_id is None:
        dataset_index = None
    else:
        dataset_id = int(dataset_id)
        if dataset_id not in dataset_options:
            st.error(f"Dataset with ID {dataset_id} not found. Please review the URL query parameters.")
            dataset_index = None
        else:
            dataset_index = dataset_options.index(dataset_id)

    # Show dropdown with options
    dataset_id = st.selectbox(
        label="Dataset",
        options=dataset_options,
        format_func=lambda x: DATASETS[x]["display_name"],
        key="dataset_select",
        placeholder="Select dataset",
        index=dataset_index,  # type: ignore
    )

    return dataset_id


@st.fragment
def st_show_indicator(indicator, indicator_charts):
    """Display indicator"""
    with st.container(border=False):
        # Allocate space for indicator title / URI
        st_header = st.container()
        st_metadata_left, st_metadata_right = st.columns(2)

        # Dimension selection
        # st.write(indicator.is_mdim)
        # st.write(indicator.dimensions)
        with st_metadata_right:
            # Show dimensions as pills -- TODO: add icons for recognized dimensions
            if indicator.is_mdim:
                # Dimensions
                with st.container(border=True):
                    st.markdown("**Dimensions**")
                    # with st_horizontal():
                    #     with st.container(border=True):
                    dim_values = []
                    for dim, options in indicator.dimensions.items():
                        key_pills = f"dataset_pills_{indicator.key}_{dim}"
                        st.pills(
                            dim,
                            options,
                            key=key_pills,
                            default=options[0],
                        )

                        dim_value_ = st.session_state.get(key_pills)
                        dim_values.append(dim_value_)
                    dim_values = tuple(dim_values)

                # Sanity check on dimensions
                assert all(value is not None for value in dim_values)

                # Get indicator-dimensions combination
                var = indicator.get_dimension(dim_values)
            else:
                # st.markdown("No dimensions")
                var = indicator.indicators[0]

            # Charts
            charts = indicator_charts.get_charts(var.id)
            df_charts = (
                pd.DataFrame(
                    {
                        # "thumbnail": [OWID_ENV.thumb_url(chart.slug) for chart in charts],  # type: ignore
                        "Id": [chart.id for chart in charts],  # type: ignore
                        "Chart": [_get_title_chart(chart) for chart in charts],  # type: ignore
                        # "Views (last 7 days)": [CHART_VIEWS.get(chart.slug)["views_7d"] for chart in charts],  # type: ignore
                        "Daily views (year-average)": [_get_average_daily_views(chart.slug) for chart in charts],  # type: ignore
                        "Edit": [OWID_ENV.chart_admin_site(chart.id) for chart in charts],  # type: ignore
                    }
                )
                .set_index("Id")
                .sort_values("Daily views (year-average)", ascending=False)
            )
            if not df_charts.empty:
                st.dataframe(
                    df_charts,
                    column_config={
                        "Edit": st.column_config.LinkColumn(
                            label="Chart",
                            help="Link to chart edit page",
                            display_text=r"Edit",
                            width="small",
                        ),
                    },
                    use_container_width=True,
                )

            # l = [chart.config["title"] for chart in charts]
            # st.table({"Charts": l})

        # Show indicator title and URI
        name = var.name
        iid = var.id
        with st_header:
            with st_horizontal():  # (vertical_alignment="center"):
                st.markdown(f"#### [**{name}**]({OWID_ENV.indicator_admin_site(iid)})")
                st.caption(var.catalogPath.replace("grapher/", ""))
                if indicator.is_mdim:
                    st_tag(tag_name="dimensions", color="primary", icon=":material/deployed_code")

        # Show chart (contains description, and other metadata fields)
        with st_metadata_left:
            # if var.descriptionShort:
            #     st.markdown(var.descriptionShort)
            grapher_chart(variable_id=iid, tab="map")  # type: ignore


# CONFIG
st.set_page_config(
    # page_title="Wizard: Dataset Explorer",
    layout="wide",
    page_icon="🪄",
    # initial_sidebar_state="collapsed",
)
PAGE_ITEMS_LIMIT = 25

# Session state
st.session_state.setdefault("indicator_selected", {})

# Get analytics
CHART_VIEWS = get_charts_view()

# Get datasets
DATASETS = get_datasets()
dataset_options = list(DATASETS.keys())

# Show dataset search bar
DATASET_ID = prompt_dataset_options()

# DATASET_ID = 6869, 6813
if DATASET_ID is not None:
    dataset = DATASETS[DATASET_ID]

    # 1/ Get indicators from dataset
    indicators_raw = load_variables_in_dataset(dataset_id=[int(DATASET_ID)])

    ## Chart info
    indicator_charts = IndicatorsInCharts.from_indicators(indicators_raw)

    ## Parse indicators
    indicators = parse_indicators(indicators_raw)

    # 2/ Get charts
    charts = indicator_charts.chart_id_to_chart.values()
    df_charts = (
        pd.DataFrame(
            {
                # "thumbnail": [OWID_ENV.thumb_url(chart.slug) for chart in charts],  # type: ignore
                "Id": [chart.id for chart in charts],  # type: ignore
                "Chart": [_get_title_chart(chart) for chart in charts],  # type: ignore
                # "Views (last 7 days)": [CHART_VIEWS.get(chart.slug)["views_7d"] for chart in charts],  # type: ignore
                "Daily views (year-average)": [_get_average_daily_views(chart.slug) for chart in charts],  # type: ignore
                "Edit": [OWID_ENV.chart_admin_site(chart.id) for chart in charts],  # type: ignore
            }
        )
        .set_index("Id")
        .sort_values("Daily views (year-average)", ascending=False)
    )

    # 3/ Present Dataset
    title = dataset["name"]
    st.header(f"[{title}]({OWID_ENV.dataset_admin_site(DATASET_ID)})")

    with st_horizontal():
        if dataset["isPrivate"] == 1:
            st_tag("Private", color="blue", icon=":material/lock")
        if dataset["isArchived"] == 1:
            st_tag("Archived", color="red", icon=":material/delete_forever")

        st.markdown(f":material/schedule: Last modified: {dataset['updatedAt'].strftime('%Y-%m-%d')}")
        st.markdown(f"{len(indicators)} indicators")
        st.markdown(f"{len(charts)} charts")

    # Tabs
    tab_indicators, tab_charts = st.tabs(["Indicators", "Charts"])

    with tab_indicators:
        # Apply filters / sorting
        indicators = filter_sort_indicators(indicators)

        # Use pagination
        pagination = Pagination(
            items=indicators,
            items_per_page=PAGE_ITEMS_LIMIT,
            pagination_key="pagination-dataset-search",
        )

        if len(indicators) > PAGE_ITEMS_LIMIT:
            pagination.show_controls(mode="bar")

        # Show items (only current page)
        for item in pagination.get_page_items():
            st_show_indicator(item, indicator_charts)
            st.divider()

    with tab_charts:
        if not df_charts.empty:
            st.dataframe(
                df_charts,
                column_config={
                    "Edit": st.column_config.LinkColumn(
                        label="Chart",
                        help="Link to chart edit page",
                        display_text=r"Edit",
                        width="small",
                    ),
                },
                use_container_width=True,
            )
        # st.text(f"{len(charts)} charts")
