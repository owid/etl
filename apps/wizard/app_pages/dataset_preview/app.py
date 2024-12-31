"""Show indicators in datasets from database.

The idea is to quickly prototype a better way to show indicators in datasets.

TODO: only works for ETL-based datasets.
"""

from collections import defaultdict

import streamlit as st

from apps.wizard.app_pages.dataset_preview.dependency_graph import load_dag_cached, show_modal_dependency_graph
from apps.wizard.app_pages.dataset_preview.utils import (
    IndicatorsInCharts,
    IndicatorsInExplorers,
    IndicatorSingleDimension,
    IndicatorWithDimensions,
    get_charts_views,
    get_datasets,
    get_explorers_views,
    get_table_charts,
    get_table_explorers,
    get_users,
    show_table_charts,
    show_table_explorers,
)
from apps.wizard.utils.components import Pagination, grapher_chart, st_horizontal, st_tag
from etl.config import OWID_ENV
from etl.grapher.io import load_variables_in_dataset

ICONS_DIMENSIONS = {
    "age": ":material/cake:",
    "sex": ":material/wc:",
}


def parse_indicators(indicators_raw):
    """Build list with indicators.

    It groups indicators whenever applicable. This will make it easier to show them in the UI.
    """
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
    """Optional function to sort list of indicators.

    TODO: add filters / sorting options in UI.
    """
    indicators.sort(key=lambda x: (x.key is None, x.key))
    return indicators


def prompt_dataset_options():
    """Ask user which dataset they want!

    It also syncs the selection with query params.
    """
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


def prompt_display_charts():
    """Show charts or not."""
    if "display_charts" in st.session_state:
        st.query_params["displayCharts"] = str(st.session_state["display_charts"])
    show_charts = st.query_params.get("displayCharts", "True") == "True"
    return st.checkbox(
        "Display charts",
        key="display_charts",
        help="Uncheck to show only indicator descriptions. This avoids rendering charts and can improve performance.",
        value=show_charts,
    )


@st.fragment
def st_show_indicator(indicator, indicator_charts, display_charts=True):
    """Display indicator."""
    with st.container(border=False):
        # Allocate space for indicator title / URI
        st_header = st.container()
        st_metadata_left, st_metadata_right = st.columns(2)

        with st_metadata_right:
            # Show dimensions as pills -- TODO: add icons for recognized dimensions
            if indicator.is_mdim:
                # Dimensions
                with st.container(border=True):
                    st.markdown("**Dimensions**")
                    dim_values_dix = {}
                    for dim in indicator.dimensions.keys():
                        key_pills = f"dataset_pills_{indicator.key}_{dim}"
                        options = indicator.get_dimensions_conditioned(dim, dim_values_dix)
                        st.pills(
                            dim,
                            options,
                            key=key_pills,
                            default=options[0],
                        )

                        dim_value_ = st.session_state.get(key_pills)
                        dim_values_dix[dim] = dim_value_
                    dim_values = tuple(dim_values_dix.values())

                # Sanity check on dimensions
                assert all(value is not None for value in dim_values)

                # Get indicator-dimensions combination
                var = indicator.get_dimension(dim_values)
            else:
                # st.markdown("No dimensions")
                var = indicator.indicators[0]

            # Charts
            df_charts = get_table_charts(indicator_charts, USERS, CHART_VIEWS, var.id)
            show_table_charts(df_charts)

            # Explorers
            df_explorers = get_table_explorers(indicator_explorers, EXPLORER_VIEWS, var.id)
            show_table_explorers(df_explorers)

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
            if not display_charts:
                if var.descriptionShort:
                    st.markdown(var.descriptionShort)
            else:
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

# Get datasets from DB / cached
DATASETS = get_datasets()
CHART_VIEWS = get_charts_views()
EXPLORER_VIEWS = get_explorers_views()
USERS = get_users()
DAG = load_dag_cached()

# Get datasets
dataset_options = list(DATASETS.keys())

# Show dataset search bar
DATASET_ID = prompt_dataset_options()
# DATASET_ID = 6617  # DEBUG
DISPLAY_CHARTS = prompt_display_charts()

# DATASET_ID = 6869, 6813
if DATASET_ID is not None:
    dataset = DATASETS[DATASET_ID]

    # 1/ Get indicators from dataset
    indicators_raw = load_variables_in_dataset(dataset_id=[int(DATASET_ID)])

    ## Chart info
    indicator_charts = IndicatorsInCharts.from_indicators(indicators_raw)

    ## Chart info
    indicator_explorers = IndicatorsInExplorers.from_indicators(indicators_raw)

    ## Parse indicators
    indicators = parse_indicators(indicators_raw)

    # 2/ Get charts
    df_charts = get_table_charts(indicator_charts, USERS, CHART_VIEWS)
    df_explorers = get_table_explorers(indicator_explorers, EXPLORER_VIEWS)

    # 3/ Present Dataset
    title = dataset["name"]
    st.header(f"[{title}]({OWID_ENV.dataset_admin_site(DATASET_ID)})")

    with st_horizontal():
        st.markdown(f":material/schedule: Last modified: {dataset['updatedAt'].strftime('%Y-%m-%d')}")
        st.markdown(f"{len(indicators)} indicators")
        st.markdown(f"{len(df_charts)} charts")

        if dataset["isPrivate"] == 1:
            st_tag("Private", color="blue", icon=":material/lock")
        if dataset["isArchived"] == 1:
            st_tag("Archived", color="red", icon=":material/delete_forever")
        # Any mdim?
        if any(ind.is_mdim for ind in indicators):
            st_tag(tag_name="indicators with dimensions", color="primary", icon=":material/deployed_code")

    @st.fragment
    def show_button():
        st.button(
            "Dependency graph",
            icon=":material/account_tree:",
            on_click=lambda dataset=dataset: show_modal_dependency_graph(dataset, DAG),
        )

    show_button()

    # 4/ Tabs
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
            st_show_indicator(item, indicator_charts, DISPLAY_CHARTS)
            st.divider()

    with tab_charts:
        st.markdown("#### Charts")
        show_table_charts(df_charts)
        st.markdown("#### Explorers")
        show_table_explorers(df_explorers)
        st.markdown("#### Most frequent chart editors")
        user_counts = df_charts["User"].value_counts()
        st.dataframe(user_counts, use_container_width=True)
