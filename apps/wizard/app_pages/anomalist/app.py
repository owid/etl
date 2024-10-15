"""Anomalist app page.

The main structure of the app is implemented. Its main logic is:

1. User fills the form with datasets.
2. User submits the form.
3. The app loads the datasets and checks the database if there are already anomalies for this dataset.
    3.1 If yes: the app loads already existing anomalies. (will show a warning with option to refresh)
    3.2 If no: the app estimates the anomalies.
4. The app shows the anomalies, along with filters to sort / re-order them.

TODO:
- how to display missing-points-type 'anomalies'?
    - idea: have special 'anomaly box'
        - works at dataset level (multiple indincators may behave similarly)
        - have LLMs summarise this?
- have this working with upgrades
- have filters working

"""

import random

import pandas as pd
import streamlit as st

from apps.anomalist.cli import anomaly_detection
from apps.wizard.app_pages.anomalist.utils import create_tables, get_datasets_and_mapping_inputs
from apps.wizard.utils import cached, set_states
from apps.wizard.utils.components import Pagination, grapher_chart, st_horizontal, tag_in_md
from apps.wizard.utils.db import WizardDB

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Anomalist",
    page_icon="🪄",
    layout="wide",
)

# OTHER CONFIG
ANOMALY_TYPES = {
    "time_change": {
        "tag_name": "Time change",
        "color": "orange",
        "icon": ":material/timeline",
    },
    "upgrade_change": {
        "tag_name": "Version change",
        "color": "blue",
        "icon": ":material/upgrade",
    },
    "upgrade_missing": {
        "tag_name": "Missing point",
        "color": "red",
        "icon": ":material/hide_source",
    },
    # "ai": {
    #     "tag_name": "AI",
    #     "color": "rainbow",
    #     "icon": ":material/lightbulb",
    # },
}
ANOMALY_TYPE_NAMES = {k: v["tag_name"] for k, v in ANOMALY_TYPES.items()}

SORTING_STRATEGIES = {
    "relevance": "Relevance",
    "score": "Anomaly score",
    "population": "Population",
    "views": "Chart views",
    "population+views": "Population+views",
}
# SESSION STATE
# Datasets selected by the user in first multiselect
st.session_state.datasets_selected = st.session_state.get("datasets_selected", [])

# Indicators corresponding to datasets selected by the user
st.session_state.indicators = st.session_state.get("indicators", {})


# FLAG: True when user clicks submits form with datasets. Set to false by the end of the execution.
st.session_state.anomalist_datasets_submitted = st.session_state.get("anomalist_datasets_submitted", False)

# List with anomalies found in the selected datasets (dataset last submitted in the form by the user)
st.session_state.anomalist_anomalies = st.session_state.get("anomalist_anomalies", [])
st.session_state.anomalist_df = st.session_state.get("anomalist_df", None)
# FLAG: True if the anomalies were directly loaded from DB (not estimated)
st.session_state.anomalist_anomalies_out_of_date = st.session_state.get("anomalist_anomalies_out_of_date", False)

# Filter: Entities and indicators
st.session_state.anomalist_filter_entities = st.session_state.get("anomalist_filter_entities", [])
st.session_state.anomalist_filter_indicators = st.session_state.get("anomalist_filter_indicators", [])

# Sorting
st.session_state.anomalist_sorting_columns = st.session_state.get("anomalist_sorting_columns", [])

######################################################################
# MOCK VARIABLES AND FUNCTIONS
######################################################################
# DEBUGGING
# This should be removed and replaced with dynamic fields
YEAR_MIN = 1950
YEAR_MAX = 2021
ENTITIES_DEFAULT = [
    "Spain",
    "France",
    "Germany",
    "Italy",
    "United Kingdom",
    "United States",
    "China",
    "India",
    "Japan",
    "Brazil",
    "Russia",
    "Canada",
    "South Africa",
    "Australia",
    "Venezuela",
    "Croatia",
    "Azerbaijan",
]


def mock_anomalies_df_time_change(indicators_id, n=5):
    records = [
        {
            "entity": random.sample(ENTITIES_DEFAULT, 1)[0],
            "year": random.randint(1950, 2020),
            "score": round(random.random(), 2),
            "indicator_id": random.sample(indicators_id, 1)[0],
        }
        for i in range(n)
    ]

    df = pd.DataFrame(records)
    return df


def mock_anomalies_df_upgrade_change(indicators_id, n=5):
    records = [
        {
            "entity": random.sample(ENTITIES_DEFAULT, 1)[0],
            "year": random.randint(1950, 2020),
            "score": round(random.random(), 2),
            "indicator_id": random.sample(indicators_id, 1)[0],
        }
        for i in range(n)
    ]

    df = pd.DataFrame(records)
    return df


def mock_anomalies_df_upgrade_missing(indicators_id, n=5):
    records = [
        {
            "entity": random.sample(ENTITIES_DEFAULT, 1)[0],
            "year": random.randint(1950, 2020),
            "score": random.randint(0, 50),
            "indicator_id": random.sample(indicators_id, 1)[0],
        }
        for i in range(n)
    ]

    df = pd.DataFrame(records)
    return df


@st.cache_data(ttl=60 * 60)
def mock_anomalies_df(indicators_id, n=5):
    # 1/ Get anomalies df
    ## Time change
    df_change = mock_anomalies_df_time_change(indicators_id, n)
    df_change["type"] = "time_change"
    ## Upgrade: value change
    df_upgrade_change = mock_anomalies_df_upgrade_change(indicators_id, n)
    df_upgrade_change["type"] = "upgrade_change"
    ## Upgrade: Missing data point
    df_upgrade_miss = mock_anomalies_df_upgrade_missing(indicators_id, n)
    df_upgrade_miss["type"] = "upgrade_missing"

    # 2/ Combine
    df = pd.concat([df_change, df_upgrade_change, df_upgrade_miss])

    # Ensure there is only one row per entity, anomaly type and indicator
    df = df.sort_values("score", ascending=False).drop_duplicates(["entity", "type", "indicator_id"])

    # Replace entity name with entity ID
    entity_mapping = cached.load_entity_ids()
    entity_mapping_inv = {v: k for k, v in entity_mapping.items()}
    df["entity_id"] = df["entity"].map(entity_mapping_inv)
    # st.write(entity_mapping)

    # 3/ Add meta scores
    num_scores = len(df)
    df["score_population"] = [random.random() for i in range(num_scores)]
    df["score_views"] = [random.random() for i in range(num_scores)]

    # 4/ Weighed combined score
    # Weighed combined score
    w_score = 1
    w_pop = 1
    w_views = 1
    df["score_weighed"] = (w_score * df["score"] + w_pop * df["score_population"] + w_views * df["score_views"]) / (
        w_score + w_pop + w_views
    )
    return df


######################################################################
# FUNCTIONS
######################################################################


def _change_chart_selection(indicator_id):
    """Change selection in grapher chart."""
    st.toast(f"Changing entity in indicator {indicator_id}")


@st.fragment
def show_anomaly_compact(index, df):
    """Show anomaly compactly.

    Container with all anomalies of a certain type and for a concrete indicator.
    """
    indicator_id, an_type = index
    row = 0

    # Get relevant metadata for this view
    entity = df.iloc[row]["entity"]
    entities = df["entity_id"].tolist()
    year = df.iloc[row]["year"]
    indicator_uri = st.session_state.indicators.get(indicator_id)

    if an_type == "time_change":
        text = f"There are significant changes for {entity} in {year} compared to the old version of the indicator. There might be other data points affected."
    elif an_type == "upgrade_change":
        text = f"There are abrupt changes for {entity} in {year}! There might be other data points affected."
    else:
        raise ValueError(f"Unknown anomaly type: {an_type}")

    with st.container(border=True):
        st.markdown(f"{tag_in_md(**ANOMALY_TYPES[an_type])} **{indicator_uri}**")
        col1, col2 = st.columns(2)
        # Overview, description, others
        with col1:
            # Chart
            grapher_chart(variable_id=indicator_id, selected_entities=[entity], included_entities=entities)
        with col2:
            # Description
            st.markdown(text)
            # Others
            st.divider()
            st.markdown("View other affected entities")
            st.dataframe(
                df[["entity"] + st.session_state.anomalist_sorting_columns],
                selection_mode=["multi-row"],
                key=f"anomaly_table_{indicator_id}_{an_type}",
                on_select=lambda indicator_id=indicator_id: _change_chart_selection(indicator_id),
                hide_index=True,
            )


def show_anomaly(anomaly, indicator_id):
    """Show anomaly details.

    Renders an anomaly. Title, description and possibly a chart.

    TODO: use if we want to expand anomalies to have one box per entity too.
    """
    with st.container(border=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"##### {anomaly['title']}")
            st.markdown(f"{anomaly['description']}")
        with col2:
            # st.write(indicator.id)
            grapher_chart(variable_id=indicator_id, selected_entities=[anomaly["country"]])


def filter_df(df: pd.DataFrame):
    """Apply filters from user to the dataframe.

    Filter parameters are stored in the session state:

        - `anomalist_filter_entities`: list of entities to filter.
        - `anomalist_filter_indicators`: list of indicators to filter.
        - `anomalist_filter_anomaly_types`: list of anomaly types to filter.
        - `anomalist_min_year`: minimum year to filter.
        - `anomalist_max_year`: maximum year to filter.
        - `anomalist_sorting_strategy`: sorting strategy.
    """
    ## Year
    df = df[(df["year"] >= st.session_state.anomalist_min_year) & (df["year"] <= st.session_state.anomalist_max_year)]
    ## Anomaly type
    df = df[~df["type"].isin(st.session_state.anomalist_filter_anomaly_types)]
    ## Entities
    if len(st.session_state.anomalist_filter_entities) > 0:
        df = df[df["entity"].isin(st.session_state.anomalist_filter_entities)]
    # Indicators
    if len(st.session_state.anomalist_filter_indicators) > 0:
        df = df[df["indicator_id"].isin(st.session_state.anomalist_filter_indicators)]
    ## Sort
    st.session_state.anomalist_sorting_columns = []
    match st.session_state.anomalist_sorting_strategy:
        case "relevance":
            st.session_state.anomalist_sorting_columns = ["score_weighed"]
        case "score":
            st.session_state.anomalist_sorting_columns = ["score"]
        case "population":
            st.session_state.anomalist_sorting_columns = ["score_population"]
        case "views":
            st.session_state.anomalist_sorting_columns = ["score_views"]
        case "population+views":
            st.session_state.anomalist_sorting_columns = ["score_population", "score_views"]
        case _:
            pass
    if st.session_state.anomalist_sorting_columns != []:
        df = df.sort_values(st.session_state.anomalist_sorting_columns, ascending=False)
    return df


# Load the main inputs:
# * List of all Grapher datasets.
# * List of newly created Grapher datasets (the ones we most likely want to inspect).
# * The variable mapping generated by "indicator upgrader", if there was any.
DATASETS_ALL, DATASETS_NEW, VARIABLE_MAPPING = get_datasets_and_mapping_inputs()

# Create DB tables
create_tables()

############################################################################
# RENDER
# Below you can find the different elements of Anomalist being rendered.
############################################################################

# 1/ PAGE TITLE
# Show title
st.title(":material/planner_review: Anomalist")


# 2/ DATASET FORM
# Ask user to select datasets. By default, we select the new datasets (those that are new in the current PR compared to master).
st.markdown(
    """
    <style>
       .stMultiSelect [data-baseweb=select] span{
            max-width: 1000px;
        }
    </style>""",
    unsafe_allow_html=True,
)

with st.form(key="dataset_search"):
    st.session_state.datasets_selected = st.multiselect(
        "Select datasets",
        # options=cached.load_dataset_uris(),
        options=DATASETS_ALL.keys(),
        # max_selections=1,
        default=DATASETS_NEW.keys(),
        format_func=DATASETS_ALL.get,
    )

    st.form_submit_button(
        "Detect anomalies",
        type="primary",
        help="This will load the indicators from the selected datasets and scan for anomalies. This can take some time.",
        on_click=lambda: set_states({"anomalist_datasets_submitted": True}),
    )


# 3/ SCAN FOR ANOMALIES
# If anomalies for dataset already exist in DB, load them. Warn user that these are being loaded from DB
if st.session_state.anomalist_datasets_submitted:
    # 3.1/ Check if anomalies are already there in DB
    st.session_state.anomalist_anomalies = WizardDB.load_anomalies(st.session_state.datasets_selected)

    # 3.2/ No anomaly found in DB, estimate them
    if len(st.session_state.anomalist_anomalies) == 0:
        # Reset flag
        st.session_state.anomalist_anomalies_out_of_date = False

        # Load indicators in selected datasets
        st.session_state.indicators = cached.load_variables_display_in_dataset(
            dataset_id=st.session_state.datasets_selected,
            only_slug=True,
        )

        # Get indicator IDs
        variable_ids = list(st.session_state.indicators.keys())

        # Get variable mapping, if exists. Then keep only 'relevant' variables
        mapping = WizardDB.get_variable_mapping()
        mapping = {k: v for k, v in mapping.items() if k in variable_ids}

        with st.spinner("Scanning for anomalies... This can take some time."):
            # If there is mapping, estimate anomalies in 'upgrade mode'
            if len(mapping) > 0:
                anomaly_detection(
                    anomaly_types=("nan",),
                    variable_ids=variable_ids,
                    dataset_ids=[],
                    variable_mapping=str(mapping),
                    dry_run=False,
                    reset_db=False,
                )
            # Otherwise, estimate anomalies in 'normal mode'
            else:
                # Scan for anomalies
                anomaly_detection(
                    anomaly_types=("nan",),
                    variable_ids=variable_ids,
                    dataset_ids=[],
                    variable_mapping=None,
                    dry_run=False,
                    reset_db=False,
                )

        # Fill list of anomalies...
        st.session_state.anomalist_anomalies = WizardDB.load_anomalies(st.session_state.datasets_selected)

    # 3.3/ Anomalies found in DB. If outdated, set FLAG to True, so we can show a warning later on.
    else:
        # Check if data in DB is out of date
        data_out_of_date = True

        # Set flag (if data is out of date)
        if data_out_of_date:
            st.session_state.anomalist_anomalies_out_of_date = True
        else:
            st.session_state.anomalist_anomalies_out_of_date = False

    # 3.4/ Parse obtained anomalist into dataframe
    if len(st.session_state.anomalist_anomalies) > 0:
        ###############################################################################################################
        # DEMO
        # The following code loads a mock dataframe. Instead, we should retrieve this from the database.
        indicators_id = list(st.session_state.indicators.keys())
        st.session_state.anomalist_df = mock_anomalies_df(indicators_id, n=1000)
        ###############################################################################################################
    else:
        st.session_state.anomalist_df = None

# 4/ SHOW ANOMALIES (only if any are found)
if st.session_state.anomalist_df is not None:
    # 4.0/ WARNING: Show warning if anomalies are loaded from DB without re-computing
    # TODO: we could actually know if anomalies are out of sync from dataset/indicators. Maybe based on dataset/indicator checksums? Starting to implement this idea with data_out_of_date
    if st.session_state.anomalist_anomalies_out_of_date:
        st.caption(
            "Anomalies are being loaded from the database. This might be out of sync with current dataset. Click on button below to run the anomaly-detection algorithm again."
        )
        st.button("Re-scan datasets for anomalies", icon="🔄")

    # 4.1/ ASK FOR FILTER PARAMS
    # User can customize which anomalies are shown to them
    with st.container(border=True):
        st.markdown("##### Select filters")

        # If there is a dataset selected, load the indicators
        if len(st.session_state.datasets_selected) > 0:
            # Load anomalies
            st.session_state.indicators = cached.load_variables_display_in_dataset(
                dataset_id=st.session_state.datasets_selected,
                only_slug=True,
            )

        col1, col2 = st.columns([10, 4])
        # Indicator
        with col1:
            st.multiselect(
                label="Indicators",
                options=st.session_state.indicators,
                format_func=st.session_state.indicators.get,
                help="Show anomalies affecting only a selection of indicators.",
                placeholder="Select indicators",
                key="anomalist_filter_indicators",
            )

        with col2:
            # Entity
            st.multiselect(
                label="Entities",
                options=ENTITIES_DEFAULT,
                help="Show anomalies affecting only a selection of entities.",
                placeholder="Select entities",
                key="anomalist_filter_entities",
            )

        # Anomaly type
        col1, col2, _ = st.columns(3)
        with col1:
            cols = st.columns(2)
            with cols[0]:
                st.selectbox(
                    label="Sort by",
                    options=SORTING_STRATEGIES.keys(),
                    format_func=SORTING_STRATEGIES.get,
                    help="Sort anomalies by a certain criteria.",
                    key="anomalist_sorting_strategy",
                )
            with cols[1]:
                st.multiselect(
                    label="Exclude types",
                    options=ANOMALY_TYPE_NAMES.keys(),
                    format_func=ANOMALY_TYPE_NAMES.get,
                    help="Exclude anomalies of a certain type.",
                    placeholder="Select anomaly types",
                    key="anomalist_filter_anomaly_types",
                )
        with col2:
            with st_horizontal():
                st.number_input(
                    "Min year",
                    value=YEAR_MIN,
                    min_value=YEAR_MIN,
                    max_value=YEAR_MAX,
                    step=1,
                    key="anomalist_min_year",
                )
                st.number_input(
                    "Max year",
                    value=YEAR_MAX,
                    min_value=YEAR_MIN,
                    max_value=YEAR_MAX,
                    step=1,
                    key="anomalist_max_year",
                )

    # 4.3/ APPLY FILTERS
    ###############################################################################################################
    # DEMO
    # The following code loads a mock dataframe. Instead, we should retrieve this from the database.
    indicators_id = list(st.session_state.indicators.keys())
    df = mock_anomalies_df(indicators_id, n=1000)
    ###############################################################################################################

    # Filter dataframe
    df = filter_df(df)

    # 4.4/ SHOW ANOMALIES
    # Different types need formatting
    mask = df["type"] == "upgrade_missing"
    df_missing = df[mask]
    df_change = df[~mask]

    # Show anomalies with time and version changes
    if not df_change.empty:
        # st.dataframe(df_change)
        groups = df_change.groupby(["indicator_id", "type"], sort=False)
        items = list(groups)
        items_per_page = 10

        # Define pagination
        pagination = Pagination(
            items=items,
            items_per_page=items_per_page,
            pagination_key="pagination-demo",
        )

        # Show controls only if needed
        if len(items) > items_per_page:
            pagination.show_controls(mode="bar")

        # Show items (only current page)
        for item in pagination.get_page_items():
            show_anomaly_compact(item[0], item[1])

# Reset state
set_states({"anomalist_datasets_submitted": False})
