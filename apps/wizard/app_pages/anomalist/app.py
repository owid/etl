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

from typing import List, Tuple, cast

import pandas as pd
import streamlit as st

from apps.anomalist.anomalist_api import anomaly_detection
from apps.utils.gpt import OpenAIWrapper
from apps.wizard.app_pages.anomalist.utils import AnomalyTypeEnum, create_tables, get_datasets_and_mapping_inputs
from apps.wizard.utils import cached, set_states
from apps.wizard.utils.chart_config import bake_chart_config
from apps.wizard.utils.components import Pagination, grapher_chart, st_horizontal, tag_in_md
from apps.wizard.utils.db import WizardDB
from etl.config import OWID_ENV

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Anomalist",
    page_icon="🪄",
    layout="wide",
)


# OTHER CONFIG
ANOMALY_TYPES = {
    AnomalyTypeEnum.TIME_CHANGE.value: {
        "tag_name": "Time change",
        "color": "gray",
        "icon": ":material/timeline",
    },
    AnomalyTypeEnum.UPGRADE_CHANGE.value: {
        "tag_name": "Version change",
        "color": "orange",
        "icon": ":material/upgrade",
    },
    AnomalyTypeEnum.UPGRADE_MISSING.value: {
        "tag_name": "Missing point",
        "color": "red",
        "icon": ":material/hide_source",
    },
    AnomalyTypeEnum.GP_OUTLIER.value: {
        "tag_name": "Gaussian Process",
        "color": "blue",
        "icon": ":material/notifications",
    },
}
ANOMALY_TYPE_NAMES = {k: v["tag_name"] for k, v in ANOMALY_TYPES.items()}
# TODO: Remove the `t != AnomalyTypeEnum.GP_OUTLIER.value` bit to also query for GP outliers.
ANOMALY_TYPES_TO_DETECT = tuple(t for t in ANOMALY_TYPES.keys() if t != AnomalyTypeEnum.GP_OUTLIER.value)


SORTING_STRATEGIES = {
    "relevance": "Relevance",
    "score": "Anomaly score",
    "population": "Population",
    "views": "Chart views",
    "population+views": "Population+views",
}

# SESSION STATE
# Datasets selected by the user in first multiselect
st.session_state.anomalist_datasets_selected = st.session_state.get("anomalist_datasets_selected", [])

# Indicators corresponding to datasets selected by the user (plus variable mapping)
st.session_state.anomalist_indicators = st.session_state.get("anomalist_indicators", {})
st.session_state.anomalist_mapping = st.session_state.get("anomalist_mapping", {})

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

# FLAG: True to trigger anomaly detection manually
st.session_state.anomalist_trigger_detection = st.session_state.get("anomalist_trigger_detection", False)


######################################################################
# FUNCTIONS
######################################################################


@st.cache_data(ttl=60)
def get_variable_mapping(variable_ids):
    """Get variable mapping for specific variable IDs."""
    # Get variable mapping, if exists. Then keep only 'relevant' variables
    mapping = WizardDB.get_variable_mapping()
    mapping = {k: v for k, v in mapping.items() if v in variable_ids}
    return mapping


def parse_anomalies_to_df() -> pd.DataFrame | None:
    """Given a list of anomalies, parse them into a dataframe.

    This function takes the anomalies stored in st.session_state.anomalist_anomalies and parses them into a single dataframe.

        - It loads dfScore from each anomaly.
        - It keeps only one row per entity and anomaly type, based on the highest anomaly score.
        - Concatenates all dataframes.
        - Renames columns to appropriate names.
        - Adjusts dtypes.
        - Adds population and analytics scores.
        - Calculates weighed combined score

    """
    # Only return dataframe if there are anomalies!
    if len(st.session_state.anomalist_anomalies) > 0:
        dfs = []
        for anomaly in st.session_state.anomalist_anomalies:
            # Load
            df = anomaly.dfReduced
            if isinstance(df, pd.DataFrame):
                # Assign anomaly type in df
                df["type"] = anomaly.anomalyType
                # Add to list
                dfs.append(df)
            else:
                raise ValueError(f"Anomaly {anomaly} has no dfScore attribute.")

        # Concatenate all dfs
        df = cast(pd.DataFrame, pd.concat(dfs, ignore_index=True))

        # Rename columns
        df = df.rename(
            columns={
                "variable_id": "indicator_id",
                "anomaly_score": "score",
            }
        )

        # Dtypes
        df = df.astype(
            {
                "year": int,
            }
        )

        # Add population and analytics score:
        df["score_population"] = 1
        df["score_analytics"] = 1

        # Weighed combined score
        w_score = 1
        w_pop = 1
        w_views = 1
        df["score_weighed"] = (
            w_score * df["score"] + w_pop * df["score_population"] + w_views * df["score_analytics"]
        ) / (w_score + w_pop + w_views)

        return df


def ask_llm_for_summary(df: pd.DataFrame):
    variable_ids = df["indicator_id"].unique()

    # Get metadata summary
    metadata_summary = ""

    # Get dataframe
    df = df[["entity_name", "year", "type", "indicator_id", "weighed_score"]]
    # Reshape, pivot indicator_score to have one score column per id
    df = df.pivot_table(index=["entity_name", "year", "type"], columns="indicator_id", values="weighed_score")

    # Dataframe as string
    df_str = cast(str, df.to_csv()).replace(".0,", ",")

    # Ask LLM for summary
    client = OpenAIWrapper()

    # Prepare messages for Insighter
    messages = [
        {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": (
                        f"""
                        The user has obtained anomalies for a list of indicators. This list comes in the format of a dataframe with columns:
                            - 'entity_name': Typically a country name.
                            - 'year': The year in which the anomaly was detected.
                            - 'type': The type of anomaly detected. Allowed types are:
                                - 'time_change': A significant change in the indicator over time.
                                - 'upgrade_change': A significant change in the indicator after an upgrade.
                                - 'upgrade_missing': A missing value in the indicator after an upgrade.
                                - 'gp_outlier': An outlier detected using Gaussian processes.

                            Additionally, there is a column per indicator (identified by the indicator ID), with the weighed score of the anomaly. The weighed score is an estimate on how relevant the anomaly is, based on the anomaly score, population in the country, and views of charts using this indicator.

                        The user will provide this dataframe.

                        You should try to summarise this list of anomalies, so that the information is more digestable. Some ideas:

                            - Try to find if there are common patterns across entities or years.
                            - Try to remove redundant information as much as possible. For instance: if the same entity has multiple anomalies of the same type, you can group them together. Or if the same entity has multiple anomalies of different types, you can group them together.
                            - Try to find the most relevant anomalies. Either because these affect multiple entities or because they have a high weighed score.

                        Indicators are identified by column 'indicator_id'. To do a better judgement, find below the metadata details for each indicator. Use this information to provide a more insightful summary.

                        {metadata_summary}
                        """
                    ),
                },
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": df_str,
                },
            ],
        },
    ]

    with st.chat_message("assistant"):
        # Ask GPT (stream)
        stream = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,  # type: ignore
            max_tokens=3000,
            stream=True,
        )
        response = cast(str, st.write_stream(stream))


# Functions to filter the results
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
    # Filter dataframe
    df = _filter_df(
        df=df,
        year_min=st.session_state.anomalist_min_year,
        year_max=st.session_state.anomalist_max_year,
        anomaly_types=st.session_state.anomalist_filter_anomaly_types,
        entities=st.session_state.anomalist_filter_entities,
        indicators=st.session_state.anomalist_filter_indicators,
    )
    ## Sort dataframe
    df, st.session_state.anomalist_sorting_columns = _sort_df(df, st.session_state.anomalist_sorting_strategy)
    return df


@st.cache_data
def _filter_df(df: pd.DataFrame, year_min, year_max, anomaly_types, entities, indicators) -> pd.DataFrame:
    """Used in filter_df."""
    ## Year
    df = df[(df["year"] >= year_min) & (df["year"] <= year_max)]
    ## Anomaly type
    df = df[~df["type"].isin(anomaly_types)]
    ## Entities
    if len(entities) > 0:
        df = df[df["entity_name"].isin(entities)]
    # Indicators
    if len(indicators) > 0:
        df = df[df["indicator_id"].isin(indicators)]

    return df


@st.cache_data
def _sort_df(df: pd.DataFrame, sort_strategy: str) -> Tuple[pd.DataFrame, List[str]]:
    """Used in filter_df."""
    ## Sort
    columns_sort = []
    match sort_strategy:
        case "relevance":
            columns_sort = ["score_weighed"]
        case "score":
            columns_sort = ["score"]
        case "population":
            columns_sort = ["score_population"]
        case "views":
            columns_sort = ["score_analytics"]
        case "population+views":
            columns_sort = ["score_population", "score_analytics"]
        case _:
            pass
    if columns_sort != []:
        df = df.sort_values(columns_sort, ascending=False)

    return df, columns_sort


# Functions to show the anomalies
@st.fragment
def show_anomaly_compact(index, df):
    """Show anomaly compactly.

    Container with all anomalies of a certain type and for a concrete indicator.
    """
    indicator_id, an_type = index
    row = 0

    key = f"{indicator_id}_{an_type}"
    key_table = f"anomaly_table_{key}"
    key_selection = f"selected_entities_{key}"

    # Get relevant metadata for this view
    # By default, the entity with highest score, but user may have selected other ones!
    entity_default = df.iloc[row]["entity_name"]
    entities = st.session_state.get(f"selected_entities_{key}", entity_default)
    entities = entities if entities != [] else [entity_default]

    # entities = df["entity_id"].tolist()
    year_default = df.iloc[row]["year"]
    indicator_uri = st.session_state.anomalist_indicators.get(indicator_id)

    # Generate descriptive text. Only contains information about top-scoring entity.
    if an_type == AnomalyTypeEnum.TIME_CHANGE.value:
        text = f"There are significant changes for {entity_default} in {year_default} compared to the old version of the indicator. There might be other data points affected."
    elif an_type == AnomalyTypeEnum.UPGRADE_CHANGE.value:
        text = f"There are abrupt changes for {entity_default} in {year_default}! There might be other data points affected."
    elif an_type == AnomalyTypeEnum.UPGRADE_MISSING.value:
        text = f"There are missing values for {entity_default}! There might be other data points affected."
    elif an_type == AnomalyTypeEnum.GP_OUTLIER.value:
        text = f"There are some outliers for {entity_default}! These were detected using Gaussian processes. There might be other data points affected."
    else:
        raise ValueError(f"Unknown anomaly type: {an_type}")

    # Render
    with st.container(border=True):
        # Title
        link = OWID_ENV.indicator_admin_site(indicator_id)
        st.markdown(f"{tag_in_md(**ANOMALY_TYPES[an_type])} **[{indicator_uri}]({link})**")
        col1, col2 = st.columns(2)
        # Chart
        with col1:
            # Bake chart config
            # If the anomaly is compared to previous indicator, then we need to show two indicators (old and new)!
            if an_type in {AnomalyTypeEnum.UPGRADE_CHANGE.value, AnomalyTypeEnum.UPGRADE_MISSING.value}:
                display = [
                    {
                        "name": "New",
                    },
                    {
                        "name": "Old",
                    },
                ]
                assert indicator_id in st.session_state.anomalist_mapping_inv, "Indicator ID not found in mapping!"
                indicator_id_old = st.session_state.anomalist_mapping_inv[indicator_id]
                config = bake_chart_config(
                    variable_id=[indicator_id, indicator_id_old],
                    selected_entities=entities,
                    display=display,
                )
                config["title"] = indicator_uri
                config["subtitle"] = "Comparison of old and new indicator versions."

                # config = bake_chart_config(
                #     variable_id=[indicator_id],
                #     selected_entities=entities,
                # )
            else:
                config = bake_chart_config(variable_id=indicator_id, selected_entities=entities)
            config["hideAnnotationFieldsInTitle"]["time"] = True
            # Actually plot
            grapher_chart(chart_config=config, owid_env=OWID_ENV)

        # Description and other entities
        with col2:
            # Description
            st.info(text)
            # Other entities
            with st.container(border=False):
                st.markdown("**Select** other affected entities")
                st.dataframe(
                    df[["entity_name"] + st.session_state.anomalist_sorting_columns],
                    selection_mode=["multi-row"],
                    key=key_table,
                    on_select=lambda df=df, key_table=key_table, key_selection=key_selection: _change_chart_selection(
                        df, key_table, key_selection
                    ),
                    hide_index=True,
                )

            # TODO: Enable anomaly-specific hiding
            # key_btn = f"button_{key}"
            # st.button("Hide anomaly", key=key_btn, icon=":material/hide:")


def _change_chart_selection(df, key_table, key_selection):
    """Change selection in grapher chart."""
    # st.toast(f"Changing entity in indicator {indicator_id}")
    # Get selected row number
    rows = st.session_state[key_table]["selection"]["rows"]

    # Update entities in chart
    st.session_state[key_selection] = df.iloc[rows]["entity_name"].tolist()


######################################################################


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
    st.session_state.anomalist_datasets_selected = st.multiselect(
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
    st.session_state.anomalist_anomalies = WizardDB.load_anomalies(st.session_state.anomalist_datasets_selected)

    # Load indicators in selected datasets
    st.session_state.anomalist_indicators = cached.load_variables_display_in_dataset(
        dataset_id=st.session_state.anomalist_datasets_selected,
        only_slug=True,
    )

    # Get indicator IDs
    variable_ids = list(st.session_state.anomalist_indicators.keys())
    st.session_state.anomalist_mapping = get_variable_mapping(variable_ids)
    st.session_state.anomalist_mapping_inv = {v: k for k, v in st.session_state.anomalist_mapping.items()}

    # 3.2/ No anomaly found in DB, estimate them
    if (len(st.session_state.anomalist_anomalies) == 0) | (st.session_state.anomalist_trigger_detection):
        # Reset flag
        st.session_state.anomalist_anomalies_out_of_date = False

        with st.spinner("Scanning for anomalies... This can take some time."):
            anomaly_detection(
                anomaly_types=ANOMALY_TYPES_TO_DETECT,
                variable_ids=variable_ids,
                variable_mapping=st.session_state.anomalist_mapping,
                dry_run=False,
                reset_db=False,
            )

        # Fill list of anomalies...
        st.session_state.anomalist_anomalies = WizardDB.load_anomalies(st.session_state.anomalist_datasets_selected)

        # Reset manual trigger
        st.session_state.anomalist_trigger_detection = False

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
    st.session_state.anomalist_df = parse_anomalies_to_df()

# 4/ SHOW ANOMALIES (only if any are found)
if st.session_state.anomalist_df is not None:
    ENTITIES_AVAILABLE = st.session_state.anomalist_df["entity_name"].unique()
    YEAR_MIN = st.session_state.anomalist_df["year"].min()
    YEAR_MAX = st.session_state.anomalist_df["year"].max()
    INDICATORS_AVAILABLE = st.session_state.anomalist_df["indicator_id"].unique()
    ANOMALY_TYPES_AVAILABLE = st.session_state.anomalist_df["type"].unique()

    # 4.0/ WARNING: Show warning if anomalies are loaded from DB without re-computing
    # TODO: we could actually know if anomalies are out of sync from dataset/indicators. Maybe based on dataset/indicator checksums? Starting to implement this idea with data_out_of_date
    if st.session_state.anomalist_anomalies_out_of_date:
        st.caption(
            "Anomalies are being loaded from the database. This might be out of sync with current dataset. Click on button below to run the anomaly-detection algorithm again."
        )
        st.button(
            "Re-scan datasets for anomalies",
            icon="🔄",
            on_click=lambda: set_states(
                {
                    "anomalist_trigger_detection": True,
                    "anomalist_datasets_submitted": True,
                }
            ),
        )

    # 4.1/ ASK FOR FILTER PARAMS
    # User can customize which anomalies are shown to them
    with st.container(border=True):
        st.markdown("##### Select filters")

        # If there is a dataset selected, load the indicators
        if len(st.session_state.anomalist_datasets_selected) > 0:
            # Load anomalies
            st.session_state.anomalist_indicators = cached.load_variables_display_in_dataset(
                dataset_id=st.session_state.anomalist_datasets_selected,
                only_slug=True,
            )

        col1, col2 = st.columns([10, 4])
        # Indicator
        with col1:
            st.multiselect(
                label="Indicators",
                options=INDICATORS_AVAILABLE,
                format_func=st.session_state.anomalist_indicators.get,
                help="Show anomalies affecting only a selection of indicators.",
                placeholder="Select indicators",
                key="anomalist_filter_indicators",
            )

        with col2:
            # Entity
            st.multiselect(
                label="Entities",
                options=ENTITIES_AVAILABLE,
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
                    help=(
                        """
                        Sort anomalies by a certain criteria.

                        - **Relevance**: This is a combined score based on population in country, views of charts using this indicator, and anomaly-algorithm error score. The higher this score, the more relevant the anomaly.
                        - **Anomaly score**: The anomaly detection algorithm assigns a score to each anomaly based on its significance.
                        - **Population**: Population score, based on the population in the affected country.
                        - **Views**: Views of charts using this indicator.
                        - **Population+views**: Combined population and chart views to rank.
                        """
                    ),
                    key="anomalist_sorting_strategy",
                )
            with cols[1]:
                st.multiselect(
                    label="Hide types",
                    options=ANOMALY_TYPES_AVAILABLE,
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
    df = filter_df(st.session_state.anomalist_df)

    # 5/ SHOW ANOMALIES
    # Different types need formatting
    # mask = df["type"] == "upgrade_missing"
    # df_missing = df[mask]
    # df_change = df[~mask]

    # Show anomalies with time and version changes
    if not df.empty:
        # st.dataframe(df_change)
        groups = df.groupby(["indicator_id", "type"], sort=False)
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
