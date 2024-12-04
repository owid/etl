"""Anomalist app page.

The main structure of the app is implemented. Its main logic is:

1. User fills the form with datasets.
2. User submits the form.
3. The app loads the datasets and checks the database if there are already anomalies for this dataset.
    3.1 If yes: the app loads already existing anomalies. (will show a warning with option to refresh)
    3.2 If no: the app estimates the anomalies.
4. The app shows the anomalies, along with filters to sort / re-order them.

TODO:
- Test with upgrade flow more extensively.
- For datasets that use dates instead of years, Anomalist works, but it still shows "year". It would be good to show the correct dates instead.
- We can infer if the anomalies are out of sync (because user has updated the data) by checking the dataset checksum. NOTE: checksum might change bc of metadata changes, so might show several false positives.
- Further explore LLM summary:
    - We should store the LLM summary in the DB. We need a new table for this. Each summary is associated with a set of anomalies (Anomaly table), at a precise moment. We should detect out-of-sync here too.
- Hiding capabilities. Option to hide anomalies would be great. Idea: have a button in each anomaly box to hide it. We need a register of the hidden anomalies. We then could have a st.popover element in the filter section which only appears if there are anomalies hidden. Then, we can list them there, in case the user wants to unhide some.

"""

from typing import List, Tuple, Union, cast

import pandas as pd
import streamlit as st

from apps.anomalist.anomalist_api import anomaly_detection, load_detector, pretty_print_number
from apps.utils.gpt import OpenAIWrapper, get_cost_and_tokens, get_number_tokens
from apps.wizard.app_pages.anomalist.utils import (
    AnomalyTypeEnum,
    create_tables,
    get_datasets_and_mapping_inputs,
    get_scores,
)
from apps.wizard.utils import cached, set_states, url_persist
from apps.wizard.utils.chart_config import bake_chart_config
from apps.wizard.utils.components import Pagination, grapher_chart, st_horizontal, st_multiselect_wider, tag_in_md
from apps.wizard.utils.db import WizardDB
from etl.config import OWID_ENV
from etl.grapher_io import load_variables

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
ANOMALY_TYPES_TO_DETECT = tuple(ANOMALY_TYPES.keys())

# GPT
MODEL_NAME = "gpt-4o"

# Map sorting strategy to name to show in UI.
SORTING_STRATEGIES = {
    "relevance": "Relevance",
    "score": "Anomaly score",
    "scale": "Scale",
    "population": "Population",
    "views": "Chart views",
}
# Map sorting strategy to columns in the reduced scores dataframe.
SORTING_COLUMNS = {
    "relevance": "score_weighted",
    "score": "score",
    "scale": "score_scale",
    "population": "score_population",
    "views": "score_analytics",
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
st.session_state.anomalist_sorting_columns = st.session_state.get("anomalist_sorting_columns", SORTING_COLUMNS.values())
st.session_state.anomalist_sorting_strategy = st.session_state.get("anomalist_sorting_strategy", [])

# FLAG: True to trigger anomaly detection manually
st.session_state.anomalist_trigger_detection = st.session_state.get("anomalist_trigger_detection", False)

# Scores.
# These are the default thresholds for the different scores.
# Only anomalies with scores above the following thresholds will be shown by default.
# NOTE: For some reason, streamlit raises an error when the minimum is zero.
#  To avoid this, set it to a positive number (above, e.g. 1e-9).
st.session_state.anomalist_min_anomaly_score = st.session_state.get("anomalist_min_anomaly_score", 0.3)
st.session_state.anomalist_min_weighted_score = st.session_state.get("anomalist_min_weighted_score", 0.1)
st.session_state.anomalist_min_population_score = st.session_state.get("anomalist_min_population_score", 1e-9)
st.session_state.anomalist_min_analytics_score = st.session_state.get("anomalist_min_analytics_score", 1e-9)
st.session_state.anomalist_min_scale_score = st.session_state.get("anomalist_min_scale_score", 1e-9)

# Advanced expander.
st.session_state.anomalist_expander_advanced_options = st.session_state.get(
    "anomalist_expander_advanced_options", False
)

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


@st.fragment()
def llm_ask(df: pd.DataFrame):
    st.button(
        "AI Summary",
        on_click=lambda: llm_dialog(df),
        icon=":material/robot:",
        help=f"Ask GPT {MODEL_NAME} to summarize the anomalies. This is experimental.",
    )


@st.fragment()
def download_anomalies(df: pd.DataFrame):
    csv_data = convert_df_to_csv(df)
    st.download_button(
        "Export data (CSV)",
        data=csv_data,
        file_name="data.csv",
        mime="text/csv",
        icon=":material/download:",
        help="Download the anomalies as a CSV file. Selected filters apply!",
    )


@st.dialog("AI summary of anomalies", width="large")
def llm_dialog(df: pd.DataFrame):
    """Ask LLM for summary of the anomalies."""
    ask_llm_for_summary(df)


@st.cache_data
def ask_llm_for_summary(df: pd.DataFrame):
    NUM_ANOMALIES_PER_TYPE = 2_000

    variable_ids = list(df["indicator_id"].unique())
    metadata = load_variables(variable_ids)
    # Get metadata summary
    metadata_summary = ""
    for m in metadata:
        _summary = f"- {m.name}\n" f"- {m.descriptionShort}\n" f"- {m.unit}"
        metadata_summary += f"{_summary}\n-------------\n"

    # df = st.session_state.anomalist_df
    # Get dataframe
    df = df[["entity_name", "year", "type", "indicator_id", "score_weighted"]]
    # Keep top anomalies based on weighed score
    df = df.sort_values("score_weighted", ascending=False)
    df = cast(pd.DataFrame, df.head(NUM_ANOMALIES_PER_TYPE))

    # Round score (reduce token number)
    df["score_weighted"] = df["score_weighted"].apply(lambda x: int(round(100 * x)))
    # Reshape, pivot indicator_score to have one score column per id
    df = df.pivot_table(
        index=["entity_name", "year", "type"], columns="indicator_id", values="score_weighted"
    ).reset_index()
    # As string (one per anomaly type)
    groups = df.groupby("type")
    df_str = ""
    for group in groups:
        _text = group[0]
        _df = group[1].set_index(["entity_name", "year"]).drop(columns="type")
        # Dataframe as string
        _df_str = cast(str, _df.to_csv()).replace(".0,", ",")
        text = f"### Anomalies of type '{_text}'\n\n{_df_str}\n\n-------------------\n\n"
        df_str += text

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

                        Indicators are identified by column 'indicator_id'. To do a better judgement, find below the name, description and units details for each indicator. Use this information to provide a more insightful summary.

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

    text_in = "\n".join([m["content"][0]["text"] for m in messages])
    num_tokens = get_number_tokens(text_in, MODEL_NAME)

    # Check if the number of tokens is within limits
    if num_tokens > 128_000:
        st.warning(
            f"There are too many tokens in the GPT query to model {MODEL_NAME}. The query has {num_tokens} tokens, while the maximum allowed is 128,000. We will support this in the future. Raise this issue to re-prioritize it."
        )
    else:
        # Ask GPT (stream)
        stream = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,  # type: ignore
            max_tokens=3000,
            stream=True,
        )
        response = cast(str, st.write_stream(stream))

        cost, num_tokens = get_cost_and_tokens(text_in, response, cast(str, MODEL_NAME))
        cost_msg = f"**Cost**: ≥{cost} USD.\n\n **Tokens**: ≥{num_tokens}."
        st.info(cost_msg)


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
        min_weighted_score=st.session_state.anomalist_min_weighted_score,
        min_anomaly_score=st.session_state.anomalist_min_anomaly_score,
        min_population_score=st.session_state.anomalist_min_population_score,
        min_analytics_score=st.session_state.anomalist_min_analytics_score,
        min_scale_score=st.session_state.anomalist_min_scale_score,
    )
    ## Sort dataframe
    df, st.session_state.anomalist_sorting_columns = _sort_df(df, st.session_state.anomalist_sorting_strategy)
    return df


@st.cache_data
def _filter_df(
    df: pd.DataFrame,
    year_min,
    year_max,
    anomaly_types,
    entities,
    indicators,
    min_weighted_score,
    min_anomaly_score,
    min_population_score,
    min_analytics_score,
    min_scale_score,
) -> pd.DataFrame:
    """Used in filter_df."""
    ## Year and scores
    df = df[
        (df["year"] >= year_min)
        & (df["year"] <= year_max)
        & (df["score_weighted"] >= min_weighted_score)
        & (df["score"] >= min_anomaly_score)
        & (df["score_population"] >= min_population_score)
        & (df["score_analytics"] >= min_analytics_score)
        & (df["score_scale"] >= min_scale_score)
    ]
    ## Anomaly type
    if len(anomaly_types) > 0:
        df = df[df["type"].isin(anomaly_types)]
    ## Entities
    if len(entities) > 0:
        df = df[df["entity_name"].isin(entities)]
    # Indicators
    if len(indicators) > 0:
        df = df[df["indicator_id"].isin(indicators)]

    return df


@st.cache_data
def _sort_df(df: pd.DataFrame, sort_strategy: Union[str, List[str]]) -> Tuple[pd.DataFrame, list[str]]:
    """Used in filter_df."""
    if not sort_strategy:
        columns_sort = list(SORTING_COLUMNS.values())
    elif isinstance(sort_strategy, str):
        columns_sort = [SORTING_COLUMNS[sort_strategy]]
    else:
        columns_sort = [SORTING_COLUMNS[_sort_strategy] for _sort_strategy in sort_strategy]
    df = df.sort_values(columns_sort, ascending=False)

    return df, columns_sort


# Function to convert DataFrame to CSV
@st.cache_data
def convert_df_to_csv(df):
    df["indicator_uri"] = df["indicator_id"].apply(lambda x: st.session_state.anomalist_indicators.get(x))
    return df.to_csv(index=False).encode("utf-8")


# Functions to show the anomalies
@st.fragment
def show_anomaly_compact(index, df):
    """Show anomaly compactly.

    Container with all anomalies of a certain type and for a concrete indicator.

    :param df: DataFrame with a single anomaly type and indicator
    """
    indicator_id, anomaly_type = index
    row = 0

    key = f"{indicator_id}_{anomaly_type}"
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
    text = load_detector(anomaly_type).get_text(entity_default, year_default)

    # Render
    with st.container(border=True):
        # Title
        link = OWID_ENV.indicator_admin_site(indicator_id)
        st.markdown(f"{tag_in_md(**ANOMALY_TYPES[anomaly_type])} **[{indicator_uri}]({link})**")
        col1, col2 = st.columns(2)
        # Chart
        with col1:
            # Bake chart config
            # If the anomaly is compared to previous indicator, then we need to show two indicators (old and new)!
            if anomaly_type in {AnomalyTypeEnum.UPGRADE_CHANGE.value, AnomalyTypeEnum.UPGRADE_MISSING.value}:
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
            config["hideFacetControl"] = False
            config["hideShareButton"] = True
            config["hideExploreTheDataButton"] = True
            # config["isSocialMediaExport"] = False

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
                    # df[["entity_name"] + st.session_state.anomalist_sorting_columns],
                    _score_table(df=df),
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


def _score_table(df: pd.DataFrame) -> pd.DataFrame:
    """Return a table of scores and other useful columns for a given indicator. Return styled dataframe."""
    # Filter df_all for the indicator and anomaly type currently displayed.
    df_show = df.copy()
    # Columns in df_all:
    # ['entity_name', 'year', 'indicator_id', 'score', 'score_scale', 'type', 'population', 'score_population', 'views', 'score_analytics', 'score_weighted']

    # Select certain columns, and rename them.
    df_show = df_show.drop(columns=["type", "indicator_id"]).rename(
        columns={"score": "score_anomaly", "score_weighted": "score_relevance", "entity_name": "entity"}
    )

    # Store population and analytics scores for later use.
    score_population = df_show["score_population"].copy()
    score_analytics = df_show["score_analytics"].copy()

    # Create a column that shows both population and population score, and another one for views and analytics score.
    df_show["population_and_score"] = df_show.apply(
        lambda row: f"{row['score_population']:.0%} ({pretty_print_number(row['population'])})", axis=1
    )
    df_show["analytics_and_score"] = df_show.apply(
        lambda row: f"{row['score_analytics']:.0%} ({pretty_print_number(row['views'])})", axis=1
    )
    df_show = df_show.drop(columns=["population", "score_population", "views", "score_analytics"])

    # Rearrange and rename columns more conveniently.
    df_show = df_show[
        [
            "entity",
            "year",
            "score_relevance",
            "score_anomaly",
            "score_scale",
            "population_and_score",
            "analytics_and_score",
        ]
    ]
    df_show = df_show.rename(columns={column: column.replace("score_", "") for column in df_show.columns})
    df_show = df_show.rename(columns={"population_and_score": "population", "analytics_and_score": "views"})

    # Apply styling to columns that only show percentages.
    df_style = df_show.style.format("{:.0%}", subset=["relevance", "anomaly", "scale"]).background_gradient(
        subset=["relevance", "anomaly", "scale"],
        vmin=0,
        vmax=1,
    )

    # Apply styling to special columns population and views.
    df_style = df_style.background_gradient(
        subset=["population"],
        gmap=score_population,
        vmin=0,
        vmax=1,
    )
    df_style = df_style.background_gradient(
        subset=["views"],
        gmap=score_analytics,
        vmin=0,
        vmax=1,
    )

    return df_style


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
st_multiselect_wider()

with st.form(key="dataset_search"):
    query_dataset_ids = [int(v) for v in st.query_params.get_all("anomalist_datasets_selected")]

    st.session_state.anomalist_datasets_selected = st.multiselect(
        "Select datasets",
        # options=cached.load_dataset_uris(),
        options=DATASETS_ALL.keys(),
        # max_selections=1,
        default=query_dataset_ids or DATASETS_NEW.keys(),
        format_func=DATASETS_ALL.get,
    )
    st.query_params["anomalist_datasets_selected"] = st.session_state.anomalist_datasets_selected  # type: ignore

    st.form_submit_button(
        "Detect anomalies",
        type="primary",
        help="This will load the indicators from the selected datasets and scan for anomalies. This can take some time.",
        on_click=lambda: set_states({"anomalist_datasets_submitted": True}),
    )


# 3/ SCAN FOR ANOMALIES
# If anomalies for dataset already exist in DB, load them. Warn user that these are being loaded from DB
if not st.session_state.anomalist_anomalies or st.session_state.anomalist_datasets_submitted:
    # 3.1/ Check if anomalies are already there in DB
    with st.spinner("Loading anomalies (already detected) from database..."):
        st.session_state.anomalist_anomalies = WizardDB.load_anomalies(st.session_state.anomalist_datasets_selected)

    # Load indicators in selected datasets
    st.session_state.anomalist_indicators = cached.load_variables_display_in_dataset(
        dataset_id=st.session_state.anomalist_datasets_selected,
        only_slug=True,
    )

    # Get indicator IDs
    variable_ids = list(st.session_state.anomalist_indicators.keys())
    st.session_state.anomalist_mapping = {k: v for k, v in VARIABLE_MAPPING.items() if v in variable_ids}
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
    if len(st.session_state.anomalist_anomalies) > 0:
        # Combine scores from all anomalies, reduce them (to get the maximum anomaly score for each entity-indicator),
        # and add population and analytics scores.
        df = get_scores(anomalies=st.session_state.anomalist_anomalies)

        st.session_state.anomalist_df = df
    else:
        st.session_state.anomalist_df = None

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
            options = [
                indicator for indicator in INDICATORS_AVAILABLE if indicator in st.session_state.anomalist_indicators
            ]

            url_persist(st.multiselect)(
                label="Indicators",
                options=options,
                format_func=st.session_state.anomalist_indicators.get,
                help="Show anomalies affecting only a selection of indicators.",
                placeholder="Select indicators",
                key="anomalist_filter_indicators",
            )

        with col2:
            # Entity
            url_persist(st.multiselect)(
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
                url_persist(st.multiselect)(
                    label="Sort by",
                    options=SORTING_STRATEGIES.keys(),
                    format_func=SORTING_STRATEGIES.get,
                    placeholder="Select sorting strategy",
                    help=(
                        """
                        Sort anomalies by a certain criteria.

                        - **Relevance**: This is a combined score based on the anomaly score, the scale of the anomaly, the population in the country, and the views of charts using this indicator.
                        - **Anomaly score**: The anomaly detection algorithm assigns a score to each anomaly based on its significance.
                        - **Scale**: Scale score, based on how big the anomaly as a share of the range of values of the indicator.
                        - **Population**: Population score, based on the population in the affected country.
                        - **Views**: Views of charts using this indicator.
                        """
                    ),
                    key="anomalist_sorting_strategy",
                )
            with cols[1]:
                url_persist(st.multiselect)(
                    label="Detectors",
                    options=ANOMALY_TYPES_AVAILABLE,
                    format_func=ANOMALY_TYPE_NAMES.get,
                    help="Show anomalies of a certain type.",
                    placeholder="Select anomaly types",
                    key="anomalist_filter_anomaly_types",
                )
        with col2:
            with st_horizontal():
                url_persist(st.number_input)(
                    "Min year",
                    value=YEAR_MIN,
                    min_value=YEAR_MIN,
                    max_value=YEAR_MAX,
                    step=1,
                    key="anomalist_min_year",
                )
                url_persist(st.number_input)(
                    "Max year",
                    value=YEAR_MAX,
                    min_value=YEAR_MIN,
                    max_value=YEAR_MAX,
                    step=1,
                    key="anomalist_max_year",
                )

        with st.expander("Advanced options", expanded=st.session_state.anomalist_expander_advanced_options):
            for score_name in ["weighted", "anomaly", "scale", "population", "analytics"]:
                # For some reason, if the slider minimum value is zero, streamlit raises an error when the slider is
                # dragged to the minimum. Set it to a small, non-zero number.
                url_persist(st.slider)(
                    f"Minimum {score_name} score",
                    min_value=1e-9,
                    max_value=1.0,
                    # step=0.001,
                    key=f"anomalist_min_{score_name}_score",
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
        # Top option buttons
        with st_horizontal():
            # LLM summary option
            llm_ask(df)
            download_anomalies(df)

        # st.dataframe(df_change)
        groups = df.groupby(["indicator_id", "type"], sort=False, observed=True)
        items = list(groups)
        items_per_page = 10

        # Define pagination
        pagination = Pagination(
            items=items,
            items_per_page=items_per_page,
            pagination_key="pagination-demo",
        )

        # Show items (only current page)
        for item in pagination.get_page_items():
            show_anomaly_compact(index=item[0], df=item[1])

        # Show controls only if needed
        if len(items) > items_per_page:
            pagination.show_controls(mode="bar")
else:
    st.success("Ha! We did not find any no anomalies in the selected datasets! What were the odds of that?")
# Reset state
set_states({"anomalist_datasets_submitted": False})
