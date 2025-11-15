"""Harmonize entities."""

import json
from operator import itemgetter
from pathlib import Path
from typing import List, cast

import streamlit as st
from owid.catalog import Dataset

from apps.wizard.utils import get_datasets_in_etl, set_states
from apps.wizard.utils.components import st_title_with_expert
from etl.config import ENV_IS_REMOTE
from etl.harmonize import Harmonizer
from etl.paths import STEP_DIR
from etl.steps import load_from_uri

# RANK OF PREFERED TABLE NAMES
TABLE_NAME_PRIORITIES = ["main", "core"]
# RANK OF PREFERED COLUMN NAMES (for country)
COLUMN_NAME_PRIORITIES = ["country", "state", "location", "region", "iso", "entity"]
# Number of suggestions to show per entity
NUM_SUGGESTIONS = 1000


####################################################################################################
# FUNCTIONS & other configs
####################################################################################################
def sort_values(values: List[str], values_priority: List[str]) -> List[str]:
    """Sort values based on priorities.

    For instance, we want column 'country' to be shown first. The top prefered values are in `values_priority`.
    The rest is sorted alphabetically.
    """
    # Build score list
    ## [(value_1, score_1), (value_2, score_2), ...]
    ## score 0 means no priority at all, score X (integer) means it fully matched a priority value, score X.5 means it partially matched a priority value
    values_with_priority = []
    for value in values[:]:
        added = False
        for score, value_priority in enumerate(values_priority, start=1):
            if value == value_priority:
                values_with_priority.append((value, -score))
                added = True
                break
            elif value in value_priority:
                values_with_priority.append((value, -score - 0.5))
                added = True
                break
            else:
                continue
        if not added:
            values_with_priority.append((value, 0))

    # Sort values
    values_with_priority = sorted(values_with_priority, key=itemgetter(1))
    values_top = [v[0] for v in values_with_priority if v[1] != 0]
    values_bottom = sorted([v[0] for v in values_with_priority if v[1] == 0])
    values = values_top + values_bottom
    return values


def sort_table_names(dataset: Dataset) -> List[str]:
    """Sort table names based on priorities.

    For instance, we want table 'location' to be shown first. The top prefered values are in `values_priority`.
    The rest is sorted alphabetically.
    """
    # Init
    if dataset.metadata.short_name is not None:
        priorities = [dataset.metadata.short_name] + TABLE_NAME_PRIORITIES
    else:
        priorities = TABLE_NAME_PRIORITIES
    table_names = sort_values(dataset.table_names, priorities)

    return table_names


def sort_indicators(indicators: List[str]) -> List[str]:
    """Sort indicators based on priorities.

    For instance, we want column 'country' to be shown first. The top prefered values are in `values_priority`.
    The rest is sorted alphabetically.
    """
    # Init
    indicators = sort_values(indicators, COLUMN_NAME_PRIORITIES)
    return indicators


def ask_for_dataset(step_uri):
    options = get_datasets_in_etl(
        snapshots=False,
        prefixes=None if st.session_state.show_all else ["data://meadow"],
    )
    dataset_uri = st.selectbox(
        label="Select a dataset",
        placeholder="Select a dataset",
        options=options,
        index=options.index(step_uri) if step_uri in options else None,
        help="By default, only meadow datasets are shown in the dataset search bar.",
    )
    st.toggle(
        "Show all datasets",
        help="By default, only meadow datasets are shown in the dataset search bar.",
        on_change=lambda: set_states({"show_all": not st.session_state.show_all}),
    )

    return dataset_uri


@st.cache_data
def load_dataset_cached(dataset_uri):
    try:
        dataset = cast(Dataset, load_from_uri(dataset_uri))
        return dataset
    except FileNotFoundError as e:
        st.error(e)
        st.stop()


def ask_for_table(dataset):
    table_names = sort_table_names(dataset)
    table_name = st.selectbox(
        "Select a table",
        options=table_names,
        placeholder="Choose a table",
        index=None if len(table_names) != 1 else 0,
    )
    if table_name:
        return _get_table_cached(dataset, dataset_uri=dataset.m.uri, table_name=table_name)
    return


@st.cache_data
def _get_table_cached(_dataset, dataset_uri, table_name):
    return _dataset[table_name].reset_index()


def ask_for_indicator(tb):
    columns = sort_indicators(tb.columns)
    column_name = st.selectbox(
        label="Select the entity column",
        options=columns,
        placeholder="Choose an indicator",
        index=0 if set(COLUMN_NAME_PRIORITIES).intersection(set(columns)) else None,
    )
    return column_name


def validate_indicator(tb, column_name):
    # Raise error if nan
    if tb[column_name].isna().any():
        st.error(f"Column '{column_name}' contains missing values. Please clean the data before harmonizing.")
        st.stop()

    # Sanity check on typing: only support for indicators of type string
    if not tb[column_name].apply(type).eq(str).all():
        # if tb[column_name].dtype not in ["object", "category"]:
        st.error(
            f"Column '{column_name}' is of type `{tb[column_name].dtype}` but 'string' is expected. Harmonization for non-string columns is not supported yet."
        )
        st.stop()


@st.fragment
def show_manual_mapping(harmonizer, entity, i, border=False):
    # Get suggestions for entity
    suggestions = harmonizer.get_suggestions(
        region=entity,
        institution=None,
        num_suggestions=NUM_SUGGESTIONS,
    )

    # Col1: selectbox, Col2: ignore checkbox
    with st.container(border=border):
        col1, col2 = st.columns([3, 1], vertical_alignment="bottom")
        with col1:
            value_selected = st.selectbox(
                label=f"**{entity}** (original name)",
                options=suggestions,
                index=0,
                # label_visibility="collapsed",
                key=f"region_suggestion_new_{i}",
                accept_new_options=True,
            )
        with col2:
            value_ignore = st.checkbox(
                label="Ignore",
                key=f"region_ignore_{i}",
            )

    # Add defined mapping (only if not ignored)
    if not value_ignore:
        st.session_state.entity_mapping[entity] = value_selected


@st.fragment
def show_submit_section(path_export: str):
    if ENV_IS_REMOTE:
        # Submit button
        export_btn = st.button(
            label="Generate mapping",
            type="primary",
        )

    else:
        path_export = st.text_input(
            label="Export to...",
            value=path_export,
        )
        # Submit button
        export_btn = st.button(
            label="Export mapping",
            type="primary",
        )
    return export_btn


def render(step_uri):
    # Page config
    st_title_with_expert("Entity Harmonizer", icon=":material/music_note:")

    # Set states
    st.session_state["show_all"] = st.session_state.get("show_all", False)
    st.session_state["entity_mapping"] = st.session_state.get("entity_mapping", {})

    # INTRO
    st.markdown(
        "Harmonize entity names with this tool. Start by loading an indicator from a dataset below. If you find any problem, remember you can still run `etl harmonize` in the terminal."
    )
    ####################################################################################################
    # SELECT DATASET, TABLE and INDICATOR
    ####################################################################################################
    # 1/ DATASET
    dataset_uri = ask_for_dataset(step_uri)

    if dataset_uri:
        # Load dataset
        dataset = load_dataset_cached(dataset_uri)

        # 2/ TABLE
        col1, col2 = st.columns(2, gap="small")
        with col1:
            tb = ask_for_table(dataset)

        if tb is not None:
            # 3/ INDICATOR
            with col2:
                column_name = ask_for_indicator(tb)

            ####################################################################################################
            # HARMONIZATION (generation)
            ####################################################################################################
            if column_name:
                # Transform to string if category
                if tb[column_name].apply(type).eq("category").all():
                    tb[column_name] = tb[column_name].astype(str)

                # Validate indicator
                validate_indicator(tb, column_name)

                # HARMONIZER
                # Build harmonizer
                harmonizer = Harmonizer(
                    tb=tb,
                    colname=column_name,
                    output_file=f"{STEP_DIR}/data/garden/{dataset.m.namespace}/{dataset.m.version}/{dataset.m.short_name}.countries.json",
                )

                # Automatic harmonization (no user input needed)
                harmonizer.run_automatic()

                # Manual harmonization (user input needed)
                st.divider()

                # Show automatic mapping
                if harmonizer.mapping:
                    with st.popover("Automatically mapped entities"):
                        st.dataframe(harmonizer.mapping)

                ambiguous = cast(List, harmonizer.ambiguous)

                ## 1/ AUTOMATIC
                st.session_state.entity_mapping = harmonizer.mapping

                ## 2/ MANUAL (user input needed)
                with st.container(border=True):
                    # Title
                    st.markdown(f"#### Manual mapping needed :small[:gray-badge[{len(ambiguous)} ambiguous regions]]")
                    # st.markdown(f"{len(ambiguous)} ambiguous regions")

                    # Create a container for each region with:
                    # - selectbox to choose name (with suggestions, and accepting new values)
                    # - a toggle to ignore the region (no mapping)
                    for i, entity in enumerate(ambiguous, 1):
                        show_manual_mapping(
                            harmonizer=harmonizer,
                            entity=entity,
                            i=i,
                            border=False,
                        )

                    # 3/ PATH to export & export button
                    path_export = cast(str, harmonizer.output_file)
                    export_btn = show_submit_section(path_export)

                ####################################################################################################
                # EXPORT
                ####################################################################################################
                if export_btn:
                    if ENV_IS_REMOTE:
                        path_export = Path(path_export).name
                        json_string = json.dumps(st.session_state.entity_mapping)
                        st.download_button(
                            label="Download the mapping",
                            data=json_string,
                            key="mapping",
                            file_name=path_export,
                            help="This file will be downloaded to your computer.",
                        )
                    else:
                        path_export = Path(path_export)
                        # Sanity checks
                        if not path_export.parent.exists():
                            st.error(f"Directory {path_export.parent} does not exist!")
                            st.stop()
                        if not (suf := path_export.suffix) == ".json":
                            st.error(
                                f"Please provide a valid extension of the file. Should be a JSON file, but instead '{suf}' was given."
                            )
                            st.stop()

                        # Export
                        with st.popover("Show entity mapping"):
                            st.json(st.session_state.entity_mapping, expanded=True)
                        with open(path_export, "w") as ostream:
                            json.dump(st.session_state.entity_mapping, ostream, indent=2)

                        st.success(f"Harmonization mapping exported to {path_export}")
