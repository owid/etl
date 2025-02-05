"""Harmonize entities."""

import json
from operator import itemgetter
from pathlib import Path
from typing import List, cast

import streamlit as st
from owid.catalog import Dataset

from apps.wizard.utils import get_datasets_in_etl, set_states
from etl.config import ENV_IS_REMOTE
from etl.harmonize import Harmonizer
from etl.paths import STEP_DIR
from etl.steps import load_from_uri

# RANK OF PREFERED TABLE NAMES
TABLE_NAME_PRIORITIES = ["main", "core"]
# RANK OF PREFERED COLUMN NAMES (for country)
COLUMN_NAME_PRIORITIES = ["country", "state", "location", "region", "iso"]


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


def render(step_uri):
    # Page config
    st.title(":material/music_note: Entity Harmonizer")

    # Set states
    st.session_state["show_all"] = st.session_state.get("show_all", False)
    st.session_state["entity_mapping"] = st.session_state.get("entity_mapping", {})

    # OTHER PARAMS
    NUM_SUGGESTIONS = 1000

    # INTRO
    st.markdown(
        "Harmonize entity names with this tool. Start by loading an indicator from a dataset below. If you find any problem, remember you can still run `etl harmonize` in the terminal."
    )
    ####################################################################################################
    # SELECT DATASET, TABLE and INDICATOR
    ####################################################################################################
    # 1/ DATASET
    # show_all = False
    options = get_datasets_in_etl(
        snapshots=False,
        prefixes=None if st.session_state.show_all else ["data://meadow"],
    )
    option = st.selectbox(
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

    if option:
        # Load dataset
        try:
            dataset = cast(Dataset, load_from_uri(option))
        except FileNotFoundError as e:
            st.error(e)
            st.stop()

        # 2/ TABLE
        table_names = sort_table_names(dataset)
        table_name = st.selectbox(
            "Select a table",
            options=table_names,
            placeholder="Choose a table",
            index=None if len(table_names) != 1 else 0,
        )

        if table_name:
            tb = dataset[table_name].reset_index()

            # 3/ INDICATOR
            columns = sort_indicators(tb.columns)
            column_name = st.selectbox(
                label="Select the entity column",
                options=columns,
                placeholder="Choose an indicator",
                index=0 if set(COLUMN_NAME_PRIORITIES).intersection(set(columns)) else None,
            )

            ####################################################################################################
            # HARMONIZATION (generation)
            ####################################################################################################
            if column_name:
                # Transform to string if category
                if tb[column_name].apply(type).eq("category").all():
                    tb[column_name] = tb[column_name].astype(str)

                # Raise error if nan
                if tb[column_name].isna().any():
                    st.error(
                        f"Column '{column_name}' contains missing values. Please clean the data before harmonizing."
                    )
                    st.stop()

                # Sanity check on typing: only support for indicators of type string
                if not tb[column_name].apply(type).eq(str).all():
                    # if tb[column_name].dtype not in ["object", "category"]:
                    st.error(
                        f"Column '{column_name}' is of type `{tb[column_name].dtype}` but 'string' is expected. Harmonization for non-string columns is not supported yet."
                    )
                    st.stop()

                harmonizer = Harmonizer(
                    tb=tb,
                    colname=column_name,
                    output_file=f"{STEP_DIR}/data/garden/{dataset.m.namespace}/{dataset.m.version}/{dataset.m.short_name}.countries.json",
                )
                harmonizer.run_automatic()

                ambiguous = cast(List, harmonizer.ambiguous)
                mapping = harmonizer.mapping

                st.divider()

                ## 1/ AUTOMATIC
                st.session_state.entity_mapping = mapping
                if mapping:
                    with st.expander("Automatically mapped entities", expanded=False):
                        st.dataframe(mapping)

                ## 2/ MANUAL (user input needed)
                with st.container(border=True):
                    # with st.form("form"):
                    # Title
                    st.markdown("#### Manual entity mapping needed")
                    st.markdown(f"{len(ambiguous)} ambiguous regions")

                    # Create a container for each region with:
                    # - three columns: original entity name, suggestions (as a selectbox), and free text input for custom name
                    # - a toggle to ignore the region (no mapping)
                    for i, region in enumerate(ambiguous, 1):
                        # no exact match, get nearby matches
                        suggestions = harmonizer.get_suggestions(
                            region, institution=None, num_suggestions=NUM_SUGGESTIONS
                        )
                        with st.container(border=True):
                            # Original name
                            st.markdown(f"**{region}**")
                            col1, col2 = st.columns(2)

                            # New name, custom (useful if no suggestion is good enough)
                            with col2:
                                value_custom = st.text_input(
                                    label="Enter custom name",
                                    key=f"region_custom_{i}",
                                    # placeholder="Enter custom name",
                                    # label_visibility="collapsed",
                                    help="Use this when no suggestion is good enough",
                                )
                            # New name, from selectbox
                            with col1:
                                value_selected = st.selectbox(
                                    label="Select a region",
                                    options=suggestions,
                                    index=0,
                                    # label_visibility="collapsed",
                                    key=f"region_suggestion_{i}",
                                    disabled=value_custom != "",
                                )
                            value_ignore = st.toggle(
                                label="Ignore",
                                key=f"region_ignore_{i}",
                            )
                            # Add defined mapping (only if not ignored)
                            if not value_ignore:
                                st.session_state.entity_mapping[region] = cast(
                                    str, value_custom if value_custom not in ("", None) else value_selected
                                )
                    # 3/ PATH to export & export button
                    path_export = cast(str, harmonizer.output_file)
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
                        st.json(st.session_state.entity_mapping, expanded=False)
                        with open(path_export, "w") as ostream:
                            json.dump(st.session_state.entity_mapping, ostream, indent=2)

                        st.success(f"Harmonization mapping exported to {path_export}")
