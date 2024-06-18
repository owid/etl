"""Concerns the second stage of wizard charts, when the indicator mapping is constructed."""

import time
from itertools import islice
from typing import Any, Dict, List

import pandas as pd
import streamlit as st
from pydantic import BaseModel
from streamlit_extras.grid import grid
from structlog import get_logger

from apps.backport.datasync.data_metadata import variable_data_df_from_s3
from apps.wizard.app_pages.indicator_upgrade.explore_mode import st_explore_indicator_dialog
from apps.wizard.app_pages.indicator_upgrade.utils import get_indicators_from_datasets
from apps.wizard.utils import set_states
from etl.config import OWID_ENV
from etl.db import get_engine
from etl.match_variables import find_mapping_suggestions, preliminary_mapping

# Logger
log = get_logger()
# App can't hanle too many indicators to map. We set an upper limit
LIMIT_VARS_TO_MAP = 100


@st.cache_data(show_spinner=False)
def preliminary_mapping_cached(old_indicators, new_indicators, match_identical):
    mapping, missing_old, missing_new = preliminary_mapping(
        old_indicators=old_indicators,
        new_indicators=new_indicators,
        match_identical=match_identical,
    )

    if not mapping.empty:
        indicator_mapping_auto = mapping.set_index("id_old")["id_new"].to_dict()
    else:
        indicator_mapping_auto = {}
    return indicator_mapping_auto, missing_old, missing_new


@st.cache_data(show_spinner=False)
def find_mapping_suggestions_cached(missing_old, missing_new, similarity_name):
    print("start")
    t0 = time.time()
    with st.spinner():
        suggestions = find_mapping_suggestions(
            missing_old=missing_old,
            missing_new=missing_new,
            similarity_name=similarity_name,
        )  # type: ignore
    print(time.time() - t0)
    # Sort by max similarity: First suggestion is that one that has the highest similarity score with any of its suggested new vars.
    suggestions = sorted(suggestions, key=lambda x: x["new"]["similarity"].max(), reverse=True)
    return suggestions


@st.cache_data(show_spinner=False)
def get_indicator_id_to_display(old_indicators, new_indicators):
    df = pd.concat([old_indicators, new_indicators], ignore_index=True)  # .drop_duplicates()
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    indicator_id_to_display = df.set_index("id")["display_name"].to_dict()
    return indicator_id_to_display


def st_show_score(similarity_max):
    if similarity_max > 80:
        color = "blue"
    elif similarity_max > 60:
        color = "green"
    elif similarity_max > 40:
        color = "orange"
    else:
        color = "red"
    st.markdown(f":{color}[**{similarity_max}%**]")


def st_mappings_auto(indicator_mapping_auto, enable_bulk_explore, cols, indicator_id_to_display, df_data):
    old_var_selectbox = []
    ignore_selectbox = []
    new_var_selectbox = []

    if len(indicator_mapping_auto) > LIMIT_VARS_TO_MAP:
        st.warning(
            f"Too many indicators to map ({len(indicator_mapping_auto)})! Showing only the first {LIMIT_VARS_TO_MAP}. If you want to map more indicators, do it in batches. That is, first map this batch and approve the generated chart revisions in admin. Once you are done, run this app again. Make sure you have approved the previously generated revisions!"
        )
        indicator_mapping_auto = dict(islice(indicator_mapping_auto.items(), LIMIT_VARS_TO_MAP))

    # Automatically mapped indicators (non-editable)
    grid_indicators_auto = grid(cols)

    # Loop over automatically mapped indicators
    for i, (indicator_old, indicator_new) in enumerate(indicator_mapping_auto.items()):
        # Ignore checkbox
        check = st.session_state.get("ignore-all")
        check = check if check else st.session_state.get(f"auto-ignore-{i}", False)
        element = grid_indicators_auto.checkbox(
            "Ignore",
            key=f"auto-ignore-{i}",
            label_visibility="collapsed",
            value=st.session_state.get("ignore-all", st.session_state.get(f"auto-ignore-{i}", False)),
            on_change=lambda: set_states({"submitted_indicators": False}),
        )
        ignore_selectbox.append(element)
        # Old indicator selectbox
        grid_indicators_auto.selectbox(
            label=f"auto-{i}-left",
            options=[indicator_old],
            disabled=True,
            label_visibility="collapsed",
            format_func=indicator_id_to_display.get,
        )
        # element = grid_indicators_auto.write(
        #     indicator_id_to_display.get(indicator_old),
        # )
        old_var_selectbox.append(indicator_old)
        # New indicator selectbox
        element = grid_indicators_auto.selectbox(
            label=f"auto-{i}-right",
            options=[indicator_new],
            disabled=True,
            label_visibility="collapsed",
            format_func=indicator_id_to_display.get,
        )
        new_var_selectbox.append(element)
        # Score
        grid_indicators_auto.markdown(":violet[**100%**]")
        ## Explore mode checkbox
        show_explore = grid_indicators_auto.button(
            label="🔎",
            key=f"auto-explore-{i}",
        )
        if show_explore:
            if not enable_bulk_explore:
                df_data = get_indicator_data_cached([indicator_old, indicator_new])
            st_explore_indicator_dialog(
                df_data,
                indicator_old,
                indicator_new,
                indicator_id_to_display,
            )  # type: ignore

    return old_var_selectbox, ignore_selectbox, new_var_selectbox


def st_mapping_manual(
    suggestions,
    enable_bulk_explore,
    cols,
    indicator_id_to_display,
    old_var_selectbox,
    ignore_selectbox,
    new_var_selectbox,
    df_data,
):
    grid_indicators_manual = grid(cols)

    # Show only first 100 indicators to map (otherwise app crashes)
    if len(suggestions) > LIMIT_VARS_TO_MAP:
        st.warning(
            f"Too many indicators to map ({len(suggestions)})! Showing only the first {LIMIT_VARS_TO_MAP}. If you want to map more indicators, do it in batches. That is, first map this batch and approve the generated chart revisions in admin. Once you are done, run this app again. Make sure you have approved the previously generated revisions!"
        )
        suggestions = suggestions[:LIMIT_VARS_TO_MAP]

    for i, suggestion in enumerate(suggestions):
        indicator_old = suggestion["old"]["id_old"]
        similarity_max = int(suggestion["new"]["similarity"].max().round(0))

        # Ignore checkbox
        ## If ignore-all is checked, then inherit. Otherwise preserve value.
        check = st.session_state.get("ignore-all")
        check = check if check else st.session_state.get(f"manual-ignore-{i}", False)
        element_ignore = grid_indicators_manual.checkbox(
            "Ignore",
            key=f"manual-ignore-{i}",
            label_visibility="collapsed",
            value=check,
            on_change=lambda: set_states({"submitted_indicators": False}),
        )
        ignore_selectbox.append(element_ignore)
        # Old indicator
        grid_indicators_manual.selectbox(
            label=f"manual-{i}-left",
            options=[indicator_old],
            disabled=True,
            label_visibility="collapsed",
            format_func=indicator_id_to_display.get,
        )

        old_var_selectbox.append(indicator_old)
        # New indicator selectbox
        indicator_new_manual = grid_indicators_manual.selectbox(
            label=f"manual-{i}-right",
            options=suggestion["new"]["id_new"],
            disabled=False,
            label_visibility="collapsed",
            format_func=indicator_id_to_display.get,
            on_change=lambda: set_states({"submitted_indicators": False}),
        )
        new_var_selectbox.append(indicator_new_manual)
        # Score
        with grid_indicators_manual.container():
            st_show_score(similarity_max)
        ## Explore mode checkbox
        show_explore = grid_indicators_manual.button(
            label="🔎",
            key=f"manual-explore-{i}",
        )
        if show_explore:
            if not enable_bulk_explore:
                df_data = get_indicator_data_cached([indicator_old, indicator_new_manual])
            st_explore_indicator_dialog(
                df_data,
                indicator_old,
                indicator_new_manual,
                indicator_id_to_display,
            )  # type: ignore

    return old_var_selectbox, ignore_selectbox, new_var_selectbox


def ask_and_get_indicator_mapping(search_form) -> "IndicatorConfig":
    """Ask and get indicator mapping."""
    indicator_config = IndicatorConfig()

    ###########################################################################
    # 1/ PROCESSING: Get indicators, find similarities and suggestions, etc.
    ###########################################################################

    # 1.1/ INTERNAL PROCESSING
    # Get indicators from old and new datasets
    old_indicators, new_indicators = get_indicators_from_datasets(
        search_form.dataset_old_id,
        search_form.dataset_new_id,
        show_new_not_in_old=False,
    )

    # 1.2/ Build display mappings: id -> display_name
    ## This is to display the indicators in the selectboxes with format "[id] name"
    indicator_id_to_display = get_indicator_id_to_display(old_indicators, new_indicators)

    # 1.3/ Get auto indicator mapping (if mapping by identical name is enabled)
    ## [OPTIONAL] Note that when the old and new datasets are the same, this option is disabled. Otherwise all indicators are mapped (which probably does not make sense?) In that case, set map_identical_indicators to False (when search_form.dataser_old_id == search_form.dataset_new_id)
    indicator_mapping_auto, missing_old, missing_new = preliminary_mapping_cached(
        old_indicators=old_indicators,
        new_indicators=new_indicators,
        match_identical=search_form.map_identical_indicators,
    )

    # 1.4/ Get remaining mapping suggestions
    # This is for those indicators which couldn't be automatically mapped
    suggestions = find_mapping_suggestions_cached(
        missing_old=missing_old,
        missing_new=missing_new,
        similarity_name=search_form.similarity_function_name,
    )  # type: ignore

    # [OPTIONAL] 1.5 EXPLORE MODE
    # Get data points
    if search_form.enable_bulk_explore:
        df_data = get_indicator_data_cached(list(set(old_indicators["id"]) | set(new_indicators["id"])))
    else:
        df_data = None
    ###########################################################################
    #
    # 2/ DISPLAY: Show the indicator mapping form
    #
    ###########################################################################
    if not indicator_mapping_auto and not suggestions:
        st.warning(
            f"It looks as the dataset [{search_form.dataset_old_id}]({OWID_ENV.dataset_admin_site(search_form.dataset_old_id)}) has no indicator in use in any chart! Therefore, no mapping is needed."
        )
    else:
        with st.container(border=True):
            # 2.1/ DISPLAY MAPPING SECTION
            ## Header
            st.header("Map indicators")
            st.markdown(
                "Map indicators from the old to the new dataset. The idea is that the indicators in the new dataset will replace those from the old dataset in our charts. You can choose to ignore some indicators if you want to.",
            )
            # Column proportions per row (out of 1)
            cols = [10, 43, 43, 7, 4.5]  # if search_form.enable_bulk_explore else [10, 45, 45, 5]

            #################################
            # 2.2/ Header: Titles, links, general checkboxes
            #################################
            grid_indicators_header = grid(cols, cols)

            # Row 1
            grid_indicators_header.empty()
            grid_indicators_header.subheader("Old dataset")
            grid_indicators_header.subheader("New dataset")
            grid_indicators_header.empty()
            grid_indicators_header.empty()
            # Row 2
            grid_indicators_header.checkbox(
                "Skip",
                help="Check to ignore all indicator mappings. Check individual rows to only ignore a particular indicator mapping.",
                on_change=lambda: set_states({"submitted_indicators": False}),
                key="ignore-all",
            )
            grid_indicators_header.link_button(
                "Explore dataset", OWID_ENV.dataset_admin_site(search_form.dataset_old_id)
            )
            grid_indicators_header.link_button(
                "Explore dataset",
                OWID_ENV.dataset_admin_site(search_form.dataset_new_id),
            )
            grid_indicators_header.caption(
                "Score",
                help="Similarity score between the old indicator and the 'closest' new indicator (from 0 to 100%). Indicators with low scores are likely not to have a good match.",
            )
            grid_indicators_header.caption(
                "🔎",
                help="Explore the distribution of the currently compared indicators.",
            )

            #################################
            # 2.3/ Automatically mapped indicators
            # Show columns with indicators that were automatically mapped
            #
            #################################
            old_var_selectbox, ignore_selectbox, new_var_selectbox = st_mappings_auto(
                indicator_mapping_auto,
                search_form.enable_bulk_explore,
                cols,
                indicator_id_to_display,
                df_data,
            )

            #################################
            # 2.4/ Manually mapped indicators
            # Sow columns with indicators that couldn't be mapped automatically and had to be mapped manually
            #
            #################################
            old_var_selectbox, ignore_selectbox, new_var_selectbox = st_mapping_manual(
                suggestions,
                search_form.enable_bulk_explore,
                cols,
                indicator_id_to_display,
                old_var_selectbox,
                ignore_selectbox,
                new_var_selectbox,
                df_data,
            )

            #################################
            # 2.5/ Submit button
            #################################
            # Form button
            st.button(
                label="Next (2/3)",
                type="primary",
                use_container_width=True,
                on_click=set_states_after_submitting,
            )

            if st.session_state.submitted_indicators:
                # BUILD MAPPING
                indicator_mapping = _build_indicator_mapping(
                    old_var_selectbox,
                    new_var_selectbox,
                    ignore_selectbox,
                )
                indicator_config = IndicatorConfig(indicator_mapping=indicator_mapping)
    return indicator_config


class IndicatorConfig(BaseModel):
    is_valid: bool = False
    indicator_mapping: Dict[int, int] = {}

    def __init__(self, **data: Any) -> None:
        """Construct indicator config object."""
        if "indicator_mapping" in data:
            data["is_valid"] = True
        super().__init__(**data)


@st.cache_data(show_spinner=False)
def _build_indicator_mapping(old_var_selectbox, new_var_selectbox, ignore_selectbox) -> Dict[int, int]:
    if len(old_var_selectbox) != len(new_var_selectbox):
        raise ValueError("Something went wrong! The number of old and new indicators is different.")
    if len(old_var_selectbox) != len(ignore_selectbox):
        raise ValueError("Something went wrong! The number of old indicators and ignore checkboxes is different.")
    if len(new_var_selectbox) != len(ignore_selectbox):
        raise ValueError("Something went wrong! The number of new indicators and ignore checkboxes is different.")
    indicator_mapping = {
        int(old): int(new)
        for old, new, ignore in zip(old_var_selectbox, new_var_selectbox, ignore_selectbox)
        if not ignore
    }
    return indicator_mapping


@st.cache_data(show_spinner=False)
def get_indicator_data_cached(indicator_ids: List[int]):
    with st.spinner(
        "Retrieving data values from S3. This might take some time... If you don't need this, disable the 'Explore' option from the 'parameters' section."
    ):
        df = variable_data_df_from_s3(
            get_engine(),
            variable_ids=[int(v) for v in indicator_ids],
            workers=10,
            value_as_str=False,
        )
    return df


def reset_indicator_form() -> None:
    """ "Reset indicator form."""
    # Create dictionary with checkboxes set to False
    checks = {
        str(k): False
        for k in st.session_state.keys()
        if str(k).startswith("auto-ignore-") or str(k).startswith("manual-ignore-")
    }

    # Create dictionary with widgets set to False
    set_states(
        {
            "ignore-all": False,
            **checks,
        }
    )


def set_states_after_submitting():
    set_states(
        {
            "submitted_indicators": True,
            "submitted_charts": False,
        },
        logging=True,
    )
