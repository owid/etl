"""Concerns the second stage of wizard charts, when the indicator mapping is constructed."""

from typing import Dict, List, cast

import pandas as pd
import streamlit as st
from structlog import get_logger

from apps.backport.datasync.data_metadata import variable_data_df_from_s3
from apps.wizard.app_pages.indicator_upgrade.explore_mode import st_explore_indicator
from apps.wizard.app_pages.indicator_upgrade.utils import (
    find_mapping_suggestions_cached,
    get_indicators_from_datasets,
    preliminary_mapping_cached,
)
from apps.wizard.utils import Pagination, set_states
from etl.config import OWID_ENV
from etl.db import get_engine

# Logger
log = get_logger()
# App can't hanle too many indicators to map. We set an upper limit
# Max number of indicator mappings in one page
MAPPING_PER_PAGE_OPTIONS = [
    5,
    10,
    20,
    50,
    100,
    500,
]
MAPPINGS_PER_PAGE_DEFAULT = 50
# COLUMN layout
COLUMN_LAYOUT = [10, 40, 50, 7, 4.5]


def render_indicator_mapping(search_form) -> Dict[int, int]:
    """Render indicator mapping section."""

    indicator_mapping = {}

    ###########################################################################
    # 1/ PROCESSING: Get indicators, find similarities and suggestions, etc.
    ###########################################################################
    indicator_upgrades, indicator_id_to_display, df_data = get_params(search_form)

    ###########################################################################
    # 2/ DISPLAY: Show the indicator mapping form
    ###########################################################################
    if indicator_upgrades == []:
        st.warning(
            f"It looks as the dataset [{search_form.dataset_old_id}]({OWID_ENV.dataset_admin_site(search_form.dataset_old_id)}) has no indicator in use in any chart! Therefore, no mapping is needed."
        )
    else:
        with st.container(border=True):
            # 1/ Title, description, options
            st_show_header()

            # 2/ Map indicators
            # Show columns with indicators that were automatically (and manually) mapped
            indicator_upgrades_shown = st_show_indicator_upgrades(
                indicator_ups=indicator_upgrades,
                pagination_key="pagination_indicator_mapping",
                indicator_id_to_display=indicator_id_to_display,
                df_data=df_data,
            )

            # 3/ Submit button
            st.button(
                label="Next (2/3)",
                type="primary",
                use_container_width=True,
                on_click=set_states_after_submitting,
                help="Only indicators in this page will be submitted. If you want to map molre indicators at once, make the number of elements per page bigger.",
            )

            if st.session_state.submitted_indicators:
                # Define mapping of indicators
                indicator_mapping = {
                    iu.id_old: iu.id_new_selected
                    for iu in indicator_upgrades_shown
                    if not iu.skip and iu.id_new_selected is not None
                }
    return indicator_mapping


@st.cache_data
def _get_params_cached(
    dataset_old_id,
    dataset_new_id,
    map_identical_indicators,
    similarity_function_name,
    enable_bulk_explore,
):
    """Cached version of `get_params`.

    `get_params` can't be hashed because its input is a custom object.
    """
    # 1/ Get indicators from old and new datasets
    old_indicators, new_indicators = get_indicators_from_datasets(
        dataset_old_id,
        dataset_new_id,
        show_new_not_in_old=False,
    )

    # 2/ Build display mappings: id -> display_name
    ## This is to display the indicators in the selectboxes with format "[id] name"
    indicator_id_to_display = get_indicator_id_to_display(
        old_indicators,
        new_indicators,
    )

    # 3/ Build indicator upgrades
    ## [OPTIONAL] Note that when the old and new datasets are the same, this option is disabled. Otherwise all indicators are mapped (which probably does not make sense?) In that case, set map_identical_indicators to False (when search_form.dataser_old_id == search_form.dataset_new_id)
    indicator_mapping_auto, missing_old, missing_new = preliminary_mapping_cached(
        old_indicators=old_indicators,
        new_indicators=new_indicators,
        match_identical=map_identical_indicators,
    )

    # 1.4/ Get remaining mapping suggestions
    # This is for those indicators which couldn't be automatically mapped
    suggestions = find_mapping_suggestions_cached(
        missing_old=missing_old,
        missing_new=missing_new,
        similarity_name=similarity_function_name,
    )  # type: ignore

    iu = []
    if indicator_mapping_auto or suggestions:
        # Build list with automatic indicator upgrades
        iu_auto = [
            IndicatorUpgrade.from_auto(
                id_old=old,
                id_new=new,
            )
            for old, new in indicator_mapping_auto.items()
        ]
        # Build list with manual indicator upgrades
        iu_man = [
            IndicatorUpgrade.from_manual(
                id_old=suggestion["old"]["id_old"],
                suggestions=suggestion["new"],
                # ids_new=list(suggestion["new"]["id_new"]),
            )
            for suggestion in suggestions
        ]
        # Combine lists
        iu = iu_auto + iu_man

    # Get datapoints
    if enable_bulk_explore:
        df_data = get_indicator_data_cached(list(set(old_indicators["id"]) | set(new_indicators["id"])))
    else:
        df_data = None

    return iu, indicator_id_to_display, df_data


def get_params(search_form):
    """Get all the parameters required to display the widgets.

    It creates IndicatorUpgrade objects.
    """
    iu, indicator_id_to_display, df_data = _get_params_cached(
        search_form.dataset_old_id,
        search_form.dataset_new_id,
        search_form.map_identical_indicators,
        search_form.similarity_function_name,
        search_form.enable_bulk_explore,
    )

    # Set states
    st.session_state.indicator_upgrades_ignore = {i.key: False for i in iu}

    return iu, indicator_id_to_display, df_data


def st_show_header():
    """Show title, description, etc."""
    # Title
    st.header("Map indicators")
    st.markdown(
        "Map indicators from the [Old dataset]({OWID_ENV.dataset_admin_site(search_form.dataset_new_id)}) to the [New dataset]({OWID_ENV.dataset_admin_site(search_form.dataset_new_id)}). The idea is that the indicators in the new dataset will replace those from the old dataset in our charts. You can choose to ignore some indicators if you want to.",
    )

    # Row 1
    with st.popover("Options"):
        st.selectbox(
            "Number of mappings per page",
            options=MAPPING_PER_PAGE_OPTIONS,
            key="mappings-per-page",
            help="Select the number of indicator mappings to display per page. A larger number of mappings shown per page cna slow the app.",
            index=MAPPING_PER_PAGE_OPTIONS.index(MAPPINGS_PER_PAGE_DEFAULT),
            on_change=lambda: set_states({"submitted_indicators": False}),
        )

        st.divider()
        st.button(
            label="🗑️ Skip all indicators",
            on_click=lambda: set_states(
                {
                    "submitted_indicators": False,
                    "ignore-all": True,
                    "not-ignore-all": False,
                }
            ),
        )
        st.button(
            label="↩️ Unskip all indicators",
            on_click=lambda: set_states(
                {
                    "submitted_indicators": False,
                    "not-ignore-all": True,
                    "ignore-all": False,
                }
            ),
        )


def st_show_indicator_upgrades(
    indicator_ups, pagination_key, indicator_id_to_display, df_data
) -> List["IndicatorUpgrade"]:
    """Display indicator upgrades."""
    # Pagination
    pagination = Pagination(
        indicator_ups,
        items_per_page=st.session_state["mappings-per-page"],
        pagination_key=pagination_key,
        on_click=lambda: set_states({"submitted_indicators": False}),
    )
    ## Show controls only if needed
    if len(indicator_ups) > st.session_state["mappings-per-page"]:
        pagination.show_controls("bar")

    # Show indicator mapping
    indicator_upgrades_shown = pagination.get_page_items()
    for indicator_up in indicator_upgrades_shown:
        IndicatorUpgradeShow(indicator_up).render(
            indicator_id_to_display=indicator_id_to_display,
            df_data=df_data,
        )

    return indicator_upgrades_shown


class IndicatorUpgrade:
    def __init__(self, id_old: int, ids_new: List[int], auto: bool, scores: Dict[int, float] = {}):
        """Handle a single indicator mapping."""
        self.id_old = id_old
        self.ids_new = ids_new
        self.auto = auto
        if not auto:
            self.scores = scores
        else:
            self.scores = {self.id_old: 100}
        self.score_max = max(scores.values()) if scores else 100

        # Keys
        self.key = str(self.id_old)
        self.key_ignore = f"{self.key}-ignore"
        self.key_id_new_selected = f"{self.key}-new"
        self.key_explore = f"{self.key}-explore"

    @classmethod
    def from_auto(cls, id_old: int, id_new: int):
        """Create automatic mapping."""
        return cls(
            id_old=id_old,
            ids_new=[id_new],
            auto=True,
        )

    @classmethod
    def from_manual(cls, id_old: int, suggestions: pd.DataFrame):
        """Create instance from manual mapping.

        Different than from_auto because it has to handle multiple suggestions.
        """
        ids_new = list(suggestions["id_new"])
        scores = suggestions.set_index("id_new")["similarity"].to_dict()
        scores = cast(Dict[int, float], scores)
        return cls(
            id_old=id_old,
            ids_new=ids_new,
            scores=scores,
            auto=False,
        )

    @property
    def skip(self) -> bool:
        def _get_skip_state() -> bool:
            if "indicator_upgrades_ignore" in st.session_state:
                if self.key in st.session_state["indicator_upgrades_ignore"]:
                    return st.session_state["indicator_upgrades_ignore"][self.key]
                else:
                    error_msg = f"No key {self.key} in st.session_state['indicator_upgrades_ignore']! It should be created when creating the indicator upgrades."
                    log.error(error_msg)
                    raise ValueError(error_msg)
            else:
                raise ValueError(
                    "No indicator_upgrades_ignore in session state! It should be created when creating the indicator upgrades."
                )

        def _set_skip_state(value: bool) -> None:
            if "indicator_upgrades_ignore" in st.session_state:
                if self.key in st.session_state["indicator_upgrades_ignore"]:
                    st.session_state["indicator_upgrades_ignore"][self.key] = value

        """If this upgrade should be skipped."""
        if st.session_state.get("ignore-all"):
            _set_skip_state(True)
            return True
        if st.session_state.get("not-ignore-all"):
            _set_skip_state(False)
            return False
        return _get_skip_state()

    @property
    def id_new_selected(self):
        selected = st.session_state.get(self.key_id_new_selected, self.ids_new[0])
        return selected

    @property
    def text_similarity_score(self) -> int | float:
        if self.auto:
            return 100
        else:
            assert self.id_new_selected in self.scores
            return self.scores[self.id_new_selected]


class IndicatorUpgradeShow:
    """Wrapper around IndicatorUpgrade to help visualise it in Streamlit."""

    def __init__(self, indicator_upgrade: "IndicatorUpgrade"):
        self.iu = indicator_upgrade
        self.colun_layout = [10, 100, 7, 4.5]
        self.show_explore = None

    @st.experimental_fragment
    def render(self, indicator_id_to_display, df_data=None):
        with st.container(border=True):
            cols = [100, 10, 10]
            cols = st.columns(cols, vertical_alignment="bottom")

            # Indicators (old, new)
            with cols[0]:
                indicator_chosen = self._st_show_indicators(indicator_id_to_display)

            with cols[1]:
                # Similarity score
                self._st_show_text_score()

            # Extra options
            with cols[2]:
                options = self._st_show_extra_options()

            if options["explore"]:
                self._st_explore_indicator_modal(
                    indicator_old=self.iu.id_old,
                    indicator_new=indicator_chosen,
                    indicator_id_to_display=indicator_id_to_display,
                    df=df_data,
                    # enable_bulk_explore,
                )  # type: ignore

            return indicator_chosen

    def _st_show_indicators(self, indicator_id_to_display):
        # Old indicator selectbox
        text = f"{indicator_id_to_display.get(self.iu.id_old)}"
        st.markdown(text)

        # New indicator selectbox
        indicator_chosen = st.selectbox(
            label="New indicator",
            key=self.iu.key_id_new_selected,
            options=self.iu.ids_new,  # indicator_up.ids_new,
            disabled=self.iu.auto,
            label_visibility="collapsed",
            format_func=indicator_id_to_display.get,
            on_change=lambda: set_states({"submitted_indicators": False}),
        )

        return indicator_chosen

    def _st_show_text_score(self):
        """Show score of selected new indicator."""
        score = self.iu.scores[self.iu.id_new_selected] if not self.iu.auto else 100
        if self.iu.auto:
            color = "violet"
        else:
            if score > 80:
                color = "blue"
            elif score > 60:
                color = "green"
            elif score > 40:
                color = "orange"
            else:
                color = "red"
        score = int(round(self.iu.text_similarity_score, 0))
        st.markdown(f":{color}[**{score}%**]")

    def _st_show_extra_options(self):
        self._st_show_checkbox_ignore()

        explore = False
        if self.iu.id_old != self.iu.id_new_selected:
            # Explore mode checkbox
            explore = st.button(
                label="🔎",
                key=f"{self.iu.key_explore}",
            )

        return {
            "explore": explore,
        }

    def _st_show_checkbox_ignore(self):
        def _set_states_checkbox():
            set_states(
                {
                    "submitted_indicators": False,
                    "ignore-all": False,
                    "not-ignore-all": False,
                }
            )
            k = "indicator_upgrades_ignore"
            st.session_state[k][self.iu.key] = not st.session_state[k][self.iu.key]

        st.checkbox(
            label="Ignore",
            key=self.iu.key_ignore,
            # label_visibility="collapsed",
            value=self.iu.skip,
            on_change=_set_states_checkbox,
        )

    @st.experimental_dialog("Explore changes in the new indicator", width="large")  # type: ignore
    def _st_explore_indicator_modal(self, indicator_old, indicator_new, indicator_id_to_display, df=None) -> None:
        """Same as st_explore_indicator but framed in a dialog.

        NOTE: We are currently using a DIALOG, which is called from a FRAGMENT. Apparently this is possible?
        If not, we can always move the explore mode right below of the indicator mapping form.

        Could consider using 'stateful button': https://arnaudmiribel.github.io/streamlit-extras/extras/stateful_button/

        More on dialogs: https://docs.streamlit.io/develop/api-reference/execution-flow/st.dialog
        """
        if indicator_old == indicator_new:
            st.warning("Both indicators are the same")
        else:
            # Get data if necessary
            if df is None:
                df = get_indicator_data_cached([indicator_old, indicator_new])
            st_explore_indicator(
                df,
                indicator_old,
                indicator_new,
                indicator_id_to_display,
            )


@st.cache_data(show_spinner=False)
def get_indicator_id_to_display(old_indicators, new_indicators):
    df = pd.concat([old_indicators, new_indicators], ignore_index=True)  # .drop_duplicates()
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    indicator_id_to_display = df.set_index("id")["display_name"].to_dict()
    return indicator_id_to_display


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
        },
    )


def set_states_after_submitting():
    set_states(
        {
            "submitted_indicators": True,
            "submitted_charts": False,
        },
        logging=True,
    )
