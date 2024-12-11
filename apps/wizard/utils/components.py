import json
from contextlib import contextmanager
from copy import deepcopy
from typing import Any, Callable, Dict, List, Literal, Optional

import numpy as np
import streamlit as st
import streamlit.components.v1 as components

from apps.wizard.utils.chart_config import bake_chart_config
from etl.config import OWID_ENV, OWIDEnv
from etl.grapher_model import Variable

HORIZONTAL_STYLE = """<style class="hide-element">
    /* Hides the style container and removes the extra spacing */
    .element-container:has(.hide-element) {
        display: none;
    }
    /*
        The selector for >.element-container is necessary to avoid selecting the whole
        body of the streamlit app, which is also a stVerticalBlock.
    */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker) {
        display: flex;
        flex-direction: row !important;
        flex-wrap: wrap;
        gap: 1rem;
        align-items: baseline;
    }
    /* Override the default width of selectboxes in horizontal layout */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker) select {
        min-width: 200px;  /* Set a minimum width for selectboxes */
        max-width: 400px;  /* Optional: Set a max-width to avoid overly wide selectboxes */
    }
    /* Buttons and their parent container all have a width of 704px, which we need to override */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker) div {
        width: max-content !important;
    }
    /* Just an example of how you would style buttons, if desired */
    /*
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker) button {
        border-color: red;
    }
    */
</style>
"""


@contextmanager
def st_horizontal():
    st.markdown(HORIZONTAL_STYLE, unsafe_allow_html=True)
    with st.container():
        st.markdown('<span class="hide-element horizontal-marker"></span>', unsafe_allow_html=True)
        yield


def default_converter(o):
    if isinstance(o, np.integer):  # ignore
        return int(o)
    else:
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def grapher_chart(
    catalog_path: Optional[str] = None,
    variable_id: Optional[int | List[int]] = None,
    variable: Optional[Variable | List[Variable]] = None,
    chart_config: Optional[Dict[str, Any]] = None,
    owid_env: OWIDEnv = OWID_ENV,
    selected_entities: Optional[list] = None,
    included_entities: Optional[list] = None,
    height=600,
    **kwargs,
):
    """Plot a Grapher chart using the Grapher API.

    You can either plot a given chart config (using chart_config) or plot an indicator with its default metadata using either catalog_path, variable_id or variable.

    Note: You can find more details on our Grapher API at https://files.ourworldindata.org/schemas/grapher-schema.latest.json.

    Parameters
    ----------
    catalog_path : Optional[str], optional
        Path to the catalog file, by default None
    variable_id : Optional[int], optional
        Variable ID, by default None
    variable : Optional[Variable], optional
        Variable object, by default None
    chart_config : Optional[Dict[str, Any]], optional
        Configuration of the chart, by default None
    owid_env : OWIDEnv, optional
        Environment configuration, by default OWID_ENV
    selected_entities : Optional[list], optional
        List of entities to plot, by default None. If None, a random sample of num_sample_selected_entities will be plotted. Use entity names!
    included_entities : Optional[list], optional
        NOT WORKING ATM AS EXPECTED ATM. List of entities to include in chart. The rest are excluded! This is equivalent to `includedEntities` in Grapher. Use entity IDs!
    height : int, optional
        Height of the chart, by default 600
    """
    # Get data / metadata if no chart config is provided
    if chart_config is None:
        chart_config = bake_chart_config(
            catalog_path=catalog_path,
            variable_id=variable_id,
            variable=variable,
            selected_entities=selected_entities,
            included_entities=included_entities,
            owid_env=owid_env,
        )

    _chart_html(chart_config, owid_env, height=height, **kwargs)


def grapher_chart_from_url(chart_url: str, height=600):
    """Plot a Grapher chart using the Grapher API."""
    chart_animation_iframe_html = f"""
    <iframe src="{chart_url}" loading="lazy"
            style="width: 100%; height: 600px; border: 0px none;"
            allow="web-share; clipboard-write"></iframe>
    """
    return st.components.v1.html(chart_animation_iframe_html, height=height)  # type: ignore


def _chart_html(chart_config: Dict[str, Any], owid_env: OWIDEnv, height=600, **kwargs):
    """Plot a Grapher chart using the Grapher API.

    Parameters
    ----------
    chart_config : Dict[str, Any]
        Configuration of the chart.
    owid_env : OWIDEnv
        Environment configuration. This is needed to access the correct API (changes between servers).
    """
    chart_config_tmp = deepcopy(chart_config)

    chart_config_tmp["bakedGrapherURL"] = f"{owid_env.base_site}/grapher"
    chart_config_tmp["adminBaseUrl"] = owid_env.base_site
    chart_config_tmp["dataApiUrl"] = f"{owid_env.indicators_url}/"

    HTML = f"""
    <link href="https://fonts.googleapis.com/css?family=Lato:300,400,400i,700,700i|Playfair+Display:400,700&amp;display=swap" rel="stylesheet" />
    <link rel="stylesheet" href="https://ourworldindata.org/assets/owid.css" />
    <div class="StandaloneGrapherOrExplorerPage">
        <main>
            <figure data-grapher-src></figure>
        </main>
        <script> document.cookie = "isAdmin=true;max-age=31536000" </script>
        <script type="module" src="https://ourworldindata.org/assets/owid.mjs"></script>
        <script type="module">
            var jsonConfig = {json.dumps(chart_config_tmp, default=default_converter)}; window.Grapher.renderSingleGrapherOnGrapherPage(jsonConfig);
        </script>
    </div>
    """

    components.html(HTML, height=height, **kwargs)


def tag_in_md(tag_name: str, color: str, icon: Optional[str] = None):
    """Create a custom HTML tag.

    Parameters
    ----------
    tag_name : str
        Tag name.
    color : str
        Color of the tag. Must be replaced with any of the following supported colors: blue, green, orange, red, violet, gray/grey, rainbow
    icon: str
        Icon of the tag. Can be material (e.g. ':material/upgrade:') or emoji (e.g. '🪄').
    """
    if icon is not None:
        return f":{color}-background[{icon}: {tag_name}]"
    else:
        return f":{color}-background[{tag_name}]"


def st_tag(tag_name: str, color: str, icon: str):
    """Create a custom HTML tag.

    Parameters
    ----------
    tag_name : str
        Tag name.
    color : str
        Color of the tag. Must be replaced with any of the following supported colors: blue, green, orange, red, violet, gray/grey, rainbow
    icon: str
        Icon of the tag. Can be material (e.g. ':material/upgrade:') or emoji (e.g. '🪄').
    """
    st.markdown(tag_in_md(tag_name, color, icon))


class Pagination:
    """Use pagination to show a list of items in Streamlit.

    Example:

    def st_show(item):
        # Function to render item
        ...

    # Parameters
    items = []
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
        st_show(item)

    """

    def __init__(self, items: list[Any], items_per_page: int, pagination_key: str, on_click: Optional[Callable] = None):
        """Construct Pagination.

        Parameters
        ----------
        items : list[Any]
            List of items to paginate.
        items_per_page : int
            Number of items per page.
        pagination_key : str
            Key to store the current page in session state.
        on_click : Optional[Callable], optional
            Action to perform when interacting with any of the buttons, by default None
        """
        self.items = items
        self.items_per_page = items_per_page
        self.pagination_key = pagination_key
        # Action to perform when interacting with any of the buttons.
        ## Example: Change the value of certain state in session_state
        self.on_click = on_click
        # Initialize session state for the current page
        if self.pagination_key not in st.session_state:
            self.page = 1

    @property
    def page(self):
        value = st.session_state[self.pagination_key]
        return value

    @page.setter
    def page(self, value):
        st.session_state[self.pagination_key] = value

    @property
    def total_pages(self) -> int:
        return (len(self.items) - 1) // self.items_per_page + 1

    def get_page_items(self) -> list[Any]:
        page = self.page
        start_idx = (page - 1) * self.items_per_page
        end_idx = start_idx + self.items_per_page
        return self.items[start_idx:end_idx]

    def show_controls(self, mode: Literal["buttons", "bar"] = "buttons") -> None:
        if mode == "bar":
            self.show_controls_bar()
        elif mode == "buttons":
            self.show_controls_buttons()
        else:
            raise ValueError("Mode must be either 'buttons' or 'bar'.")

    def show_controls_buttons(self):
        # Pagination controls
        # col1, col2, col3 = st.columns([1, 1, 1], vertical_alignment="center")

        with st.container(border=True):
            with st_horizontal():
                # with col1:
                key = f"previous-{self.pagination_key}"
                if self.page > 1:
                    if st.button("⏮️ Previous", key=key):
                        self.page -= 1
                        if self.on_click is not None:
                            self.on_click()
                        st.rerun()
                else:
                    st.button("⏮️ Previous", disabled=True, key=key)

                s = st.empty()

                # with col3:
                key = f"next-{self.pagination_key}"
                if self.page < self.total_pages:
                    if st.button("Next ⏭️", key=key):
                        self.page += 1
                        if self.on_click is not None:
                            self.on_click()
                        st.rerun()
                else:
                    st.button("Next ⏭️", disabled=True, key=key)

                # with col2:
                s.text(f"Page {self.page} of {self.total_pages}")

    def show_controls_bar(self) -> None:
        def _change_page():
            # Internal action

            # External action
            if self.on_click is not None:
                self.on_click()

        with st_horizontal():
            st.number_input(
                label=f"**Go to page** (results per page: {self.items_per_page}; total pages: {self.total_pages})",
                min_value=1,
                max_value=self.total_pages,
                # value=self.page,
                on_change=_change_page,
                key=self.pagination_key,
            )


def st_multiselect_wider(num_px: int = 1000):
    st.markdown(
        f"""
        <style>
        .stMultiSelect [data-baseweb=select] span{{
                max-width: {num_px}px;
            }}
        </style>""",
        unsafe_allow_html=True,
    )


def st_info(text):
    st.info(text, icon=":material/info:")
