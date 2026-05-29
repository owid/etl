import hashlib
import json
import random
import re
import urllib.parse
from collections.abc import Callable
from contextlib import contextmanager
from copy import deepcopy
from functools import wraps
from pathlib import Path
from typing import Any, Literal

import numpy as np
import streamlit as st
import streamlit.components.v1 as components
import streamlit.errors
from structlog import get_logger

from apps.wizard.config import PAGES_BY_ALIAS
from apps.wizard.utils import cache_all, is_running_in_streamlit
from apps.wizard.utils.chart_config import bake_chart_config
from etl.config import OWID_ENV, OWIDEnv
from etl.grapher.model import Variable

log = get_logger()

HORIZONTAL_STYLE = """<style class="hide-element">
    /* Hides the style container and removes the extra spacing */
    .element-container:has(.hide-element) {{
        display: none;
    }}
    /*
        The selector for >.element-container is necessary to avoid selecting the whole
        body of the streamlit app, which is also a stVerticalBlock.
    */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker-{hash}) {{
        display: flex;
        flex-direction: row !important;
        flex-wrap: wrap;
        gap: 1rem;
        align-items: {vertical_alignment};
        justify-content: {justify_content};
    }}
    /* Override the default width of selectboxes in horizontal layout */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker-{hash}) select {{
        min-width: 200px;  /* Set a minimum width for selectboxes */
        max-width: 400px;  /* Optional: Set a max-width to avoid overly wide selectboxes */
    }}
    /* Buttons and their parent container all have a width of 704px, which we need to override */
    div[data-testid="stVerticalBlock"]:has(> .element-container .horizontal-marker-{hash}) div {{
        width: auto !important; /* Previously set to max-content */
    }}
</style>
"""


def _generate_6char_hash():
    random_input = str(random.random()).encode()  # Random input as bytes
    hash_object = hashlib.sha256(random_input)  # Generate hash
    return hash_object.hexdigest()[:6]  # Return first 6 characters of the hash


@contextmanager
def st_horizontal(vertical_alignment="baseline", justify_content="flex-start", hash_string=None):
    """This is not very efficient, and is OK for few elements. If you want to use it several times (e.g. for loop) consider an alternative.

    Example alternatives:
        - If you want a row of buttons, consider using st.pills or st.segmented_control.
        - In the general case, you can just use st.columns
    """
    if hash_string is None:
        hash_string = _generate_6char_hash()
    h_style = HORIZONTAL_STYLE.format(
        hash=hash_string,
        vertical_alignment=vertical_alignment,
        justify_content=justify_content,
    )
    st.markdown(h_style, unsafe_allow_html=True)
    with st.container():
        st.markdown(f'<span class="hide-element horizontal-marker-{hash_string}"></span>', unsafe_allow_html=True)
        yield


def default_converter(o):
    if isinstance(o, np.integer):  # ignore
        return int(o)
    else:
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def grapher_chart(
    catalog_path: str | None = None,
    variable_id: int | list[int] | None = None,
    variable: Variable | list[Variable] | None = None,
    chart_config: dict[str, Any] | None = None,
    owid_env: OWIDEnv = OWID_ENV,
    selected_entities: list | None = None,
    included_entities: list | None = None,
    tab: str | None = None,
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
    tab : str, optional
        Default tab to show in the chart, by default None (which is equivalent to "chart")
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
            tab=tab,
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
    return components.html(chart_animation_iframe_html, height=height, width=1.6 * height)  # ty: ignore


def explorer_chart(
    base_url: str, explorer_slug: str, view: dict, height: int = 600, default_display: str | None = None
):
    # First HTML definition with parameters
    url = f"{base_url}/{explorer_slug}"

    params = {
        # "Metric": "Confirmed cases",
        # "Frequency": "7-day average",
        # "Relative to population": "false",
        # "country": "COD~BDI~UGA~CAF",
        "hideControls": "true",
        **view,
    }
    if default_display is not None:
        dd = default_display.lower()
        if dd in ["map", "table", "chart"]:
            params["tab"] = dd

    query_string = "?" + urllib.parse.urlencode(params)

    HTML = f"""
    <!-- Redirect to the external URL -->
    <meta http-equiv="refresh" content="0; url={url}{query_string}">
    """

    # Render the HTML
    return st.components.v1.html(HTML, height=height, width=1.6 * height)  # ty: ignore


def mdim_chart(url: str, view: dict, height: int = 600, default_display: str | None = None):
    params = {
        "hideControls": "true",
        **view,
    }
    if default_display is not None:
        dd = default_display.lower()
        if dd in ["map", "table", "chart"]:
            params["tab"] = dd

    query_string = "?" + urllib.parse.urlencode(params)

    HTML = f"""
    <!-- Redirect to the external URL -->
    <meta http-equiv="refresh" content="0; url={url}{query_string}">
    """

    # Render the HTML
    return st.components.v1.html(HTML, height=height, width=1.6 * height)  # ty: ignore


def _chart_html(chart_config: dict[str, Any], owid_env: OWIDEnv, height=600, **kwargs):
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
            var jsonConfig = {json.dumps(chart_config_tmp, default=default_converter)}; window.renderSingleGrapherOnGrapherPage({{ config: jsonConfig, dataApiUrl: "{owid_env.data_api_url}/v1/indicators/", catalogUrl: "{owid_env.catalog_url}" }});
        </script>
    </div>
    """

    components.html(HTML, height=height, width=int(1.6 * height), **kwargs)


def tag_in_md(tag_name: str, color: str, icon: str | None = None):
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

    def __init__(
        self,
        items: list[Any],
        items_per_page: int,
        pagination_key: str,
        on_click: Callable | None = None,
        save_in_query: bool = False,
    ):
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
        save_in_query : bool, optional
            Whether to save the current page in the query string, by default False
        """
        self.items = items
        self.items_per_page = items_per_page
        self.pagination_key = pagination_key
        self.save_in_query = save_in_query
        # Action to perform when interacting with any of the buttons.
        ## Example: Change the value of certain state in session_state
        self.on_click = on_click
        # Initialize session state for the current page
        if self.pagination_key not in st.session_state:
            # Get page from query parameters
            if self.save_in_query and self.pagination_key in st.query_params:
                self.page = int(st.query_params[self.pagination_key])
            else:
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
            if self.save_in_query:
                if self.page == 1:
                    st.query_params.pop(self.pagination_key)
                else:
                    st.query_params.update({self.pagination_key: self.page})

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


def config_style_html() -> None:
    """Increase font-size of expander headers."""
    st.markdown(
        """
    <style>
    .streamlit-expanderHeader {
        font-size: x-large;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


def st_wizard_page_link(alias: str, border: bool = False, **kwargs) -> None:
    """Link to page."""
    if "page" not in kwargs:
        kwargs["page"] = PAGES_BY_ALIAS[alias]["entrypoint"]
    if "label" not in kwargs:
        kwargs["label"] = PAGES_BY_ALIAS[alias]["title"]
    if "icon" not in kwargs:
        kwargs["icon"] = PAGES_BY_ALIAS[alias]["icon"]

    try:
        if border:
            with st.container(border=True):
                st.page_link(**kwargs)
        else:
            st.page_link(**kwargs)
    except (streamlit.errors.StreamlitPageNotFoundError, KeyError):
        # it must be run as a multi-page app to display the link, show warning
        # if run via `streamlit .../app.py`
        st.warning(f"App must be run via `make wizard` to display link to `{alias}`.")


def st_title_with_expert(title: str, icon: str | None = None, **kwargs):
    container = st.container(border=False, horizontal=True, vertical_alignment="bottom")
    if icon is not None:
        title = f"{icon} {title}"
    with container:
        st.title(title, **kwargs)
        st_wizard_page_link(
            alias="expert",
            label=":rainbow[**Ask the Expert**]",
            help="Ask the expert any documentation question!",
            width="content",
            border=False,
        )


# ---------------------------------------------------------------------------
# st_wizard_card — native replacement for the legacy `streamlit_card` component.
# Renders a clickable card (background image + dark overlay + centered label and
# optional caption) using only st.container + st.page_link, styled via CSS
# targeting the container's `st-key-wcard-<slug>` class. Used on the Wizard
# home page and any other page that needs image-tile navigation.
#
# `!important` is kept only where Streamlit's own themed styles would otherwise
# win on specificity (background, color, border, text-decoration).
# The whole card is clickable: `a::after` stretches an invisible overlay over
# the card; the caption sits above it with `pointer-events: none` so clicks on
# the caption fall through to the anchor below.
# ---------------------------------------------------------------------------
_WIZARD_CARD_CSS = """
<style>
div[class*="st-key-wcard-"] {
    position: relative;
    min-height: var(--card-h, 80px);
    border-radius: 8px;
    overflow: hidden;
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
    transition: filter 120ms ease, transform 120ms ease;
    cursor: pointer;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: stretch;
}
div[class*="st-key-wcard-"]:hover {
    filter: brightness(1.15);
    transform: translateY(-1px);
}
/* Inner Streamlit wrappers: transparent, full width */
div[class*="st-key-wcard-"] [data-testid="stVerticalBlock"],
div[class*="st-key-wcard-"] [data-testid="stElementContainer"],
div[class*="st-key-wcard-"] [data-testid="stPageLink"],
div[class*="st-key-wcard-"] .stPageLink {
    background: transparent !important;
    border: none !important;
    width: 100%;
    gap: 0.1rem;
}
/* The page_link anchor: content sits naturally; ::after is the whole-card
   invisible hit target so clicks anywhere navigate. */
div[class*="st-key-wcard-"] a {
    background: transparent !important;
    border: none !important;
    text-decoration: none !important;
    width: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
    text-align: center;
    padding: 0.15rem 0.75rem;
}
div[class*="st-key-wcard-"] a::after {
    content: '';
    position: absolute;
    inset: 0;
    z-index: 1;
}
/* All text inside the card → white, centered */
div[class*="st-key-wcard-"] a,
div[class*="st-key-wcard-"] a *,
div[class*="st-key-wcard-"] [data-testid="stCaptionContainer"] * {
    color: #fff !important;
    text-align: center;
    margin: 0;
}
div[class*="st-key-wcard-"] a p,
div[class*="st-key-wcard-"] a strong {
    font-weight: 700;
    font-size: 1.3rem;
    line-height: 1.2;
}
/* Caption stays visible above the hit overlay, but clicks pass through */
div[class*="st-key-wcard-"] [data-testid="stCaptionContainer"] {
    position: relative;
    z-index: 2;
    pointer-events: none;
    padding: 0 0.75rem;
}
div[class*="st-key-wcard-"] [data-testid="stCaptionContainer"] * {
    font-weight: 600;
    font-size: 0.9rem;
    line-height: 1.2;
}
</style>
"""


def _wizard_card_slug(s: str) -> str:
    """Turn an entrypoint path into a stable CSS-safe key suffix."""
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def _wizard_card_css_url(image_url: str) -> str:
    """Escape a URL for use inside a CSS ``url('...')`` expression."""
    return image_url.replace("\\", "\\\\").replace("'", "%27")


def st_wizard_card(
    entrypoint: str,
    title: str,
    image_url: str,
    caption: str = "",
    height: int = 80,
) -> None:
    """Render a clickable image-tile card that links to ``entrypoint``.

    A native replacement for ``streamlit_card.card`` — no React iframe, uses
    ``st.page_link`` for real multi-page navigation, styled with CSS. The
    entire card is clickable, not just the title.

    Parameters
    ----------
    entrypoint
        Page path accepted by ``st.page_link`` (e.g. ``"apps/wizard/etl_steps/snapshot.py"``).
    title
        Card title (rendered as a bold white label).
    image_url
        URL used as the card's background image (a dark overlay is blended on top).
    caption
        Optional caption rendered below the title.
    height
        Minimum card height in pixels.
    """
    # Shared CSS is idempotent; emit on every call so it is present on every rerun.
    st.markdown(_WIZARD_CARD_CSS, unsafe_allow_html=True)
    key = f"wcard-{_wizard_card_slug(entrypoint)}"
    overlay = "linear-gradient(rgba(0,0,0,0.55), rgba(0,0,0,0.55))"
    bg = f"{overlay}, url('{_wizard_card_css_url(image_url)}')" if image_url else overlay
    st.markdown(
        f"<style>div.st-key-{key} {{ --card-h: {height}px; background-image: {bg}; }}</style>",
        unsafe_allow_html=True,
    )
    with st.container(border=False, key=key):
        try:
            st.page_link(entrypoint, label=f"**{title}**")
        except streamlit.errors.StreamlitPageNotFoundError:
            # Not running as a multi-page app (e.g. `streamlit run home.py`).
            st.markdown(f"**{title}**")
        if caption:
            st.caption(caption)


def preview_file(
    file_path: str | Path, prefix: str = "File", language: str = "python", custom_header: str | None = None
) -> None:
    """Preview file in streamlit."""
    with open(file_path) as f:
        code = f.read()
    if custom_header is None:
        custom_header = f"{prefix}: `{file_path}`"
    with st.expander(custom_header, expanded=False):
        st.code(code, language=language)


def st_toast_error(message: str) -> None:
    """Show error message."""
    st.toast(f"❌ :red[{message}]")


def st_toast_success(message: str) -> None:
    """Show success message."""
    st.toast(f"✅ :green[{message}]")


def update_query_params(key: str, side_effect: Callable[[], None] | None = None):
    def _update_query_params():
        value = st.session_state[key]
        if value is not None:
            st.query_params.update({key: value})
        else:
            st.query_params.pop(key, None)

        if side_effect is not None:
            side_effect()

    return _update_query_params


def remove_query_params(key):
    st.query_params.pop(key, None)


def url_persist(component: Any) -> Any:
    """Wrapper around streamlit components that persist values in the URL query string.
    If value is equal to default value, it will not be added to the query string.
    This is useful to avoid cluttering the URL with default values.

    :param component: Streamlit component to wrap

    Usage:
        url_persist(st.multiselect)(
          key="abc",
          ...
        )

    Important notes:
        - Boolean values (checkbox/toggle) are stored as "True"/"False" strings in URL
        - The component parses these strings back to booleans when rendered
        - If you need to check the value before the component renders (e.g., in filtering logic),
          check st.session_state first, then fall back to parsing st.query_params manually:
            value = st.session_state.get("key")
            if value is None:
                value = st.query_params.get("key") != "False"
    """

    def _persist(*args, **kwargs):
        assert "key" in kwargs, "key should be passed to persist"

        key = kwargs["key"]

        on_change = kwargs.pop("on_change", None)

        # Get default from `value` field
        default = kwargs.pop("value", None)

        # If parameter is in session state, set it to the value in the query string
        if st.session_state.get(key) is None:
            if key in st.query_params:
                # Obtain params from query string
                params = _get_params(component, key)
            else:
                params = default

            # Store params in session state
            if params is not None:
                st.session_state[key] = params

            # Check if the value given for an option via the URL is actually accepted!
            # Allow empty values! NOTE: Might want to re-evaluate this, and add a flag to the function, e.g. 'allow_empty'
            _check_options_params(kwargs, params)

        else:
            # Set the value in query params, but only if it isn't default
            if default is None or st.session_state.get(key) != default:
                update_query_params(key)()
            elif st.session_state[key] == default:
                remove_query_params(key)

        kwargs["on_change"] = update_query_params(key, side_effect=on_change)

        return component(*args, **kwargs)

    return _persist


def _check_options_params(kwargs, params):
    """Check that the options in the URL query are valid.

    NOTE: Empty values are allowed.

    Wrong values will raise a ValueError.
    """
    if "options" in kwargs:
        if isinstance(params, list):
            not_expected = [p for p in params if p not in kwargs["options"]]
            if (params != []) or (len(not_expected) != 0):
                raise ValueError(
                    f"Please review the URL query. Values {not_expected} not in options {kwargs['options']}."
                )
        elif params is not None:
            # Set default value in query params
            if params not in kwargs["options"]:
                raise ValueError(f"Please review the URL query. Value {params} not in options {kwargs['options']}.")


def _get_params(component, key):
    """Get params from query string.

    Converts the params to the correct type if needed.
    """
    if component == st.multiselect:
        params = st.query_params.get_all(key)
        # convert to int if digit
        return [int(q) if q.isdigit() else q for q in params]
    elif component == st.checkbox or component == st.toggle:
        params = st.query_params.get(key)
        return params == "True"
    else:
        params = st.query_params.get(key)
        if params and params.isdigit():
            return int(params)
        elif params and params.replace(".", "", 1).isdigit():
            return float(params)
        else:
            return params


def st_cache_data(
    func: Callable | None = None,
    *,
    custom_text: str = "Running...",
    show_spinner: bool = False,
    show_time: bool = False,
    **cache_kwargs,
):
    """
    A custom decorator that wraps `st.cache_data` when running in Streamlit,
    or uses standard caching when not in Streamlit.

    Args:
        func: The function to be cached.
        custom_text (str): The custom spinner text to display (Streamlit only).
        show_spinner (bool): Whether to show the default Streamlit spinner message. Defaults to False.
        show_time (bool): Whether to show the elapsed time (Streamlit only). Defaults to False.
        **cache_kwargs: Additional arguments passed to `st.cache_data`.
    """

    def decorator(f):
        if is_running_in_streamlit():
            # Use Streamlit caching with spinner when in Streamlit context
            cached_func = st.cache_data(show_spinner=show_spinner, **cache_kwargs)(f)

            @wraps(f)
            def wrapper(*args, **kwargs):
                with st.spinner(custom_text, show_time=show_time):
                    return cached_func(*args, **kwargs)

            return wrapper
        else:
            # Use standard Python caching when not in Streamlit
            cached_func = cache_all(f)

            @wraps(f)
            def wrapper(*args, **kwargs):
                log.info(custom_text)
                return cached_func(*args, **kwargs)

            return wrapper

    # If used as @custom_cache_data without parentheses
    if func is not None:
        return decorator(func)

    return decorator
