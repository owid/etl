import json
import os
import urllib.parse
from collections.abc import Callable
from copy import deepcopy
from functools import wraps
from pathlib import Path
from typing import Any

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

# Grapher is consumed as a published npm package (`@ourworldindata/grapher`). The built
# bundles of every release are also served on our Tailnet, which is all a browser needs —
# unlike the npm registry, these URLs require no auth token. Set GRAPHER_PACKAGE_URL to
# pin a version (e.g. .../ourworldindata/grapher/v0.1.0) instead of tracking the latest.
GRAPHER_PACKAGE_URL = os.environ.get(
    "GRAPHER_PACKAGE_URL",
    "https://owid-packages.tail6e23.ts.net/ourworldindata/grapher/latest",
).rstrip("/")


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
    """Embed an explorer view in an iframe."""
    return mdim_chart(f"{base_url}/{explorer_slug}", view=view, height=height, default_display=default_display)


def mdim_chart(url: str, view: dict, height: int = 600, default_display: str | None = None):
    """Embed an MDIM (or explorer) view in an iframe."""
    params = {
        "hideControls": "true",
        **view,
    }
    if default_display is not None:
        dd = default_display.lower()
        if dd in ["map", "table", "chart"]:
            params["tab"] = dd

    query_string = "?" + urllib.parse.urlencode(params)

    return st.iframe(f"{url}{query_string}", height=height, width=int(1.6 * height))


def _grapher_html(chart_config: dict[str, Any], owid_env: OWIDEnv, height: int = 600) -> str:
    """Build the HTML that mounts a Grapher chart with the `@ourworldindata/grapher` package."""
    config = deepcopy(chart_config)

    # Env-specific URLs Grapher uses for its share / embed / edit links.
    config["bakedGrapherURL"] = f"{owid_env.base_site}/grapher"
    config["adminBaseUrl"] = owid_env.base_site

    return f"""
    <link rel="stylesheet" href="https://ourworldindata.org/fonts.css" />
    <link rel="stylesheet" href="{GRAPHER_PACKAGE_URL}/grapher.css" />
    <style>
        html, body {{ margin: 0; padding: 0; }}
        /* The chart fills its container, so the container needs an explicit size. */
        #grapher {{ width: 100%; height: {height}px; }}
    </style>
    <div id="grapher"></div>
    <script type="module">
        import {{ GrapherLoader }} from "{GRAPHER_PACKAGE_URL}/grapher.standalone.min.js";
        GrapherLoader.fromApi({{
            config: {json.dumps(config, default=default_converter)},
            dataApiUrl: "{owid_env.indicators_url}/",
        }}).mount(document.getElementById("grapher"));
    </script>
    """


def _chart_html(chart_config: dict[str, Any], owid_env: OWIDEnv, height=600, **kwargs):
    """Plot a Grapher chart using the Grapher API.

    Parameters
    ----------
    chart_config : Dict[str, Any]
        Configuration of the chart.
    owid_env : OWIDEnv
        Environment configuration. This is needed to access the correct API (changes between servers).
    """
    HTML = _grapher_html(chart_config, owid_env, height=height)

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


class Pagination:
    """Use pagination to show a list of items in Streamlit.

    Thin wrapper around `st.pagination` that also slices the item list to the current page.

    Example:

    def st_show(item):
        # Function to render item
        ...

    # Define pagination
    pagination = Pagination(
        items=items,
        items_per_page=10,
        pagination_key="pagination-example",
    )

    # Show controls (hidden automatically if there is a single page)
    pagination.show_controls()

    # Show items (only current page)
    for item in pagination.get_page_items():
        st_show(item)

    """

    def __init__(
        self,
        items: list[Any],
        items_per_page: int,
        pagination_key: str,
        on_change: Callable | None = None,
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
        on_change : Optional[Callable], optional
            Action to perform when the page changes, by default None
        """
        self.items = items
        self.items_per_page = items_per_page
        self.pagination_key = pagination_key
        self.on_change = on_change

    @property
    def page(self) -> int:
        return st.session_state.get(self.pagination_key, 1)

    @property
    def total_pages(self) -> int:
        return max(1, (len(self.items) - 1) // self.items_per_page + 1)

    def get_page_items(self) -> list[Any]:
        start_idx = (self.page - 1) * self.items_per_page
        end_idx = start_idx + self.items_per_page
        return self.items[start_idx:end_idx]

    def show_controls(self, position: str = "top") -> None:
        """Show pagination controls.

        Can be called more than once per page (e.g. above and below the item list) — pass a
        distinct `position` for each call; all copies stay in sync. The "top" copy's key is
        the canonical page state.
        """
        if self.total_pages == 1:
            return
        # If the item list shrank (e.g. filters changed), the remembered page may be out of range.
        if self.page > self.total_pages:
            st.session_state[self.pagination_key] = 1

        if position == "top":
            key = self.pagination_key
        else:
            key = f"{self.pagination_key}--{position}"
            # Mirror the canonical page before instantiating this copy.
            st.session_state[key] = self.page

        def _on_change():
            # Keep the canonical key in sync when a secondary copy is used (no-op for "top").
            st.session_state[self.pagination_key] = st.session_state[key]
            if self.on_change:
                self.on_change()

        st.pagination(
            self.total_pages,
            key=key,
            on_change=_on_change,
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
