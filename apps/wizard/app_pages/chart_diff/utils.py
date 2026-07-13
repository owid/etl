import random
from datetime import datetime

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.utils.components import url_persist
from etl import config
from etl.config import OWID_ENV, OWIDEnv
from etl.grapher import model as gm

log = get_logger()

WARN_MSG = []

SOURCE = OWID_ENV

ANALYTICS_NUM_DAYS = 30

# Try to compare against production DB if possible, otherwise compare against staging-site-master
if config.ENV_FILE_PROD:
    TARGET = OWIDEnv.from_env_file(config.ENV_FILE_PROD)
else:
    warning_msg = "ENV file doesn't connect to production DB, comparing against `staging-site-master`."
    log.warning(warning_msg)
    WARN_MSG.append(warning_msg)
    TARGET = OWIDEnv.from_staging("master")


@st.cache_resource
def get_engines() -> tuple[Engine, Engine]:
    assert OWID_ENV.env_remote != "production", "Your .env points to production DB, please use a staging environment."
    return SOURCE.engine, TARGET.engine


def prettify_date(chart):
    """Obtain prettified date from a chart.

    Format is:
        - Previous years: `Jan 10, 2020 10:15`
        - This year: `Mar 15, 10:15` (no need to explicitly show the year)
    """
    if chart.updatedAt.year == datetime.now().date().year:
        return chart.updatedAt.strftime("%b %d, %H:%M")
    else:
        return chart.updatedAt.strftime("%b %d, %Y %H:%M")


@st.cache_data
def indicators_in_charts(_engine: Engine, chart_ids: list[int]) -> dict[int, str]:
    # Get a list of used indicators in chart diffs
    with Session(_engine) as session:
        indicator_ids = gm.ChartDimensions.indicators_in_charts(session, chart_ids)
        rows = gm.Variable.from_id(session, variable_id=list(indicator_ids), columns=["id", "name"])
        return {r.id: r.name for r in rows}  # ty: ignore


########################################################################################
# View-selector helpers shared by the MDIM and Explorer diff sections.
########################################################################################


def truncate_lines(s: str, max_lines: int) -> str:
    """Truncate a string to a maximum number of lines."""
    lines = s.splitlines()
    if len(lines) > max_lines:
        st.warning(f"The diff is too long to display in full. Showing only the first {max_lines} lines.")
        return "\n".join(lines[:max_lines]) + "\n... (truncated)"
    return s


def _fill_missing_dimensions(views: list[dict]) -> list[dict]:
    """Fill missing dimensions in views with '-'.

    This is to ensure that all views have the same dimensions for comparison.
    """
    dim_names = {n for v in views for n in v.keys()}
    for view in views:
        for dim in dim_names:
            if dim not in view:
                view[dim] = "-"
    return views


def _display_view_options(slug: str, views: list[dict]) -> dict:
    """Display cascading view selectors and return the selected view (used for explorers and MDIMs).

    The options of each dimension are restricted to combinations that actually exist in `views`
    (given the dimensions selected so far), so the returned selection always corresponds to a
    real view.
    """
    dim_names = list(views[0].keys())
    cols = st.columns(len(dim_names) + 1, vertical_alignment="bottom")

    view: dict = {}
    remaining = views
    for i, dim in enumerate(dim_names):
        values = sorted({v[dim] for v in remaining})
        key = f"{slug}_{dim}"

        # Drop stale selections that became invalid given the dimensions chosen above.
        if key in st.session_state and st.session_state[key] not in values:
            del st.session_state[key]
        if key not in st.session_state:
            param = st.query_params.get(key)
            if param is not None and param not in values:
                st.query_params.pop(key)

        choice = url_persist(cols[i].selectbox)(dim, options=values, key=key)
        view[dim] = choice
        remaining = [v for v in remaining if v[dim] == choice]

    # Random view
    with cols[-1]:
        if st.button("🎲", key=f"{slug}-random-view", help=f"Pick a random view ({len(views)} available)."):
            random_view = random.choice(views)
            for dim, val in random_view.items():
                st.session_state[f"{slug}_{dim}"] = val
            st.rerun(scope="fragment")

    return view


def st_display_option(has_map: bool | None = None) -> None:
    """Selectbox for the tab (map/table/chart) the embedded MDIM/explorer previews open on.

    If `has_map` is False, the Map option is not offered.
    """
    options = ["Default", "Map", "Table", "Chart"]
    if has_map is False:
        options.remove("Map")

    # Drop a stale selection that is invalid for the current view (e.g. Map on a map-less view).
    key = "default_display"
    if key in st.session_state and st.session_state[key] not in options:
        del st.session_state[key]
    if key not in st.session_state:
        param = st.query_params.get(key)
        if param is not None and param not in options:
            st.query_params.pop(key)

    url_persist(st.selectbox)(
        "Open previews on tab",
        value="Default",
        options=options,
        key=key,
        help="Tab the previews below open on (map, data table, or chart). 'Default' uses the view's own default tab.",
        # The label is folded into the displayed value so the widget stays a single line
        # (it sits next to a one-line heading).
        format_func=lambda x: f"Tab on: {x}",
        label_visibility="collapsed",
    )
