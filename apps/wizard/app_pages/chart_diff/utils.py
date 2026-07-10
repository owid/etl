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


def _extract_all_dimensions(views: list[dict]) -> dict[str, list]:
    dim_names = list(views[0].keys())

    # Extract all unique dimensions across views
    all_dimensions = {dim: set() for dim in dim_names}
    for view in views:
        for dim in dim_names:
            all_dimensions[dim].add(view[dim])

    # Convert sets to lists for selectboxes
    return {dim: sorted(list(values)) for dim, values in all_dimensions.items()}


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
    """Display view options UI and return the selected view (used for explorers and MDIMs)."""
    all_dimensions = _extract_all_dimensions(views)

    st.subheader("Select view options")

    # Create random view button
    if st.button(f"🎲 Random view ({len(views)} views available)"):
        if views:
            random_view = random.choice(views)
            # Update session state with the random view values
            for dim, val in random_view.items():
                st.session_state[f"{slug}_{dim}"] = val
            # Rerun to apply the changes
            st.rerun(scope="fragment")

    # Arrange selectboxes horizontally using columns
    cols = st.columns(len(all_dimensions)) if all_dimensions else []

    selected_options = {}
    for i, (dim, values) in enumerate(all_dimensions.items()):
        selected_options[dim] = url_persist(cols[i].selectbox)(f"{dim}", options=values, key=f"{slug}_{dim}")

    view = selected_options if selected_options else (views[0] if views else {})

    # Check if the selected combination exists in any of the views
    combination_exists = False
    for candidate in views:
        if all(dim in candidate and candidate[dim] == val for dim, val in view.items()):
            combination_exists = True
            break

    # Display warning if combination doesn't exist
    if not combination_exists and view:
        st.warning(
            "⚠️ This specific combination of options does not exist in the views. It may show unexpected results."
        )

    return view


def st_display_option() -> None:
    """Selectbox for the default tab (map/table/chart) used when embedding MDIM/explorer views."""
    url_persist(st.selectbox)(
        "Display",
        value="Default",
        options=["Default", "Map", "Table", "Chart"],
        key="default_display",
        help="Tab to open the embedded views on.",
    )
