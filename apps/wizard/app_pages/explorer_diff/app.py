import json
import random  # Add import for random selection
import re
import urllib.parse
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

# from st_copy_to_clipboard import st_copy_to_clipboard
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.chart_diff import get_chart_diffs_from_grapher
from apps.wizard.app_pages.chart_diff.chart_diff_show import st_show
from apps.wizard.app_pages.chart_diff.utils import WARN_MSG, get_engines, indicators_in_charts
from apps.wizard.utils import set_states
from apps.wizard.utils.components import Pagination, explorer_chart, grapher_chart, url_persist
from etl.config import OWID_ENV
from etl.db import get_engine, read_sql
from etl.grapher import model as gm

log = get_logger()

# Config
st.set_page_config(
    page_title="Wizard: Explorer Diff",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
    menu_items={
        "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
    },
)

EXPLORER_CONTROLS = ["Radio", "Checkbox", "Dropdown"]

# Paths
CURRENT_DIR = Path(__file__).resolve().parent

# DB access
# Create connections to DB
SOURCE_ENGINE, TARGET_ENGINE = get_engines()


def _show_options():
    """Show options pane."""
    with st.popover("⚙️ Options", use_container_width=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            url_persist(st.selectbox)(
                "Explorer Display", value="Default", options=["Default", "Map", "Table", "Chart"], key="default_display"
            )


def _explorer_options(slug: str):
    """
    return a list of options for the explorer, e.g.

    [{'Metric': 'Confirmed cases',
    'Frequency': '7-day average',
    'Relative to population': 'false'}]
    """
    engine = get_engine()

    # TODO: use params instead of string interpolation
    q = f"""
    select config from explorers where slug = '{slug}';
    """
    df = pd.read_sql(q, engine)
    # TODO: if we have zero or more than one rows, raise an error
    config = json.loads(df.iloc[0].config)

    options = []
    for block in config["blocks"]:
        for view in block.get("block", []) or []:
            dims = {}
            for k, v in view.items():
                for comp in EXPLORER_CONTROLS:
                    if k.endswith(comp):
                        dims[k.replace(comp, "").strip()] = v
            if dims:
                options.append(dims)

    return options


def main():
    st.title(
        ":material/difference: Explorer Diff",
        help=f"""
**Explorer diff** is a page that compares explorer between [`production`](http://owid.cloud) and your [`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment.
""",
    )

    _show_options()

    # Fetch available explorers
    q = """
    select slug from explorers where isPublished = 1 order by updatedAt desc
    """
    available_explorers = read_sql(q)["slug"].tolist()

    # Select explorer to compare
    # TODO: make it persistent
    explorer_slug = url_persist(st.selectbox)(
        "Select Explorer",
        key="explorer",
        options=available_explorers,
        index=available_explorers.index("monkeypox"),
        # cleanup query params on explorer change
        on_change=st.query_params.clear,
    )

    explorer_views = _explorer_options(explorer_slug)

    # Extract all unique dimensions across views
    all_dimensions = {}
    for view in explorer_views:
        for dim, val in view.items():
            if dim not in all_dimensions:
                all_dimensions[dim] = set()
            all_dimensions[dim].add(val)

    # Convert sets to lists for selectboxes
    all_dimensions = {dim: sorted(list(values)) for dim, values in all_dimensions.items()}

    st.subheader("Select Explorer View Options")

    # Create random view button
    if st.button(f"🎲 Random view ({len(explorer_views)} views available)"):
        # Select a random view from explorer_views
        if explorer_views:
            random_view = random.choice(explorer_views)
            # Update session state with the random view values
            for dim, val in random_view.items():
                st.session_state[f"{explorer_slug}_{dim}"] = val
            # Rerun to apply the changes
            st.rerun()

    # Arrange selectboxes horizontally using columns
    cols = st.columns(len(all_dimensions)) if all_dimensions else []

    selected_options = {}
    for i, (dim, values) in enumerate(all_dimensions.items()):
        selected_options[dim] = url_persist(cols[i].selectbox)(f"{dim}", options=values, key=f"{explorer_slug}_{dim}")

    view = selected_options if selected_options else (explorer_views[0] if explorer_views else {})

    # Check if the selected combination exists in any of the views
    combination_exists = False
    for explorer_view in explorer_views:
        if all(dim in explorer_view and explorer_view[dim] == val for dim, val in view.items()):
            combination_exists = True
            break

    # Display warning if combination doesn't exist
    if not combination_exists and view:
        st.warning(
            "⚠️ This specific combination of options does not exist in the explorer views. The explorer may show unexpected results."
        )

    # Create columns for side by side comparison
    col1, col2 = st.columns(2)

    kwargs = {"explorer_slug": explorer_slug, "view": view, "default_display": st.session_state.get("default_display")}

    with col1:
        st.subheader("Production Explorer")
        explorer_chart(base_url="https://ourworldindata.org/explorers", **kwargs)

    with col2:
        st.subheader("Staging Explorer")
        explorer_chart(base_url=OWID_ENV.site + "/explorers", **kwargs)


main()
