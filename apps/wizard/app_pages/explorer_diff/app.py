import json
import random
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.chart_diff_show import compare_strings, st_show_diff
from apps.wizard.app_pages.chart_diff.utils import get_engines
from apps.wizard.utils.components import explorer_chart, url_persist
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


def _fetch_explorer_views(slug: str) -> list[dict]:
    """
    Return a list of views for the explorer, e.g.

    [{
        'Metric': 'Confirmed cases',
        'Frequency': '7-day average',
        'Relative to population': 'false'
    }]
    """
    engine = get_engine()

    q = """
    select config from explorers where slug = %(slug)s;
    """
    df = pd.read_sql(q, engine, params={"slug": slug})
    if len(df) != 1:
        raise ValueError(f"Expected exactly one explorer with slug '{slug}', got {len(df)}.")
    config = json.loads(df.iloc[0].config)

    views = []
    for block in config["blocks"]:
        for view in block.get("block", []) or []:
            dims = {}
            for k, v in view.items():
                for comp in EXPLORER_CONTROLS:
                    if k.endswith(comp):
                        dims[k.replace(comp, "").strip()] = v
            if dims:
                views.append(dims)

    return views


def _fetch_explorer_slugs() -> list[str]:
    """Fetch all published explorer slugs."""
    q = """
    select slug from explorers where isPublished = 1 order by updatedAt desc
    """
    return read_sql(q)["slug"].tolist()


def _extract_all_dimensions(explorer_views: list[dict]) -> dict[str, list]:
    # Extract all unique dimensions across views
    all_dimensions = {}
    for view in explorer_views:
        for dim, val in view.items():
            if dim not in all_dimensions:
                all_dimensions[dim] = set()
            all_dimensions[dim].add(val)
    # Convert sets to lists for selectboxes
    return {dim: sorted(list(values)) for dim, values in all_dimensions.items()}


def main():
    st.warning("This application is currently in beta. We greatly appreciate your feedback and suggestions!")
    st.title(
        ":material/difference: Explorer Diff",
        help=f"""
**Explorer diff** is a page that compares explorer between [`production`](http://owid.cloud) and your [`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment.
""",
    )

    _show_options()

    explorer_slugs = _fetch_explorer_slugs()

    # Select explorer to compare
    explorer_slug = url_persist(st.selectbox)(
        "Select Explorer",
        key="explorer",
        options=explorer_slugs,
        # cleanup query params on explorer change
        on_change=st.query_params.clear,
    )

    explorer_views = _fetch_explorer_views(explorer_slug)

    all_dimensions = _extract_all_dimensions(explorer_views)

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
        assert OWID_ENV.site
        explorer_chart(base_url=OWID_ENV.site + "/explorers", **kwargs)

    st.subheader("TSV Diff")

    with Session(SOURCE_ENGINE) as session:
        tsv_source = gm.Explorer.load_explorer(session, explorer_slug).tsv

    with Session(TARGET_ENGINE) as session:
        tsv_target = gm.Explorer.load_explorer(session, explorer_slug).tsv

    # DRY with chart_diff_show.py
    diff_str = compare_strings(tsv_target, tsv_source, fromfile="production", tofile="staging")
    st_show_diff(diff_str)


if __name__ == "__main__":
    main()
