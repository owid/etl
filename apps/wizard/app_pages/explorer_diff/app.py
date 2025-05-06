import json
import random
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.chart_diff_show import compare_strings, st_show_diff
from apps.wizard.app_pages.chart_diff.utils import get_engines
from apps.wizard.app_pages.explorer_diff.utils import truncate_lines
from apps.wizard.utils.components import explorer_chart, url_persist
from etl.config import OWID_ENV
from etl.db import get_engine, read_sql
from etl.files import yaml_dump
from etl.grapher import model as gm

log = get_logger()


def _set_page_config(title: str):
    # Config
    st.set_page_config(
        page_title=f"Wizard: {title}",
        layout="wide",
        page_icon="🪄",
        initial_sidebar_state="collapsed",
        menu_items={
            "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
        },
    )


_set_page_config("Explorer Diff")

EXPLORER_CONTROLS = ["Radio", "Checkbox", "Dropdown"]

# Paths
CURRENT_DIR = Path(__file__).resolve().parent

# DB access
# Create connections to DB
SOURCE_ENGINE, TARGET_ENGINE = get_engines()

# Params
MAX_DIFF_LINES = 100


def _show_options():
    """Show options pane."""
    with st.popover("⚙️ Options", use_container_width=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            url_persist(st.toggle)(
                "**Hide** explorers with no change",
                key="hide_unchanged_explorers",
                value=True,
                help="Show only explorers with different TSV config.",
            )
            url_persist(st.selectbox)(
                "Explorer Display", value="Default", options=["Default", "Map", "Table", "Chart"], key="default_display"
            )


def _fill_missing_dimensions(views: list[dict]) -> list[dict]:
    """
    Fill missing dimensions in views with '-'.
    This is to ensure that all views have the same dimensions for comparison.
    """
    # If view doesn't have all dimensions, use '-'
    dim_names = {n for v in views for n in v.keys()}
    for view in views:
        for dim in dim_names:
            # If dimension is missing in a view, use '-'
            if dim not in view:
                view[dim] = "-"
    return views


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

    views = _fill_missing_dimensions(views)

    return views


def _fetch_explorer_slugs(hide_unchanged_explorers: bool) -> list[str]:
    """Fetch all published explorer slugs."""
    if not hide_unchanged_explorers:
        q = """
        select slug from explorers where isPublished = 1 order by updatedAt desc
        """
        return read_sql(q, engine=SOURCE_ENGINE)["slug"].tolist()
    else:
        q = """
        select slug, md5(trim(both '\n' from tsv)) as tsv_hash from explorers where isPublished = 1 order by updatedAt desc
        """
        df_source = read_sql(q, engine=SOURCE_ENGINE)
        df_target = read_sql(q, engine=TARGET_ENGINE)

        # Get slugs with different tsv hashes
        df = pd.merge(df_source, df_target, on="slug", suffixes=("_source", "_target"))
        df = df[df["tsv_hash_source"] != df["tsv_hash_target"]]
        df = df[["slug"]]
        return df["slug"].tolist()


def _extract_all_dimensions(explorer_views: list[dict]) -> dict[str, list]:
    dim_names = list(explorer_views[0].keys())

    # Extract all unique dimensions across views
    all_dimensions = {dim: set() for dim in dim_names}
    for view in explorer_views:
        for dim in dim_names:
            all_dimensions[dim].add(view[dim])

    # Convert sets to lists for selectboxes
    return {dim: sorted(list(values)) for dim, values in all_dimensions.items()}


def _set_tab_title_size(font_size="2rem"):
    css = f"""
    <style>
        .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {{
        font-size:{font_size};
        }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def _display_explorer_selection(hide_unchanged_explorers: bool) -> str | None:
    """Display explorer selection UI and return the selected explorer slug."""
    explorer_slugs = _fetch_explorer_slugs(hide_unchanged_explorers=hide_unchanged_explorers)

    # Select explorer to compare
    explorer_slug = url_persist(st.selectbox)(
        "Select Explorer",
        key="explorer",
        options=explorer_slugs,
        # cleanup query params on explorer change
        on_change=st.query_params.clear,
    )

    if not explorer_slug:
        if hide_unchanged_explorers:
            st.info('No explorers with changes. Turn off "Hide explorers with no change" in the options to see them.')
        else:
            st.info("Select an explorer.")
        return None

    return explorer_slug


def _display_view_options(slug: str, views: list[dict]) -> dict:
    """Display explorer view options UI and return the selected view."""
    all_dimensions = _extract_all_dimensions(views)

    st.subheader("Select Explorer View Options")

    # Create random view button
    if st.button(f"🎲 Random view ({len(views)} views available)"):
        # Select a random view from views
        if views:
            random_view = random.choice(views)
            # Update session state with the random view values
            for dim, val in random_view.items():
                st.session_state[f"{slug}_{dim}"] = val
            # Rerun to apply the changes
            st.rerun()

    # Arrange selectboxes horizontally using columns
    cols = st.columns(len(all_dimensions)) if all_dimensions else []

    selected_options = {}
    for i, (dim, values) in enumerate(all_dimensions.items()):
        selected_options[dim] = url_persist(cols[i].selectbox)(f"{dim}", options=values, key=f"{slug}_{dim}")

    view = selected_options if selected_options else (views[0] if views else {})

    # Check if the selected combination exists in any of the views
    combination_exists = False
    for explorer_view in views:
        if all(dim in explorer_view and explorer_view[dim] == val for dim, val in view.items()):
            combination_exists = True
            break

    # Display warning if combination doesn't exist
    if not combination_exists and view:
        st.warning(
            "⚠️ This specific combination of options does not exist in the explorer views. The explorer may show unexpected results."
        )

    return view


def _display_explorer_comparison(explorer_slug: str, view: dict):
    """Display side-by-side explorer comparison."""
    # Create columns for side by side comparison
    col1, col2 = st.columns(2)

    kwargs = {"explorer_slug": explorer_slug, "view": view, "default_display": st.session_state.get("default_display")}

    with col1:
        st.subheader("Production Explorer")
        # This is the non-preview version of an explorer
        explorer_chart(base_url="https://ourworldindata.org/explorers", **kwargs)

    with col2:
        st.subheader("Staging Explorer")
        assert OWID_ENV.site
        # Show preview from a staging server to see changes instantly
        explorer_chart(base_url=OWID_ENV.site + "/admin/explorers/preview", **kwargs)


def _fetch_explorer_data(explorer_slug: str):
    """Fetch explorer data from both environments."""

    # Helper function to load explorer data
    def load_explorer_data(engine, columns):
        """Load explorer data from database."""
        with Session(engine) as session:
            return gm.Explorer.load_explorer(session, explorer_slug, columns=columns)

    # NOTE: loading data for some explorers can take >10s!
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_source = executor.submit(load_explorer_data, SOURCE_ENGINE, ["tsv", "config"])
        future_target = executor.submit(load_explorer_data, TARGET_ENGINE, ["tsv", "config"])

        source_data = future_source.result()
        target_data = future_target.result()
        assert source_data and target_data, "Failed to load explorer data"

        # Move blocks as the last key in config
        source_data.config["blocks"] = source_data.config.pop("blocks")
        target_data.config["blocks"] = target_data.config.pop("blocks")

    return source_data, target_data


def _display_explorer_diffs(source_data, target_data):
    """Display explorer diffs in tabs."""
    # Create tabs for diffs
    tsv_tab, yaml_tab = st.tabs(["**TSV Diff**", "**YAML Diff**"])
    _set_tab_title_size("1.5rem")

    with tsv_tab:
        # Show diff
        diff_str = compare_strings(target_data.tsv, source_data.tsv, fromfile="production", tofile="staging")
        st_show_diff(diff_str, height=800)

        # Create columns to show TSV files side by side
        st.subheader("Side by side")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Production")
            st.code(truncate_lines(target_data.tsv, MAX_DIFF_LINES), line_numbers=True, language="diff")
        with col2:
            st.subheader("Staging")
            st.code(truncate_lines(source_data.tsv, MAX_DIFF_LINES), line_numbers=True, language="diff")

    with yaml_tab:
        # Show diff
        diff_str = compare_strings(
            yaml_dump(target_data.config).strip(),
            yaml_dump(source_data.config).strip(),
            fromfile="production",
            tofile="staging",
        )
        st_show_diff(diff_str, height=800)


def main():
    st.warning("This application is currently in beta. We greatly appreciate your feedback and suggestions!")
    st.title(
        ":material/difference: Explorer Diff",
        help=f"""
**Explorer diff** is a page that compares explorer between [`production`](http://owid.cloud) and your [`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment.
""",
    )

    _show_options()

    hide_unchanged_explorers: bool = st.session_state.get("hide_unchanged_explorers")  # type: ignore

    # Step 1: Display explorer selection UI
    explorer_slug = _display_explorer_selection(hide_unchanged_explorers)
    if not explorer_slug:
        return

    # Step 2: Display explorer view options UI
    explorer_views = _fetch_explorer_views(explorer_slug)
    view = _display_view_options(explorer_slug, explorer_views)

    # Step 3: Display side-by-side explorer comparison
    _display_explorer_comparison(explorer_slug, view)

    # Step 4: Fetch explorer data
    source_data, target_data = _fetch_explorer_data(explorer_slug)

    # Step 5: Display diffs
    _display_explorer_diffs(source_data, target_data)


main()
