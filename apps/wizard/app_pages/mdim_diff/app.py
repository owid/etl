import json
import random
import urllib.parse
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.chart_diff_show import compare_strings, st_show_diff
from apps.wizard.app_pages.chart_diff.utils import get_engines
from apps.wizard.app_pages.explorer_diff.utils import truncate_lines
from apps.wizard.utils.components import mdim_chart, url_persist
from etl.config import OWID_ENV
from etl.db import get_engine, read_sql
from etl.files import yaml_dump
from etl.grapher import model as gm

log = get_logger()

# Config
st.set_page_config(
    page_title="Wizard: MDIM Diff",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
    menu_items={
        "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
    },
)

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
                "**Hide** mdims with no change",
                key="hide_unchanged_mdims",
                value=True,
                help="Show only mdims with different TSV config.",
            )


def _fetch_mdim_catalog_paths(hide_unchanged_mdims: bool) -> list[str]:
    """Fetch all published mdim catalog paths."""
    q = """
    select
        catalogPath,
        configMd5
    from multi_dim_data_pages
    where catalogPath is not null
    order by updatedAt desc
    """
    df_source = read_sql(q, engine=SOURCE_ENGINE)

    if hide_unchanged_mdims:
        df_target = read_sql(q, engine=TARGET_ENGINE)

        # Filter catalogPath with same hashes
        df_source = pd.merge(df_source, df_target, on="catalogPath", suffixes=("_source", "_target"))
        df_source = df_source[df_source["configMd5_source"] != df_source["configMd5_target"]]

    return df_source["catalogPath"].tolist()


def _display_mdim_selection(hide_unchanged_mdims: bool) -> str | None:
    """Display MDIM selection UI and return the selected MDIM catalog path."""
    mdim_catalog_paths = _fetch_mdim_catalog_paths(hide_unchanged_mdims=hide_unchanged_mdims)

    # Select mdims to compare
    mdim_catalog_path = url_persist(st.selectbox)(
        "Select MDIM",
        key="mdim",
        options=mdim_catalog_paths,
        # cleanup query params on mdim change
        on_change=st.query_params.clear,
    )

    if not mdim_catalog_path:
        if hide_unchanged_mdims:
            st.info('No mdims with changes. Turn off "Hide mdims with no change" in the options to see them.')
        else:
            st.info("Select an MDIM.")
        return None

    return mdim_catalog_path


def _fetch_mdims(mdim_catalog_path: str) -> tuple[gm.MultiDimDataPage, gm.MultiDimDataPage]:
    """Fetch MDIMs from both environments."""

    def load_mdim_config(engine) -> gm.MultiDimDataPage:
        with Session(engine) as session:
            return gm.MultiDimDataPage.load_mdim(session, catalogPath=mdim_catalog_path)  # type: ignore

    source_mdim = load_mdim_config(SOURCE_ENGINE)
    target_mdim = load_mdim_config(TARGET_ENGINE)

    return source_mdim, target_mdim


def _display_config_diff(config_source, config_target):
    """Display MDIM config diff."""
    st.subheader("Config Diff")

    diff_str = compare_strings(
        yaml_dump(config_target), yaml_dump(config_source), fromfile="production", tofile="staging"
    )
    if diff_str == "":
        st.success("No differences found.")
    else:
        st_show_diff(truncate_lines(diff_str, MAX_DIFF_LINES))


def _display_config_in_tabs(config_target, config_source, max_lines):
    """Display config sections in tabs for easy comparison."""
    st.subheader("Config Sections")

    tab_base, tab_dimensions, tab_views = st.tabs(["Base Config", "Dimensions", "Views"])

    # Helper function to display a config section in two columns
    def display_section(tab, section_key=None):
        with tab:
            col1, col2 = st.columns(2)

            # Prepare content based on section_key
            if section_key is None:
                # Base config (excluding dimensions and views)
                content_target = {k: v for k, v in config_target.items() if k not in ("dimensions", "views")}
                content_source = {k: v for k, v in config_source.items() if k not in ("dimensions", "views")}
            else:
                # Specific section (dimensions or views)
                content_target = {section_key: config_target[section_key]}
                content_source = {section_key: config_source[section_key]}

            # Display in columns
            with col1:
                st.subheader("Production")
                st.code(
                    truncate_lines(yaml_dump(content_target), max_lines),
                    line_numbers=True,
                    language="diff",
                )
            with col2:
                st.subheader("Staging")
                st.code(
                    truncate_lines(yaml_dump(content_source), max_lines),
                    line_numbers=True,
                    language="diff",
                )

    # Display each section
    display_section(tab_base)
    display_section(tab_dimensions, "dimensions")
    display_section(tab_views, "views")


def _display_mdim_comparison(mdim_slug: str, catalog_path: str, view: dict):
    """Display side-by-side explorer comparison."""
    # Create columns for side by side comparison
    col1, col2 = st.columns(2)

    kwargs = {"view": view, "default_display": st.session_state.get("default_display")}

    with col1:
        st.subheader("Production MDIM")
        baked_url = f"https://ourworldindata.org/grapher/admin/grapher/{mdim_slug}"
        mdim_chart(baked_url, **kwargs)

    with col2:
        st.subheader("Staging MDIM")
        assert OWID_ENV.site
        preview_url = f"{OWID_ENV.site}/admin/grapher/{urllib.parse.quote(catalog_path, safe='')}"
        # baked_url = f"{OWID_ENV.site}/grapher/{mdim_slug}"
        mdim_chart(preview_url, **kwargs)


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

    # TODO: use gm.MultiDimDataPage.load_mdim() to fetch the config
    q = """
    select config from multi_dim_data_pages where slug = %(slug)s;
    """
    df = pd.read_sql(q, engine, params={"slug": slug})
    if len(df) != 1:
        raise ValueError(f"Expected exactly one explorer with slug '{slug}', got {len(df)}.")
    config = json.loads(df.iloc[0].config)

    views = [v["dimensions"] for v in config["views"]]

    # If view doesn't have all dimensions, use '-'
    dim_names = {n for v in views for n in v.keys()}
    for view in views:
        for dim in dim_names:
            # If dimension is missing in a view, use '-'
            if dim not in view:
                view[dim] = "-"

    return views


def _display_explorer_view_options(explorer_slug: str) -> dict:
    """Display explorer view options UI and return the selected view."""
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

    return view


def _extract_all_dimensions(explorer_views: list[dict]) -> dict[str, list]:
    dim_names = list(explorer_views[0].keys())

    # Extract all unique dimensions across views
    all_dimensions = {dim: set() for dim in dim_names}
    for view in explorer_views:
        for dim in dim_names:
            all_dimensions[dim].add(view[dim])

    # Convert sets to lists for selectboxes
    return {dim: sorted(list(values)) for dim, values in all_dimensions.items()}


def main():
    st.warning("This application is currently in beta. We greatly appreciate your feedback and suggestions!")
    st.title(
        ":material/difference: MDIM Diff",
        help=f"""
**MDIM diff** is a page that compares mdims between [`production`](http://owid.cloud) and your [`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment.
""",
    )

    _show_options()

    hide_unchanged_mdims: bool = st.session_state.get("hide_unchanged_mdims")  # type: ignore

    # Step 1: Display MDIM selection UI
    mdim_catalog_path = _display_mdim_selection(hide_unchanged_mdims)
    if not mdim_catalog_path:
        return

    # Fetch MDIMs
    source_mdim, target_mdim = _fetch_mdims(mdim_catalog_path)
    assert source_mdim.slug, f"MDIM slug does not exist for {mdim_catalog_path}"
    assert source_mdim.catalogPath, f"MDIM catalogPath does not exist for {mdim_catalog_path}"

    view = _display_explorer_view_options(source_mdim.slug)

    # Step 2: Display MDIM comparison
    st.warning("If you see **Sorry, that page doesn’t exist!**, it means the MDIM has not been published yet.")
    _display_mdim_comparison(source_mdim.slug, source_mdim.catalogPath, view)

    # Step 3: Display config diff
    _display_config_diff(source_mdim.config, target_mdim.config)

    # Step 4: Display config sections in tabs
    _display_config_in_tabs(source_mdim.config, target_mdim.config, MAX_DIFF_LINES)


main()
