import urllib.parse
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.chart_diff_show import compare_strings, st_show_diff
from apps.wizard.app_pages.chart_diff.utils import get_engines
from apps.wizard.app_pages.explorer_diff.utils import (
    _display_view_options,
    _fill_missing_dimensions,
    _set_page_config,
    truncate_lines,
)
from apps.wizard.utils.components import mdim_chart, st_horizontal, st_wizard_page_link, url_persist
from etl.config import OWID_ENV
from etl.db import read_sql
from etl.files import yaml_dump
from etl.grapher import model as gm

log = get_logger()

_set_page_config("MDIM Diff")

# Paths
CURRENT_DIR = Path(__file__).resolve().parent

# DB access
# Create connections to DB
SOURCE_ENGINE, TARGET_ENGINE = get_engines()

# Params
MAX_DIFF_LINES = 100


def _show_options():
    """Show options pane."""
    with st.popover("⚙️ Options", width="stretch"):
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
        df_source = pd.merge(df_source, df_target, on="catalogPath", suffixes=("_source", "_target"), how="left")
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

    if source_mdim.slug is None:
        source_mdim.slug = source_mdim.catalogPath.split("/")[-1]  # type: ignore
    if target_mdim is not None and target_mdim.slug is None:
        target_mdim.slug = target_mdim.catalogPath.split("/")[-1]  # type: ignore

    return source_mdim, target_mdim


def _display_config_diff(config_source: Dict, config_target: Optional[Dict]):
    """Display MDIM config diff."""
    st.subheader("Config Diff")

    diff_str = compare_strings(
        yaml_dump(config_target) if config_target else "",
        yaml_dump(config_source),
        fromfile="production",
        tofile="staging",
    )
    if diff_str == "":
        st.success("No differences found.")
    else:
        st_show_diff(truncate_lines(diff_str, MAX_DIFF_LINES))


def _display_config_in_tabs(config_source: Dict, config_target: Optional[Dict], max_lines: int):
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
                content_source = {k: v for k, v in config_source.items() if k not in ("dimensions", "views")}
                if config_target:
                    content_target = {k: v for k, v in config_target.items() if k not in ("dimensions", "views")}
                else:
                    content_target = {}
            else:
                # Specific section (dimensions or views)
                content_source = {section_key: config_source[section_key]}
                if config_target:
                    content_target = {section_key: config_target[section_key]}
                else:
                    content_target = {}

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


def _get_mdim_views(db_mdim: gm.MultiDimDataPage) -> list[dict]:
    """
    Return a list of views for the explorer, e.g.

    [{
        'Metric': 'Confirmed cases',
        'Frequency': '7-day average',
        'Relative to population': 'false'
    }]
    """
    views = [v["dimensions"] for v in db_mdim.config["views"]]

    views = _fill_missing_dimensions(views)

    return views


@st.fragment
def display_mdim_comparison(source_mdim):
    explorer_views = _get_mdim_views(source_mdim)
    view = _display_view_options(source_mdim.slug, explorer_views)

    # Step 2: Display MDIM comparison
    st.warning("If you see **Sorry, that page doesn’t exist!**, it means the MDIM has not been published yet.")
    _display_mdim_comparison(source_mdim.slug, source_mdim.catalogPath, view)


def main():
    st.warning("This application is currently in beta. We greatly appreciate your feedback and suggestions!")
    st.title(
        ":material/difference: MDIM Diff",
        help=f"""
**MDIM diff** is a page that compares mdims between [`production`](http://owid.cloud) and your [`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment.
""",
    )

    with st_horizontal(vertical_alignment="center"):
        st.markdown("Other links: ")
        st_wizard_page_link("chart-diff")
        st_wizard_page_link("explorer-diff")

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

    # Step 2: Display MDIM comparison
    display_mdim_comparison(source_mdim)

    # Step 3: Display config diff
    _display_config_diff(source_mdim.config, target_mdim.config if target_mdim else None)

    # Step 4: Display config sections in tabs
    _display_config_in_tabs(source_mdim.config, target_mdim.config if target_mdim else None, MAX_DIFF_LINES)


main()
