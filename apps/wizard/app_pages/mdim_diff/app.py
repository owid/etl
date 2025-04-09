from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.chart_diff_show import compare_strings, st_show_diff
from apps.wizard.app_pages.chart_diff.utils import get_engines
from apps.wizard.app_pages.explorer_diff.utils import truncate_lines
from apps.wizard.utils.components import url_persist
from etl.config import OWID_ENV
from etl.db import read_sql
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
    """Fetch all published explorer catalog paths."""
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

    mdim_catalog_paths = _fetch_mdim_catalog_paths(hide_unchanged_mdims=hide_unchanged_mdims)

    # Select mdims to compare
    mdim_catalog_path = url_persist(st.selectbox)(
        "Select MDIM",
        key="explorer",
        options=mdim_catalog_paths,
        # cleanup query params on explorer change
        on_change=st.query_params.clear,
    )

    if not mdim_catalog_path:
        if hide_unchanged_mdims:
            st.info('No mdims with changes. Turn off "Hide mdims with no change" in the options to see them.')
        else:
            st.info("Select an MDIM.")
        return

    st.subheader("Config Diff")

    def load_mdim_config(engine):
        with Session(engine) as session:
            return gm.MultiDimDataPage.load_mdim(session, catalogPath=mdim_catalog_path).config  # type: ignore

    config_source = load_mdim_config(SOURCE_ENGINE)
    config_target = load_mdim_config(TARGET_ENGINE)

    diff_str = compare_strings(
        yaml_dump(config_target), yaml_dump(config_source), fromfile="production", tofile="staging"
    )
    if diff_str == "":
        st.success("No differences found.")
    else:
        st_show_diff(truncate_lines(diff_str, MAX_DIFF_LINES))

    # Display configs in tabs
    st.subheader("Config Sections")
    _display_config_in_tabs(config_target, config_source, MAX_DIFF_LINES)

    # dict_keys(["title", "views", "dimensions", "defaultSelection"])


def _display_config_in_tabs(config_target, config_source, max_lines):
    """Display config sections in tabs for easy comparison."""
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


main()
