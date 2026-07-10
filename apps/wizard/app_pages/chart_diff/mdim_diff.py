"""MDIM diff: compare multi-dimensional data pages (MDIMs) between staging and production.

This is rendered as a section of the chart-diff app. Unlike charts, MDIM configs are fully
defined in ETL (export steps), so there is no approval workflow: merging the PR redeploys
them. This section exists to *review* the changes, not to approve them.
"""

import urllib.parse

import pandas as pd
import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.utils import (
    SOURCE,
    TARGET,
    _display_view_options,
    _fill_missing_dimensions,
    st_display_option,
    truncate_lines,
)
from apps.wizard.utils.components import mdim_chart, url_persist
from etl.db import read_sql
from etl.files import yaml_dump
from etl.grapher import model as gm

log = get_logger()

# Maximum number of lines shown in config diffs
MAX_DIFF_LINES = 100


def get_mdim_changes(source_engine: Engine, target_engine: Engine) -> pd.DataFrame:
    """Get all MDIMs from staging, flagging those whose config differs from production.

    Returns a dataframe indexed by catalogPath (most recently updated first) with columns:
        - changed: config differs between environments (or MDIM is missing in production)
        - is_new: MDIM does not exist in production
    """
    q = """
    select
        catalogPath,
        configMd5
    from multi_dim_data_pages
    where catalogPath is not null
    order by updatedAt desc
    """
    df_source = read_sql(q, engine=source_engine)
    df_target = read_sql(q, engine=target_engine)

    df = pd.merge(df_source, df_target, on="catalogPath", suffixes=("_source", "_target"), how="left")
    df["is_new"] = df["configMd5_target"].isnull()
    df["changed"] = df["configMd5_source"] != df["configMd5_target"]
    return df.set_index("catalogPath")[["changed", "is_new"]]


def _clear_mdim_view_params() -> None:
    """Remove URL params of the previously selected MDIM's view selectors.

    NOTE: Don't use `st.query_params.clear()` here — it would also wipe the chart-diff
    filters, which live on the same page.
    """
    for key in st.session_state.get("mdim-view-param-keys", []):
        st.query_params.pop(key, None)


def _display_selection(df_changes: pd.DataFrame, hide_unchanged: bool) -> str | None:
    """Display MDIM selection UI and return the selected MDIM catalog path."""
    df = df_changes[df_changes["changed"]] if hide_unchanged else df_changes
    options = df.index.tolist()

    # Keep a deep-linked (or previously selected) MDIM selectable even if it is filtered out.
    selected = st.session_state.get("mdim") or st.query_params.get("mdim")
    if selected and selected not in df_changes.index:
        # Stale deep link (e.g. the MDIM was renamed or deleted) — drop it instead of crashing.
        st.query_params.pop("mdim", None)
        st.session_state.pop("mdim", None)
        selected = None
    if selected and selected not in options:
        options = [selected] + options

    def _format(path: str) -> str:
        return f"{path} 🆕" if df_changes.loc[path, "is_new"] else path

    mdim_catalog_path = url_persist(st.selectbox)(
        "Select MDIM",
        key="mdim",
        options=options,
        format_func=_format,
        on_change=_clear_mdim_view_params,
    )

    if not mdim_catalog_path:
        if hide_unchanged:
            st.info('No MDIMs with changes. Turn off "Hide MDIMs with no change" to browse all of them.')
        else:
            st.info("Select an MDIM.")
        return None

    return mdim_catalog_path


def _fetch_mdims(
    source_engine: Engine, target_engine: Engine, catalog_path: str
) -> tuple[gm.MultiDimDataPage, gm.MultiDimDataPage | None]:
    """Fetch an MDIM from both environments (target may not have it)."""

    def _load(engine: Engine) -> gm.MultiDimDataPage | None:
        with Session(engine) as session:
            return gm.MultiDimDataPage.load_mdim(session, catalogPath=catalog_path)

    source_mdim = _load(source_engine)
    target_mdim = _load(target_engine)

    assert source_mdim is not None, f"MDIM {catalog_path} not found in staging."
    if source_mdim.slug is None:
        source_mdim.slug = catalog_path.split("/")[-1]

    return source_mdim, target_mdim


@st.fragment
def _display_comparison(
    source_mdim: gm.MultiDimDataPage, target_mdim: gm.MultiDimDataPage | None, catalog_path: str
) -> None:
    """Display view selector and side-by-side comparison of an MDIM."""
    views = [v["dimensions"] for v in source_mdim.config.get("views", [])]
    views = _fill_missing_dimensions(views)
    if not views:
        st.warning("This MDIM has no views.")
        return

    assert source_mdim.slug
    view = _display_view_options(source_mdim.slug, views)

    # Remember which URL params belong to this MDIM's view selectors, so they can be
    # cleaned up when another MDIM is selected.
    st.session_state["mdim-view-param-keys"] = [f"{source_mdim.slug}_{dim}" for dim in views[0].keys()]

    kwargs = {"view": view, "default_display": st.session_state.get("default_display")}

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Production")
        if target_mdim is None:
            st.info("This MDIM does not exist in production yet.")
        elif target_mdim.published:
            mdim_chart(f"{TARGET.site}/grapher/{target_mdim.slug}", **kwargs)
        else:
            # Unpublished in production: use the admin preview (requires being logged in to the prod admin).
            st.caption("Not published in production — showing the admin preview (requires admin login).")
            mdim_chart(f"{TARGET.admin_site}/grapher/{urllib.parse.quote(catalog_path, safe='')}", **kwargs)

    with col2:
        st.subheader("Staging")
        # Use the admin preview: it always reflects the current DB config, even if the MDIM
        # has not been published or baked yet.
        mdim_chart(f"{SOURCE.admin_site}/grapher/{urllib.parse.quote(catalog_path, safe='')}", **kwargs)


def _display_config_diff(config_source: dict, config_target: dict | None) -> None:
    """Display MDIM config diff."""
    # Import here to avoid a hard dependency at module import time (chart_diff_show pulls in heavier deps).
    from apps.wizard.app_pages.chart_diff.chart_diff_show import compare_strings, st_show_diff

    st.subheader("Config diff")

    diff_str = compare_strings(
        yaml_dump(config_target) if config_target else "",  # ty: ignore
        yaml_dump(config_source),  # ty: ignore
        fromfile="production",
        tofile="staging",
    )
    if diff_str == "":
        st.success("No differences found.")
    else:
        st_show_diff(truncate_lines(diff_str, MAX_DIFF_LINES))


def _display_config_in_tabs(config_source: dict, config_target: dict | None, max_lines: int) -> None:
    """Display config sections in tabs for easy comparison."""
    st.subheader("Config sections")

    tab_base, tab_dimensions, tab_views = st.tabs(["Base config", "Dimensions", "Views"])

    def display_section(tab, section_key: str | None = None):
        with tab:
            col1, col2 = st.columns(2)

            # Prepare content based on section_key
            if section_key is None:
                # Base config (excluding dimensions and views)
                content_source = {k: v for k, v in config_source.items() if k not in ("dimensions", "views")}
                content_target = (
                    {k: v for k, v in config_target.items() if k not in ("dimensions", "views")}
                    if config_target
                    else {}
                )
            else:
                # Specific section (dimensions or views)
                content_source = {section_key: config_source.get(section_key, [])}
                content_target = {section_key: config_target.get(section_key, [])} if config_target else {}

            with col1:
                st.markdown("**Production**")
                st.code(
                    truncate_lines(yaml_dump(content_target), max_lines),  # ty: ignore
                    line_numbers=True,
                    language="yaml",
                )
            with col2:
                st.markdown("**Staging**")
                st.code(
                    truncate_lines(yaml_dump(content_source), max_lines),  # ty: ignore
                    line_numbers=True,
                    language="yaml",
                )

    display_section(tab_base)
    display_section(tab_dimensions, "dimensions")
    display_section(tab_views, "views")


def st_show_mdim_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the MDIM diff section of the chart-diff app."""
    st.caption(
        "MDIM configs are fully defined in ETL, so there is nothing to approve here — merging your PR "
        "redeploys them. Use this section to review how your changes affect each MDIM."
    )

    df_changes = get_mdim_changes(source_engine, target_engine)
    if df_changes.empty:
        st.warning("No MDIMs found in the staging environment.")
        return

    col1, col2 = st.columns(2, vertical_alignment="bottom")
    with col1:
        url_persist(st.toggle)(
            "**Hide** MDIMs with no change",
            key="hide_unchanged_mdims",
            value=True,
            help="Show only MDIMs whose config differs between staging and production.",
        )
    with col2:
        st_display_option()
    hide_unchanged = bool(st.session_state.get("hide_unchanged_mdims", True))

    n_changed = int(df_changes["changed"].sum())
    st.markdown(f"ℹ️ {n_changed}/{len(df_changes)} MDIMs with changes")

    # Step 1: Select an MDIM
    catalog_path = _display_selection(df_changes, hide_unchanged)
    if not catalog_path:
        return

    # Step 2: Fetch it from both environments
    source_mdim, target_mdim = _fetch_mdims(source_engine, target_engine, catalog_path)

    # Step 3: Side-by-side comparison
    _display_comparison(source_mdim, target_mdim, catalog_path)

    # Step 4: Config diff
    _display_config_diff(source_mdim.config, target_mdim.config if target_mdim else None)

    # Step 5: Config sections side by side
    _display_config_in_tabs(source_mdim.config, target_mdim.config if target_mdim else None, MAX_DIFF_LINES)
