"""Explorer diff: compare explorers between staging and production.

This is rendered as a section of the chart-diff app. Like MDIMs, explorer configs are not
synced by chart-sync (ETL-managed explorers redeploy on merge; legacy ones are edited in
the prod admin directly), so this section is review-only.
"""

import json
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import NoResultFound
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
from apps.wizard.utils.components import explorer_chart, url_persist
from etl.db import read_sql
from etl.grapher import model as gm

log = get_logger()

# Explorer control widgets that define view dimensions
EXPLORER_CONTROLS = ["Radio", "Checkbox", "Dropdown"]

# Maximum number of lines shown in side-by-side TSVs
MAX_DIFF_LINES = 100


def get_explorer_changes(source_engine: Engine, target_engine: Engine) -> pd.DataFrame:
    """Get all published explorers from staging, flagging those whose TSV differs from production.

    Returns a dataframe indexed by slug (most recently updated first) with columns:
        - changed: TSV differs between environments (or explorer is missing in production)
        - is_new: explorer does not exist in production
    """
    q = """
    select slug, md5(trim(both '\n' from tsv)) as tsv_hash
    from explorers where isPublished = 1 order by updatedAt desc
    """
    df_source = read_sql(q, engine=source_engine)
    df_target = read_sql(q, engine=target_engine)

    df = pd.merge(df_source, df_target, on="slug", suffixes=("_source", "_target"), how="left")
    df["is_new"] = df["tsv_hash_target"].isnull()
    df["changed"] = df["tsv_hash_source"] != df["tsv_hash_target"]
    return df.set_index("slug")[["changed", "is_new"]]


def _clear_explorer_view_params() -> None:
    """Remove URL params of the previously selected explorer's view selectors.

    NOTE: Don't use `st.query_params.clear()` here — it would also wipe the chart-diff
    filters, which live on the same page.
    """
    for key in st.session_state.get("explorer-view-param-keys", []):
        st.query_params.pop(key, None)


def _display_selection(df_changes: pd.DataFrame, show_unchanged: bool) -> str | None:
    """Display explorer selection UI and return the selected explorer slug."""
    df = df_changes if show_unchanged else df_changes[df_changes["changed"]]
    options = df.index.tolist()

    # Keep a deep-linked (or previously selected) explorer selectable even if it is filtered out.
    selected = st.session_state.get("explorer") or st.query_params.get("explorer")
    if selected and selected not in df_changes.index:
        # Stale deep link (e.g. the explorer was renamed or unpublished) — drop it instead of crashing.
        st.query_params.pop("explorer", None)
        st.session_state.pop("explorer", None)
        selected = None
    if selected and selected not in options:
        options = [selected] + options

    def _format(slug: str) -> str:
        return f"{slug} 🆕" if df_changes.loc[slug, "is_new"] else slug

    explorer_slug = url_persist(st.selectbox)(
        "Select explorer",
        key="explorer",
        options=options,
        format_func=_format,
        on_change=_clear_explorer_view_params,
    )

    if not explorer_slug:
        if not show_unchanged:
            st.info('No explorers with changes. Turn on "Show unchanged" to browse all of them.')
        else:
            st.info("Select an explorer.")
        return None

    return explorer_slug


def _fetch_explorer_views(source_engine: Engine, slug: str) -> tuple[list[dict], list[bool]]:
    """Return the views of the explorer plus, per view, whether it has a map tab.

    Views look like:

    [{
        'Metric': 'Confirmed cases',
        'Frequency': '7-day average',
        'Relative to population': 'false'
    }]
    """
    df = read_sql("select config from explorers where slug = %(slug)s", engine=source_engine, params={"slug": slug})
    if len(df) != 1:
        raise ValueError(f"Expected exactly one explorer with slug '{slug}', got {len(df)}.")
    config = json.loads(df.iloc[0].config)

    views = []
    has_maps = []
    for block in config["blocks"]:
        for view in block.get("block", []) or []:
            dims = {}
            for k, v in view.items():
                for comp in EXPLORER_CONTROLS:
                    if k.endswith(comp):
                        dims[k.replace(comp, "").strip()] = v
            if dims:
                views.append(dims)
                # Explorers only show a map when the row sets hasMapTab to true
                has_maps.append(str(view.get("hasMapTab", "")).lower() == "true")

    return _fill_missing_dimensions(views), has_maps


@st.fragment
def _display_comparison(source_engine: Engine, explorer_slug: str, is_new: bool = False) -> None:
    """Display view selector and side-by-side comparison of an explorer."""
    views, has_maps = _fetch_explorer_views(source_engine, explorer_slug)
    if not views:
        st.warning("This explorer has no views.")
        return

    with st.container(border=True):
        # Header row: title + tab to open the previews on (conceptually a rendering option,
        # not a view dimension, hence not in the dimensions row).
        col_head, col_open = st.columns([3, 1], vertical_alignment="center")
        with col_head:
            st.markdown("##### :material/visibility: Preview")

        view = _display_view_options(explorer_slug, views)
        with col_open:
            st_display_option(has_map=next((has_map for dims, has_map in zip(views, has_maps) if dims == view), None))

        # Remember which URL params belong to this explorer's view selectors, so they can be
        # cleaned up when another explorer is selected.
        st.session_state["explorer-view-param-keys"] = [f"{explorer_slug}_{dim}" for dim in views[0].keys()]

        kwargs = {
            "explorer_slug": explorer_slug,
            "view": view,
            "default_display": st.session_state.get("default_display"),
        }

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Production**")
            if is_new:
                st.info("This explorer does not exist in production yet.")
            else:
                # This is the non-preview (published) version of the explorer
                explorer_chart(base_url=f"{TARGET.site}/explorers", **kwargs)
        with col2:
            st.markdown(":green[**Staging**]")
            # Show the admin preview from staging to see changes instantly
            explorer_chart(base_url=f"{SOURCE.site}/admin/explorers/preview", **kwargs)


def _fetch_explorer_data(source_engine: Engine, target_engine: Engine, explorer_slug: str):
    """Fetch explorer data from both environments (target may not have it)."""

    def load_explorer_data(engine: Engine):
        with Session(engine) as session:
            try:
                return gm.Explorer.load_explorer(session, explorer_slug, columns=["tsv", "config"])
            except NoResultFound:
                # Explorer doesn't exist in this environment (e.g. it is new on staging)
                return None

    # NOTE: loading data for some explorers can take >10s, hence parallel fetch
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_source = executor.submit(load_explorer_data, source_engine)
        future_target = executor.submit(load_explorer_data, target_engine)

        source_data = future_source.result()
        target_data = future_target.result()

    assert source_data, f"Explorer {explorer_slug} not found in staging."
    return source_data, target_data


def _display_explorer_diffs(source_data, target_data) -> None:
    """Display explorer TSV diffs in tabs."""
    # Import here to avoid a hard dependency at module import time (chart_diff_show pulls in heavier deps).
    from apps.wizard.app_pages.chart_diff.chart_diff_show import compare_strings, st_show_diff

    container = st.container(border=True)
    with container:
        st.markdown("##### :material/data_object: TSV config")

        target_tsv = target_data.tsv if target_data is not None else ""

        tsv_tab, side_by_side = st.tabs(["Diff", "Side by side"])

    with tsv_tab:
        diff_str = compare_strings(target_tsv, source_data.tsv, fromfile="production", tofile="staging")
        if diff_str == "":
            st.success("No differences found.")
        else:
            st_show_diff(diff_str, height=800)

    with side_by_side:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Production")
            st.code(truncate_lines(target_tsv, MAX_DIFF_LINES), line_numbers=True, language="diff")
        with col2:
            st.subheader("Staging")
            st.code(truncate_lines(source_data.tsv, MAX_DIFF_LINES), line_numbers=True, language="diff")


def st_show_explorer_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the explorer diff section of the chart-diff app."""
    df_changes = get_explorer_changes(source_engine, target_engine)
    if df_changes.empty:
        st.warning("No published explorers found in the staging environment.")
        return

    # Top row: selection (primary) + options
    col_select, col_show = st.columns([3, 1], vertical_alignment="bottom")
    with col_show:
        url_persist(st.toggle)(
            "Show unchanged",
            key="show_unchanged_explorers",
            value=False,
            help="Also list explorers whose TSV is identical in staging and production.",
        )
    show_unchanged = bool(st.session_state.get("show_unchanged_explorers", False))
    with col_select:
        explorer_slug = _display_selection(df_changes, show_unchanged)

    n_changed = int(df_changes["changed"].sum())
    st.caption(
        f"{n_changed} of {len(df_changes)} published explorers on this staging server differ from production."
        + ("" if show_unchanged else " Only the changed ones are listed."),
        help="Explorer configs are not synced by chart-sync, so there is nothing to approve here — ETL-managed "
        "explorers redeploy when your PR merges. Use this section to review how your changes affect each explorer.",
    )

    if not explorer_slug:
        return

    # Side-by-side preview
    _display_comparison(source_engine, explorer_slug, is_new=bool(df_changes.loc[explorer_slug, "is_new"]))

    # TSV diffs
    source_data, target_data = _fetch_explorer_data(source_engine, target_engine, explorer_slug)
    _display_explorer_diffs(source_data, target_data)
