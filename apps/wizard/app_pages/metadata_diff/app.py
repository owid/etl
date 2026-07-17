"""Metadata Diff: review how a branch changes the user-visible metadata texts of MDIMs.

Two connected views:
- "Blast radius": a horizontal tree of all views of an MDIM (following its control
  order), colored by whether the view's texts differ between this staging server and
  production. Leaves link to the View diff.
- "View diff": side-by-side production/staging comparison of the changed texts of one
  view, with the MDIM controls as navigation.

Unlike the config diff in chart-diff, this compares the *rendered* texts end users see:
indicator metadata (e.g. `description_key`) merged with any MDIM view-level overrides —
so it also catches changes coming from garden step templates.
"""

import html
import urllib.parse
from typing import Any

import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    CHART_FIELDS,
    METADATA_FIELDS,
    ViewDiff,
    diff_views,
    field_label,
    inline_diff_html,
)
from apps.wizard.app_pages.metadata_diff.data import build_env_bundles, get_mdim_changes, load_mdim_config
from apps.wizard.app_pages.metadata_diff.tree import render_tree_html
from apps.wizard.utils.components import url_persist
from etl import config
from etl.config import OWID_ENV, OWIDEnv

log = get_logger()

st.set_page_config(
    page_title="Wizard: Metadata Diff",
    page_icon="🪄",
    layout="wide",
)

DIM_PARAM_PREFIX = "d_"
FIELD_ORDER = list(METADATA_FIELDS) + [CHART_FIELD_PREFIX + f for f in CHART_FIELDS]

DIFF_CSS = """
<style>
.mdd-text { border: 1px solid #e0e0e0; border-radius: 6px; padding: 10px 14px; line-height: 1.5;
            background: #fff; }
.mdd-text ul { margin: 0 0 0 18px; padding: 0; }
.mdd-text li { margin-bottom: 8px; }
.mdd-text del.mdd-del { background: #ffe3e3; color: #c92a2a; text-decoration: line-through; }
.mdd-text ins.mdd-ins { background: #d3f9d8; color: #2b8a3e; text-decoration: none; }
.mdd-empty { color: #999; font-style: italic; }
</style>
"""

# Environments: source = this staging server, target = production (same logic as chart-diff).
SOURCE = OWID_ENV
if config.ENV_FILE_PROD:
    TARGET = OWIDEnv.from_env_file(config.ENV_FILE_PROD)
else:
    TARGET = OWIDEnv.from_staging("master")


@st.cache_resource
def get_engines() -> tuple[Engine, Engine]:
    assert OWID_ENV.env_remote != "production", "Metadata Diff must run on a staging server, not production."
    return SOURCE.engine, TARGET.engine


@st.cache_data(ttl=300, show_spinner="Computing metadata diff for all views…")
def compute_diff(
    catalog_path: str,
    _source_engine: Engine,
    _target_engine: Engine,
    cache_key: str,
) -> tuple[list[dict[str, Any]], list[ViewDiff]]:
    """Diff every view of an MDIM between staging and production.

    `cache_key` only busts the cache when configs change; indicator metadata changes
    are picked up by the TTL.
    """
    source_config = load_mdim_config(_source_engine, catalog_path)
    assert source_config is not None, f"MDIM {catalog_path} not found in staging."
    target_config = load_mdim_config(_target_engine, catalog_path)

    source_bundles = build_env_bundles(_source_engine, source_config)
    target_bundles = build_env_bundles(_target_engine, target_config) if target_config else []

    dimensions = source_config.get("dimensions") or []
    return dimensions, diff_views(source_bundles, target_bundles)


def _clear_view_params() -> None:
    """Drop the previous MDIM's view-selector params when another MDIM is selected."""
    for key in list(st.query_params.keys()):
        if key.startswith(DIM_PARAM_PREFIX):
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)


def _view_url(env, catalog_path: str, published_slug: str | None, dims: dict[str, str]) -> str:
    """URL of a view in a given environment (site if published there, admin preview otherwise)."""
    params = urllib.parse.urlencode(dims)
    if published_slug:
        return f"{env.site}/grapher/{published_slug}?{params}"
    return f"{env.admin_site}/grapher/{urllib.parse.quote(catalog_path, safe='')}/?{params}"


def _render_text_html(value: Any, other: Any, side: str) -> str:
    """One side of the side-by-side diff, with word-level highlights against the other side."""
    old, new = (other, value) if side == "new" else (value, other)

    def _one(o: Any, n: Any) -> str:
        return inline_diff_html(str(o or ""), str(n or ""), side=side)

    if isinstance(value, list) or isinstance(other, list):
        value_list = value if isinstance(value, list) else ([value] if value else [])
        other_list = other if isinstance(other, list) else ([other] if other else [])
        items = []
        for i in range(max(len(value_list), len(other_list))):
            v = value_list[i] if i < len(value_list) else ""
            o = other_list[i] if i < len(other_list) else ""
            rendered = _one(o, v) if side == "new" else _one(v, o)
            if v or rendered:
                items.append(f"<li>{rendered}</li>")
        return f'<div class="mdd-text"><ul>{"".join(items)}</ul></div>'

    if value in (None, ""):
        return '<div class="mdd-text mdd-empty">(empty)</div>'
    return f'<div class="mdd-text">{_one(old, new)}</div>'


def _plain_text_html(value: Any) -> str:
    if isinstance(value, list):
        items = "".join(f"<li>{html.escape(str(v))}</li>" for v in value if v)
        return f'<div class="mdd-text"><ul>{items}</ul></div>'
    if value in (None, ""):
        return '<div class="mdd-text mdd-empty">(empty)</div>'
    return f'<div class="mdd-text">{html.escape(str(value))}</div>'


def render_view_diff_page(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    mdim_row: Any,
) -> None:
    """The View diff page: MDIM controls as navigation + side-by-side text diffs."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)

    # --- MDIM controls (navigation across views) ---------------------------------
    selection: dict[str, str] = {}
    columns = st.columns(min(4, max(1, len(dimensions))))
    for i, dim in enumerate(dimensions):
        dim_slug = dim["slug"]
        key = DIM_PARAM_PREFIX + dim_slug
        # Choices available given the selection of the previous controls.
        available = []
        for v in view_diffs:
            if all(v.dimensions.get(s) == c for s, c in selection.items()):
                choice = v.dimensions.get(dim_slug)
                if choice is not None and choice not in available:
                    available.append(choice)
        names = {c["slug"]: (c.get("name") or c["slug"]) for c in dim.get("choices", [])}
        # Drop a stale URL value (e.g. after switching MDIM) so the widget doesn't crash.
        if st.query_params.get(key) not in available:
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)
        with columns[i % len(columns)]:
            selection[dim_slug] = url_persist(st.selectbox)(
                dim.get("name") or dim_slug,
                key=key,
                options=available,
                format_func=lambda slug, names=names: names.get(slug, slug),
            )

    view = next((v for v in view_diffs if v.dimensions == selection), None)
    if view is None:
        st.warning("No view exists for this combination of controls.")
        return

    # --- Header: status + links --------------------------------------------------
    # NOTE: `published_target` is NaN when the MDIM doesn't exist in production (left join).
    prod_slug = mdim_row.get("slug_target") if mdim_row.get("published_target") == 1 else None
    prod_url = _view_url(TARGET, catalog_path, prod_slug, view.dimensions)
    staging_url = _view_url(SOURCE, catalog_path, None, view.dimensions)

    links = [f"[Current view (production)]({prod_url})"]
    if view.changed:
        links.append(f"[Changed view (this staging server)]({staging_url})")

    if view.is_new:
        st.info("This view is **new** — it does not exist in production. " + " · ".join(links))
    elif view.changed:
        n = len(view.fields)
        st.warning(f"**{n} field{'s' if n > 1 else ''} changed** in this view. " + " · ".join(links))
    else:
        st.success("No changes in this view. " + " · ".join(links))

    # --- Field diffs --------------------------------------------------------------
    changed_fields = [f for f in FIELD_ORDER if f in view.fields]
    for field_name in changed_fields:
        change = view.fields[field_name]
        st.markdown(f"##### {field_label(field_name)}")
        col_old, col_new = st.columns(2)
        with col_old:
            st.markdown(":gray[**Production**]")
            st.markdown(_render_text_html(change["old"], change["new"], side="old"), unsafe_allow_html=True)
        with col_new:
            st.markdown(":green[**Staging**]")
            st.markdown(_render_text_html(change["new"], change["old"], side="new"), unsafe_allow_html=True)


def main() -> None:
    st.title(":material/difference: Metadata Diff")
    st.caption(
        "Review how this staging server changes the metadata texts end users see on MDIMs "
        "(e.g. *What you should know about this data*), view by view, compared to production. "
        "This includes changes coming from garden step templates, which don't show up in config diffs."
    )

    if not config.ENV_FILE_PROD:
        st.warning("No production env file found — comparing against `staging-site-master` instead.")

    source_engine, target_engine = get_engines()

    df_mdims = get_mdim_changes(source_engine, target_engine)
    if df_mdims.empty:
        st.warning("No MDIMs found on this staging server.")
        return

    def _format_mdim(path: str) -> str:
        row = df_mdims.loc[path]
        if row["is_new"]:
            return f"{path} 🆕"
        if row["config_changed"]:
            return f"{path} ✏️"
        return path

    col_select, col_mode = st.columns([3, 1], vertical_alignment="bottom")
    with col_select:
        catalog_path = url_persist(st.selectbox)(
            "MDIM",
            key="mdim",
            options=df_mdims.index.tolist(),
            format_func=_format_mdim,
            on_change=_clear_view_params,
            help="✏️ = the MDIM config differs from production. Texts can also change "
            "through indicator metadata without a config change — the diff below catches both.",
        )
    with col_mode:
        mode = url_persist(st.radio)(
            "Mode",
            key="mode",
            options=["tree", "view"],
            format_func=lambda m: "🌳 Blast radius" if m == "tree" else "🔍 View diff",
            horizontal=True,
            label_visibility="collapsed",
        )

    if not catalog_path:
        st.info("Select an MDIM.")
        return

    dimensions, view_diffs = compute_diff(
        catalog_path,
        source_engine,
        target_engine,
        cache_key=f"{df_mdims.loc[catalog_path, 'configMd5_source']}-{df_mdims.loc[catalog_path, 'configMd5_target']}",
    )

    if not view_diffs:
        st.warning("This MDIM has no views.")
        return

    if mode == "view":
        render_view_diff_page(catalog_path, dimensions, view_diffs, df_mdims.loc[catalog_path])
    else:
        n_changed = sum(1 for v in view_diffs if v.changed)
        if n_changed == 0:
            st.success("No metadata changes in any view of this MDIM. The tree below shows all views.")
        tree_html, height = render_tree_html(
            catalog_path, dimensions, view_diffs, dim_param_prefix=DIM_PARAM_PREFIX
        )
        # NOTE: nothing should be rendered below the component — it resizes itself to its
        # content, and Streamlit-rendered siblings would overlap during the resize.
        components.html(tree_html, height=height, scrolling=True)


main()
