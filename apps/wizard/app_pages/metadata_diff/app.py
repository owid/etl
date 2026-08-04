"""Metadata Diff: review how a branch changes the user-visible metadata texts of MDIMs.

Two connected views, against a selectable baseline (production, or staging-site-master to
isolate this branch's changes from production deploy lag):
- "Blast radius": a horizontal tree of all views of an MDIM (following its control
  order), colored by whether the view's texts differ between this staging server and
  the baseline. Leaves link to the View diff.
- "View diff": side-by-side baseline/staging comparison of the changed texts of one
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
from apps.wizard.app_pages.metadata_diff.usage import charts_using_indicators, mdims_using_indicators
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

# Source environment: this staging server. The baseline ("target") is selectable in the
# UI: production answers "what will readers see change?", while staging-site-master
# answers "what does this branch change?" — the latter stays clean even when production
# lags behind master (e.g. a failed production sync).
SOURCE = OWID_ENV

BASELINES = {
    "production": "production",
    "master": "staging-site-master",
}


def _baseline_env(baseline: str) -> OWIDEnv:
    if baseline == "production":
        if config.ENV_FILE_PROD:
            return OWIDEnv.from_env_file(config.ENV_FILE_PROD)
        # No production credentials on this server — master is the closest baseline.
        return OWIDEnv.from_staging("master")
    return OWIDEnv.from_staging("master")


@st.cache_resource
def get_engines(baseline: str) -> tuple[Engine, Engine]:
    assert OWID_ENV.env_remote != "production", "Metadata Diff must run on a staging server, not production."
    return SOURCE.engine, _baseline_env(baseline).engine


@st.cache_data(ttl=300, show_spinner="Computing metadata diff for all views…")
def compute_diff(
    catalog_path: str,
    _source_engine: Engine,
    _target_engine: Engine,
    cache_key: str,
) -> tuple[list[dict[str, Any]], list[ViewDiff]]:
    """Diff every view of an MDIM between staging and the selected baseline.

    `cache_key` busts the cache when configs or the baseline change; indicator metadata
    changes are picked up by the TTL.
    """
    source_config = load_mdim_config(_source_engine, catalog_path)
    assert source_config is not None, f"MDIM {catalog_path} not found in staging."
    target_config = load_mdim_config(_target_engine, catalog_path)

    source_bundles = build_env_bundles(_source_engine, source_config)
    target_bundles = build_env_bundles(_target_engine, target_config) if target_config else []

    dimensions = source_config.get("dimensions") or []
    return dimensions, diff_views(source_bundles, target_bundles)


@st.cache_data(ttl=300, show_spinner="Finding affected charts and MDIMs…")
def compute_usage(
    indicator_ids: tuple[int, ...],
    catalog_path: str,
    _source_engine: Engine,
    cache_key: str,
) -> dict[int, dict[str, list[dict[str, Any]]]]:
    """For each changed indicator, the charts and *other* MDIMs on this staging server that use it."""
    ids = list(indicator_ids)
    if not ids:
        return {}
    charts = charts_using_indicators(_source_engine, ids)
    mdims = mdims_using_indicators(_source_engine, ids, exclude_catalog_path=catalog_path)
    return {i: {"charts": charts.get(i, []), "mdims": mdims.get(i, [])} for i in ids}


def _view_impact(view: ViewDiff, usage: dict[int, dict[str, list[dict[str, Any]]]]) -> tuple[list, list]:
    """(charts, other_mdims) affected by this view's indicator-layer change; empty if MDim-only."""
    if not (view.affects_indicator and view.indicator_id is not None):
        return [], []
    entry = usage.get(view.indicator_id, {})
    return entry.get("charts", []), entry.get("mdims", [])


def _impact_counts(view: ViewDiff, usage: dict[int, dict[str, list[dict[str, Any]]]]) -> dict[str, int]:
    """Per-view external-surface counts, for the tree markers.

    Only shown on views that visibly changed, to stay consistent with the View diff page. (A view
    whose indicator changed but whose text is masked by an override is a rare edge case we skip.)
    """
    if not view.changed:
        return {"charts": 0, "mdims": 0}
    charts, mdims = _view_impact(view, usage)
    return {"charts": len(charts), "mdims": len(mdims)}


def _view_label(view: ViewDiff, dimensions: list[dict[str, Any]]) -> str:
    """Human-readable 'Choice · Choice · …' label for a view, in dimension order."""
    parts = []
    for dim in dimensions:
        slug = view.dimensions.get(dim["slug"])
        if slug is None:
            continue
        names = {c["slug"]: (c.get("name") or c["slug"]) for c in dim.get("choices", [])}
        name = names.get(slug, slug)
        if name and str(name).strip():
            parts.append(str(name))
    return " · ".join(parts) if parts else "(view)"


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


def _render_impact(view: ViewDiff, usage: dict[int, dict[str, list[dict[str, Any]]]]) -> None:
    """The 'does this also affect charts / other MDIMs?' flag for one view, with an expandable list."""
    if not view.affects_indicator:
        st.caption(
            "🔒 **MDim-only change** — the underlying indicator metadata is unchanged, so no standalone "
            "charts or other MDIMs are affected. (The change comes from an MDIM-level override.)"
        )
        return

    charts, mdims = _view_impact(view, usage)
    n_c, n_m = len(charts), len(mdims)

    if n_c == 0 and n_m == 0:
        st.info(
            "↗ This change is in the **shared indicator metadata**, but no published charts or other "
            "MDIMs currently use this indicator — so nothing else is affected."
        )
        return

    parts = []
    if n_c:
        parts.append(f"**{n_c}** chart{'s' if n_c != 1 else ''}")
    if n_m:
        parts.append(f"**{n_m}** other MDIM{'s' if n_m != 1 else ''}")
    st.warning(
        "↗ This change is in the **shared indicator metadata** — it also affects "
        + " and ".join(parts)
        + " that use this indicator."
    )

    with st.expander(f"Show the {n_c + n_m} affected surface{'s' if (n_c + n_m) != 1 else ''}"):
        if charts:
            chart_diff_url = f"{SOURCE.wizard_url}/chart-diff?diff-type=charts&indicator_id={view.indicator_id}"
            st.markdown(f"**Charts** — [open all {n_c} in Chart Diff ↗]({chart_diff_url})")
            for c in charts:
                label = c.get("title") or c.get("slug") or f"chart {c.get('chartId')}"
                if c.get("slug"):
                    st.markdown(f"- [{label}]({SOURCE.site}/grapher/{c['slug']})")
                else:
                    st.markdown(f"- {label}")
        if mdims:
            st.markdown(f"**Other MDIMs** ({n_m})")
            for m in mdims:
                st.markdown(f"- `{m.get('catalogPath')}`")


def render_view_diff_page(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    mdim_row: Any,
    baseline: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> None:
    """The View diff page: MDIM controls as navigation + side-by-side text diffs."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)

    # --- Jump straight to a changed view -----------------------------------------
    # Direct navigation to the changes, so the user doesn't have to hunt through the controls (the
    # 🟡 dots below help once a dropdown is open, but this is the glance-able shortcut). Written via
    # a callback because url_persist only reads the URL when a control's state is still empty.
    changed_views = [v for v in view_diffs if v.changed]
    if changed_views:

        def _jump_to_changed() -> None:
            raw = st.session_state.get("mdd_jump")
            if raw in (None, ""):
                return
            target = changed_views[int(raw)]
            for dim in dimensions:
                slug = dim["slug"]
                if slug in target.dimensions:
                    st.session_state[DIM_PARAM_PREFIX + slug] = target.dimensions[slug]
                    st.query_params[DIM_PARAM_PREFIX + slug] = target.dimensions[slug]

        def _jump_label(i: Any) -> str:
            if i == "":
                return "Select a changed view…"
            cv = changed_views[int(i)]
            marker = "🆕" if cv.is_new else "🟡"
            charts, mdims = _view_impact(cv, usage)
            suffix = f"  —  ↗ {len(charts)} charts" if charts else ""
            return f"{marker} {_view_label(cv, dimensions)}{suffix}"

        st.selectbox(
            f"⚡ Jump to a changed view ({len(changed_views)})",
            options=[""] + list(range(len(changed_views))),
            format_func=_jump_label,
            key="mdd_jump",
            on_change=_jump_to_changed,
        )

    # --- MDIM controls (navigation across views) ---------------------------------
    st.caption("🟡 marks a control option that leads to a changed view — follow the dots to the changes.")
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
        # Choices from which at least one *changed* view is reachable given the current selection —
        # so the user can drill straight to the changes instead of hunting blindly.
        changed_choices = {
            v.dimensions.get(dim_slug)
            for v in view_diffs
            if v.changed and all(v.dimensions.get(s) == c for s, c in selection.items())
        }
        names = {c["slug"]: (c.get("name") or c["slug"]) for c in dim.get("choices", [])}
        # Drop a stale URL value (e.g. after switching MDIM) so the widget doesn't crash.
        if st.query_params.get(key) not in available:
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)

        def _fmt(slug, names=names, changed_choices=changed_choices):
            # Prefix (not suffix) so the marker survives the selectbox's "…" truncation and shows
            # in the collapsed box too.
            label = names.get(slug, slug)
            return f"🟡 {label}" if slug in changed_choices else label

        with columns[i % len(columns)]:
            selection[dim_slug] = url_persist(st.selectbox)(
                dim.get("name") or dim_slug,
                key=key,
                options=available,
                format_func=_fmt,
            )

    view = next((v for v in view_diffs if v.dimensions == selection), None)
    if view is None:
        st.warning("No view exists for this combination of controls.")
        return

    # --- Header: status + links --------------------------------------------------
    baseline_name = BASELINES[baseline]
    # NOTE: `published_target` is NaN when the MDIM doesn't exist in the baseline (left join).
    baseline_slug = mdim_row.get("slug_target") if mdim_row.get("published_target") == 1 else None
    baseline_url = _view_url(_baseline_env(baseline), catalog_path, baseline_slug, view.dimensions)
    staging_url = _view_url(SOURCE, catalog_path, None, view.dimensions)

    links = [f"[Current view ({baseline_name})]({baseline_url})"]
    if view.changed:
        links.append(f"[Changed view (this staging server)]({staging_url})")

    if view.is_new:
        st.info(f"This view is **new** — it does not exist in {baseline_name}. " + " · ".join(links))
    elif view.changed:
        n = len(view.fields)
        st.warning(f"**{n} field{'s' if n > 1 else ''} changed** in this view. " + " · ".join(links))
    else:
        st.success("No changes in this view. " + " · ".join(links))

    # --- Blast radius: does this change escape the MDIM? --------------------------
    if view.changed and not view.is_new:
        _render_impact(view, usage)

    # --- Field diffs --------------------------------------------------------------
    changed_fields = [f for f in FIELD_ORDER if f in view.fields]
    for field_name in changed_fields:
        change = view.fields[field_name]
        shared = field_name in view.indicator_changed_fields
        tag = " · :orange[↗ shared — also on charts / other MDIMs]" if shared else " · :gray[MDim-only]"
        st.markdown(f"##### {field_label(field_name)}{tag}")
        col_old, col_new = st.columns(2)
        with col_old:
            st.markdown(f":gray[**{baseline_name.capitalize()}**]")
            st.markdown(_render_text_html(change["old"], change["new"], side="old"), unsafe_allow_html=True)
        with col_new:
            st.markdown(":green[**This staging server**]")
            st.markdown(_render_text_html(change["new"], change["old"], side="new"), unsafe_allow_html=True)


def main() -> None:
    st.title(":material/difference: Metadata Diff")
    st.caption(
        "Review how this staging server changes the metadata texts end users see on MDIMs "
        "(e.g. *What you should know about this data*), view by view, against a baseline. "
        "This includes changes coming from garden step templates, which don't show up in config diffs."
    )

    baseline = url_persist(st.radio)(
        "Compare against",
        key="baseline",
        options=list(BASELINES),
        format_func=lambda b: (
            "🌍 Production (what readers will see change)"
            if b == "production"
            else "🌿 master (what this branch changes)"
        ),
        horizontal=True,
        help="Production can lag behind master (e.g. while a deploy is pending), which shows up "
        "here as changes this branch didn't make. Compare against `staging-site-master` to "
        "isolate exactly what this branch changes.",
    )

    if baseline == "production" and not config.ENV_FILE_PROD:
        st.warning("No production env file found — comparing against `staging-site-master` instead.")

    source_engine, target_engine = get_engines(baseline)

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
            help="✏️ = the MDIM config differs from the baseline. Texts can also change "
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
        cache_key=f"{baseline}-{df_mdims.loc[catalog_path, 'configMd5_source']}-{df_mdims.loc[catalog_path, 'configMd5_target']}",
    )

    if not view_diffs:
        st.warning("This MDIM has no views.")
        return

    # Blast radius: which charts / other MDIMs use the indicators whose metadata this branch changed.
    changed_indicator_ids = sorted(
        {v.indicator_id for v in view_diffs if v.affects_indicator and v.indicator_id is not None}
    )
    usage = compute_usage(
        tuple(changed_indicator_ids),
        catalog_path,
        source_engine,
        cache_key=str(df_mdims.loc[catalog_path, "configMd5_source"]),
    )

    if mode == "view":
        render_view_diff_page(catalog_path, dimensions, view_diffs, df_mdims.loc[catalog_path], baseline, usage)
    else:
        n_changed = sum(1 for v in view_diffs if v.changed)
        if n_changed == 0:
            st.success("No metadata changes in any view of this MDIM. The tree below shows all views.")
        external_impacts = [_impact_counts(v, usage) for v in view_diffs]
        tree_html, height = render_tree_html(
            catalog_path,
            dimensions,
            view_diffs,
            dim_param_prefix=DIM_PARAM_PREFIX,
            external_impacts=external_impacts,
        )
        # NOTE: nothing should be rendered below the component — it resizes itself to its
        # content, and Streamlit-rendered siblings would overlap during the resize.
        components.html(tree_html, height=height, scrolling=True)


main()
