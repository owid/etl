"""Metadata Diff: review how a branch changes the user-visible metadata texts of MDims.

Two connected views, against a selectable baseline (production, or staging-site-master to
isolate this branch's changes from production deploy lag):
- "Blast radius": a horizontal tree of all views of an MDim (following its control
  order), colored by whether the view's texts differ between this staging server and
  the baseline. Leaves link to the View diff.
- "View diff": side-by-side baseline/staging comparison of the changed texts of one
  view, with the MDim controls as navigation.

Unlike the config diff in chart-diff, this compares the *rendered* texts end users see:
indicator metadata (e.g. `description_key`) merged with any MDim view-level overrides —
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
    as_bullets,
    diff_preview_html,
    diff_views,
    field_label,
    inline_diff_html,
)
from apps.wizard.app_pages.metadata_diff.data import (
    build_chart_bundle,
    build_env_bundles,
    get_mdim_changes,
    load_mdim_config,
)
from apps.wizard.app_pages.metadata_diff.tree import render_affected_charts_html, render_tree_html
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
    """Diff every view of an MDim between staging and the selected baseline.

    `cache_key` busts the cache when configs or the baseline change; indicator metadata
    changes are picked up by the TTL.
    """
    source_config = load_mdim_config(_source_engine, catalog_path)
    assert source_config is not None, f"MDim {catalog_path} not found in staging."
    target_config = load_mdim_config(_target_engine, catalog_path)

    source_bundles = build_env_bundles(_source_engine, source_config)
    target_bundles = build_env_bundles(_target_engine, target_config) if target_config else []

    dimensions = source_config.get("dimensions") or []
    return dimensions, diff_views(source_bundles, target_bundles)


@st.cache_data(ttl=300, show_spinner="Finding affected charts and MDims…")
def compute_usage(
    indicator_ids: tuple[int, ...],
    catalog_path: str,
    _source_engine: Engine,
    cache_key: str,
) -> dict[int, dict[str, list[dict[str, Any]]]]:
    """For each changed indicator, the charts and *other* MDims on this staging server that use it."""
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
    """Drop the previous MDim's view-selector params when another MDim is selected."""
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
    # Reflect the field's real structure: a description_key stored as a "- a\n- b" markdown string
    # (or a JSON list) renders as bullets; genuine prose renders as prose.
    value, other = as_bullets(value), as_bullets(other)
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


def _render_impact(view: ViewDiff, usage: dict[int, dict[str, list[dict[str, Any]]]], unit: str = "view") -> None:
    """The 'does this also affect charts / other MDims?' flag for one view, with an expandable list."""
    if not view.affects_indicator:
        if unit == "chart":
            st.caption(
                "🔒 **Chart-only change** — this is the chart's own config text (title/subtitle/footnote); "
                "the indicator metadata is unchanged, so no other charts or MDims are affected."
            )
        else:
            st.caption(
                "🔒 **MDim-only change** — the underlying indicator metadata is unchanged, so no standalone "
                "charts or other MDims are affected. (The change comes from an MDim-level override.)"
            )
        return

    charts, mdims = _view_impact(view, usage)
    n_c, n_m = len(charts), len(mdims)

    if n_c == 0 and n_m == 0:
        st.info(
            "This change is in the **shared indicator metadata**, but no published charts or other "
            "MDims currently use this indicator — so nothing else is affected."
        )
        return

    parts = []
    if n_c:
        parts.append(f"**{n_c}** chart{'s' if n_c != 1 else ''}")
    if n_m:
        parts.append(f"**{n_m}** other MDim{'s' if n_m != 1 else ''}")

    # Yellow warning box (matching the status box above) with the button right next to it.
    col_msg, col_btn = st.columns([5, 2], vertical_alignment="center")
    with col_msg:
        st.warning(
            "This change is in the **shared indicator metadata** — it also affects "
            + " and ".join(parts)
            + " that use this indicator."
        )
    with col_btn:
        btn_label = (
            f"📊 Show {n_c} affected chart{'s' if n_c != 1 else ''}"
            if n_c
            else f"🧭 Show {n_m} affected MDim{'s' if n_m != 1 else ''}"
        )
        with st.popover(btn_label, use_container_width=True):
            _render_affected_lists(view, charts, mdims)


def _render_affected_lists(view: ViewDiff, charts: list[dict], mdims: list[dict]) -> None:
    """The affected charts (paginated, hover-to-preview) and other MDims shown inside the popover."""
    if charts:
        chart_diff_url = f"{SOURCE.wizard_url}/chart-diff?diff-type=charts&indicator_id={view.indicator_id}"
        # The charts all inherit this view's indicator, so they all show the same change — build the
        # preview once from the indicator-layer fields and reuse it as every chart's hover tooltip.
        indicator_fields = {f: view.fields[f] for f in view.indicator_changed_fields if f in view.fields}
        preview_html = diff_preview_html(ViewDiff(dimensions=view.dimensions, fields=indicator_fields))
        component_html, height = render_affected_charts_html(charts, preview_html, SOURCE.site, chart_diff_url)
        components.html(component_html, height=height, scrolling=True)
    if mdims:
        st.markdown(f"**Other MDims ({len(mdims)})** — also use this indicator:")
        for m in mdims:
            st.markdown(f"- `{m.get('catalogPath')}`")


def _render_diff_body(
    view_diff: ViewDiff,
    baseline_name: str,
    links: list[str],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    unit: str = "view",
) -> None:
    """Status banner + blast-radius flag + side-by-side field diffs — shared by MDim views and charts."""
    if view_diff.is_new:
        st.info(f"This {unit} is **new** — it does not exist in {baseline_name}. " + " · ".join(links))
    elif view_diff.changed:
        n = len(view_diff.fields)
        st.warning(f"**{n} field{'s' if n > 1 else ''} changed** in this {unit}. " + " · ".join(links))
    else:
        st.success(f"No changes in this {unit}. " + " · ".join(links))

    if view_diff.changed and not view_diff.is_new:
        _render_impact(view_diff, usage, unit=unit)

    for field_name in [f for f in FIELD_ORDER if f in view_diff.fields]:
        change = view_diff.fields[field_name]
        st.markdown(f"##### {field_label(field_name)}")
        col_old, col_new = st.columns(2)
        with col_old:
            st.markdown(f":gray[**{baseline_name.capitalize()}**]")
            st.markdown(_render_text_html(change["old"], change["new"], side="old"), unsafe_allow_html=True)
        with col_new:
            st.markdown(":green[**This staging server**]")
            st.markdown(_render_text_html(change["new"], change["old"], side="new"), unsafe_allow_html=True)


def render_view_diff_page(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    mdim_row: Any,
    baseline: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> None:
    """The View diff page: MDim controls as navigation + side-by-side text diffs."""
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

        jump_col, _jump_spacer = st.columns([2, 3])
        with jump_col:
            st.selectbox(
                f"⚡ Changes detected — jump to a changed view ({len(changed_views)})",
                options=[""] + list(range(len(changed_views))),
                format_func=_jump_label,
                key="mdd_jump",
                on_change=_jump_to_changed,
            )

    # --- MDim controls (navigation across views) ---------------------------------
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
        # Drop a stale URL value (e.g. after switching MDim) so the widget doesn't crash.
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
    # NOTE: `published_target` is NaN when the MDim doesn't exist in the baseline (left join).
    baseline_slug = mdim_row.get("slug_target") if mdim_row.get("published_target") == 1 else None
    baseline_url = _view_url(_baseline_env(baseline), catalog_path, baseline_slug, view.dimensions)
    staging_url = _view_url(SOURCE, catalog_path, None, view.dimensions)

    # These links open the indicator's data page (where these "what you should know" texts render).
    links = [f"[Data page — {baseline_name}]({baseline_url})"]
    if view.changed:
        links.append(f"[Data page — this staging server]({staging_url})")

    _render_diff_body(view, baseline_name, links, usage, unit="view")


def _chart_flow(source_engine: Engine, target_engine: Engine, baseline: str) -> None:
    """Review a standalone chart's data-page WYSK (the indicator metadata it inherits), vs the baseline."""
    baseline_name = BASELINES[baseline]
    ref = st.text_input(
        "Chart",
        key="chart",
        placeholder="Chart slug, id, or grapher URL (e.g. daily-mean-income)",
        help="A standalone chart's 'what you should know' text is its indicator's metadata. "
        "Multi-indicator charts (scatters) have no data page, so their WYSK isn't shown to readers.",
    )
    if not ref:
        st.info("Enter a chart slug, ID, or grapher URL.")
        return

    src = build_chart_bundle(source_engine, ref)
    if src is None:
        st.warning(f"No published chart found for “{ref}”. Check the slug/id.")
        return
    src_bundle, chart = src
    tgt = build_chart_bundle(target_engine, str(chart["slug"]))
    target_bundle = tgt[0] if tgt is not None else None

    diff = diff_views([src_bundle], [target_bundle] if target_bundle is not None else [])[0]

    # Blast radius on the chart's indicator — but exclude the chart itself from its own affected list.
    usage: dict[int, dict[str, list[dict[str, Any]]]] = {}
    if diff.affects_indicator and diff.indicator_id is not None:
        raw = compute_usage(
            (diff.indicator_id,),
            f"chart:{chart['slug']}",
            source_engine,
            cache_key=f"{baseline}-chart-{chart['slug']}",
        )
        cur = int(chart["chartId"])
        usage = {
            vid: {"charts": [c for c in e.get("charts", []) if c.get("chartId") != cur], "mdims": e.get("mdims", [])}
            for vid, e in raw.items()
        }

    st.markdown(f"#### {chart.get('title') or chart['slug']}")
    baseline_url = f"{_baseline_env(baseline).site}/grapher/{chart['slug']}"
    staging_url = f"{SOURCE.site}/grapher/{chart['slug']}"
    links = [f"[Data page — {baseline_name}]({baseline_url})", f"[Data page — this staging server]({staging_url})"]
    _render_diff_body(diff, baseline_name, links, usage, unit="chart")


def main() -> None:
    st.title(":material/difference: Metadata Diff")
    st.caption(
        "Review how this staging server changes the metadata texts end users see — on **MDims** "
        "(view by view) or on an **individual chart's data page** (e.g. *What you should know about "
        "this data*) — against a baseline. This includes changes coming from garden step templates, "
        "which don't show up in config diffs."
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

    target = url_persist(st.segmented_control)(
        label="Review",
        options=["mdim", "chart"],
        format_func=lambda x: {
            "mdim": ":material/dashboard: MDim",
            "chart": ":material/show_chart: Charts",
        }[x],
        key="target",
        value="mdim",
        label_visibility="collapsed",
    )
    if target == "chart":
        _chart_flow(source_engine, target_engine, baseline)
        return

    df_mdims = get_mdim_changes(source_engine, target_engine)
    if df_mdims.empty:
        st.warning("No MDims found on this staging server.")
        return

    def _format_mdim(path: str) -> str:
        row = df_mdims.loc[path]
        if row["is_new"]:
            return f"{path} 🆕"
        if row["config_changed"]:
            return f"{path} ✏️"
        return path

    col_select, col_mode, _spacer = st.columns([2, 1, 1], vertical_alignment="bottom")
    with col_select:
        catalog_path = url_persist(st.selectbox)(
            "MDim",
            key="mdim",
            options=df_mdims.index.tolist(),
            format_func=_format_mdim,
            on_change=_clear_view_params,
            help="Select the MDim to review — type in the box to search it. "
            "✏️ marks MDims whose config differs from the baseline; texts can also change through "
            "indicator metadata without a config change, and the diff catches both.",
        )
    with col_mode:
        mode = url_persist(st.radio)(
            "Mode",
            key="mode",
            options=["tree", "view"],
            format_func=lambda m: "💥 Blast radius" if m == "tree" else "🔍 View diff",
            horizontal=True,
            label_visibility="collapsed",
        )

    if not catalog_path:
        st.info("Select an MDim.")
        return

    dimensions, view_diffs = compute_diff(
        catalog_path,
        source_engine,
        target_engine,
        cache_key=f"{baseline}-{df_mdims.loc[catalog_path, 'configMd5_source']}-{df_mdims.loc[catalog_path, 'configMd5_target']}",
    )

    if not view_diffs:
        st.warning("This MDim has no views.")
        return

    # Blast radius: which charts / other MDims use the indicators whose metadata this branch changed.
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
            st.success("No metadata changes in any view of this MDim. The tree below shows all views.")
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
