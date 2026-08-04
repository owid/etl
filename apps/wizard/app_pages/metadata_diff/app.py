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
    group_changes,
    inline_diff_html,
    override_snippet,
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


def _orange_banner(html_msg: str) -> None:
    """A theme-safe orange banner (matches the 🟠 'shared indicator metadata' idea). Takes HTML."""
    st.markdown(
        '<div style="background:rgba(232,89,12,0.12);border-left:4px solid #e8590c;'
        f'padding:10px 14px;border-radius:6px;">{html_msg}</div>',
        unsafe_allow_html=True,
    )


def _render_impact(view: ViewDiff, usage: dict[int, dict[str, list[dict[str, Any]]]], unit: str = "view") -> None:
    """The 'does this also affect charts / other MDims?' flag for one view, with the affected list and
    (for MDim views) the 'change only this view' override — both as on-demand buttons on the right."""
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
    parts = []
    if n_c:
        parts.append(f"<b>{n_c}</b> chart{'s' if n_c != 1 else ''}")
    if n_m:
        parts.append(f"<b>{n_m}</b> other MDim{'s' if n_m != 1 else ''}")

    col_msg, col_btn = st.columns([5, 2], vertical_alignment="center")
    with col_msg:
        if parts:
            _orange_banner(
                "This change is in the <b>shared indicator metadata</b> — it also affects "
                + " and ".join(parts)
                + " that use this indicator."
            )
        else:
            _orange_banner(
                "This change is in the <b>shared indicator metadata</b>, but no published charts or other "
                "MDims currently use this indicator — so nothing else is affected."
            )
    with col_btn:
        if n_c or n_m:
            btn_label = (
                f"📊 Show {n_c} affected chart{'s' if n_c != 1 else ''}"
                if n_c
                else f"🧭 Show {n_m} affected MDim{'s' if n_m != 1 else ''}"
            )
            with st.popover(btn_label, use_container_width=True):
                _render_affected_lists(view, charts, mdims)
        # MDim views can instead scope the change to themselves. The checkbox *records the decision*
        # (it's what the PR will act on); the exact edits/code are revealed on demand only.
        if unit == "view":
            dims_key = "-".join(f"{k}={val}" for k, val in sorted(view.dimensions.items()))
            if st.checkbox(
                "✏️ Change only this view",
                key=f"override::{dims_key}",
                help="Scope this shared change to THIS view only — an MDim override instead of a shared "
                "indicator change. Ticking records the decision for the PR; open the details for the exact edits.",
            ):
                with st.popover("Override details & code", use_container_width=True):
                    _render_override_body(view)


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


def _render_override_body(view_diff: ViewDiff) -> None:
    """Popover content: scope a shared change to THIS view only.

    The change currently lives in the shared indicator, so it reaches every chart / MDim view above.
    Scoping it here keeps the indicator (and all those other surfaces) on the old text and applies the
    new text as a view override — two edits (a garden revert + this override), both spelled out. The
    generated snippet is set to the new/staging text.
    """
    shared_fields = [f for f in FIELD_ORDER if f in view_diff.fields and f in view_diff.indicator_changed_fields]
    if not shared_fields:
        st.caption("No shared-indicator field to scope in this view.")
        return

    fields_str = ", ".join(f"`{field_label(f)}`" for f in shared_fields)
    st.markdown(
        "Scope this change to **only this view**, in two edits:\n\n"
        f"1. **Revert the shared change** ({fields_str}) in the indicator's garden `.meta.yml`, so every "
        "other chart and MDim view keeps the old text.\n"
        "2. **Add this override** to the MDim's Python step (after its `c.views` are built) — it re-applies "
        "the new text to this view alone:"
    )
    for f in shared_fields:
        st.markdown(f"**{field_label(f)}**")
        st.code(override_snippet(view_diff, f, view_diff.fields[f]["new"]), language="python")


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


_REVIEW_STATUSES = ["⏳ Pending", "✅ Approve", "🚩 Flag"]


def _dims_str(dims: dict[str, str]) -> str:
    return ", ".join(f"{k}={v}" for k, v in dims.items()) or "(default view)"


def _as_plaintext(val: Any) -> str:
    if isinstance(val, list):
        return " · ".join(str(x) for x in val)
    if val in (None, ""):
        return "—"
    return str(val)


def _review_markdown(
    catalog_path: str,
    baseline_name: str,
    groups: list[Any],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> str:
    """Compile the reviewer's sign-off + comments into a copy-pasteable punch-list for the author."""
    flagged = sum(
        1 for i in range(len(groups)) if st.session_state.get(f"rev-status::{catalog_path}::{i}", "").startswith("🚩")
    )
    lines = [f"# Metadata review — `{catalog_path}`", "", f"_Baseline: {baseline_name}_", ""]
    lines.append(f"**{len(groups)} distinct change(s)** — {flagged} flagged.")
    lines.append("")
    for i, g in enumerate(groups):
        status = st.session_state.get(f"rev-status::{catalog_path}::{i}", _REVIEW_STATUSES[0])
        comment = st.session_state.get(f"rev-comment::{catalog_path}::{i}", "").strip()
        scope = "shared indicator metadata" if g.affects_indicator else "MDim override"
        reach = f"{len(g.view_dims)} view(s)"
        if g.affects_indicator and g.indicator_id is not None:
            n_charts = len(usage.get(g.indicator_id, {}).get("charts", []))
            if n_charts:
                reach += f", {n_charts} chart(s)"
        lines.append(f"## {field_label(g.field)} — {status}")
        lines.append(f"- **Scope:** {scope}; affects {reach}")
        views = "; ".join(_dims_str(d) for d in g.view_dims[:8]) + (" …" if len(g.view_dims) > 8 else "")
        lines.append(f"- **Views:** {views}")
        lines.append(f"- **Before:** {_as_plaintext(g.old)}")
        lines.append(f"- **After:** {_as_plaintext(g.new)}")
        if comment:
            lines.append(f"- **💬 Comment:** {comment}")
        lines.append("")
    return "\n".join(lines)


def render_review_page(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    baseline: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> None:
    """Review mode: every distinct change in one place, each with sign-off + comment, plus a punch-list."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)
    groups = group_changes(view_diffs)
    if not groups:
        st.success("No metadata changes in any view of this MDim — nothing to review.")
        return

    baseline_name = BASELINES[baseline]
    n_shared = sum(1 for g in groups if g.affects_indicator)
    reviewed = sum(
        1
        for i in range(len(groups))
        if not st.session_state.get(f"rev-status::{catalog_path}::{i}", _REVIEW_STATUSES[0]).startswith("⏳")
    )
    st.markdown(
        f"**{len(groups)} distinct text change{'s' if len(groups) != 1 else ''}** to review "
        f"({n_shared} shared / indicator-level), ranked by reach · **{reviewed}/{len(groups)} signed off**."
    )
    st.caption("Each row is one distinct change — a shared indicator edit is judged once here, not view by view.")

    for i, g in enumerate(groups):
        status_key = f"rev-status::{catalog_path}::{i}"
        comment_key = f"rev-comment::{catalog_path}::{i}"
        status = st.session_state.get(status_key, _REVIEW_STATUSES[0])
        comment = st.session_state.get(comment_key, "").strip()

        imp = usage.get(g.indicator_id, {}) if (g.affects_indicator and g.indicator_id is not None) else {}
        charts, mdims = imp.get("charts", []), imp.get("mdims", [])
        reach_bits = [f"**{len(g.view_dims)}** view{'s' if len(g.view_dims) != 1 else ''} in this MDim"]
        if charts:
            reach_bits.append(f"**{len(charts)}** chart{'s' if len(charts) != 1 else ''}")
        if mdims:
            reach_bits.append(f"**{len(mdims)}** other MDim{'s' if len(mdims) != 1 else ''}")

        # Collapse once a decision is reached — but keep a 🚩 flag open until its comment is written,
        # so "flag then type" works (approving collapses immediately; flagging waits for the note).
        expanded = status.startswith("⏳") or (status.startswith("🚩") and not comment)
        reach_word = f"{len(g.view_dims)} view{'s' if len(g.view_dims) != 1 else ''}"
        if charts:
            reach_word += f" · {len(charts)} chart{'s' if len(charts) != 1 else ''}"
        header = f"{status.split()[0]} {field_label(g.field)} — {'shared' if g.affects_indicator else 'override'} · {reach_word}"
        if comment:
            header += "  💬"

        with st.expander(header, expanded=expanded):
            scope = "🔗 shared indicator metadata" if g.affects_indicator else "🔒 MDim override"
            st.caption(f"{scope} — affects " + " · ".join(reach_bits))
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f":gray[**{baseline_name.capitalize()}**]")
                st.markdown(_render_text_html(g.old, g.new, side="old"), unsafe_allow_html=True)
            with c2:
                st.markdown(":green[**This staging server**]")
                st.markdown(_render_text_html(g.new, g.old, side="new"), unsafe_allow_html=True)
            s1, s2 = st.columns([1, 3])
            with s1:
                st.radio("Sign-off", _REVIEW_STATUSES, key=status_key, label_visibility="collapsed")
            with s2:
                st.text_area(
                    "Comment",
                    key=comment_key,
                    placeholder="Optional note or suggested wording for the author…",
                    label_visibility="collapsed",
                )

    st.divider()
    with st.expander("📋 Review summary — copy as Markdown for the author"):
        st.code(_review_markdown(catalog_path, baseline_name, groups, usage), language="markdown")


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
    # Which changed views have been opened (scoped to this MDim) — drives the reviewed count + dot colours.
    visited: set[int] = st.session_state.setdefault(f"mdd_visited::{catalog_path}", set())
    if changed_views:
        n_changed = len(changed_views)

        # The changed view (if any) the current control selection is sitting on — so "Next" is relative
        # to where you are, and the current view counts as reviewed.
        cur_sel = {dim["slug"]: st.query_params.get(DIM_PARAM_PREFIX + dim["slug"]) for dim in dimensions}
        cur_idx = next(
            (
                i
                for i, cv in enumerate(changed_views)
                if cv.dimensions and all(cur_sel.get(s) == c for s, c in cv.dimensions.items())
            ),
            None,
        )
        if cur_idx is not None:
            visited.add(cur_idx)

        def _goto(idx: int) -> None:
            target = changed_views[idx % n_changed]
            for dim in dimensions:
                slug = dim["slug"]
                if slug in target.dimensions:
                    st.session_state[DIM_PARAM_PREFIX + slug] = target.dimensions[slug]
                    st.query_params[DIM_PARAM_PREFIX + slug] = target.dimensions[slug]
            visited.add(idx % n_changed)

        def _jump_to_changed() -> None:
            raw = st.session_state.get("mdd_jump")
            if raw not in (None, ""):
                _goto(int(raw))

        def _jump_label(i: Any) -> str:
            if i == "":
                return "Select a changed view…"
            cv = changed_views[int(i)]
            # 🟢 once reviewed, 🟡 not yet, 🆕 for a view that doesn't exist in the baseline.
            marker = "🆕" if cv.is_new else ("🟢" if int(i) in visited else "🟡")
            charts, _ = _view_impact(cv, usage)
            suffix = f"  —  ↗ {len(charts)} charts" if charts else ""
            return f"{marker} {_view_label(cv, dimensions)}{suffix}"

        jump_col, nav_col, _spacer = st.columns([2, 1, 2], vertical_alignment="bottom")
        with jump_col:
            st.selectbox(
                f"⚡ Changes detected — jump to a changed view ({len(visited)}/{n_changed} reviewed)",
                options=[""] + list(range(n_changed)),
                format_func=_jump_label,
                key="mdd_jump",
                on_change=_jump_to_changed,
            )
        with nav_col:
            st.button(
                "Next change ▶",
                on_click=_goto,
                args=(0 if cur_idx is None else cur_idx + 1,),
                use_container_width=True,
                help="Jump to the next view with changes (cycles back to the first at the end).",
            )

    # --- MDim controls (navigation across views) ---------------------------------
    st.caption(
        "🟡 marks a control option that leads to a changed view; it turns 🟢 once you've viewed that "
        "change (viewed — not necessarily approved). Follow the dots, or use **Next change ▶** to step "
        "through them one by one."
    )
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
        # A changed choice turns 🟢 once every changed view reachable through it has been viewed.
        viewed_choices = set()
        for choice in changed_choices:
            reachable = [
                j
                for j, cv in enumerate(changed_views)
                if cv.dimensions.get(dim_slug) == choice
                and all(cv.dimensions.get(s) == c for s, c in selection.items())
            ]
            if reachable and all(j in visited for j in reachable):
                viewed_choices.add(choice)
        names = {c["slug"]: (c.get("name") or c["slug"]) for c in dim.get("choices", [])}
        # Drop a stale URL value (e.g. after switching MDim) so the widget doesn't crash.
        if st.query_params.get(key) not in available:
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)

        def _fmt(slug, names=names, changed_choices=changed_choices, viewed_choices=viewed_choices):
            # Prefix (not suffix) so the marker survives the selectbox's "…" truncation and shows
            # in the collapsed box too.
            label = names.get(slug, slug)
            if slug in viewed_choices:
                return f"🟢 {label}"
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
        help="Select a chart to see changes to its data page.",
    )
    if not ref:
        st.info("Select a chart to see changes to its data page.")
        return

    src = build_chart_bundle(source_engine, ref)
    if src is None:
        st.warning(f"No published chart found for “{ref}”. Check the slug/id.")
        return
    src_bundle, chart = src

    # Grapher renders a data page only for single-indicator charts — say so when it doesn't.
    if not chart.get("has_data_page", True):
        st.warning(
            f"**{chart.get('title') or chart['slug']}** is a **multi-indicator chart** "
            f"({chart['n_indicators']} indicators) — it has **no data page**, so this text isn't shown to "
            "readers here. The diff below is the indicator's metadata for reference only."
        )
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


_ALL_NS = "(all namespaces)"


def _mdim_namespace(catalog_path: str) -> str:
    """Namespace segment of an MDim catalogPath (e.g. 'emissions'), used to filter the picker."""
    parts = catalog_path.split("#", 1)[0].strip("/").split("/")
    if parts and parts[0] == "grapher":
        parts = parts[1:]
    return parts[0] if parts else catalog_path


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

    namespaces = sorted({_mdim_namespace(p) for p in df_mdims.index})
    col_ns, col_select = st.columns([1, 3], vertical_alignment="bottom")
    with col_ns:
        ns_filter = url_persist(st.selectbox)(
            "Namespace",
            key="mdim_ns",
            options=[_ALL_NS] + namespaces,
            on_change=_clear_view_params,
            help="Filter the MDim list to one namespace, so you can scope a review to a dataset area "
            "instead of every MDim.",
        )
    mdim_options = [p for p in df_mdims.index if ns_filter in (_ALL_NS, None) or _mdim_namespace(p) == ns_filter]
    # Drop a persisted MDim that the namespace filter no longer includes, so the widget doesn't crash.
    if st.query_params.get("mdim") not in mdim_options:
        st.query_params.pop("mdim", None)
        st.session_state.pop("mdim", None)
    with col_select:
        catalog_path = url_persist(st.selectbox)(
            "MDim",
            key="mdim",
            options=mdim_options,
            format_func=_format_mdim,
            on_change=_clear_view_params,
            help="Select the MDim to review — type in the box to search it. "
            "✏️ marks MDims whose config differs from the baseline; texts can also change through "
            "indicator metadata without a config change, and the diff catches both.",
        )

    if not catalog_path:
        st.info("Select an MDim.")
        return

    mode = url_persist(st.radio)(
        "Mode",
        key="mode",
        options=["tree", "view", "review"],
        format_func=lambda m: {"tree": "💥 Blast radius", "view": "🔍 View diff", "review": "📋 Review"}[m],
        captions=[
            "how far each change reaches",
            "the proposed metadata changes, view by view",
            "sign off & comment on each change",
        ],
        horizontal=True,
        label_visibility="collapsed",
    )

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
    elif mode == "review":
        render_review_page(catalog_path, dimensions, view_diffs, baseline, usage)
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
