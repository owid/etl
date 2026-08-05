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
import json
import urllib.parse
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    CHART_FIELDS,
    METADATA_FIELDS,
    OVERRIDE_TARGET,
    ChangeGroup,
    ViewDiff,
    as_bullets,
    change_group_identity,
    diff_preview_html,
    diff_views,
    distinct_indicator_short_names,
    field_label,
    group_changes,
    inline_diff_html,
    override_snippet,
    parse_catalog_path,
    text_change_key,
    yaml_field_snippet,
)
from apps.wizard.app_pages.metadata_diff.data import (
    build_chart_bundle,
    build_env_bundles,
    delete_review,
    get_mdim_changes,
    load_mdim_config,
    load_reviews,
    load_scopes,
    set_scope,
    upsert_review,
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


@st.cache_data(ttl=300, show_spinner="Checking which MDims changed…")
def list_mdim_changes(_source_engine: Engine, _target_engine: Engine, cache_key: str) -> pd.DataFrame:
    """MDim list + change flags, cached across reruns.

    Streamlit reruns the whole script on every widget interaction, and this compares the metadata of
    every indicator the MDims use — so without a cache it would re-query on each click. Indicator
    metadata changes are picked up by the TTL, same as `compute_diff`.
    """
    return get_mdim_changes(_source_engine, _target_engine)


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


def _render_text_html(value: Any, other: Any, side: str, changed_only: bool = False) -> str:
    """One side of the side-by-side diff, with word-level highlights against the other side.

    Reflect the field's real structure: a description_key stored as a "- a\\n- b" markdown string (or a
    JSON list) renders as bullets; genuine prose renders as prose. With `changed_only` (used in the
    review), a list field (WYSK) shows only the bullets that changed on this side — hiding bullets that
    are unchanged — so the reviewer sees just the relevant points, not the whole list.
    """
    value, other = as_bullets(value), as_bullets(other)
    old, new = (other, value) if side == "new" else (value, other)

    def _one(o: Any, n: Any) -> str:
        return inline_diff_html(str(o or ""), str(n or ""), side=side)

    if isinstance(value, list) or isinstance(other, list):
        value_list = value if isinstance(value, list) else ([value] if value else [])
        other_list = other if isinstance(other, list) else ([other] if other else [])
        if changed_only:
            # Only bullets not present (unchanged) on the other side: additions/edits on the new side,
            # removals/edits on the old side. Unchanged bullets are hidden.
            unchanged = {str(x).strip() for x in other_list if x}
            items = [
                f"<li>{(_one('', v) if side == 'new' else _one(v, ''))}</li>"
                for v in value_list
                if v and str(v).strip() not in unchanged
            ]
            if not items:
                return '<div class="mdd-text mdd-empty">(no changes here)</div>'
            return f'<div class="mdd-text"><ul>{"".join(items)}</ul></div>'
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
    """The 'does this also affect charts / other MDims?' banner for one view, with the affected list
    available on demand in a popover."""
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

    banner = (
        "This change is in the <b>shared indicator metadata</b> — it also affects "
        + " and ".join(parts)
        + " that use this indicator."
        if parts
        else "This change is in the <b>shared indicator metadata</b>, but no published charts or other "
        "MDims currently use this indicator — so nothing else is affected."
    )
    reach_short = f"{n_c} chart{'s' if n_c != 1 else ''}" + (f" · {n_m} other MDim{'s' if n_m else ''}" if n_m else "")

    # Banner + a peek popover (a clean window) to see the affected charts / MDims on demand.
    col_msg, col_btn = st.columns([5, 2], vertical_alignment="center")
    with col_msg:
        _orange_banner(banner)
    with col_btn:
        if n_c or n_m:
            with st.popover(f"📊 Show {reach_short}", use_container_width=True):
                _render_affected_lists(view, charts, mdims)


def _render_affected_lists(view: ViewDiff, charts: list[dict], mdims: list[dict]) -> None:
    """The affected charts (paginated, hover-to-preview) and other MDims shown inside the popover."""
    if charts:
        # The charts all inherit this view's indicator, so they all show the same change — build the
        # preview once from the indicator-layer fields and reuse it as every chart's hover tooltip.
        indicator_fields = {f: view.fields[f] for f in view.indicator_changed_fields if f in view.fields}
        preview_html = diff_preview_html(ViewDiff(dimensions=view.dimensions, fields=indicator_fields))
        component_html, height = render_affected_charts_html(charts, preview_html, SOURCE.site)
        components.html(component_html, height=height, scrolling=True)
    if mdims:
        st.markdown(f"**Other MDims ({len(mdims)})** — also use this indicator:")
        for m in mdims:
            st.markdown(f"- `{m.get('catalogPath')}`")


def _render_author_scope(
    catalog_path: str,
    view_diff: ViewDiff,
    field_name: str,
    change: dict[str, Any],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    source_engine: Engine,
    scopes: dict[str, str],
    multi: bool = False,
) -> None:
    """The AUTHOR's per-change scope toggle, shown right under the affected-charts button: apply the
    shared change everywhere the indicator is used, or scope it to only this view. Default is
    **scope to this view** (the conservative choice — the other charts keep their existing text).
    Persisted (`metadata_scope`) so the reviewer is *shown* the decision — they approve or reject it,
    they don't set it. `multi` prefixes the field name when a view has several shared changes."""
    key = text_change_key(catalog_path, field_name, change["old"], change["new"])
    imp = usage.get(view_diff.indicator_id, {}) if view_diff.indicator_id is not None else {}
    n_c, n_m = len(imp.get("charts", [])), len(imp.get("mdims", []))
    reach = f"{n_c} chart{'s' if n_c != 1 else ''}"
    if n_m:
        reach += f" · {n_m} other MDim{'s' if n_m != 1 else ''}"

    sk = f"scope::{key}"
    if sk not in st.session_state:
        # Default to the conservative "only this view" unless the author explicitly chose "apply to all".
        st.session_state[sk] = "all" if scopes.get(key) == "all" else "scoped"

    def _save() -> None:
        set_scope(source_engine, catalog_path, key, st.session_state.get(sk, "scoped"), _reviewer())

    labels = {"all": f"Apply to all — {reach}", "scoped": "Scope to only this view"}
    radio_label = f"“{field_label(field_name)}” applies to" if multi else "This change applies to"
    st.radio(
        radio_label,
        options=["scoped", "all"],
        format_func=lambda x: labels[x],
        key=sk,
        on_change=_save,
        horizontal=True,
        help="The author's decision: apply this shared change everywhere the indicator is used, or only to "
        "this view (the default — check the affected charts in the banner above before applying to all). "
        "The reviewer is shown this and approves or rejects it — they don't set it.",
    )

    # Choosing "apply to all" must show WHAT it applies to: a count is not something the author can
    # check, so name every chart here, at the moment of the decision (and again in the PR brief).
    if st.session_state.get(sk) == "all" and (n_c or n_m):
        rows = []
        for c in sorted(imp.get("charts", []), key=lambda c: str(c.get("slug") or "")):
            slug = c.get("slug") or f"chart {c.get('chartId')}"
            flag = "" if c.get("wysk_shown", True) else " ⚠️ no data page — WYSK not shown to readers"
            rows.append(f"- [`{slug}`]({SOURCE.site}/grapher/{slug}){flag}")
        for m in sorted(imp.get("mdims", []), key=lambda m: str(m.get("slug") or "")):
            rows.append(f"- MDim `{m.get('slug') or m.get('catalogPath')}`")
        st.warning(f"**This will change {reach}.** These are the surfaces that get the new text:\n" + "\n".join(rows))


def _render_diff_body(
    view_diff: ViewDiff,
    baseline_name: str,
    baseline_url: str,
    staging_url: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    unit: str = "view",
    catalog_path: str = "",
    source_engine: Engine | None = None,
    scopes: dict[str, str] | None = None,
) -> None:
    """Status banner + blast-radius flag + side-by-side field diffs — shared by MDim views and charts.

    The per-env page link lives on each column header (e.g. WYSK → the indicator's data page), not in
    the status line. On MDim views, each shared field also gets the author's scope toggle.
    """
    if view_diff.is_new:
        st.info(
            f"This {unit} is **new** — it does not exist in {baseline_name}. "
            f"[{baseline_name} (data page)]({baseline_url}) · [this staging server (data page)]({staging_url})"
        )
        return
    if not view_diff.changed:
        st.success(
            f"No changes in this {unit}. "
            f"[{baseline_name} (data page)]({baseline_url}) · [this staging server (data page)]({staging_url})"
        )
        return

    n = len(view_diff.fields)
    st.warning(f"**{n} field{'s' if n > 1 else ''} changed** in this {unit}.")
    _render_impact(view_diff, usage, unit=unit)

    # The author's scope decision(s) sit under the banner — scope is about those shared charts.
    if unit == "view" and source_engine is not None:
        shared_fields = [f for f in FIELD_ORDER if f in view_diff.fields and f in view_diff.indicator_changed_fields]
        for field_name in shared_fields:
            _render_author_scope(
                catalog_path,
                view_diff,
                field_name,
                view_diff.fields[field_name],
                usage,
                source_engine,
                scopes or {},
                multi=len(shared_fields) > 1,
            )

    for field_name in [f for f in FIELD_ORDER if f in view_diff.fields]:
        change = view_diff.fields[field_name]
        st.markdown(f"##### {field_label(field_name)}")
        # WYSK / description fields render on the indicator's data page; chart FAUST on the chart itself.
        link_kind = "chart ↗" if field_name.startswith(CHART_FIELD_PREFIX) else "data page ↗"
        col_old, col_new = st.columns(2)
        with col_old:
            st.markdown(f":gray[**{baseline_name.capitalize()}**] · [{link_kind}]({baseline_url})")
            st.markdown(_render_text_html(change["old"], change["new"], side="old"), unsafe_allow_html=True)
        with col_new:
            st.markdown(f":green[**This staging server**] · [{link_kind}]({staging_url})")
            st.markdown(_render_text_html(change["new"], change["old"], side="new"), unsafe_allow_html=True)


def _reviewer() -> str | None:
    """Identity of the person signing off (audit trail), from session state if set. There is currently no
    reviewer input in the UI, so this is normally None — sign-offs are recorded without a name."""
    return (st.session_state.get("mdd_reviewer") or "").strip() or None


def _dims_str(dims: dict[str, str]) -> str:
    return ", ".join(f"{k}={v}" for k, v in dims.items()) or "(default view)"


def _as_plaintext(val: Any) -> str:
    if isinstance(val, list):
        return " · ".join(str(x) for x in val)
    if val in (None, ""):
        return "—"
    return str(val)


def _review_status_key(catalog_path: str, change_key: str) -> str:
    return f"rev-status::{catalog_path}::{change_key}"


def _review_comment_key(catalog_path: str, change_key: str) -> str:
    return f"rev-comment::{catalog_path}::{change_key}"


def _review_markdown(
    catalog_path: str,
    baseline_name: str,
    resolved: list[dict[str, Any]],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> str:
    """Compile the reviewer's sign-off + comments into a copy-pasteable punch-list for the author."""

    def _label(r: dict[str, Any]) -> str:
        if r["stale"]:
            return "⚠️ edited since review"
        return st.session_state.get(_review_status_key(catalog_path, r["change_key"]), r["seed_label"])

    flagged = sum(1 for r in resolved if _label(r).startswith("🚩"))
    lines = [f"# Metadata review — `{catalog_path}`", "", f"_Baseline: {baseline_name}_", ""]
    lines.append(f"**{len(resolved)} distinct change(s)** — {flagged} flagged.")
    lines.append("")
    for r in resolved:
        g = r["g"]
        status = _label(r)
        comment = (st.session_state.get(_review_comment_key(catalog_path, r["change_key"]), "") or "").strip()
        scope = "shared indicator metadata" if g.affects_indicator else "MDim override"
        reach = f"{len(g.view_dims)} view(s)"
        if g.affects_indicator:
            n_charts = len(_group_usage(g, usage).get("charts", []))
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


def _decision(catalog_root: str, r: dict[str, Any]) -> str:
    """The reviewer's call for one change — approved | flagged | pending | stale — used to route it
    into the PR brief's Apply / Hold / Pending sections."""
    if r["stale"]:
        return "stale"
    label = st.session_state.get(_review_status_key(catalog_root, r["change_key"]), r["seed_label"])
    return _STATUS_TO_DB.get(label, "pending")


# Shared instruction header — spells out what each review decision means, so it never has to be
# re-explained to whoever executes the PR.
_BRIEF_LEGEND = [
    "**▶ To open the PR:** copy this whole brief, paste it to Claude Code, and ask it to open the PR — it "
    "carries the changes, the checks to run, and a ready-to-paste PR description.",
    "",
    "**How to action each change (from the review decisions):**",
    "- ✅ **Approve → add to the PR** — apply the edit shown.",
    "- 🚩 **Flag → hold** — do NOT add; the reviewer wants a change (see the note).",
    "- ⏳ **Pending → do nothing** — not reviewed yet; leave as-is.",
    "",
    "_The **value** to set is exact. The **location** (file + key) is a best guess from the indicator's "
    "catalogPath — confirm it against the metadata build before committing, since a value set via "
    "`definitions`/anchors, `shared.meta.yml`, Jinja, or the step `.py` can live elsewhere._",
    "",
]


def _yaml_block(field_name: str, value: Any) -> list[str]:
    return ["```yaml", yaml_field_snippet(field_name, value), "```"]


def _group_usage(g: ChangeGroup, usage: dict[int, dict[str, list[dict[str, Any]]]]) -> dict[str, list[dict[str, Any]]]:
    """Union the blast radius over *every* indicator the change touches, deduped.

    A shared definition renders into many indicators, so "apply to all" reaches the union of all their
    charts and MDims. Reading only the group's first indicator (`usage[g.indicator_id]`) undercounts
    that reach — for a shared-definition edit, badly. We aggregate over `g.indicator_ids` (falling back
    to the single `indicator_id` for older groups), deduping charts by chartId and MDims by catalogPath.
    """
    ids = g.indicator_ids or ({g.indicator_id} if g.indicator_id is not None else set())
    charts: dict[int, dict[str, Any]] = {}
    mdims: dict[str, dict[str, Any]] = {}
    for iid in ids:
        imp = usage.get(iid, {})
        for c in imp.get("charts", []):
            charts.setdefault(c["chartId"], c)
        for m in imp.get("mdims", []):
            mdims.setdefault(m["catalogPath"], m)
    return {"charts": list(charts.values()), "mdims": list(mdims.values())}


def _garden_location_lines(g: ChangeGroup, reach: str) -> list[str]:
    """File + key hint for a shared indicator field, from its catalogPath.

    Two cases. If the identical text change lands on a single indicator, point at that variable's key.
    If it lands on *several* indicators (the fingerprint of a shared `definitions.*`/anchor — one Jinja
    template renders into many variables), point at the definition instead of guessing a variable, and
    flag the diff-observed reach as a floor. The single-variable key is a wrong, misleading target for a
    shared-definition edit, which is exactly the mistake this tool exists to prevent."""
    parsed = parse_catalog_path(g.catalog_path)
    garden_dir = parsed[0] if parsed else None
    table = parsed[1] if parsed else None
    file_line = (
        f"- **File (best guess):** `{garden_dir}.meta.yml` — or `{garden_dir}.meta.override.yml`"
        if garden_dir
        else "- **Where:** the indicator's garden `.meta.yml` (catalogPath unavailable)"
    )

    shared_names = distinct_indicator_short_names(g.catalog_paths)
    if len(shared_names) > 1:
        preview = ", ".join(f"`{n}`" for n in shared_names[:6]) + (" …" if len(shared_names) > 6 else "")
        dont_edit = f"`tables.{table}.variables.<short>`" if table else "any single variable"
        return [
            file_line,
            f"- **Likely a shared definition/anchor** — the identical text renders on at least "
            f"{len(shared_names)} indicators ({preview}) *within this MDim alone*, which happens through a "
            "shared `definitions.*` (Jinja) block or `shared.meta.yml`, not a per-variable field.",
            f"- **Find it:** grep the garden `.meta.yml` for the changed text and edit the `definitions.` "
            f"entry (or `shared.meta.yml`) — do **not** edit {dont_edit} directly.",
            f"- **Reach (observed in this diff):** {reach} — **treat as a floor.** This diff only sees the "
            "indicators used by this MDim; the definition is typically referenced by many more, so grep it "
            "to get the real count before deciding. (A branched definition changes only the matching branch "
            "— e.g. wealth views, not income — so verify which branch you edited.)",
        ]
    if parsed:
        return [
            file_line,
            f"- **Key:** `tables.{table}.variables.{parsed[2]}`",
            f"- **Reach (observed in this diff):** {reach}.",
        ]
    return [
        file_line,
        f"- **Reach (observed in this diff):** {reach}.",
    ]


# Indicator fields that a chart's own text can inherit. A chart that sets the corresponding key in
# its config patch is "shielded" — it keeps its own text and does NOT change with the indicator edit
# (see the edit-faust-metadata skill's per-field inheritance analysis).
INHERITED_TO_CHART_TEXT = {"titlePublic": "title", "descriptionShort": "subtitle"}

# Surfaces the blast radius here does NOT cover, and where to get each one. Named explicitly because
# an unlisted surface reads as "nothing else is affected", which is the one wrong signal to send.
_UNCOVERED_SURFACES = [
    "**Narrative charts** — children of an affected chart or MDim view. They inherit the parent's text, "
    "but the stored merged config can be stale, so an inheriting child keeps showing the OLD text until "
    "its patch is re-saved. A child that overrides the field keeps its own text permanently.",
    "**Explorer views** — deliberately not queried here (explorers are being phased out); legacy "
    "CSV-backed explorers are invisible to the DB tables entirely.",
    "**Data insights, static viz, key-chart slots, article links & embeds** — not queried here. Embeds "
    "don't break, but the text a reader sees changes.",
]


def _changed_text_lines(g: ChangeGroup) -> list[str]:
    """The exact text that changed, as a diff — the right payload for a shared-definition edit.

    For a shared `definitions.*` edit the pastable full-field YAML is actively wrong: the diffed value
    is the *rendered* output, so pasting it under a variable hardcodes rendered text and destroys the
    Jinja branches for every other dimension. What the executor needs is the one line to find and
    replace inside the definition, so we emit only the changed bullet(s) as a diff."""
    old, new = as_bullets(g.old), as_bullets(g.new)
    if isinstance(old, list) and isinstance(new, list):
        old_set = {str(x).strip() for x in old}
        new_set = {str(x).strip() for x in new}
        removed = [str(x) for x in old if str(x).strip() not in new_set]
        added = [str(x) for x in new if str(x).strip() not in old_set]
    else:
        removed = [str(old)] if str(old).strip() else []
        added = [str(new)] if str(new).strip() else []
    if not removed and not added:
        return []
    out = [
        "- **The text that changed** — find this inside the definition and replace it "
        "(do not paste a rendered value into a variable, it would break the Jinja branches):",
        "```diff",
    ]
    out += [f"- {t}" for t in removed]
    out += [f"+ {t}" for t in added]
    out.append("```")
    return out


def _surface_lines(g: ChangeGroup, usage: dict[int, dict[str, list[dict[str, Any]]]], scope: str) -> list[str]:
    """Name every chart and MDim this change lands on — not just a count.

    A count ("10 charts") is not something an author can check. Applying to all means those specific
    charts change, so the brief lists them by slug, and flags any chart that does NOT render a data page
    (a multi-indicator chart has no single data page, so a WYSK edit is invisible to its readers). When
    the change is scoped, the same list is what *keeps* the old text — equally worth seeing."""
    imp = _group_usage(g, usage)
    charts, mdims = imp.get("charts", []), imp.get("mdims", [])
    if not charts and not mdims:
        return ["- **Affected surfaces:** none — no published chart or other MDim uses these indicators."]

    verb = "will change" if scope != "scoped" else "keep the old text (scoped)"
    out: list[str] = []
    if charts:
        out.append(f"- **Charts that {verb} ({len(charts)}):**")
        for c in sorted(charts, key=lambda c: str(c.get("slug") or "")):
            slug = c.get("slug") or f"chart {c.get('chartId')}"
            note = (
                ""
                if c.get("wysk_shown", True)
                else " — ⚠️ multi-indicator chart: no data page, so WYSK is not shown to readers"
            )
            out.append(f"  - [`{slug}`](https://ourworldindata.org/grapher/{slug}){note}")
    if mdims:
        out.append(f"- **Other MDims that {verb} ({len(mdims)}):**")
        for m in sorted(mdims, key=lambda m: str(m.get("slug") or "")):
            out.append(f"  - `{m.get('slug') or m.get('catalogPath')}`")
    # These fields also feed a chart's title/subtitle by inheritance, and a chart carrying its own
    # value for that field is *shielded* — it keeps its current text. We list usage, not inheritance,
    # so for those fields the list above is an upper bound on the charts whose visible text changes.
    if g.field in INHERITED_TO_CHART_TEXT and charts:
        out.append(
            f"- _⚠️ Upper bound: `{field_label(g.field)}` also feeds the chart's "
            f"{INHERITED_TO_CHART_TEXT[g.field]} by inheritance, and a chart that sets its own "
            f"{INHERITED_TO_CHART_TEXT[g.field]} in its config keeps that text. This list is indicator "
            "usage, not per-field inheritance — for the exact set, run "
            f"`blast_radius.py --field {INHERITED_TO_CHART_TEXT[g.field]}` (edit-faust-metadata skill)._"
        )
    return out


def _pending_lines(header: str, rows: list[dict[str, Any]]) -> list[str]:
    out = [header]
    for r in rows:
        tag = "edited since review" if r["stale"] else "not reviewed"
        out.append(f"- {field_label(r['g'].field)} — {tag}")
    out.append("")
    return out


def _markdown_output(text: str, filename: str, key: str) -> None:
    """Render a Markdown output with a reliable copy button + a clipboard-free download.

    Streamlit's built-in `st.code` copy icon uses the async Clipboard API, which silently no-ops
    when the page isn't a secure context or runs in an iframe without clipboard permission — both
    common on the staging Wizard, which is why the built-in button "doesn't work" there. Our button
    falls back to `execCommand('copy')` on a scratch textarea (works in non-secure contexts), and the
    download button needs no clipboard at all."""
    st.code(text, language="markdown")
    payload = json.dumps(text)  # safe JS string literal: handles quotes, newlines, unicode
    btn_id = f"cp_{key}"
    components.html(
        f"""
        <button id="{btn_id}" style="font:inherit;padding:4px 12px;border:1px solid #ccc;
                border-radius:6px;background:#f6f6f6;cursor:pointer">📋 Copy to clipboard</button>
        <script>
        const _t = {payload};
        const _b = document.getElementById("{btn_id}");
        _b.addEventListener("click", async () => {{
            let ok = false;
            try {{ await navigator.clipboard.writeText(_t); ok = true; }} catch (e) {{
                try {{
                    const ta = document.createElement("textarea");
                    ta.value = _t; ta.style.position = "fixed"; ta.style.opacity = "0";
                    document.body.appendChild(ta); ta.focus(); ta.select();
                    ok = document.execCommand("copy"); ta.remove();
                }} catch (e2) {{ ok = false; }}
            }}
            _b.textContent = ok ? "✓ Copied" : "⚠ Select the text and press Ctrl/Cmd+C";
            setTimeout(() => {{ _b.textContent = "📋 Copy to clipboard"; }}, 1600);
        }});
        </script>
        """,
        height=44,
    )
    st.download_button("⬇ Download .md", data=text, file_name=filename, mime="text/markdown", key=f"dl_{key}")


def _change_one_liner(g: ChangeGroup) -> str:
    """One-line summary of an approved change for the PR-description draft."""
    label = field_label(g.field)
    if g.field.startswith(CHART_FIELD_PREFIX):
        return f"**{label}** — chart config (edited on the chart itself, not the ETL repo)"
    where = "shared indicator metadata" if g.affects_indicator else "MDim-level override"
    return f"**{label}** — {where}"


def _ship_section(approved_groups: list[ChangeGroup], baseline_name: str) -> list[str]:
    """The 'best of both' tail of the brief: a rigor checklist (blast radius, make check, metadata quality
    checks, staging rebuild + verify, Codex) plus a ready-to-paste PR description — so the copied brief is a
    complete, rigorous spec for opening the PR, not just a list of edits."""
    # Distinct garden datasets touched by shared-indicator edits, for concrete rebuild/upsert commands.
    datasets: list[str] = []
    for g in approved_groups:
        parsed = parse_catalog_path(g.catalog_path)
        if parsed:
            ds = parsed[0].replace("etl/steps/data/garden/", "")
            if ds not in datasets:
                datasets.append(ds)
    if datasets:
        build = "\n".join(
            f"  - `.venv/bin/etlr garden/{ds} grapher/{ds} --private` → "
            f"`STAGING=1 .venv/bin/etlr grapher://grapher/{ds} --grapher`"
            for ds in datasets
        )
    else:
        build = (
            "  - rebuild the edited garden step(s), then `STAGING=1 .venv/bin/etlr grapher://grapher/<step> --grapher`"
        )

    shared = any(g.affects_indicator for g in approved_groups)
    shared_def = any(len(distinct_indicator_short_names(g.catalog_paths)) > 1 for g in approved_groups)
    blast = (
        "shared indicator metadata — reaches every chart / MDim view using the indicator(s); see per-change **Reach** above"
        if shared
        else "contained — no surface beyond the target is affected"
    )
    if shared_def:
        blast += " — includes a **shared definition/anchor** edit reaching multiple indicators (the **Reach** counts above are floors)"
    fields = ", ".join(sorted({field_label(g.field) for g in approved_groups}))

    out = [
        "## 🚀 Ship it — run before opening the PR",
        "_Scope every check to the edited text only._",
        "- [ ] **Blast radius** reviewed (see *Reach* above) — apply-to-all vs scope decided",
        *(
            [
                "- [ ] **Shared definition** — confirmed the edit is in `definitions.*` / `shared.meta.yml` "
                "(not a single variable), and checked every indicator & dimension branch it renders on"
            ]
            if shared_def
            else []
        ),
        "- [ ] `make check`",
        "- [ ] **Typos** — `/check-metadata-typos`",
        "- [ ] **Jinja spacing** — `/check-metadata-spacing`",
        "- [ ] **Style guide** — `/check-metadata-style`",
        "- [ ] **Claims vs the producer** — `/adversarial-data-review` (only the new/edited text, against the source's docs)",
        "- [ ] **Rebuild + upsert to staging:**",
        build,
        "- [ ] **Verify on staging** — indicator metadata API / data page",
        *(
            [
                "- [ ] **Surfaces this brief did NOT check** — sweep them before merge:",
                *[f"  - {s}" for s in _UNCOVERED_SURFACES],
                "  - Full reference sweep: `find-chart-references` skill. Per-field inheritance (which "
                "surfaces an edit actually reaches, which are shielded): `blast_radius.py --field <f>` "
                "(edit-faust-metadata skill).",
            ]
            if shared
            else []
        ),
        "- [ ] **Open the PR** with the description below; post a bare `@codex review`; run the pr-babysitter loop",
        "",
        "## 📝 PR description (draft — paste as the PR body)",
        # Attribution is mandatory on anything posted to GitHub under a human's identity. Left as
        # placeholders on purpose: the tool can't know which assistant/model opens the PR, nor whose
        # handle is at the wheel, and a wrong @-tag pings a real person.
        "> _Written by <assistant> <model name> — @<handle> at the wheel (fill these in)._",
        "",
        f"Update user-facing metadata ({fields}) — {len(approved_groups)} change(s), reviewed and approved in the "
        "Metadata Diff tool.",
        "",
        "**Changes**",
        *[f"- {_change_one_liner(g)}" for g in approved_groups],
        "",
        f"**Blast radius:** {blast}."
        + (
            " Counts cover published charts and MDims; narrative charts, explorer views, data insights "
            "and static viz were checked separately (see below)."
            if shared
            else ""
        ),
        "**Checks:** `make check` · typos · Jinja spacing · style guide · claims-vs-producer.",
        f"**Reviewed against baseline:** {baseline_name} (Metadata Diff tool).",
        "**Verification:** staging preview — <link>.",
        "",
        "**Still open**",
        "- _Handed off / Proposed / Unverified — fill from the checklist results before merge._",
        "",
    ]
    return out


def _pr_brief_markdown(
    catalog_path: str,
    baseline_name: str,
    resolved: list[dict[str, Any]],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> str:
    """Decision-grouped PR brief for an MDim: **Apply** (approved) carries a turnkey edit — a pastable
    YAML value with a best-guess file+key, or a scoped `.py` override; **Hold** (flagged) carries the
    reviewer's note and is explicitly not applied; **Pending** is listed for reference only."""
    approved = [r for r in resolved if _decision(catalog_path, r) == "approved"]
    flagged = [r for r in resolved if _decision(catalog_path, r) == "flagged"]
    pending = [r for r in resolved if _decision(catalog_path, r) in ("pending", "stale")]

    lines = [
        f"# PR brief — `{catalog_path}`",
        "",
        f"_Baseline: {baseline_name}. ✅ {len(approved)} to apply · 🚩 {len(flagged)} on hold · "
        f"⏳ {len(pending)} pending._",
        "",
        *_BRIEF_LEGEND,
        f"## ✅ Apply — add to the PR ({len(approved)})",
    ]
    if not approved:
        lines.append("_Nothing approved yet._")
    for r in approved:
        g = r["g"]
        field = g.field
        lines.append(f"### {field_label(field)}")
        if not g.affects_indicator:
            lines.append(
                f"- **Where:** MDim-level field in `{catalog_path}` — set it on the view(s) in this MDim's step."
            )
            lines += _yaml_block(field, g.new)
        elif r["scope"] == "scoped" and field in OVERRIDE_TARGET:
            n_c = len(r["charts"])
            others = f"the {n_c} other chart(s) keep the old text" if n_c else "no other surface changes"
            lines.append(
                "- **Where:** scope to these views — add an override in this MDim's `.py` step **and** revert "
                f"the shared change in the indicator's garden `.meta.yml` ({others})."
            )
            lines.append("```python")
            for dims in g.view_dims[:8]:
                lines.append(override_snippet(ViewDiff(dimensions=dims), field, g.new))
            if len(g.view_dims) > 8:
                lines.append(f"# … and {len(g.view_dims) - 8} more view(s) — same override")
            lines.append("```")
            lines += _surface_lines(g, usage, "scoped")
        else:
            imp = _group_usage(g, usage)
            n_c, n_m = len(imp.get("charts", [])), len(imp.get("mdims", []))
            reach = f"{n_c} chart(s)" + (f" · {n_m} other MDim(s)" if n_m else "") + f" · {len(g.view_dims)} view(s)"
            lines += _garden_location_lines(g, reach)
            # Applying to all means these specific charts change — name them, so the author can check.
            lines += _surface_lines(g, usage, "all")
            # For a shared definition the pastable full-field YAML would break the Jinja branches, so
            # show the changed line instead; a plain per-variable field still gets the pastable block.
            # The changed line is always the safe, minimal edit. The full rendered field is kept for
            # reference but explicitly NOT pastable unless the field is authored literally — most
            # description_key fields are lists of `{definitions.*}` refs, and overwriting them with a
            # rendered value silently drops every other definition and Jinja branch.
            lines += _changed_text_lines(g)
            lines.append(
                "- _Full rendered value, for reference — do **not** paste it over the field unless "
                "the field is authored literally (no `{definitions.*}` refs, no Jinja):_"
            )
            lines += _yaml_block(field, g.new)
        lines.append("")

    if flagged:
        lines.append(f"## 🚩 Hold — flagged, do NOT add ({len(flagged)})")
        for r in flagged:
            g = r["g"]
            comment = (st.session_state.get(_review_comment_key(catalog_path, r["change_key"]), "") or "").strip()
            lines.append(f"### {field_label(g.field)}")
            if comment:
                lines.append(f"- **Reviewer:** {comment}")
            lines.append(f"- **Proposed:** {_as_plaintext(g.old)} → {_as_plaintext(g.new)}")
            lines.append("")

    if pending:
        lines += _pending_lines(f"## ⏳ Pending — no action ({len(pending)})", pending)
    if approved:
        lines += _ship_section([r["g"] for r in approved], baseline_name)
    return "\n".join(lines)


def _chart_pr_brief_markdown(
    chart: dict[str, Any],
    baseline_name: str,
    resolved: list[dict[str, Any]],
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    catalog_root: str,
) -> str:
    """Decision-grouped PR brief for a standalone chart. Approved indicator-layer changes carry a turnkey
    garden YAML edit; approved chart-config changes (title/subtitle/footnote) are flagged as NOT an ETL
    edit (they belong on the chart itself). Flagged → hold; pending → no action."""
    approved = [r for r in resolved if _decision(catalog_root, r) == "approved"]
    flagged = [r for r in resolved if _decision(catalog_root, r) == "flagged"]
    pending = [r for r in resolved if _decision(catalog_root, r) in ("pending", "stale")]
    slug = chart.get("slug")

    lines = [
        f"# PR brief — chart `{slug}`",
        "",
        f"_Baseline: {baseline_name}. ✅ {len(approved)} to apply · 🚩 {len(flagged)} on hold · "
        f"⏳ {len(pending)} pending._",
        "",
        *_BRIEF_LEGEND,
        f"## ✅ Apply — add to the PR ({len(approved)})",
    ]
    if not approved:
        lines.append("_Nothing approved yet._")
    for r in approved:
        g = r["g"]
        field = g.field
        lines.append(f"### {field_label(field)}")
        if field.startswith(CHART_FIELD_PREFIX):
            lines.append(
                "- ⚠️ **Not an ETL edit** — this is the chart's own config (title/subtitle/footnote). Change "
                "it on the chart itself (grapher admin / chart-diff), not in the ETL repo."
            )
            lines.append(f"- **Set to:** {_as_plaintext(g.new)}")
        elif g.affects_indicator:
            imp = _group_usage(g, usage)
            n_c, n_m = len(imp.get("charts", [])), len(imp.get("mdims", []))
            reach = f"{n_c} other chart(s)" + (f" · {n_m} MDim(s)" if n_m else "") or "no other surface"
            lines += _garden_location_lines(g, reach)
            lines += _surface_lines(g, usage, "all")
            # The changed line is always the safe, minimal edit. The full rendered field is kept for
            # reference but explicitly NOT pastable unless the field is authored literally — most
            # description_key fields are lists of `{definitions.*}` refs, and overwriting them with a
            # rendered value silently drops every other definition and Jinja branch.
            lines += _changed_text_lines(g)
            lines.append(
                "- _Full rendered value, for reference — do **not** paste it over the field unless "
                "the field is authored literally (no `{definitions.*}` refs, no Jinja):_"
            )
            lines += _yaml_block(field, g.new)
        else:
            lines.append("- **Where:** the indicator's garden `.meta.yml`.")
            lines += _yaml_block(field, g.new)
        lines.append("")

    if flagged:
        lines.append(f"## 🚩 Hold — flagged, do NOT add ({len(flagged)})")
        for r in flagged:
            g = r["g"]
            lines.append(f"### {field_label(g.field)}")
            lines.append(f"- **Proposed:** {_as_plaintext(g.old)} → {_as_plaintext(g.new)}")
            lines.append("")

    if pending:
        lines += _pending_lines(f"## ⏳ Pending — no action ({len(pending)})", pending)
    if approved:
        lines += _ship_section([r["g"] for r in approved], baseline_name)
    return "\n".join(lines)


# The reviewer only accepts or rejects — the scope decision belongs to the author (View diff toggle).
_REVIEW_STATUSES = ["⏳ Pending", "✅ Approve", "🚩 Flag"]
_STATUS_TO_DB = {"✅ Approve": "approved", "🚩 Flag": "flagged"}
_STATUS_FROM_DB = {"approved": "✅ Approve", "flagged": "🚩 Flag"}


def _scope_label(scope: str, g: Any, usage: dict[int, dict[str, list[dict[str, Any]]]]) -> str:
    """The blast-radius consequence of the author's scope decision, shown by each change in the review."""
    if not g.affects_indicator:
        return "🔒 MDim override — local to this view; no other charts or MDims are affected."
    imp = usage.get(g.indicator_id, {}) if g.indicator_id is not None else {}
    n_c, n_m = len(imp.get("charts", [])), len(imp.get("mdims", []))
    if not n_c and not n_m:
        return "🔗 Shared indicator metadata — no other charts or MDims use it, so nothing else changes."
    also = f"{n_c} chart{'s' if n_c != 1 else ''}" + (f" and {n_m} other MDim{'s' if n_m != 1 else ''}" if n_m else "")
    if scope == "scoped":
        return f"✏️ {also} also use this indicator — **scoped to this MDim only**, so they keep their current text."
    return f"🔗 {also} also use this indicator — **all will change** with this edit."


def render_review_page(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    mdim_row: Any,
    baseline: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    source_engine: Engine,
) -> None:
    """Review mode: each distinct change with a DB-persisted, content-bound reviewer sign-off (Approve /
    Flag) + comment, the AUTHOR's scope decision shown for context, a lock-in gate, and a punch-list.
    The reviewer accepts or rejects; the scope decision is the author's (set on the View diff)."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)
    groups = group_changes(view_diffs)
    if not groups:
        st.success("No metadata changes in any view of this MDim — nothing to review.")
        return

    baseline_name = BASELINES[baseline]
    baseline_slug = mdim_row.get("slug_target") if mdim_row.get("published_target") == 1 else None
    reviews = load_reviews(source_engine, catalog_path)
    scopes = load_scopes(source_engine, catalog_path)
    reviewer = _reviewer()

    # Resolve each group: reviewer sign-off (content-hash lock-in) + the author's scope decision.
    resolved: list[dict[str, Any]] = []
    for g in groups:
        change_key, content_hash = change_group_identity(catalog_path, g)
        # Default to the conservative "only this view" unless the author explicitly chose "all" (matches
        # the View-diff toggle default), so the scope label and PR brief agree with what the author saw.
        scope = scopes.get(text_change_key(catalog_path, g.field, g.old, g.new), "scoped")
        imp = _group_usage(g, usage) if g.affects_indicator else {}
        charts, mdims = imp.get("charts", []), imp.get("mdims", [])
        row = reviews.get(change_key)
        stale = bool(row) and row.get("contentHash") != content_hash
        if row and not stale:
            seed_label = _STATUS_FROM_DB.get(row.get("status"), "⏳ Pending")
            seed_comment = row.get("comment") or ""
        else:
            seed_label, seed_comment = "⏳ Pending", ""
        resolved.append(
            {
                "g": g,
                "change_key": change_key,
                "content_hash": content_hash,
                "stale": stale,
                "scope": scope,
                "charts": charts,
                "mdims": mdims,
                "seed_label": seed_label,
                "seed_comment": seed_comment,
                "reviewer": (row or {}).get("reviewer"),
                "updatedAt": (row or {}).get("updatedAt"),
            }
        )

    # Seed widget state from the DB before any widget is created — so a fresh session shows stored reviews.
    for r in resolved:
        sk, ck = _review_status_key(catalog_path, r["change_key"]), _review_comment_key(catalog_path, r["change_key"])
        if sk not in st.session_state:
            st.session_state[sk] = r["seed_label"]
        if ck not in st.session_state:
            st.session_state[ck] = r["seed_comment"]

    def _effective(r: dict[str, Any]) -> str:
        if r["stale"]:
            return "stale"
        label = st.session_state.get(_review_status_key(catalog_path, r["change_key"]), r["seed_label"])
        return _STATUS_TO_DB.get(label, "pending")

    states = [_effective(r) for r in resolved]
    n = len(states)
    n_appr, n_flag, n_stale, n_pend = (
        states.count("approved"),
        states.count("flagged"),
        states.count("stale"),
        states.count("pending"),
    )

    # --- Review status: iterate on the changes, then share comments or create a PR at the end ---
    st.caption(
        "This review pass is a way to go through the metadata changes and iterate with the author. At the "
        "end of the review, you can decide whether to share comments with the author or create a PR."
    )
    if n_appr == n and n > 0:
        st.success(f"✅ **All {n} changes reviewed** — approved.")
    else:
        bits = []
        if n_pend:
            bits.append(f"**{n_pend}** pending")
        if n_flag:
            bits.append(f"**{n_flag}** flagged")
        if n_stale:
            bits.append(f"**{n_stale}** edited since review")
        st.info(f"Review pending — {', '.join(bits)} of {n}.")
    st.caption(
        f"{n_appr}/{n} approved · decisions are stored on this staging server and bound to the exact text — "
        "any later edit reopens that change for re-review."
    )

    def _make_save(change_key: str, content_hash: str):
        sk, ck = _review_status_key(catalog_path, change_key), _review_comment_key(catalog_path, change_key)

        def _save() -> None:
            label = st.session_state.get(sk, "⏳ Pending")
            comment = (st.session_state.get(ck) or "").strip() or None
            db_status = _STATUS_TO_DB.get(label)
            if db_status is None:
                delete_review(source_engine, change_key)
            else:
                upsert_review(source_engine, catalog_path, change_key, content_hash, db_status, comment, reviewer)

        return _save

    for r in resolved:
        g = r["g"]
        change_key = r["change_key"]
        sk, ck = _review_status_key(catalog_path, change_key), _review_comment_key(catalog_path, change_key)
        status = st.session_state.get(sk, r["seed_label"])
        comment = (st.session_state.get(ck) or "").strip()
        stale = r["stale"]
        eff = _effective(r)

        reach_word = f"{len(g.view_dims)} view{'s' if len(g.view_dims) != 1 else ''}"

        icon = "⚠️" if stale else status.split()[0]
        header = f"{icon} {field_label(g.field)} — {reach_word}"
        if comment:
            header += "  💬"
        if stale:
            header += "  · edited since review"

        # Collapse once decided; a 🚩 flag waits for its comment; stale/pending stay open.
        expanded = stale or eff == "pending" or (eff == "flagged" and not comment)
        save = _make_save(change_key, r["content_hash"])

        # Per-group representative view, for the data-page links on the column headers.
        rep_dims = g.view_dims[0] if g.view_dims else {}
        b_url = _view_url(_baseline_env(baseline), catalog_path, baseline_slug, rep_dims)
        s_url = _view_url(SOURCE, catalog_path, None, rep_dims)
        link_kind = "chart ↗" if g.field.startswith(CHART_FIELD_PREFIX) else "data page ↗"

        with st.expander(header, expanded=expanded):
            if stale:
                st.warning(
                    "⚠️ This text was **edited since it was last reviewed**, so the previous sign-off no "
                    "longer counts. Re-review to lock it in."
                )
            st.caption(_scope_label(r["scope"], g, usage))
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f":gray[**{baseline_name.capitalize()}**] · [{link_kind}]({b_url})")
                st.markdown(_render_text_html(g.old, g.new, side="old", changed_only=True), unsafe_allow_html=True)
            with c2:
                st.markdown(f":green[**This staging server**] · [{link_kind}]({s_url})")
                st.markdown(_render_text_html(g.new, g.old, side="new", changed_only=True), unsafe_allow_html=True)
            s1, s2 = st.columns([1, 3])
            with s1:
                st.radio("Sign-off", _REVIEW_STATUSES, key=sk, on_change=save, label_visibility="collapsed")
            with s2:
                st.text_area(
                    "Comment",
                    key=ck,
                    on_change=save,
                    placeholder="Optional note or suggested wording for the author…",
                    label_visibility="collapsed",
                )
            if r["reviewer"] and not stale and eff != "pending":
                when = f" · {r['updatedAt']}" if r.get("updatedAt") else ""
                st.caption(f"Signed off by **{r['reviewer']}**{when}")

    st.divider()
    st.markdown("**Outputs** — copy either as Markdown:")
    with st.expander("📋 Review summary — share with the author"):
        st.caption("The punch-list of decisions and comments, for the person who wrote the changes.")
        _markdown_output(_review_markdown(catalog_path, baseline_name, resolved, usage), "review-summary.md", "review")
    with st.expander("🔀 PR brief — changes to execute"):
        st.caption(
            "A complete PR spec — the changes, the checks to run, and a ready PR description. **Copy it and "
            "paste it to Claude Code, asking it to open the PR.**"
        )
        _markdown_output(_pr_brief_markdown(catalog_path, baseline_name, resolved, usage), "pr-brief.md", "mdim_brief")


def render_view_diff_page(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    mdim_row: Any,
    baseline: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    source_engine: Engine,
) -> None:
    """The View diff page: MDim controls as navigation + side-by-side text diffs."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)
    scopes = load_scopes(source_engine, catalog_path)

    # --- Jump straight to a changed view -----------------------------------------
    # Direct navigation to the changes, so the user doesn't have to hunt through the controls. Written
    # via a callback because url_persist only reads the URL when a control's state is still empty.
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
            # 🟢 once reviewed, 🟡 not yet.
            marker = "🟢" if int(i) in visited else "🟡"
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
    if changed_views:
        st.caption(
            "In the jump menu above: 🟡 a changed view · 🟢 already viewed. "
            "Use **Next change ▶** to step through the changes one by one."
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
        names = {c["slug"]: (c.get("name") or c["slug"]) for c in dim.get("choices", [])}
        # Drop a stale URL value (e.g. after switching MDim) so the widget doesn't crash.
        if st.query_params.get(key) not in available:
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)

        def _fmt(slug, names=names):
            return names.get(slug, slug)

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

    _render_diff_body(
        view,
        baseline_name,
        baseline_url,
        staging_url,
        usage,
        unit="view",
        catalog_path=catalog_path,
        source_engine=source_engine,
        scopes=scopes,
    )


def _render_chart_review(
    chart: dict[str, Any],
    diff: ViewDiff,
    source_engine: Engine,
    baseline_name: str,
    baseline_url: str,
    staging_url: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> None:
    """Per-chart review: each changed field is a collapsible holding its diff + an Approve/Flag decision
    (no comment box), collapsing once decided — same DB lock-in as the MDim review, keyed by chart slug.
    A standalone chart can't be overridden, so there's no scope decision."""
    groups = group_changes([diff])
    if not groups:
        return
    catalog_root = f"chart:{chart['slug']}"
    reviews = load_reviews(source_engine, catalog_root)
    reviewer = _reviewer()

    resolved: list[dict[str, Any]] = []
    for g in groups:
        change_key, content_hash = change_group_identity(catalog_root, g)
        row = reviews.get(change_key)
        stale = bool(row) and row.get("contentHash") != content_hash
        seed_label = _STATUS_FROM_DB.get(row.get("status"), "⏳ Pending") if (row and not stale) else "⏳ Pending"
        resolved.append(
            {
                "g": g,
                "change_key": change_key,
                "content_hash": content_hash,
                "stale": stale,
                "seed_label": seed_label,
                "reviewer": (row or {}).get("reviewer"),
                "updatedAt": (row or {}).get("updatedAt"),
            }
        )

    for r in resolved:
        sk = _review_status_key(catalog_root, r["change_key"])
        if sk not in st.session_state:
            st.session_state[sk] = r["seed_label"]

    def _eff(r: dict[str, Any]) -> str:
        if r["stale"]:
            return "stale"
        label = st.session_state.get(_review_status_key(catalog_root, r["change_key"]), r["seed_label"])
        return _STATUS_TO_DB.get(label, "pending")

    states = [_eff(r) for r in resolved]
    n = len(states)
    n_appr = states.count("approved")
    n_flag = states.count("flagged")
    n_stale = states.count("stale")
    n_pend = states.count("pending")

    st.divider()
    st.caption(
        "This review pass is a way to go through the chart's metadata changes. At the end of the review, "
        "you can create a PR of the changes."
    )
    if n_appr == n and n > 0:
        st.success(f"✅ **All {n} change{'s' if n != 1 else ''} reviewed** — approved.")
    else:
        bits = []
        if n_pend:
            bits.append(f"**{n_pend}** pending")
        if n_flag:
            bits.append(f"**{n_flag}** flagged")
        if n_stale:
            bits.append(f"**{n_stale}** edited since review")
        st.info(f"Review pending — {', '.join(bits)} of {n}.")

    def _make_save(change_key: str, content_hash: str):
        sk = _review_status_key(catalog_root, change_key)

        def _save() -> None:
            db_status = _STATUS_TO_DB.get(st.session_state.get(sk, "⏳ Pending"))
            if db_status is None:
                delete_review(source_engine, change_key)
            else:
                upsert_review(source_engine, catalog_root, change_key, content_hash, db_status, None, reviewer)

        return _save

    for r in resolved:
        g = r["g"]
        sk = _review_status_key(catalog_root, r["change_key"])
        eff = _eff(r)
        stale = r["stale"]
        status = st.session_state.get(sk, r["seed_label"])
        save = _make_save(r["change_key"], r["content_hash"])
        icon = "⚠️" if stale else status.split()[0]
        header = f"{icon} {field_label(g.field)}" + ("  · edited since review" if stale else "")
        link_kind = "chart ↗" if g.field.startswith(CHART_FIELD_PREFIX) else "data page ↗"
        with st.expander(header, expanded=(stale or eff == "pending")):
            if stale:
                st.warning(
                    "⚠️ Edited since it was last reviewed — the previous sign-off no longer counts. Re-review to lock in."
                )
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f":gray[**{baseline_name.capitalize()}**] · [{link_kind}]({baseline_url})")
                st.markdown(_render_text_html(g.old, g.new, side="old", changed_only=True), unsafe_allow_html=True)
            with c2:
                st.markdown(f":green[**This staging server**] · [{link_kind}]({staging_url})")
                st.markdown(_render_text_html(g.new, g.old, side="new", changed_only=True), unsafe_allow_html=True)
            st.radio(
                "Sign-off", _REVIEW_STATUSES, key=sk, on_change=save, horizontal=True, label_visibility="collapsed"
            )
            if r["reviewer"] and not stale and eff != "pending":
                when = f" · {r['updatedAt']}" if r.get("updatedAt") else ""
                st.caption(f"Signed off by **{r['reviewer']}**{when}")

    st.divider()
    with st.expander("🔀 PR brief — changes to execute"):
        st.caption(
            "A complete PR spec — the changes, the checks to run, and a ready PR description. **Copy it and "
            "paste it to Claude Code, asking it to open the PR.**"
        )
        _markdown_output(
            _chart_pr_brief_markdown(chart, baseline_name, resolved, usage, catalog_root), "pr-brief.md", "chart_brief"
        )


def _chart_flow(source_engine: Engine, target_engine: Engine, baseline: str) -> None:
    """Review a standalone chart's data-page WYSK (the indicator metadata it inherits), vs the baseline."""
    baseline_name = BASELINES[baseline]
    ref = st.text_input(
        "Chart",
        key="chart",
        placeholder="Chart slug, id, or grapher URL (e.g. daily-mean-income)",
        help="Select a chart to see changes to its metadata.",
    )
    if not ref:
        st.info("Select a chart to see changes to its metadata.")
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

    st.markdown(DIFF_CSS, unsafe_allow_html=True)  # same diff styling as the MDim view page
    st.markdown(f"#### {chart.get('title') or chart['slug']}")
    # Single-indicator chart data pages don't render on a staging server by default (they come up
    # blank); the admin chart preview with `forceDatapage=true` forces the data page, so WYSK /
    # description_key edits are actually visible. Use it for both envs (works on production too).
    cid = chart["chartId"]
    baseline_url = f"{_baseline_env(baseline).admin_site}/charts/{cid}/preview?forceDatapage=true"
    staging_url = f"{SOURCE.admin_site}/charts/{cid}/preview?forceDatapage=true"
    links = f"[{baseline_name} (data page)]({baseline_url}) · [this staging server (data page)]({staging_url})"

    if diff.is_new:
        st.info(f"This chart is **new** — it does not exist in {baseline_name}. " + links)
        return
    if not diff.changed:
        st.success("No changes to this chart's data-page text. " + links)
        return

    nf = len(diff.fields)
    st.warning(f"**{nf} field{'s' if nf != 1 else ''} changed** in this chart.")
    _render_impact(diff, usage, unit="chart")
    # Each changed field: collapsible with its diff + Approve/Flag decision (decision right after content).
    _render_chart_review(chart, diff, source_engine, baseline_name, baseline_url, staging_url, usage)


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
        format_func=lambda b: "🌍 Production" if b == "production" else "🌿 Master (what this branch changes)",
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

    df_mdims = list_mdim_changes(source_engine, target_engine, cache_key=baseline)
    if df_mdims.empty:
        st.warning("No MDims found on this staging server.")
        return
    if "indicator_check_failed" in df_mdims.columns and bool(df_mdims["indicator_check_failed"].any()):
        st.warning(
            "Could not compare indicator metadata against the baseline, so ✏️ only reflects MDim "
            "**config** changes here — an MDim whose texts changed may be unmarked. Open it to diff anyway."
        )

    def _format_mdim(path: str) -> str:
        row = df_mdims.loc[path]
        if row["is_new"]:
            return f"{path} 🆕"
        # Mark on either signal: a text edit usually changes indicator metadata without touching the
        # MDim config, so a config-only marker misses exactly the case this tool is for.
        if row.get("has_changes", row["config_changed"]):
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
            index=None,
            placeholder="Select an MDim…",
            format_func=_format_mdim,
            on_change=_clear_view_params,
            help="Select the MDim to review — type in the box to search it. ✏️ marks MDims that differ "
            "from the baseline — either their own config, or the metadata of an indicator they use "
            "(where most text edits land). 🆕 marks MDims that don't exist in the baseline.",
        )

    if not catalog_path:
        st.info("Select an MDim to see changes to its metadata.")
        return

    mode = url_persist(st.segmented_control)(
        "Mode",
        key="mode",
        options=["tree", "view", "review"],
        format_func=lambda m: {"tree": "💥 Blast radius", "view": "🔍 View diff", "review": "📋 Review"}[m],
        value="tree",
        label_visibility="collapsed",
    )
    mode = mode or "tree"  # segmented_control returns None if deselected
    st.caption(
        "**Blast radius**: how far each change reaches · **View diff**: the proposed changes, view by "
        "view · **Review**: sign off, comment & prepare a PR."
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
        render_view_diff_page(
            catalog_path, dimensions, view_diffs, df_mdims.loc[catalog_path], baseline, usage, source_engine
        )
    elif mode == "review":
        render_review_page(
            catalog_path, dimensions, view_diffs, df_mdims.loc[catalog_path], baseline, usage, source_engine
        )
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
            self_url=f"{SOURCE.wizard_url}/metadata-diff",
        )
        # NOTE: nothing should be rendered below the component — it resizes itself to its
        # content, and Streamlit-rendered siblings would overlap during the resize.
        components.html(tree_html, height=height, scrolling=True)


main()
