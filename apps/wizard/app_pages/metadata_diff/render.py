"""Shared rendering for the Metadata Diff app: text diffs, blast-radius banners, Markdown outputs.

Lives in its own module so the three section lists, the MDim pages and the chart flow can all use the
same rendering without importing the page entrypoint.

The baseline is the one the rest of the wizard's diffs use — production when this server has production
credentials, `staging-site-master` otherwise. Importing it from `chart_diff.utils` is deliberate: the
Metadata Diff used to offer a "Compare against" choice, which asked reviewers a question none of the
other diffs ask, and which they had to get right for the numbers to mean anything.
"""

import difflib
import html
import json
import urllib.parse
from typing import Any

import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    CHART_FIELDS,
    METADATA_FIELDS,
    ViewDiff,
    as_bullets,
    charts_in_reading_order,
    diff_preview_html,
    field_label,
    inline_diff_html,
    split_by_prominence,
    text_change_key,
)
from apps.wizard.app_pages.metadata_diff.data import set_scope
from apps.wizard.app_pages.metadata_diff.tree import render_affected_charts_html

# Display order of the diffed fields: indicator metadata first, then the chart's own text.
FIELD_ORDER = list(METADATA_FIELDS) + [CHART_FIELD_PREFIX + f for f in CHART_FIELDS]

# Name of the baseline, as shown in the UI ("production" / "staging-site-master").
BASELINE_NAME = TARGET.name

DIFF_CSS = """
<style>
.mdd-text { border: 1px solid #e0e0e0; border-radius: 6px; padding: 10px 14px; line-height: 1.5;
            background: #fff; }
.mdd-text ul { margin: 0 0 0 18px; padding: 0; }
.mdd-text li { margin-bottom: 8px; }
.mdd-text del.mdd-del { background: #ffe3e3; color: #c92a2a; text-decoration: line-through; }
.mdd-text ins.mdd-ins { background: #d3f9d8; color: #2b8a3e; text-decoration: none; }
.mdd-empty { color: #999; font-style: italic; }
.mdd-slot { font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em; color: #868e96;
            margin: 10px 0 2px; }
.mdd-slot-unchanged { font-size: 0.78rem; color: #adb5bd; border-left: 3px solid #e9ecef;
                      padding-left: 8px; margin: 6px 0; }
</style>
"""


def reviewer() -> str | None:
    """Identity of the person signing off (audit trail), from session state if set. There is currently no
    reviewer input in the UI, so this is normally None — sign-offs are recorded without a name."""
    return (st.session_state.get("mdd_reviewer") or "").strip() or None


def view_label(view: ViewDiff, dimensions: list[dict[str, Any]]) -> str:
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


def view_url(env, catalog_path: str, published_slug: str | None, dims: dict[str, str]) -> str:
    """URL of a view in a given environment (site if published there, admin preview otherwise)."""
    params = urllib.parse.urlencode(dims)
    if published_slug:
        return f"{env.site}/grapher/{published_slug}?{params}"
    return f"{env.admin_site}/grapher/{urllib.parse.quote(catalog_path, safe='')}/?{params}"


def chart_datapage_url(env, chart_id: int) -> str:
    """Admin chart preview forced onto its data page.

    A single-indicator chart's data page does not render on a staging server by default (it comes up
    blank); `forceDatapage=true` makes WYSK / description_key edits actually visible. Works on
    production too, so both sides of a diff can use the same form of link.
    """
    return f"{env.admin_site}/charts/{chart_id}/preview?forceDatapage=true"


def _norm_bullet(bullet: Any) -> str:
    return str(bullet).strip() if bullet else ""


# How alike two bullets must be to count as the same bullet edited, rather than one removed and another
# added. Low enough that a rewritten sentence inside a long bullet still pairs, high enough that two
# unrelated bullets sharing boilerplate ("For more details, see the documentation") do not.
_BULLET_MATCH_RATIO = 0.4


def pair_bullets(old_list: list[Any], new_list: list[Any]) -> list[tuple[Any | None, Any | None]]:
    """Line bullets up across the two sides: (old, new), with None where a bullet exists on one side only.

    Deterministic on purpose. Each column of the diff is rendered by its own call, so the two calls must
    reach the same pairing from the same pair of lists, or the columns would disagree about which bullet
    is which. Nothing here depends on which side is being drawn.

    Identical bullets pair first, so an edit is never matched to a bullet that survived untouched. What is
    left pairs by similarity, most-alike first, which is what lets a reworded bullet diff against its own
    earlier wording. Below the threshold a bullet is a genuine addition or removal and pairs with None.
    """
    old_norm = [_norm_bullet(b) for b in old_list]
    new_norm = [_norm_bullet(b) for b in new_list]
    pairs: dict[int, int] = {}  # new index -> the old index it came from
    used_old: set[int] = set()

    # Identical text first.
    for n_i, n_text in enumerate(new_norm):
        for o_i, o_text in enumerate(old_norm):
            if o_i not in used_old and o_text == n_text:
                pairs[n_i] = o_i
                used_old.add(o_i)
                break

    # Then the most similar remaining candidates, highest ratio first. Ties break on position, so the
    # result cannot depend on dictionary ordering.
    candidates = [
        (difflib.SequenceMatcher(None, old_norm[o_i], new_norm[n_i]).ratio(), -n_i, -o_i, n_i, o_i)
        for n_i in range(len(new_norm))
        if n_i not in pairs
        for o_i in range(len(old_norm))
        if o_i not in used_old
    ]
    for ratio, _, _, n_i, o_i in sorted(candidates, reverse=True):
        if ratio < _BULLET_MATCH_RATIO or n_i in pairs or o_i in used_old:
            continue
        pairs[n_i] = o_i
        used_old.add(o_i)

    # Emit in the new side's order — the one a reviewer reads. A removed bullet keeps its old place,
    # ahead of whichever surviving bullet used to follow it, instead of drifting to the end of the list.
    removals = [o_i for o_i in range(len(old_list)) if o_i not in used_old]
    out: list[tuple[Any | None, Any | None]] = []
    emitted: set[int] = set()
    for n_i in range(len(new_list)):
        o_i = pairs.get(n_i)
        if o_i is not None:
            for dropped in removals:
                if dropped < o_i and dropped not in emitted:
                    out.append((old_list[dropped], None))
                    emitted.add(dropped)
        out.append((old_list[o_i] if o_i is not None else None, new_list[n_i]))
    out += [(old_list[o_i], None) for o_i in removals if o_i not in emitted]
    return out


def render_text_html(value: Any, other: Any, side: str, changed_only: bool = False) -> str:
    """One side of the side-by-side diff, with word-level highlights against the other side.

    Reflect the field's real structure: a description_key stored as a "- a\\n- b" markdown string (or a
    JSON list) renders as bullets; genuine prose renders as prose. With `changed_only`, a list field
    (WYSK) shows only the bullets that changed on this side — hiding bullets that are unchanged — so the
    reviewer sees just the relevant points, not the whole list.
    """
    value, other = as_bullets(value), as_bullets(other)
    old, new = (other, value) if side == "new" else (value, other)

    def _one(o: Any, n: Any) -> str:
        return inline_diff_html(str(o or ""), str(n or ""), side=side)

    if isinstance(value, list) or isinstance(other, list):
        value_list = value if isinstance(value, list) else ([value] if value else [])
        other_list = other if isinstance(other, list) else ([other] if other else [])
        # A reorder changes no bullet's text, so membership sees nothing while the lists genuinely differ.
        # Hiding on that basis would print "(no changes here)" for a change the tool itself detected, and
        # the reviewer could sign it off without ever seeing what moved — so fall through to the full,
        # positional list, where the new order is visible.
        reordered = [str(x).strip() for x in value_list if x] != [str(x).strip() for x in other_list if x] and sorted(
            str(x).strip() for x in value_list if x
        ) == sorted(str(x).strip() for x in other_list if x)
        if not reordered:
            # Pair each bullet with its counterpart on the other side, so an edited bullet is diffed
            # against its own earlier wording and only the words that moved light up. Matching by
            # membership instead — "is this exact bullet on the other side?" — could only answer yes or
            # no, so an edited bullet was diffed against the empty string and every word of it read as
            # inserted: a one-sentence addition rendered as a wholly rewritten paragraph, which is the
            # opposite of what a reviewer needs to see.
            old_list, new_list = (other_list, value_list) if side == "new" else (value_list, other_list)
            items = []
            for old_bullet, new_bullet in pair_bullets(old_list, new_list):
                unchanged = _norm_bullet(old_bullet) == _norm_bullet(new_bullet)
                if changed_only and unchanged:
                    continue
                # A pair with nothing on this side is an insertion (or deletion) the other column shows.
                if (new_bullet if side == "new" else old_bullet) is None:
                    continue
                items.append(f"<li>{_one(old_bullet, new_bullet)}</li>")
            if not items:
                return '<div class="mdd-text mdd-empty">(no changes here)</div>'
            return f'<div class="mdd-text"><ul>{"".join(items)}</ul></div>'
        # A reorder: every bullet's text survives, so pairing finds no change at all. Show the full list
        # positionally, which is where the new order is visible.
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


def plain_text_html(value: Any) -> str:
    if isinstance(value, list):
        items = "".join(f"<li>{html.escape(str(v))}</li>" for v in value if v)
        return f'<div class="mdd-text"><ul>{items}</ul></div>'
    if value in (None, ""):
        return '<div class="mdd-text mdd-empty">(empty)</div>'
    return f'<div class="mdd-text">{html.escape(str(value))}</div>'


def orange_banner(html_msg: str) -> None:
    """A theme-safe orange banner (matches the 🟠 'shared indicator metadata' idea). Takes HTML."""
    st.markdown(
        '<div style="background:rgba(232,89,12,0.12);border-left:4px solid #e8590c;'
        f'padding:10px 14px;border-radius:6px;">{html_msg}</div>',
        unsafe_allow_html=True,
    )


# --- Blast radius --------------------------------------------------------------------------------


def view_impact(view: ViewDiff, usage: dict[int, dict[str, list[dict[str, Any]]]]) -> tuple[list, list]:
    """(charts, other_mdims) affected by this view's indicator-layer change; empty if MDim-only.

    The *unfiltered* set, for the surfaces that list charts by name. Anything reporting a count has to
    every chart using the indicator counts.
    """
    if not (view.affects_indicator and view.indicator_id is not None):
        return [], []
    entry = usage.get(view.indicator_id, {})
    return entry.get("charts", []), entry.get("mdims", [])


def impact_counts(view: ViewDiff, usage: dict[int, dict[str, list[dict[str, Any]]]]) -> dict[str, int]:
    """Per-view external-surface counts, for the tree markers.

    Only shown on views that visibly changed, to stay consistent with the View diff page. (A view
    whose indicator changed but whose text is masked by an override is a rare edge case we skip.)
    """
    if not view.changed:
        return {"charts": 0, "mdims": 0}
    charts, mdims = view_impact(view, usage)
    return {"charts": len(charts), "mdims": len(mdims)}


def render_impact(view: ViewDiff, usage: dict[int, dict[str, list[dict[str, Any]]]], unit: str = "view") -> None:
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

    charts, mdims = view_impact(view, usage)
    # The banner states *reach*, so it counts only the charts that can show this change. The popover
    # below lists the full set and flags the rest, so a chart the author expects never simply vanishes.
    n_c, n_m, n_hidden = len(charts), len(mdims), len(charts) - len(charts)
    parts = []
    if n_c:
        parts.append(f"<b>{n_c}</b> chart{'s' if n_c != 1 else ''}")
    if n_m:
        parts.append(f"<b>{n_m}</b> other MDim{'s' if n_m != 1 else ''}")

    if parts:
        banner = (
            "This change is in the <b>shared indicator metadata</b> — it also affects "
            + " and ".join(parts)
            + " that use this indicator."
        )
    elif n_hidden:
        # Not the same thing as "nothing uses this indicator", and the difference is the whole point of
        # the filter: charts do use it, but none of them has a data page to show this field on.
        verb = (
            "combines several indicators, so it renders"
            if n_hidden == 1
            else "combine several indicators, so they render"
        )
        banner = (
            "This change is in the <b>shared indicator metadata</b>, but the "
            f"<b>{n_hidden}</b> chart{'s' if n_hidden != 1 else ''} using this indicator {verb} no data "
            "page — so no reader sees this text outside this MDim."
        )
    else:
        banner = (
            "This change is in the <b>shared indicator metadata</b>, but no published charts or other "
            "MDims currently use this indicator — so nothing else is affected."
        )

    # The button describes what the list holds (every chart), not the reach the banner just stated.
    n_listed = len(charts)
    listed = [f"{n_listed} chart{'s' if n_listed != 1 else ''}"] if n_listed else []
    if n_m:
        listed.append(f"{n_m} other MDim{'s' if n_m != 1 else ''}")

    # Banner + a peek popover (a clean window) to see the affected charts / MDims on demand.
    col_msg, col_btn = st.columns([5, 2], vertical_alignment="center")
    with col_msg:
        orange_banner(banner)
    with col_btn:
        if listed:
            with st.popover(f"📊 Show {' · '.join(listed)}", use_container_width=True):
                render_affected_lists(view, charts, mdims)


def render_affected_lists(view: ViewDiff, charts: list[dict], mdims: list[dict]) -> None:
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


def render_chart_list(charts: list[dict[str, Any]], verb: str = "render this text", fields: Any = None) -> None:
    """Name the charts a change lands on, in two sections — a count is not something an author can check.

    The split is where the change meets a reader: a chart with a data page lays the text out on the page,
    while one combining several indicators keeps it behind "Learn more about this data", under the
    indicator's own entry. Both are affected — hence both listed and both counted — but they are not the
    same thing to a reviewer deciding how much care an edit needs.
    """
    if not charts:
        st.caption("No published chart uses these indicators.")
        return

    on_page, behind_drawer = split_by_prominence(charts, fields)

    def _bullets(group: list[dict[str, Any]]) -> str:
        rows = []
        for c in group:
            slug = c.get("slug") or f"chart {c.get('chartId')}"
            rows.append(f"- [`{slug}`]({SOURCE.site}/grapher/{slug})")
        return "\n".join(rows)

    if on_page:
        st.markdown(f"**{len(on_page)} data page{'s' if len(on_page) != 1 else ''} affected** — {verb}:")
        st.markdown(_bullets(on_page))
    if behind_drawer:
        st.markdown(f"**{len(behind_drawer)} via *Learn more about this data***")
        st.caption(
            "These combine several indicators, so they have no data page: their readers reach the text "
            "under the indicator's own entry in the sources drawer."
        )
        st.markdown(_bullets(behind_drawer))


def render_author_scope(
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
    # "Apply to all" is a decision about who sees the change, so it has to be offered against the reach
    # readers actually get — the same count the Charts section and the MDim cards report.
    n_c = len(imp.get("charts", []))
    n_m = len(imp.get("mdims", []))
    reach = f"{n_c} chart{'s' if n_c != 1 else ''}"
    if n_m:
        reach += f" · {n_m} other MDim{'s' if n_m != 1 else ''}"

    sk = f"scope::{key}"
    if sk not in st.session_state:
        # Default to the conservative "only this view" unless the author explicitly chose "apply to all".
        st.session_state[sk] = "all" if scopes.get(key) == "all" else "scoped"

    def _save() -> None:
        set_scope(source_engine, catalog_path, key, st.session_state.get(sk, "scoped"), reviewer())

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
        for c in charts_in_reading_order(imp.get("charts", []), {field_name}):
            slug = c.get("slug") or f"chart {c.get('chartId')}"
            flag = "" if c.get("has_data_page", True) else " — via *Learn more about this data* (no data page)"
            rows.append(f"- [`{slug}`]({SOURCE.site}/grapher/{slug}){flag}")
        for m in sorted(imp.get("mdims", []), key=lambda m: str(m.get("slug") or "")):
            rows.append(f"- MDim `{m.get('slug') or m.get('catalogPath')}`")
        st.warning(f"**This will change {reach}.** These are the surfaces that get the new text:\n" + "\n".join(rows))


def st_origin_caption(catalog_paths: set[str] | list[str], attribution: dict[str, str]) -> None:
    """Say where a difference came from — and say nothing at all when it is plainly this branch's.

    A caption on every change is a caption nobody reads. The previous version hedged whenever master had
    also touched the dataset, which on a normal branch is most of them: it fired on all ten changes of a
    PR whose ten changes were all its own. Now the text is compared against master's own environment, so
    "this is yours" is a real verdict and passes in silence; only the cases that need action speak.
    """
    from apps.wizard.app_pages.metadata_diff.discovery import MASTER, STALE, UNKNOWN

    origins = {attribution.get(p) for p in catalog_paths}
    if STALE in origins:
        st.caption(
            "🚧 **This server holds an older build of this dataset than "
            f"{BASELINE_NAME}**, so this diff reads backwards — the “new” side is *older* text. Rebuild the "
            "dataset here before reviewing it (see the banner at the top of the page)."
        )
    elif MASTER in origins:
        st.caption(
            f"🕓 This text is **master's, not this branch's** — it matches master's own server, and "
            f"{BASELINE_NAME} simply has not rebuilt the dataset yet. Nothing to review here."
        )
    elif UNKNOWN in origins:
        st.caption(
            "❔ Could not reach master's server to check whether this text is yours or an edit master made "
            f"that {BASELINE_NAME} has not rebuilt yet. Compare it against the edits you actually made."
        )


def _stamp(value: Any) -> str:
    """A build time as `YYYY-MM-DD HH:MM`, tolerating whatever the DB hands back.

    These come out of `datasets.metadataEditedAt`, so they are normally datetimes — but this banner is
    the one thing standing between a reviewer and a silently inverted diff, and it is not worth crashing
    the page over an unexpected NaT or string.
    """
    try:
        return f"{value:%Y-%m-%d %H:%M}"
    except (TypeError, ValueError):
        return str(value)


def st_stale_server_banner(stale: dict[str, tuple[Any, Any]]) -> None:
    """Warn that this server, not the branch, is what the numbers below are wrong about.

    A staging build only rebuilds steps that differ from master, so a dataset drops out of the build the
    moment a branch's edit to it is reverted — and the server then keeps serving the old build, edit
    included, indefinitely. Every diff involving it is inverted: the branch appears to have written text
    it removed, or to have reverted text it never touched. It is silent, it survives further pushes, and
    on a real branch it accounted for 33 of 44 reported changes before anyone noticed by hand.

    So this leads with the remedy: the exact commands that make the server tell the truth again.
    """
    if not stale:
        return
    commands = "\n".join(
        f"ssh owid@{SOURCE.name} 'cd etl && .venv/bin/etlr grapher://grapher/{d} --grapher'" for d in sorted(stale)
    )
    rows = "\n".join(
        f"- `{d}` — here **{_stamp(here)}**, {BASELINE_NAME} **{_stamp(there)}**"
        for d, (here, there) in sorted(stale.items())
    )
    st.error(
        f"🚧 **This staging server is behind {BASELINE_NAME} on "
        f"{len(stale)} dataset{'s' if len(stale) != 1 else ''}** — every difference involving them reads "
        "backwards, showing their *older* text as this branch's change.\n\n"
        f"{rows}\n\n"
        "This happens on its own: the staging build only rebuilds steps that differ from master, so a "
        "dataset stops being rebuilt as soon as your edit to it is reverted, and the server keeps the old "
        "build. Rebuild them here, then reload:"
    )
    st.code(commands, language="bash")


def markdown_output(text: str, filename: str, key: str) -> None:
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
