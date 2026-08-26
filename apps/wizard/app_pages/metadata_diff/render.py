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
from collections.abc import Iterable
from typing import Any

import streamlit as st
import streamlit.components.v1 as components

from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
from apps.wizard.app_pages.metadata_diff.cached import clear_discovery_caches
from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    CHART_FIELDS,
    DEFAULT_LAYOUT,
    DEFAULT_SECTION,
    LAYOUT_QUERY_KEY,
    LAYOUTS,
    METADATA_FIELDS,
    SECTIONS,
    ViewDiff,
    as_bullets,
    coerce_layout,
    coerce_section,
    diff_preview_html,
    inline_diff_html,
    section_label,
    split_by_prominence,
)
from apps.wizard.app_pages.metadata_diff.tree import render_affected_charts_html
from apps.wizard.utils.components import url_persist

# Display order of the diffed fields: indicator metadata first, then the chart's own text.
FIELD_ORDER = list(METADATA_FIELDS) + [CHART_FIELD_PREFIX + f for f in CHART_FIELDS]

# Name of the baseline, as shown in the UI ("production" / "staging-site-master").
BASELINE_NAME = TARGET.name

# The URL key Chart Diff uses for the same control, so a link keeps its section across the two pages.
SECTION_QUERY_KEY = "diff-type"
SECTION_STATE_KEY = "metadata-diff-section"
SECTION_NAV_KEY = "metadata-diff-nav"

# The switcher rides along as you scroll: the lists below it run to hundreds of rows, and switching
# section (or reading the review counter) should not mean scrolling back to the top to find the buttons.
#
# The positioning goes on the `stLayoutWrapper` Streamlit puts around the container, not on the container:
# a sticky box can only travel inside its containing block, and that wrapper is sized to its content, so
# sticking the container itself has nowhere to travel (measured: a 47px bar in a 47px wrapper). The
# wrapper's parent is the full-height page block. `top` clears the app header, which overlays the top of
# the scrolling area. If a Streamlit release renames the wrapper the rule stops matching and the bar
# simply scrolls with the page again — the styling below it stands on its own.
SECTION_NAV_CSS = f"""
<style>
div[data-testid="stLayoutWrapper"]:has(> div.st-key-{SECTION_NAV_KEY}) {{
    position: sticky;
    /* Clears the app header, which is opaque and overlays the top 3.75rem of the scrolling area. */
    top: 3.75rem;
    z-index: 100;
}}
div.st-key-{SECTION_NAV_KEY} {{
    /* Opaque, or the rows sliding underneath read through the bar. The wizard theme pins base="light"
       (.streamlit/config.toml), so the page background is white. */
    background: #ffffff;
    padding: 0.4rem 0 0.5rem 0;
    border-bottom: 1px solid rgba(49, 51, 63, 0.15);
}}
</style>
"""

DIFF_CSS = """
<style>
/* An explanatory note: caption-sized, but in the theme's own text colour rather than caption grey.
   These lines say why a card groups what it groups, so they have to be read, not skimmed past. Colour is
   inherited on purpose — it follows the light/dark theme without hardcoding either. */
.mdd-note { font-size: 0.875rem; line-height: 1.5; }
.mdd-note code { font-size: 0.8rem; padding: 1px 4px; border-radius: 4px; background: #f1f3f5; }
.mdd-text { border: 1px solid #e0e0e0; border-radius: 6px; padding: 10px 14px; line-height: 1.5;
            background: #fff; }
.mdd-text ul { margin: 0 0 0 18px; padding: 0; }
.mdd-text li { margin-bottom: 8px; }
/* Both containers: `.mdd-text` is the bordered before/after box, `.mdd-diff` an inline diff line (the
   by-edit cards). Without the second selector an <ins>/<del> there fell back to the browser's underline
   and strikethrough — the change was in the markup and invisible as a diff. */
.mdd-text del.mdd-del, .mdd-diff del.mdd-del { background: #ffe3e3; color: #c92a2a; text-decoration: line-through; }
.mdd-text ins.mdd-ins, .mdd-diff ins.mdd-ins { background: #d3f9d8; color: #2b8a3e; text-decoration: none; }
.mdd-empty { color: #999; font-style: italic; }
.mdd-slot { font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em; color: #868e96;
            margin: 10px 0 2px; }
.mdd-slot-unchanged { font-size: 0.78rem; color: #adb5bd; border-left: 3px solid #e9ecef;
                      padding-left: 8px; margin: 6px 0; }
/* Chart lists are HTML, not markdown, so they need the list styling markdown would have given them. */
.mdd-chart-list { margin: 0 0 0 18px; padding: 0; font-size: 0.9rem; }
.mdd-chart-list li { margin-bottom: 2px; }
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
    """URL of a view in a given environment (site if published there, admin preview otherwise).

    Pass `published_slug=None` for an MDim that is not published in that environment even if it already
    has a slug: `/grapher/<slug>` 404s until publication, while the admin preview renders the page and
    applies the dimensions from the query string (both verified against a staging server).
    """
    params = urllib.parse.urlencode(dims)
    if published_slug:
        return f"{env.site}/grapher/{published_slug}?{params}"
    return f"{env.admin_site}/grapher/{urllib.parse.quote(catalog_path, safe='')}/?{params}"


def st_layout_switcher(items_label: str, items_help: str) -> str:
    """The per-section "items or changes" control, with its choice kept in the URL.

    One key across the three sections on purpose: whichever section you switch, the others follow, because
    the choice is about how you are reading this branch rather than about one surface. That shared key is
    also what made the labels unstable — each section words the item option differently — so both the URL
    and the held value are read back through `coerce_layout` *before* the widget exists. Without that,
    `url_persist`'s strict check sees the label a previous section wrote and raises on load.
    """
    for store in (st.query_params, st.session_state):
        held = store.get(LAYOUT_QUERY_KEY)
        if held is not None and held not in LAYOUTS:
            store[LAYOUT_QUERY_KEY] = coerce_layout(held)

    layout = url_persist(st.segmented_control)(
        label="Layout",
        options=list(LAYOUTS),
        format_func=lambda v: {"items": items_label, "changes": "🧬 By change"}[v],
        key=LAYOUT_QUERY_KEY,
        value=DEFAULT_LAYOUT,
        label_visibility="collapsed",
    )
    layout = coerce_layout(layout)
    st.caption(
        f"{items_help} · **By change** groups the same edits — one reworded sentence listed once, with "
        "everywhere it lands underneath."
    )
    return layout


def chart_review_url(slug: str) -> str:
    """This tool's own full review of one chart — every field of it this branch changed.

    Relative on purpose. An absolute URL has to name a host, and the only host this module knows is the
    *staging* server's wizard (`SOURCE.wizard_url`) — so on a local instance every one of these links left
    the app you were using. A query-only href keeps whatever host you are on.

    Safe to render from Streamlit markdown, which is not sandboxed; a component iframe would resolve it
    against its own srcdoc origin instead, which is why the grid builds absolute links.
    """
    return "?" + urllib.parse.urlencode({"diff-type": "charts", "chart": slug})


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


def render_chart_list(
    charts: list[dict[str, Any]],
    verb: str = "render this text",
    fields: Any = None,
    drafts: list[dict[str, Any]] | None = None,
) -> None:
    """Name the charts a change lands on, in sections — a count is not something an author can check.

    The first split is where the change meets a reader: a chart with a data page lays the text out on the
    page, while one combining several indicators keeps it behind "Learn more about this data", under the
    indicator's own entry. Both are affected — hence both listed and both counted — but they are not the
    same thing to a reviewer deciding how much care an edit needs.

    `drafts` are unpublished charts, listed last and never counted with the rest: no reader can open one,
    so they are a different question — is this edit what the draft's author wants — and their links go to
    the admin editor, since `/grapher/<slug>` serves nothing until the chart is published.
    """
    drafts = drafts or []
    if not charts and not drafts:
        st.caption("No chart uses these indicators.")
        return

    on_page, behind_drawer = split_by_prominence(charts, fields)

    def _bullets(group: list[dict[str, Any]]) -> str:
        """The name opens this tool's review of that chart, in this tab; the arrow opens the chart itself.

        HTML rather than markdown because of `target`: Streamlit renders every markdown link with
        `target="_blank"`, so the review opened in a new tab and the tab the reader clicked from did not
        change — indistinguishable from a link that does nothing. The ↗ keeps `_blank`, since that one is
        genuinely leaving the tool.
        """
        rows = []
        for c in group:
            slug = str(c.get("slug") or "")
            label = html.escape(slug or f"chart {c.get('chartId')}")
            if not slug:
                rows.append(f"<li><code>{label}</code></li>")
                continue
            rows.append(
                f'<li><a href="{html.escape(chart_review_url(slug))}" target="_self">'
                f"<code>{label}</code></a> "
                f'<a href="{SOURCE.site}/grapher/{html.escape(slug)}" target="_blank" rel="noopener"'
                f' title="Open the chart itself">↗</a></li>'
            )
        return f'<ul class="mdd-chart-list">{"".join(rows)}</ul>'

    if on_page:
        st.caption("A name opens that chart's own review, here. The ↗ opens the chart itself.")
        st.markdown(f"**{len(on_page)} data page{'s' if len(on_page) != 1 else ''} affected** — {verb}:")
        st.markdown(_bullets(on_page), unsafe_allow_html=True)
    if behind_drawer:
        st.markdown(f"**{len(behind_drawer)} via *Learn more about this data***")
        st.caption(
            "These combine several indicators, so they have no data page: their readers reach the text "
            "under the indicator's own entry in the sources drawer."
        )
        st.markdown(_bullets(behind_drawer), unsafe_allow_html=True)
    if drafts:
        st.markdown(f"**{len(drafts)} unpublished draft{'s' if len(drafts) != 1 else ''}**")
        st.caption(
            "Not counted above: no reader can open a draft. Listed because the edit is in them, and "
            "whoever is building them may not expect it. Links open the chart in the admin."
        )
        rows = []
        for c in sorted(drafts, key=lambda c: str(c.get("slug") or "")):
            chart_id = c.get("chartId")
            label = c.get("slug") or f"chart {chart_id}"
            rows.append(f"- [`{label}`]({SOURCE.chart_admin_site(chart_id)})")  # ty: ignore
        st.markdown("\n".join(rows))


def st_note(text: str) -> None:
    """Render an explanatory note: caption-sized, body-text coloured, HTML rather than markdown.

    Callers pass HTML (`<b>`, `<code>`) because markdown is not parsed inside a raw HTML wrapper, and the
    wrapper is what carries the colour. Kept as one helper so every note in the tool reads the same.
    """
    st.markdown(f'<span class="mdd-note">{text}</span>', unsafe_allow_html=True)


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
        st_note(
            "🚧 <b>This server holds an older build of this dataset than "
            f"{BASELINE_NAME}</b>, so this diff reads backwards — the “new” side is <i>older</i> text. "
            "Rebuild the dataset here before reviewing it (see the banner at the top of the page)."
        )
    elif MASTER in origins:
        st_note(
            "🕓 This text is <b>master's, not this branch's</b> — it matches master's own server, and "
            f"{BASELINE_NAME} simply has not rebuilt the dataset yet. Nothing to review here."
        )
    elif UNKNOWN in origins:
        st_note(
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


def _dead_section_css(options: list[str], dead: set[str]) -> str:
    """Grey out and disable the given sections, by their position on the bar.

    `st.segmented_control` takes no per-option `disabled`, and its buttons carry no attribute naming the
    option they stand for — so position is what there is to select on. Kept honest by deriving the indices
    from the same list the control was given, in the same order.
    """
    if not dead:
        return ""
    picks = ",\n".join(
        f'div.st-key-{SECTION_NAV_KEY} [data-testid="stButtonGroup"] > div > button:nth-child({i + 1})'
        for i, section in enumerate(options)
        if section in dead
    )
    return f"<style>\n{picks} {{ opacity: .4; pointer-events: none; cursor: default; }}\n</style>"


def st_section_switcher(progress: dict[str, tuple[int, int]], empty: Iterable[str] = ()) -> str:
    """The Charts / MDims / Explorers control, with its selection kept in the URL.

    `empty` names the sections with nothing in them (see `empty_sections`): they stay on the bar, showing
    their `(0)`, but go grey and stop taking clicks. A section is never greyed while it is the one being
    shown — arriving on it by link is allowed, and a greyed-out current section would just look broken.

    Hand-rolled rather than `url_persist`ed because the labels carry review progress and therefore change
    as you review: `st.segmented_control` round-trips its value *as the label*, so a tick leaves the
    browser holding a label that no longer exists, and Streamlit hands that label back in place of the
    option. Coercing on the way in keeps the selection where the reviewer put it, and only ever writes a
    section key to the URL — which Chart Diff reads too, and validates strictly.
    """
    options = list(SECTIONS)
    from_url = coerce_section(st.query_params.get(SECTION_QUERY_KEY))
    st.session_state[SECTION_STATE_KEY] = coerce_section(st.session_state.get(SECTION_STATE_KEY), from_url)

    # Idempotent, and emitted on every rerun so the bar survives a rerun that starts mid-page.
    st.markdown(SECTION_NAV_CSS, unsafe_allow_html=True)
    with st.container(border=False, key=SECTION_NAV_KEY):
        col_sections, col_refresh = st.columns([6, 1], vertical_alignment="center")
        with col_sections:
            selected = coerce_section(
                st.segmented_control(
                    label="Section",
                    options=options,
                    format_func=lambda s: section_label(s, progress),
                    key=SECTION_STATE_KEY,
                    label_visibility="collapsed",
                ),
                from_url,
            )
            # Greying is CSS on the nth button, so it has to be emitted after the control exists and to
            # skip whichever section is currently open.
            st.markdown(_dead_section_css(options, set(empty) - {selected}), unsafe_allow_html=True)
        with col_refresh:
            # Rides in the sticky bar so it is reachable from anywhere in a long list.
            st.button(
                ":material/refresh: Re-read",
                key="mdd-refresh",
                on_click=clear_discovery_caches,
                help=(
                    "Read both servers again. The page holds its reading for 30 minutes, so use this "
                    "after rebuilding a step — or after `etlr grapher://… --grapher` on this branch."
                ),
                width="stretch",
            )

    # Keep the default out of the URL, as url_persist does, so a plain link stays plain. Written only on
    # a real change, so a run that touches nothing leaves the URL alone.
    if selected == DEFAULT_SECTION:
        st.query_params.pop(SECTION_QUERY_KEY, None)
    elif st.query_params.get(SECTION_QUERY_KEY) != selected:
        st.query_params[SECTION_QUERY_KEY] = selected
    return selected
