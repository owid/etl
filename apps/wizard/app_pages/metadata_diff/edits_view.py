"""One card per authored edit — the By-edit layout of every section.

`group_changes` keys a card on the exact text. On an explorer whose 348 views each word their subtitle a
little differently ("Mean income per day…", "…per month…") that is 348 cards for one reworded sentence,
and the reader is asked to judge the same edit 348 times. Blast radius already groups the other way — by
the words that were inserted and deleted, `group_by_edit` — so the By-edit layouts use that, scoped to
their own surface: one card, saying how many texts it renders into and where those land.

Each card carries a Reviewed tick and a note keyed to the *edit*, on its own surface, so a section can be
reviewed edit by edit as well as view by view and the two records never overwrite each other. Under the
diff, folded, is everywhere the edit lands on this surface — each chart or view a link into the
view-by-view page for it, because the card is where the edit is judged and that page is where one place
it lands is inspected and ticked.
"""

import html
from typing import Any
from urllib.parse import urlencode

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.app_pages.metadata_diff import cached, view_nav
from apps.wizard.app_pages.metadata_diff.core import (
    ViewDiff,
    diff_window_html,
    field_label,
    view_label,
    view_url,
    where_note,
)
from apps.wizard.app_pages.metadata_diff.data import load_reviews
from apps.wizard.app_pages.metadata_diff.discovery import EditGroup, edit_fields, edit_key, edits_for
from apps.wizard.app_pages.metadata_diff.render import render_chart_list, st_note
from apps.wizard.app_pages.metadata_diff.review_state import resolve_item_mark, st_review_strip, surface_key

# Cards drawn before the list says how many more there are. Sixty edits is not a branch anyone reviews
# edit by edit; the number exists so a pathological branch cannot render for a minute.
MAX_CARDS = 60
# Views listed under one MDim or explorer before the rest are handed to View by view, whose ⚡ jump
# enumerates them all. A hundred links is already past what anyone reads; four hundred is a wall.
MAX_LISTED = 100

CONTEXT_CSS = """
<style>
.mdd-context {{ color: #555; }}
.mdd-context-label {{ color: #999; font-size: 12px; text-transform: uppercase;
  letter-spacing: .04em; margin-right: 6px; }}
</style>
""".replace("{{", "{").replace("}}", "}")


def st_edit_cards(source_engine: Engine, target_engine: Engine, summary: Any, section: str) -> None:
    """The edits that land on one section, one card each: tick and note, the edit, where it goes."""
    st.markdown(CONTEXT_CSS, unsafe_allow_html=True)
    edits = edits_for(summary, section)
    if not edits:
        st.caption("No edit made on this branch lands on this surface.")
        return

    if section == "mdims" and not getattr(summary, "mdims_resolved", True):
        st.warning(
            "Too many changed MDims to diff view by view, so these cards are incomplete — the same ceiling "
            "the MDims badge reports."
        )

    surface = surface_key("item", f"edit:{section}")
    # One read for every card on the page: the tick and the note of each card resolve against it.
    recorded = load_reviews(source_engine, surface)

    n_texts = sum(e.n_texts for e in edits)
    st.markdown(
        f"**{len(edits)} edit{'s' if len(edits) != 1 else ''}** authored on this branch land here, rendering "
        f"**{n_texts} text{'s' if n_texts != 1 else ''}** between them — one card per edit, however many "
        "texts it renders into."
    )

    for edit in edits[:MAX_CARDS]:
        reach = _reach_line(edit, section)
        with st.container(border=True):
            st.markdown(
                f"**{field_label(edit.field)}** :small[:gray[{edit.n_texts} rendered "
                f"text{'s' if edit.n_texts != 1 else ''} · {reach}]]"
            )
            mark = resolve_item_mark(recorded, surface, edit_key(edit), edit_fields(edit))
            st_review_strip(source_engine, surface, mark)
            st_edit_body(edit)
            paths = {p for change in edit.changes for p in (change.catalog_paths or set())}
            note = where_note(edit.field, paths, edit.n_texts)
            if note:
                st_note(note)
            # Folded: the card is for judging the edit, and this is for getting to one place it lands.
            with st.expander(f"Where it lands · {reach}", expanded=False):
                _st_landing(source_engine, target_engine, edit, section)

    if len(edits) > MAX_CARDS:
        st.caption(f"Showing the first {MAX_CARDS} of {len(edits)} edits. Blast radius lists them all.")


def st_edit_body(group: EditGroup) -> None:
    """The edit itself: the words as one diff line, then the same edit in context.

    A rewording is one change, so it reads as one line — `old` struck through, `new` highlighted — rather
    than as an "added" statement and a "removed" statement that the reader has to pair up. The context
    line below shows where it lands, windowed on the change inside the first text carrying it: every text
    in the group shares this edit by construction, so one is a fair exemplar, labelled as one.
    """
    deleted = f'<del class="mdd-del">{html.escape(group.deleted)}</del>' if group.deleted else ""
    inserted = f'<ins class="mdd-ins">{html.escape(group.inserted)}</ins>' if group.inserted else ""

    if deleted and inserted:
        st.markdown(f'<div class="mdd-diff">{deleted} &#8594; {inserted}</div>', unsafe_allow_html=True)
    elif inserted:
        st.markdown(f'<div class="mdd-diff">added {inserted}</div>', unsafe_allow_html=True)
    elif deleted:
        st.markdown(f'<div class="mdd-diff">removed {deleted}</div>', unsafe_allow_html=True)
    else:
        # No words either way — a whitespace-only edit. A reordered list is not this case: its moved
        # bullets do read as an insertion and a deletion.
        st.caption("No words added or removed — whitespace only. The texts below show both sides.")

    if group.changes:
        exemplar = group.changes[0]
        where = "in context" if group.n_texts == 1 else f"in context, in the first of {group.n_texts} texts"
        st.markdown(
            f'<div class="mdd-diff mdd-context"><span class="mdd-context-label">{where}</span> '
            f"{diff_window_html(exemplar.old, exemplar.new, max_chars=300)}</div>",
            unsafe_allow_html=True,
        )


def _st_landing(source_engine: Engine, target_engine: Engine, edit: EditGroup, section: str) -> None:
    """Everywhere one edit lands on this surface, each place a link into its own review page.

    Charts reuse the chart list every other card uses, grouped by how the reader meets the text. MDims
    and explorers get one block each, opening with a coverage line per dimension — whether the edit hits
    every metric or only the mean is the question the list of views answers badly — and then the views,
    capped, with the rest one click away in View by view.
    """
    if section == "charts":
        charts, drafts = _chart_entries(edit)
        render_chart_list(charts, verb="render this edit", fields={edit.field}, drafts=drafts)
        return

    st.caption("A name opens that view in View by view, here. The ↗ opens the view itself.")
    if section == "mdims":
        df = cached.mdim_changes(source_engine, target_engine)
        for entry in _mdim_entries(edit):
            catalog_path = entry["catalogPath"]
            row = df.loc[catalog_path] if catalog_path in df.index else None
            cache_key = f"{row.get('configMd5_source')}-{row.get('configMd5_target')}" if row is not None else ""
            try:
                _title, dimensions, _diffs = cached.mdim_view_diffs(
                    catalog_path, source_engine, target_engine, cache_key=cache_key
                )
            except Exception:  # noqa: BLE001 — the list still works on slugs when the config cannot be read
                dimensions = []
            badge = " :orange-badge[📝 unpublished]" if entry["draft"] else ""
            st.markdown(f"**{entry['title']}**{badge} :small[:gray[`{catalog_path}` · {_views(entry['n'])}]]")
            if entry["views"]:
                st.caption(coverage_line(entry["views"], dimensions))
            live_slug = None if entry["draft"] or not entry["slug"] else entry["slug"]
            items = [
                (
                    view_label(ViewDiff(dimensions=dims), dimensions),
                    view_nav.mdim_view_link(catalog_path, dims),
                    view_url(SOURCE, catalog_path, live_slug, dims),
                )
                for dims in entry["views"]
            ]
            _st_view_list(items, entry["n"], view_nav.mdim_view_link(catalog_path), "this MDim")
        return

    for entry in _explorer_entries(edit):
        slug = entry["slug"]
        views = [ViewDiff(dimensions=dims) for dims in entry["views"]]
        dimensions = view_nav.dimensions_from_views(views)
        st.markdown(f"**{slug}** :small[:gray[{_views(entry['n'])}]]")
        if entry["views"]:
            st.caption(coverage_line(entry["views"], dimensions, known_universe=False))
        items = [
            (
                " · ".join(str(v) for v in dims.values()) or "(view)",
                view_nav.explorer_view_link(slug, dims),
                f"{SOURCE.site}/explorers/{slug}?{urlencode(dims)}",
            )
            for dims in entry["views"]
        ]
        _st_view_list(items, entry["n"], view_nav.explorer_view_link(slug), "this explorer")


def _st_view_list(items: list[tuple[str, str, str]], total: int, browser_href: str, what: str) -> None:
    """Views as links: the name opens the view in View by view, in this tab; the ↗ opens the live view.

    HTML because of `target`: Streamlit renders every markdown link with `target="_blank"`, and a review
    page opening in another tab is indistinguishable from a link that does nothing.
    """
    if not items:
        st.caption(f"{_views(total)} — their dimensions were not recorded, so they cannot be listed here.")
        return
    rows = [
        f'<li><a href="{html.escape(review)}" target="_self">{html.escape(label)}</a> '
        f'<a href="{html.escape(live)}" target="_blank" rel="noopener" title="Open the view itself">↗</a></li>'
        for label, review, live in items[:MAX_LISTED]
    ]
    st.markdown(f'<ul class="mdd-chart-list">{"".join(rows)}</ul>', unsafe_allow_html=True)
    if len(items) > MAX_LISTED:
        st.markdown(
            f'<span class="mdd-note">… and {len(items) - MAX_LISTED} more. '
            f'<a href="{html.escape(browser_href)}" target="_self">Open View by view for {what}</a> — '
            "its ⚡ jump lists every changed view.</span>",
            unsafe_allow_html=True,
        )


def coverage_line(views: list[dict[str, str]], dimensions: list[dict[str, Any]], known_universe: bool = True) -> str:
    """Per dimension, which choices the edit reaches: `metric: mean, median (2 of 5) · period: all 3`.

    The list of views answers "which views" and hides "how much of the MDim" — fifty-one links do not say
    whether every metric is covered or only two. This says it, in the MDim's own choice names.
    `known_universe` is False for an explorer, whose dimensions are inferred from the affected views
    themselves, so a total would only ever equal the count.
    """
    parts = []
    for dim in dimensions:
        slug = str(dim["slug"])
        choices = dim.get("choices") or []
        names = {c["slug"]: (c.get("name") or c["slug"]) for c in choices}
        hit: list[str] = []
        for view in views:
            choice = view.get(slug)
            if choice is not None and choice not in hit:
                hit.append(str(choice))
        if not hit:
            continue
        label = str(dim.get("name") or slug)
        if known_universe and choices and len(choices) == 1:
            parts.append(f"{label}: {names.get(hit[0], hit[0])}")
        elif known_universe and choices and len(hit) >= len(choices):
            parts.append(f"{label}: all {len(choices)}")
        else:
            shown = ", ".join(str(names.get(c, c)) for c in hit[:5]) + (" …" if len(hit) > 5 else "")
            count = f"{len(hit)} of {len(choices)}" if known_universe and choices else str(len(hit))
            parts.append(f"{label}: {shown} ({count})")
    return " · ".join(parts)


def _chart_entries(edit: EditGroup) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(published charts, drafts) this edit lands on, deduped across its texts."""
    charts = {c["chartId"]: c for change in edit.changes for c in change.charts}
    drafts = {c["chartId"]: c for change in edit.changes for c in change.draft_charts}
    return list(charts.values()), list(drafts.values())


def _mdim_entries(edit: EditGroup) -> list[dict[str, Any]]:
    """One entry per MDim this edit reaches, by title: its views deduped across the edit's texts.

    Where a reach entry carries only a count and no dimensions, the largest count any single text reported
    stands in: the tightest lower bound available, exact whenever the texts land on the same views.
    """
    per: dict[str, dict[str, Any]] = {}
    for change in edit.changes:
        for mdim in change.mdims:
            entry = per.setdefault(
                str(mdim["catalogPath"]),
                {
                    "catalogPath": str(mdim["catalogPath"]),
                    "title": str(mdim.get("title") or mdim["catalogPath"]),
                    "slug": str(mdim.get("slug") or ""),
                    "draft": bool(mdim.get("is_draft")),
                    "seen": {},
                    "counted": 0,
                },
            )
            _collect(entry["seen"], mdim.get("views") or [])
            entry["counted"] = max(entry["counted"], int(mdim.get("n_views") or 0))
    out = []
    for entry in sorted(per.values(), key=lambda e: e["title"]):
        entry["views"] = list(entry.pop("seen").values())
        entry["n"] = len(entry["views"]) or entry["counted"]
        out.append(entry)
    return out


def _explorer_entries(edit: EditGroup) -> list[dict[str, Any]]:
    """One entry per explorer this edit reaches, by slug: its views deduped across the edit's texts."""
    per: dict[str, dict[str, Any]] = {}
    for change in edit.changes:
        for explorer in change.explorers:
            slug = str(explorer["slug"])
            entry = per.setdefault(slug, {"slug": slug, "seen": {}, "counted": 0})
            _collect(entry["seen"], explorer.get("views") or [])
            entry["counted"] = max(entry["counted"], int(explorer.get("n_views") or 0))
    out = []
    for slug in sorted(per):
        entry = per[slug]
        entry["views"] = list(entry.pop("seen").values())
        entry["n"] = len(entry["views"]) or entry["counted"]
        out.append(entry)
    return out


def _collect(seen: dict[tuple, dict[str, str]], views: list[dict[str, str]]) -> None:
    """Dedupe views on their dimensions while keeping each view's own dimension order.

    The order is the label: an explorer view is named by its values in the order the explorer lists them,
    and View by view names it the same way. Sorting the keys to dedupe renamed every view in this list, so a
    link's text and the page it opened disagreed about what the view was called.
    """
    for view in views:
        key = tuple(sorted(view.items()))
        if key not in seen:
            seen[key] = dict(view)


def _views(n: int) -> str:
    return f"{n} view{'s' if n != 1 else ''}"


def _reach_line(edit: EditGroup, section: str) -> str:
    """Where this edit lands *on this surface*, deduped across its texts: views per MDim, per explorer, or charts."""
    if section == "mdims":
        parts = [
            f"{_views(e['n'])} in {e['title']}{' (unpublished)' if e['draft'] else ''}" for e in _mdim_entries(edit)
        ]
        return "; ".join(parts) or "no MDim view"

    if section == "explorers":
        parts = [f"{_views(e['n'])} in {e['slug']}" for e in _explorer_entries(edit)]
        return "; ".join(parts) or "no explorer view"

    charts, drafts = _chart_entries(edit)
    on_page = sum(1 for c in charts if c.get("has_data_page", True))
    drawer = len(charts) - on_page
    parts = []
    if on_page:
        parts.append(f"{on_page} on their data page")
    if drawer:
        parts.append(f"{drawer} via *Learn more about this data*")
    if drafts:
        parts.append(f"{len(drafts)} unpublished")
    return f"{len(charts)} chart{'s' if len(charts) != 1 else ''}" + (f" — {', '.join(parts)}" if parts else "")
