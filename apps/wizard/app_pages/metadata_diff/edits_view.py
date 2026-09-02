"""One card per authored edit — the By-edit layout of every section.

`group_changes` keys a card on the exact text. On an explorer whose 348 views each word their subtitle a
little differently ("Mean income per day…", "…per month…") that is 348 cards for one reworded sentence,
and the reader is asked to judge the same edit 348 times. Blast radius already groups the other way — by
the words that were inserted and deleted, `group_by_edit` — so the By-edit layouts use that, scoped to
their own surface: one card, saying how many texts it renders into and where those land.

Each card carries a Reviewed tick and a note keyed to the *edit*, on its own surface, so a section can be
reviewed edit by edit as well as view by view and the two records never overwrite each other.
"""

import html
from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff.core import diff_window_html, field_label, where_note
from apps.wizard.app_pages.metadata_diff.data import load_reviews
from apps.wizard.app_pages.metadata_diff.discovery import EditGroup, edit_fields, edit_key, edits_for
from apps.wizard.app_pages.metadata_diff.render import st_note
from apps.wizard.app_pages.metadata_diff.review_state import resolve_item_mark, st_review_strip, surface_key

# Cards drawn before the list says how many more there are. Sixty edits is not a branch anyone reviews
# edit by edit; the number exists so a pathological branch cannot render for a minute.
MAX_CARDS = 60

CONTEXT_CSS = """
<style>
.mdd-context {{ color: #555; }}
.mdd-context-label {{ color: #999; font-size: 12px; text-transform: uppercase;
  letter-spacing: .04em; margin-right: 6px; }}
</style>
""".replace("{{", "{").replace("}}", "}")


def st_edit_cards(source_engine: Engine, summary: Any, section: str) -> None:
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
        with st.container(border=True):
            st.markdown(
                f"**{field_label(edit.field)}** :small[:gray[{edit.n_texts} rendered "
                f"text{'s' if edit.n_texts != 1 else ''} · {_reach_line(edit, section)}]]"
            )
            mark = resolve_item_mark(recorded, surface, edit_key(edit), edit_fields(edit))
            st_review_strip(source_engine, surface, mark)
            st_edit_body(edit)
            paths = {p for change in edit.changes for p in (change.catalog_paths or set())}
            note = where_note(edit.field, paths, edit.n_texts)
            if note:
                st_note(note)

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


def _reach_line(edit: EditGroup, section: str) -> str:
    """Where this edit lands *on this surface*, deduped across its texts: views per MDim, per explorer, or charts."""
    if section == "mdims":
        per: dict[str, dict[str, Any]] = {}
        for change in edit.changes:
            for mdim in change.mdims:
                entry = per.setdefault(
                    str(mdim["catalogPath"]),
                    {
                        "title": str(mdim.get("title") or mdim["catalogPath"]),
                        "draft": bool(mdim.get("is_draft")),
                        "views": set(),
                    },
                )
                entry["views"] |= {tuple(sorted(v.items())) for v in mdim.get("views") or []}
                # A reach entry carrying only a count: the largest single count is the tightest bound.
                entry.setdefault("counted", 0)
                entry["counted"] = max(entry["counted"], int(mdim.get("n_views") or 0))
        parts = []
        for entry in sorted(per.values(), key=lambda e: e["title"]):
            n = len(entry["views"]) or entry["counted"]
            parts.append(
                f"{n} view{'s' if n != 1 else ''} in {entry['title']}{' (unpublished)' if entry['draft'] else ''}"
            )
        return "; ".join(parts) or "no MDim view"

    if section == "explorers":
        per_slug: dict[str, set] = {}
        counted: dict[str, int] = {}
        for change in edit.changes:
            for explorer in change.explorers:
                slug = str(explorer["slug"])
                per_slug.setdefault(slug, set()).update(tuple(sorted(v.items())) for v in explorer.get("views") or [])
                counted[slug] = max(counted.get(slug, 0), int(explorer.get("n_views") or 0))
        parts = []
        for slug in sorted(per_slug):
            n = len(per_slug[slug]) or counted[slug]
            parts.append(f"{n} view{'s' if n != 1 else ''} in {slug}")
        return "; ".join(parts) or "no explorer view"

    charts = {c["chartId"]: c for change in edit.changes for c in change.charts}
    drafts = {c["chartId"]: c for change in edit.changes for c in change.draft_charts}
    on_page = sum(1 for c in charts.values() if c.get("has_data_page", True))
    drawer = len(charts) - on_page
    parts = []
    if on_page:
        parts.append(f"{on_page} on their data page")
    if drawer:
        parts.append(f"{drawer} via *Learn more about this data*")
    if drafts:
        parts.append(f"{len(drafts)} unpublished")
    return f"{len(charts)} chart{'s' if len(charts) != 1 else ''}" + (f" — {', '.join(parts)}" if parts else "")
