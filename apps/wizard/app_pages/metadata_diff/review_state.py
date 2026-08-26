"""Reviewed / not-reviewed bookkeeping for the change lists.

Two properties make this safe to lean on, and both are worth stating out loud in the UI:

- **Content-bound.** The stored `contentHash` is the exact old -> new text, so editing that text again
  in the same PR makes the stored mark stale and the change reopens for review. You never carry a tick
  over from text you no longer have.
- **Never synced.** The rows live on the branch's staging DB (`metadata_review`) and nothing reads them
  at merge time. Unlike chart-diff approvals, which gate `etl chart-sync`, marking a metadata change
  reviewed has no effect on production — it is a reviewer's own progress tracker.

Sign-off (Approve / Flag, which feeds the PR brief) is a different, deeper decision and keeps its own
surface keys in the MDim Review page. These list toggles use `list:`-prefixed surfaces so the two can
never overwrite each other.
"""

from dataclasses import dataclass
from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff.core import (  # noqa: F401
    ChangeGroup,
    item_identity,
    mark_identity,
    surface_key,
)
from apps.wizard.app_pages.metadata_diff.data import (
    NOTED,
    REVIEWED,
    delete_review,
    load_reviews,
    upsert_review,
)


@dataclass
class ReviewMark:
    """One change group's reviewed state, resolved against what is stored."""

    # None for an item mark (a chart, a view): those are not change groups, and nothing reads it there.
    group: ChangeGroup | None
    change_key: str
    content_hash: str
    reviewed: bool  # ticked, and the text has not moved since
    stale: bool  # was ticked, but the text changed afterwards — counts as not reviewed
    reviewer: str | None = None
    updated_at: Any = None
    note: str = ""  # free text the reviewer wrote about this item; survives unticking

    @property
    def icon(self) -> str:
        if self.stale:
            return "⚠️"
        return "✅" if self.reviewed else "🟡"


def resolve_marks(engine: Engine, surface: str, groups: list[ChangeGroup]) -> list[ReviewMark]:
    """Attach the stored reviewed state to each change group of one surface."""
    stored = load_reviews(engine, surface)
    marks = []
    for g in groups:
        change_key, content_hash = mark_identity(surface, g)
        row = stored.get(change_key)
        stale = bool(row) and row.get("contentHash") != content_hash
        marks.append(
            ReviewMark(
                group=g,
                change_key=change_key,
                content_hash=content_hash,
                reviewed=bool(row) and not stale and row.get("status") == REVIEWED,
                stale=stale,
                reviewer=(row or {}).get("reviewer"),
                updated_at=(row or {}).get("updatedAt"),
            )
        )
    return marks


def resolve_item_mark(stored: dict[str, Any], surface: str, item_key: str, fields: dict[str, Any]) -> ReviewMark:
    """The reviewed state of one item, against rows already loaded for its surface.

    Takes `stored` rather than querying, because the item views render one item per surface per run and a
    lookup each would be a query per row on the explorer list.

    `group` is None: an item is not a change group, and nothing on the item views reads it. The rest of
    `ReviewMark` carries over unchanged, so `st_reviewed_toggle` works on both kinds of mark.
    """
    change_key, content_hash = item_identity(surface, item_key, fields)
    row = stored.get(change_key)
    stale = bool(row) and row.get("contentHash") != content_hash
    return ReviewMark(
        group=None,
        change_key=change_key,
        content_hash=content_hash,
        # Ticked, not merely present: a row can exist to hold a note and nothing else.
        reviewed=bool(row) and row.get("status") == REVIEWED and not stale,
        stale=stale,
        reviewer=(row or {}).get("reviewer"),
        updated_at=(row or {}).get("updatedAt"),
        note=str((row or {}).get("comment") or ""),
    )


def n_reviewed(marks: list[ReviewMark]) -> int:
    return sum(1 for m in marks if m.reviewed)


def reviewed_toggle_key(surface: str, mark: ReviewMark, key_suffix: str = "") -> str:
    """Session-state key for one Reviewed toggle, carrying the content hash and not just the slot.

    `change_key` deliberately identifies the slot and survives an edit to the text, so a key without the
    hash left the tick showing "Reviewed" in an open session after the text moved underneath it:
    `mark.reviewed` and the stale caption both said unreviewed, while the widget the reviewer actually
    reads said the opposite, and clearing it took a toggle off and back on. The toggle's help text
    promises this resets itself; the hash is what keeps that promise, because edited text is a new key
    that seeds from `mark.reviewed`.
    """
    return f"mdd-reviewed::{surface}::{mark.change_key}::{mark.content_hash}{key_suffix}"


def st_reviewed_toggle(engine: Engine, surface: str, mark: ReviewMark, key_suffix: str = "") -> None:
    """The per-change Reviewed toggle, persisting straight to the staging DB on change."""
    widget_key = reviewed_toggle_key(surface, mark, key_suffix)
    if widget_key not in st.session_state:
        st.session_state[widget_key] = mark.reviewed

    def _save() -> None:
        if st.session_state.get(widget_key):
            upsert_review(engine, surface, mark.change_key, mark.content_hash, REVIEWED, mark.note or None, reviewer())
        elif mark.note:
            # Unticking must not throw away what the reviewer wrote: the row stays, holding the note.
            upsert_review(engine, surface, mark.change_key, mark.content_hash, NOTED, mark.note, reviewer())
        else:
            delete_review(engine, mark.change_key)

    st.toggle(
        "Reviewed",
        key=widget_key,
        on_change=_save,
        help="Your own progress marker: it is stored on this staging server, resets automatically if this "
        "text is edited again, and is **never synced to production** — nothing here changes on merge.",
    )
    if mark.stale:
        st.caption("⚠️ Edited since you marked it reviewed — the previous tick no longer counts.")
    elif mark.reviewed and mark.reviewer:
        when = f" · {mark.updated_at}" if mark.updated_at else ""
        st.caption(f"Marked reviewed by **{mark.reviewer}**{when}")


def item_marker(stored: dict[str, Any], surface: str, item_key: str) -> str:
    """ "✅ " if this item is ticked, "📝 " if it only carries a note, "" if neither — for a picker label.

    Reads the slot only. The change key is a hash of surface and item, not of the text, so this needs
    nothing diffed — which is what makes it usable in a list of sixty-seven charts. The consequence is
    that it cannot see staleness: a tick recorded against text edited since still shows ✅ here, while the
    item's own page, which does have the text, shows it as needing another look.
    """
    change_key, _ = item_identity(surface, item_key, {})
    row = stored.get(change_key)
    if not row:
        return ""
    if row.get("status") == REVIEWED:
        return "✅ "
    return "📝 " if row.get("comment") else ""


def surface_progress(rows: list[dict[str, Any]], surface: str) -> str:
    """ "✅ 3 · 📝 1" for one surface's recorded rows, or "" when it has none.

    For the pickers one level up — which MDim, which explorer — where the question is not "is this item
    done" but "have I been here at all". No denominator: that would need every one of the surface's views
    diffed to label a dropdown, and the section itself reports the count once you are inside it.
    """
    mine = [row for row in rows if str(row.get("catalogPath")) == surface]
    if not mine:
        return ""
    ticked = sum(1 for row in mine if row.get("status") == REVIEWED)
    noted = sum(1 for row in mine if row.get("comment"))
    parts = []
    if ticked:
        parts.append(f"✅ {ticked}")
    if noted:
        parts.append(f"📝 {noted}")
    return " · ".join(parts)


def st_item_note(engine: Engine, surface: str, mark: ReviewMark, label: str = "") -> None:
    """A note box for one item, saved as you leave it, and never mistaken for a tick.

    Collapsed unless something is written, so a page of items is a page of items. A note-only row carries
    the `noted` status, so `reviewed` stays false and the Review tab can tell the two apart.
    """
    widget_key = f"mdd-note::{surface}::{mark.change_key}::{mark.content_hash[:8]}"
    if widget_key not in st.session_state:
        st.session_state[widget_key] = mark.note

    def _save() -> None:
        note = str(st.session_state.get(widget_key) or "").strip()
        if note:
            status = REVIEWED if mark.reviewed else NOTED
            upsert_review(engine, surface, mark.change_key, mark.content_hash, status, note, reviewer())
        elif mark.reviewed:
            upsert_review(engine, surface, mark.change_key, mark.content_hash, REVIEWED, None, reviewer())
        else:
            delete_review(engine, mark.change_key)

    with st.expander(
        f"📝 Note{' — written' if mark.note else ''}{f' · {label}' if label else ''}", expanded=bool(mark.note)
    ):
        st.text_area(
            "Note",
            key=widget_key,
            on_change=_save,
            height=90,
            placeholder="What you want to remember about this one — a question, a follow-up, a reason.",
            label_visibility="collapsed",
        )
        st.caption(
            "Saved on this staging server as you leave the box, and collected in the **Review** tab. Like "
            "the ticks, it is never synced anywhere on merge."
        )


def reviewer() -> str | None:
    """Identity recorded alongside a mark (audit trail); there is no reviewer input in the UI yet."""
    return (st.session_state.get("mdd_reviewer") or "").strip() or None
