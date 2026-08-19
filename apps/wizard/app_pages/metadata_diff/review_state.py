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

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff.core import ChangeGroup
from apps.wizard.app_pages.metadata_diff.data import delete_review, load_reviews, upsert_review

# Stored status for a change the reviewer has looked at. Distinct from the Review page's
# "approved"/"flagged", so a list tick is never mistaken for a sign-off.
REVIEWED = "reviewed"


def surface_key(kind: str, ident: str) -> str:
    """Namespaced key for the reviewed-state rows of one surface (`list:chart:<slug>`, ...).

    The `list:` prefix keeps these separate from the Approve/Flag sign-off rows, which key on the bare
    catalogPath (MDims) or `chart:<slug>` (the per-chart review).
    """
    return f"list:{kind}:{ident}"


@dataclass
class ReviewMark:
    """One change group's reviewed state, resolved against what is stored."""

    group: ChangeGroup
    change_key: str
    content_hash: str
    reviewed: bool  # ticked, and the text has not moved since
    stale: bool  # was ticked, but the text changed afterwards — counts as not reviewed
    reviewer: str | None = None
    updated_at: Any = None

    @property
    def icon(self) -> str:
        if self.stale:
            return "⚠️"
        return "✅" if self.reviewed else "🟡"


def mark_identity(surface: str, group: ChangeGroup) -> tuple[str, str]:
    """(slot key, content hash) for one change on one surface.

    The slot has to name *where* the change is, not just which field it is: chart-side changes carry no
    view dimensions at all (an indicator is a view with none), so keying on field + dimensions alone —
    the way the MDim review page can — would give every `description_short` change on the page the same
    key, and they would share a single row. Including the indicators the change lands on separates them.

    The hash covers only the text, so the slot survives an edit while the mark goes stale.
    """
    where = sorted(group.catalog_paths) or ([group.catalog_path] if group.catalog_path else [])
    slot = json.dumps(
        [surface, group.field, where, sorted(json.dumps(d, sort_keys=True) for d in group.view_dims)],
        sort_keys=True,
    )
    change_key = hashlib.sha256(slot.encode()).hexdigest()
    content_hash = hashlib.sha256(json.dumps([group.old, group.new], sort_keys=True, default=str).encode()).hexdigest()
    return change_key, content_hash


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
            upsert_review(engine, surface, mark.change_key, mark.content_hash, REVIEWED, None, reviewer())
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


def reviewer() -> str | None:
    """Identity recorded alongside a mark (audit trail); there is no reviewer input in the UI yet."""
    return (st.session_state.get("mdd_reviewer") or "").strip() or None
