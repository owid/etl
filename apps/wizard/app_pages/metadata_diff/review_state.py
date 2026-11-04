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
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff.core import (  # noqa: F401
    ChangeGroup,
    item_identity,
    mark_identity,
    surface_key,
)
from apps.wizard.app_pages.metadata_diff.data import (
    DECIDED,
    NOTED,
    REJECTED,
    REVIEWED,
    delete_review,
    load_reviews,
    upsert_review,
)


def verdict_reopened(row: dict[str, Any], index: dict[str, dict[str, str]]) -> bool:
    """Has the text moved since this stored verdict was recorded?

    The same rule `resolve_item_mark` applies to one item, for callers holding rows straight out of
    `load_item_notes` — the Summary tab and the section bar — which have no `ReviewMark` to read. Both were
    filtering on status alone, so a decision made on wording that has since been rewritten still counted
    as one, and a section could be badged finished over an item that had reopened.

    Only for rows the index can currently hash — every surface, charts included. Where an enumeration
    failed and left a slot without a hash, the verdict is reported as recorded rather than guessed at.
    """
    if row.get("status") not in DECIDED:
        return False
    current = (index.get(str(row.get("changeKey"))) or {}).get("hash")
    return bool(current) and row.get("contentHash") != current


def verdict_counts(row: dict[str, Any], index: dict[str, dict[str, str]]) -> bool:
    """Does this stored row count as progress against what the page currently shows?

    Three conditions, and the last two are what a bare status check misses: it has to be a decision, the
    item has to still be in the comparison (a reverted text or an unpublished chart leaves the row behind,
    and counting it against today's totals reads as work that is no longer there), and the wording has to
    be the wording it was decided on.
    """
    return row.get("status") in DECIDED and str(row.get("changeKey")) in index and not verdict_reopened(row, index)


@dataclass
class ReviewMark:
    """One change group's reviewed state, resolved against what is stored."""

    # None for an item mark (a chart, a view): those are not change groups, and nothing reads it there.
    group: ChangeGroup | None
    change_key: str
    content_hash: str
    reviewed: bool  # ticked, and the text has not moved since
    stale: bool  # was decided, but the text changed afterwards — the decision no longer counts
    # Read and rejected: this text should not ship. Mutually exclusive with `reviewed`, and stale the same
    # way — a rejection of wording that has since been rewritten is a verdict on text nobody has now.
    rejected: bool = False
    reviewer: str | None = None
    updated_at: Any = None
    note: str = ""  # free text the reviewer wrote about this item; survives unticking


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
        rejected=bool(row) and row.get("status") == REJECTED and not stale,
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


def st_decision_control(engine: Engine, surface: str, mark: ReviewMark, key_suffix: str = "") -> None:
    """One item's verdict — ✅ reviewed or ❌ reject — persisting straight to the staging DB on change.

    One control rather than two toggles, because the two answers exclude each other: a change cannot be
    both signed off and refused, and two independent switches can say exactly that. Clicking the active
    option clears it, which is the third state: nothing decided yet.

    Neither answer does anything to the data. A rejection is a record of what has to be undone, and the
    Summary tab is where those records become text to hand back to whoever is editing — said in the help
    and again under a rejection, because "❌" invites the belief that something has been reverted.
    """
    widget_key = reviewed_toggle_key(surface, mark, key_suffix)
    if widget_key not in st.session_state:
        st.session_state[widget_key] = REVIEWED if mark.reviewed else (REJECTED if mark.rejected else None)

    def _save() -> None:
        picked = st.session_state.get(widget_key)
        if picked in DECIDED:
            upsert_review(engine, surface, mark.change_key, mark.content_hash, picked, mark.note or None, reviewer())
        elif mark.note:
            # Clearing a verdict must not throw away what the reviewer wrote: the row stays with the note.
            upsert_review(engine, surface, mark.change_key, mark.content_hash, NOTED, mark.note, reviewer())
        else:
            delete_review(engine, mark.change_key)

    st.segmented_control(
        "Decision",
        options=list(DECIDED),
        format_func=lambda status: "✅ Reviewed" if status == REVIEWED else "❌ Reject",
        key=widget_key,
        on_change=_save,
        label_visibility="collapsed",
        help="Stored on this staging server, reset automatically if this text is edited again, and "
        "**never synced** — neither answer changes any text. **Reject** records that this should not "
        "ship; the **Summary** tab collects those into instructions to paste back to whoever is editing.",
    )
    if mark.stale:
        st.caption("⚠️ Edited since you decided — the previous answer no longer counts.")
    elif mark.rejected:
        who = f" by **{mark.reviewer}**" if mark.reviewer else ""
        st.caption(f"❌ Rejected{who} — nothing is changed here; take the wording from **Summary**.")
    elif mark.reviewed and mark.reviewer:
        when = f" · {mark.updated_at}" if mark.updated_at else ""
        st.caption(f"Marked reviewed by **{mark.reviewer}**{when}")


def item_marker(stored: dict[str, Any], surface: str, item_key: str) -> str:
    """ "✅ " reviewed, "❌ " rejected, "📝 " a note and no verdict, "" untouched — for a picker label.

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
    if row.get("status") == REJECTED:
        return "❌ "
    return "📝 " if row.get("comment") else ""


# Enter saves, Shift+Enter newlines. Streamlit gives no keydown hook, so the binding is a script in a
# zero-height component reaching the parent document — same-origin, the way the dimension grid does it.
# Enter blurs the box rather than submitting anything: blur is what makes Streamlit commit a text area's
# value and fire `on_change`, so the save path is the same one clicking away uses.
_ENTER_SAVES_JS = """
<script>
const doc = window.parent.document;
function bind() {
  doc.querySelectorAll('[class*="st-key-mdd-strip-"] textarea').forEach((box) => {
    if (box.dataset.mddEnter) return;
    box.dataset.mddEnter = "1";
    box.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey && !event.ctrlKey && !event.metaKey) {
        event.preventDefault();
        box.blur();
      }
    });
  });
}
bind();
// Streamlit rebuilds the DOM on every rerun, so the binding has to be reapplied rather than done once.
new MutationObserver(bind).observe(doc.body, { childList: true, subtree: true });
</script>
"""


def _st_enter_saves_script() -> None:
    """Bind the keys for every review strip on the page (idempotent, and harmless if it never runs)."""
    components.html(_ENTER_SAVES_JS, height=0)


def surface_progress(rows: list[dict[str, Any]], surface: str) -> str:
    """ "✅ 3 · ❌ 1 · 📝 1" for one surface's recorded rows, or "" when it has none.

    For the pickers one level up — which MDim, which explorer — where the question is not "is this item
    done" but "have I been here at all". No denominator: that would need every one of the surface's views
    diffed to label a dropdown, and the section itself reports the count once you are inside it.
    """
    mine = [row for row in rows if str(row.get("catalogPath")) == surface]
    if not mine:
        return ""
    ticked = sum(1 for row in mine if row.get("status") == REVIEWED)
    rejected = sum(1 for row in mine if row.get("status") == REJECTED)
    noted = sum(1 for row in mine if row.get("comment"))
    parts = []
    if ticked:
        parts.append(f"✅ {ticked}")
    if rejected:
        parts.append(f"❌ {rejected}")
    if noted:
        parts.append(f"📝 {noted}")
    return " · ".join(parts)


def st_review_strip(engine: Engine, surface: str, mark: ReviewMark) -> None:
    """One item's tick and note, side by side, directly under its name.

    Together and open, on purpose: they are two halves of one decision, and separating them — the tick in
    a header column, the note folded into an expander — left the note looking like a detail and the tick
    easy to walk past. The box is tinted (see the diff CSS) for the same reason: on a page whose body is
    two columns of prose, an untinted row of controls disappears.

    A note-only row carries the `noted` status, so writing a note never reads as a tick and the Summary tab
    can tell them apart. On a rejection the note is the useful half — it is what tells whoever is editing
    *why*, and it travels into the Summary tab's instructions.
    """
    note_key = f"mdd-note::{surface}::{mark.change_key}::{mark.content_hash[:8]}"
    if note_key not in st.session_state:
        st.session_state[note_key] = mark.note

    def _save_note() -> None:
        note = str(st.session_state.get(note_key) or "").strip()
        # Whichever verdict stands, stands: writing a note is not a decision, and it must not quietly
        # promote a rejection to reviewed or the other way round.
        verdict = REVIEWED if mark.reviewed else (REJECTED if mark.rejected else None)
        if note:
            upsert_review(engine, surface, mark.change_key, mark.content_hash, verdict or NOTED, note, reviewer())
        elif verdict:
            upsert_review(engine, surface, mark.change_key, mark.content_hash, verdict, None, reviewer())
        else:
            delete_review(engine, mark.change_key)

    with st.container(border=True, key=f"mdd-strip-{mark.change_key[:16]}"):
        col_tick, col_note = st.columns([1, 4], vertical_alignment="center")
        with col_tick:
            st_decision_control(engine, surface, mark)
        with col_note:
            st.text_area(
                "Note",
                key=note_key,
                on_change=_save_note,
                height=68,
                placeholder="Note — Enter saves, Shift+Enter starts a new line.",
                label_visibility="collapsed",
            )
    _st_enter_saves_script()


def reviewer() -> str | None:
    """Identity recorded alongside a mark (audit trail); there is no reviewer input in the UI yet."""
    return (st.session_state.get("mdd_reviewer") or "").strip() or None
