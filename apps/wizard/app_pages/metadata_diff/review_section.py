"""Review: everything you ticked or wrote a note about, in one place.

The three surface sections are where reading happens, one item at a time. This is the other half — what
came out of it, gathered across charts, MDim views and explorer views, and rendered as markdown you can
paste into the PR.

It holds no changes of its own and computes nothing: one query for the rows the item ticks and notes
wrote, grouped by surface. So it is always consistent with what the sections show, and it costs nothing to
open.
"""

from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff import cached
from apps.wizard.app_pages.metadata_diff.data import REVIEWED, load_item_notes
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME, markdown_output


def st_show_review(source_engine: Engine, _target_engine: Engine) -> None:
    """What this pass produced: ticks and notes, by surface, plus a markdown copy of the notes."""
    rows = load_item_notes(source_engine)
    ticked = [r for r in rows if r.get("status") == REVIEWED]
    noted = [r for r in rows if r.get("comment")]
    total = _reviewable_items(source_engine, _target_engine)

    if not rows:
        st.warning(
            f"**Nothing reviewed yet.** {total} item{'s' if total != 1 else ''} in this branch "
            f"{'are' if total != 1 else 'is'} waiting: tick them off in **Charts**, **MDims** or "
            "**Explorers**, or write a note on one, and they appear here."
            if total
            else "**Nothing to review.** No chart, MDim view or explorer view on this server differs from "
            f"`{BASELINE_NAME}`."
        )
        _footnote()
        return

    st.markdown(
        f"**{len(ticked)} of {total} item{'s' if total != 1 else ''} ticked** · "
        f"**{len(noted)} with a note** · against `{BASELINE_NAME}`"
    )
    # The denominator is the point: a list of what you ticked, with no total, reads as a finished job.
    if not ticked:
        st.warning(
            f"**Nothing ticked yet** — {len(noted)} note{'s' if len(noted) != 1 else ''} recorded, but no "
            "item marked reviewed."
        )
    elif len(ticked) < total:
        left = total - len(ticked)
        st.warning(
            f"**Review unfinished** — {left} item{'s' if left != 1 else ''} still to look at. The three "
            "surface sections open on the ones you have not reached."
        )
    else:
        st.success(f"**All {total} items reviewed.**")
    _footnote()

    for surface, group in sorted(_by_surface(rows).items()):
        with st.container(border=True):
            st.markdown(
                f"**{_surface_title(surface)}** :small[:gray[{len(group)} item{'s' if len(group) != 1 else ''}]]"
            )
            for row in group:
                icon = "✅" if row.get("status") == REVIEWED else "📝"
                when = f" :small[:gray[{row.get('updatedAt')}]]" if row.get("updatedAt") else ""
                st.markdown(f"{icon} `{_item_name(row)}`{when}")
                if row.get("comment"):
                    st.markdown(f"> {row['comment']}")

    markdown_output(_markdown(rows), "metadata-review.md", "mdd_review_notes")


def _footnote() -> None:
    """The two things about this record that are easy to assume wrongly."""
    st.caption(
        "Recorded on this staging server as you went, and never read at merge time — the markdown below is "
        "for pasting into the PR yourself. A tick made against text that has since been edited still "
        "appears here; the section it belongs to shows it as needing another look."
    )


def _reviewable_items(source_engine: Any, target_engine: Any) -> int:
    """How many items there are to review: changed charts, changed MDim views, changed explorer views.

    Read from the same cached summary the section badges use, plus the per-MDim view diffs the MDims
    section has already resolved — so opening this tab costs nothing that has not been paid for. An
    unreadable MDim is skipped rather than guessed at: a total that silently omits something is worse
    than one that is a little conservative, and the sections themselves report their own ceilings.
    """
    summary = cached.summary(source_engine, target_engine)
    total = summary.n_charts + summary.n_explorer_views

    df = cached.mdim_changes(source_engine, target_engine)
    flagged = [str(cp) for cp in df.index[df["in_branch"] & df["has_changes"]]]
    for catalog_path in flagged:
        row = df.loc[catalog_path]
        try:
            _title, _dimensions, view_diffs = cached.mdim_view_diffs(
                catalog_path,
                source_engine,
                target_engine,
                cache_key=f"{row['configMd5_source']}::{row['configMd5_target']}",
            )
        except Exception:  # noqa: BLE001 — a surface we cannot read is not a surface we can count
            continue
        total += sum(1 for view in view_diffs if view.changed)
    return total


def _by_surface(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Rows grouped by the surface they were recorded on."""
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(str(row.get("catalogPath") or "item:unknown"), []).append(row)
    return out


def _surface_title(surface: str) -> str:
    """A surface key as something to read: `item:mdim:grapher/x#y` -> "MDim grapher/x#y"."""
    body = surface.removeprefix("list:item:")
    for prefix, name in (("mdim:", "MDim"), ("explorer:", "Explorer"), ("chart", "Charts")):
        if body.startswith(prefix):
            rest = body.removeprefix(prefix)
            return f"{name} `{rest}`" if rest else name
    return body


def _item_name(row: dict[str, Any]) -> str:
    """What the item was called when it was ticked.

    The stored key is a hash — the slot has to survive an edit to the text — so a note carries no name of
    its own. The dimensions or slug are what a reviewer recognises, and they are in the note's own text
    when it matters; failing that, the truncated key is at least stable.
    """
    return str(row.get("changeKey") or "")[:12]


def _markdown(rows: list[dict[str, Any]]) -> str:
    """The notes as markdown, for pasting into the PR."""
    lines = ["## Metadata review notes", "", f"Against `{BASELINE_NAME}`.", ""]
    for surface, group in sorted(_by_surface(rows).items()):
        lines.append(f"### {_surface_title(surface)}")
        for row in group:
            state = "reviewed" if row.get("status") == REVIEWED else "noted"
            lines.append(f"- `{_item_name(row)}` — {state}")
            if row.get("comment"):
                lines.append(f"  - {row['comment']}")
        lines.append("")
    return "\n".join(lines)
