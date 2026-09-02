"""Review: everything you ticked or wrote a note about, in one place.

The three surface sections are where reading happens, one item at a time. This is the other half — what
came out of it, gathered across charts, MDim views and explorer views, named, linked, and counted against
how many there were.

Nothing here is computed twice: the rows come from one query, and the names come from the same cached
enumerations the sections use. A stored row carries a hash rather than a name (the slot has to survive an
edit to the text), so the index is rebuilt by hashing each known item the same way and matching.
"""

import subprocess
from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.app_pages.metadata_diff import cached
from apps.wizard.app_pages.metadata_diff.core import (
    SECTIONS,
    distinct_garden_datasets,
    field_label,
    section_progress,
    view_url,
)
from apps.wizard.app_pages.metadata_diff.data import REVIEWED, load_item_notes
from apps.wizard.app_pages.metadata_diff.discovery import group_by_edit, reach_by_surface
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    markdown_output,
)

# The PR lookup shells out, so it is asked once per session rather than on every rerun.
_PR_CACHE_KEY = "mdd-pr-url"


def st_show_review(source_engine: Engine, target_engine: Engine) -> None:
    """What this pass produced: ticks and notes, named and linked, against the number of items there are."""
    rows = load_item_notes(source_engine)
    index, totals = cached.item_index(source_engine, target_engine)
    ticked = [r for r in rows if r.get("status") == REVIEWED]
    noted = [r for r in rows if r.get("comment")]
    # Counted along whichever layout each section was reviewed in — view by view or by edit — never both
    # added, or a section finished view by view would read as unfinished for its untouched edit cards.
    by_surface: dict[str, int] = {}
    for row in ticked:
        surface = str(row.get("catalogPath") or "")
        by_surface[surface] = by_surface.get(surface, 0) + 1
    progress = section_progress(by_surface, totals)
    total = sum(t for _done, t in progress.values())
    n_done = sum(done for done, _t in progress.values())

    if not rows:
        if total:
            st.warning(
                f"**Nothing reviewed yet.** {total} item{'s' if total != 1 else ''} in this branch "
                f"{'are' if total != 1 else 'is'} waiting: tick them off in **Charts**, **MDims** or "
                "**Explorers**, or write a note on one, and they appear here."
            )
        else:
            st.success(
                "**Nothing to review.** No chart, MDim view or explorer view on this server differs from "
                f"`{BASELINE_NAME}`."
            )
        _footnote()
        return

    st.markdown(
        f"**{n_done} of {total} item{'s' if total != 1 else ''} ticked** · "
        f"**{len(noted)} with a note** · against `{BASELINE_NAME}`"
    )
    # The denominator is the point: a list of what you ticked, with no total, reads as a finished job.
    if not ticked:
        st.warning(
            f"**Nothing ticked yet** — {len(noted)} note{'s' if len(noted) != 1 else ''} recorded, but no "
            "item marked reviewed."
        )
    elif n_done < total:
        left = total - n_done
        st.warning(
            f"**Review unfinished** — {left} item{'s' if left != 1 else ''} still to look at. The three "
            "surface sections open on the ones you have not reached."
        )
    else:
        st.success(f"**All {total} items reviewed.**")
    _footnote()

    for surface, group in sorted(_by_surface(rows).items()):
        done = sum(1 for row in group if row.get("status") == REVIEWED)
        of = totals.get(surface)
        with_note = [row for row in group if row.get("comment")]
        # Ticked, and nothing written: a count says everything a list of four hundred of them would.
        bare = [row for row in group if not row.get("comment")]
        with st.container(border=True):
            counted = f"{done} of {of}" if of else str(done)
            st.markdown(
                f"**{_surface_title(surface)}** :small[:gray[{counted} reviewed · {len(with_note)} with a note]]"
            )
            for row in with_note:
                st.markdown(_row_line(row, index))
                st.markdown(_quoted(str(row["comment"])))
            if bare:
                # Folded, not dropped: "which ones did I tick" is a fair question, just not the first one.
                with st.expander(f"{len(bare)} ticked with no note"):
                    for row in bare:
                        st.markdown(_row_line(row, index))

    summary = cached.summary(source_engine, target_engine)
    st.divider()
    st.markdown("#### What this branch changed")
    st.caption(
        "The metadata edits themselves, grouped as Blast radius groups them. Copy this into an issue, a PR "
        "comment or a channel when you want to discuss the change rather than the review."
    )
    markdown_output(_changes_markdown(summary), "metadata-changes.md", "mdd_changes_digest")

    st.markdown("#### The review notes")
    st.caption(
        "What you ticked and wrote, for the same places — kept separate, since the two often go to different people."
    )
    markdown_output(_notes_markdown(rows, index, totals), "metadata-review.md", "mdd_review_notes")


def _row_line(row: dict[str, Any], index: dict[str, dict[str, str]]) -> str:
    """One recorded item: ticked or noted, named, and linked to the thing itself."""
    icon = "✅" if row.get("status") == REVIEWED else "📝"
    known = index.get(str(row.get("changeKey")))
    when = f" :small[:gray[{row.get('updatedAt')}]]" if row.get("updatedAt") else ""
    if not known:
        # A row whose item is no longer in the comparison — the text was reverted, or the chart was
        # unpublished since. Said plainly rather than shown as a bare hash.
        return f"{icon} :gray[an item that is no longer in this diff]{when}"
    name = known["name"]
    return f"{icon} [{name}]({known['url']}){when}" if known.get("url") else f"{icon} {name}{when}"


def _quoted(note: str) -> str:
    """A note as a blockquote that survives its own newlines.

    Markdown needs the marker on every line: `> one\ntwo` quotes the first line and drops the second out
    of the quote entirely, which is what a two-line note looked like on this page. Blank lines keep a bare
    `>` so the quote stays one block rather than splitting into two.
    """
    return "\n".join(f"> {line}" if line.strip() else ">" for line in note.splitlines() or [""])


def _bullet_lines(note: str) -> list[str]:
    """A note as one nested bullet, continuation lines indented to stay inside it.

    Same failure in the export: a newline ends the list item, so the rest of the note came out as a stray
    paragraph between two bullets. Four spaces keeps it within the `  - ` item.
    """
    parts = note.splitlines() or [""]
    return [f"  - {parts[0]}"] + [f"    {line}" if line.strip() else "" for line in parts[1:]]


def _provenance() -> list[str]:
    """Where this review was done: the branch, its PR if there is one, the server, and the baseline."""
    branch = SOURCE.name.removeprefix("staging-site-")
    lines = [
        f"- Branch: `{branch}`",
        f"- Staging server: {SOURCE.site}",
        f"- Compared against: `{BASELINE_NAME}`",
    ]
    pr = _pull_request_url(branch)
    if pr:
        lines.insert(1, f"- Pull request: {pr}")
    return lines


def _pull_request_url(branch: str) -> str:
    """The PR for this branch, asked of `gh` once and cached for the session.

    Best-effort on purpose: `gh` may be absent, unauthenticated, or the branch may have no PR yet, and
    none of that is a reason for the notes to fail to render. A missing line is better than a wrong one,
    so nothing is guessed from the branch name.
    """
    if _PR_CACHE_KEY in st.session_state:
        return str(st.session_state[_PR_CACHE_KEY])
    url = ""
    try:
        result = subprocess.run(
            ["gh", "pr", "view", branch, "--json", "url", "--jq", ".url"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode == 0:
            url = result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        url = ""
    st.session_state[_PR_CACHE_KEY] = url
    return url


def _footnote() -> None:
    """The two things about this record that are easy to assume wrongly."""
    st.caption(
        "Recorded on this staging server as you went, and never read at merge time — the markdown below is "
        "for pasting into the PR yourself. A tick made against text that has since been edited still "
        "appears here; the section it belongs to shows it as needing another look."
    )


def _by_surface(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Rows grouped by the surface they were recorded on."""
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(str(row.get("catalogPath") or "list:item:unknown"), []).append(row)
    return out


def _surface_title(surface: str) -> str:
    """A surface key as something to read: `list:item:mdim:grapher/x#y` -> "MDim grapher/x#y"."""
    body = surface.removeprefix("list:item:")
    if body.startswith("edit:"):
        section = body.removeprefix("edit:")
        return f"{SECTIONS[section][1] if section in SECTIONS else section} — by edit"
    for prefix, name in (("mdim:", "MDim"), ("explorer:", "Explorer"), ("chart", "Charts")):
        if body.startswith(prefix):
            rest = body.removeprefix(prefix)
            return f"{name} `{rest}`" if rest else name
    return body


def _changes_markdown(summary: Any) -> str:
    """What this branch changed, as markdown to paste into an issue, a PR comment or a channel.

    Grouped by authored edit, the way Blast radius groups it: one reworded sentence is one entry, however
    many texts it renders into and pages it lands on. Reporting the texts as separate changes overstates
    the work by an order of magnitude on a shared definition, which is why that grouping exists at all.
    """
    lines = ["## Metadata changes on this branch", ""] + _provenance() + [""]
    edits = group_by_edit(summary.reach)
    if not edits:
        return "\n".join(lines + [f"_No metadata text on this server differs from `{BASELINE_NAME}`._"])

    rows = reach_by_surface(summary.reach)
    pages = sum(1 for row in rows if row["published"])
    hidden = len(rows) - pages
    head = (
        f"**{len(edits)} edit{'s' if len(edits) != 1 else ''}** authored here, rendering "
        f"**{len(summary.reach)} distinct text{'s' if len(summary.reach) != 1 else ''}**, on "
        f"**{pages} page{'s' if pages != 1 else ''}** a reader can reach"
    )
    if hidden:
        head += f", plus {hidden} unpublished"
    lines += [head, ""]

    for edit in edits:
        surfaces = edit.surfaces()
        reach = []
        for kind, label in (("charts", "chart"), ("mdims", "MDim"), ("explorers", "explorer")):
            count = len(surfaces.get(kind) or ())
            if count:
                reach.append(f"{count} {label}{'s' if count != 1 else ''}")
        drafts = len(surfaces.get("draft_charts") or ())
        if drafts:
            reach.append(f"{drafts} unpublished chart{'s' if drafts != 1 else ''}")
        lines.append(
            f"### {field_label(edit.field)} — {edit.n_texts} text{'s' if edit.n_texts != 1 else ''}"
            + (f" · {', '.join(reach)}" if reach else " · nothing published renders it")
        )
        if edit.inserted and not edit.deleted:
            lines.append(f"- added: “{_trimmed(edit.inserted)}”")
        elif edit.deleted and not edit.inserted:
            lines.append(f"- removed: “{_trimmed(edit.deleted)}”")
        else:
            first = edit.changes[0]
            before, after = _around_change(first.old, first.new)
            lines.append(f"- before: “{before}”")
            lines.append(f"- after: “{after}”")
        lines += _where_to_look(edit)
        lines.append("")
    return "\n".join(lines)


def _where_to_look(edit: Any) -> list[str]:
    """Which dataset the edit is in, and which MDims and explorers to open — named and linked.

    The counts above say how far it reaches; these say where to go. Datasets are the garden step dirs the
    indicators resolve to, so a shared definition edited in two datasets names both — pointing at one
    would send somebody to fix half of it.
    """
    lines: list[str] = []

    datasets = distinct_garden_datasets({p for change in edit.changes for p in (change.catalog_paths or set())})
    if datasets:
        lines.append("- datasets: " + ", ".join(f"`{d}`" for d in datasets))

    mdims: dict[str, dict[str, Any]] = {}
    for change in edit.changes:
        for mdim in change.mdims:
            mdims.setdefault(str(mdim["catalogPath"]), mdim)
    if mdims:
        named = []
        for path, mdim in sorted(mdims.items(), key=lambda kv: str(kv[1].get("title") or kv[0])):
            slug = str(mdim.get("slug") or "")
            url = view_url(SOURCE, path, None if mdim.get("is_draft") else slug, {})
            draft = " (unpublished)" if mdim.get("is_draft") else ""
            named.append(f"[{mdim.get('title') or path}]({url}){draft} — `{path}`")
        lines.append("- MDims: " + "; ".join(named))

    explorers = sorted({str(e["slug"]) for change in edit.changes for e in change.explorers})
    if explorers:
        lines.append("- explorers: " + ", ".join(f"[{slug}]({SOURCE.site}/explorers/{slug})" for slug in explorers))
    return lines


def _trimmed(value: Any, limit: int = 240) -> str:
    """One readable line from whatever a field holds.

    WYSK is a list of bullets, so it is joined rather than printed as a Python list, and newlines are
    folded: this is a summary line, and the tool itself is where the full text lives.
    """
    text = _flat(value)
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _around_change(old: Any, new: Any, window: int = 110) -> tuple[str, str]:
    """The two texts trimmed around where they differ, so the difference is inside what is shown.

    Trimming from the front showed two identical 240-character openings for an edit whose words moved
    later in the sentence — the same failure the blast-radius preview had. The common prefix and suffix
    are measured, and each side is shown from a little before the divergence to a little after it.
    """
    before, after = _flat(old), _flat(new)
    if before == after:
        return _trimmed(before), _trimmed(after)

    prefix = 0
    while prefix < min(len(before), len(after)) and before[prefix] == after[prefix]:
        prefix += 1
    suffix = 0
    while (
        suffix < min(len(before), len(after)) - prefix
        and before[len(before) - 1 - suffix] == after[len(after) - 1 - suffix]
    ):
        suffix += 1

    def cut(text: str, end_of_change: int) -> str:
        start = max(0, prefix - window)
        end = min(len(text), end_of_change + window)
        piece = text[start:end].strip()
        return ("…" if start > 0 else "") + piece + ("…" if end < len(text) else "")

    return cut(before, len(before) - suffix), cut(after, len(after) - suffix)


def _flat(value: Any) -> str:
    """Whatever a field holds, as one line of text."""
    if isinstance(value, (list, tuple)):
        text = " / ".join(str(item) for item in value if item)
    else:
        text = str(value or "")
    return " ".join(text.split())


def _notes_markdown(rows: list[dict[str, Any]], index: dict[str, dict[str, str]], totals: dict[str, int]) -> str:
    """The review notes as markdown — named, linked, and saying where they came from.

    The header matters as much as the notes. Pasted into a PR comment or handed back to an assistant, the
    text used to arrive with no branch, no PR and no baseline, and item names that mean nothing without
    knowing which server they were read on.
    """
    lines = ["## Metadata review notes", ""] + _provenance() + [""]
    for surface, group in sorted(_by_surface(rows).items()):
        done = sum(1 for row in group if row.get("status") == REVIEWED)
        of = totals.get(surface)
        with_note = [row for row in group if row.get("comment")]
        head = f"### {_surface_title(surface)}"
        lines.append(f"{head} — {done} of {of} reviewed" if of else head)
        # Counts, then the notes. A list of every ticked view would bury the two sentences somebody wrote,
        # which is the only part of this a reader of the PR needs.
        if not with_note:
            lines.append("")
            continue
        for row in with_note:
            known = index.get(str(row.get("changeKey")))
            name = f"[{known['name']}]({known['url']})" if known else "an item no longer in this diff"
            lines.append(f"- {name}")
            lines.extend(_bullet_lines(str(row["comment"])))
        lines.append("")
    return "\n".join(lines)
