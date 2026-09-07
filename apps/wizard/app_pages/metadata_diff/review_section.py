"""Summary: everything you ticked or wrote a note about, in one place.

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
    COUNTED_SECTIONS,
    SECTIONS,
    distinct_garden_datasets,
    field_label,
    garden_meta_file,
    item_identity,
    section_progress,
    surface_key,
    view_url,
)
from apps.wizard.app_pages.metadata_diff.data import DECIDED, REJECTED, REVIEWED, load_item_notes
from apps.wizard.app_pages.metadata_diff.discovery import (
    dataset_owners,
    edit_fields,
    edit_key,
    edit_slot,
    edits_for,
    group_by_edit,
    reach_by_surface,
)
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    markdown_output,
)
from apps.wizard.app_pages.metadata_diff.review_state import verdict_counts, verdict_reopened

# The PR lookup shells out, so it is asked once per session rather than on every rerun.
_PR_CACHE_KEY = "mdd-pr-url"


def st_show_review(source_engine: Engine, target_engine: Engine) -> None:
    """What this pass produced: ticks and notes, named and linked, against the number of items there are."""
    rows = load_item_notes(source_engine)
    index, totals = cached.item_index(source_engine, target_engine)
    summary = cached.summary(source_engine, target_engine)
    # A verdict is bound to the text it was made on. Reword that text and the row keeps its status while
    # its `contentHash` no longer matches, which is how a rejection of wording nobody read reached the
    # hand-off document as an instruction to undo it. The section lists already reopen such an item
    # (`resolve_item_mark`); this reads the same current hash and applies the same rule here.
    reopened = [r for r in rows if verdict_reopened(r, index)]
    settled = [r for r in rows if not verdict_reopened(r, index)]
    ticked = [r for r in settled if r.get("status") == REVIEWED]
    rejected = [r for r in settled if r.get("status") == REJECTED]
    # Notes are the reviewer's own words and stay whatever happened to the text; only verdicts reopen.
    noted = [r for r in rows if r.get("comment")]
    # Counted along whichever layout each section was reviewed in — view by view or by edit — never both
    # added, or a section finished view by view would read as unfinished for its untouched edit cards.
    by_surface: dict[str, int] = {}
    for row in [*ticked, *rejected]:
        # `verdict_counts`, not the row alone: a decision on an item that has left the comparison is
        # still worth listing below — it says what was recorded — but it is not progress against totals
        # counted from what the page shows today. The section bar counts the same rows.
        if not verdict_counts(row, index):
            continue
        surface = str(row.get("catalogPath") or "")
        by_surface[surface] = by_surface.get(surface, 0) + 1
    progress = section_progress(by_surface, totals)
    n_done = sum(done for done, _t in progress.values())
    # No global denominator. Each section is counted along whichever layout its reviewer used, so summing
    # them adds 402 explorer views to 3 authored edits — and the total swung from 71 to 556 on this branch
    # when three rejections were cleared, which is not a fact about the branch. What is comparable across
    # sections is whether each one is finished, so that is what the header reports; the per-surface rows
    # below keep their own denominators, where the unit is unambiguous.
    finished = sum(1 for done, of in progress.values() if of and done >= of)
    # Sections with something in them. A branch that only touches charts has nothing to review in MDims or
    # Explorers, and counting those made a finished review read "1 of 3 sections finished" for ever — the
    # success line unreachable on the commonest shape of branch there is, over work that does not exist.
    n_sections = sum(1 for _done, of in progress.values() if of)
    total = sum(of for _done, of in progress.values())

    if not rows:
        if total:
            st.warning(
                f"**Nothing reviewed yet.** {total} item{'s' if total != 1 else ''} in this branch "
                f"{'are' if total != 1 else 'is'} waiting — or {_edit_total(summary)} authored edits, if "
                "you read them by edit. Mark them reviewed or rejected in **Charts**, **MDims** or "
                "**Explorers**, or write a note on one, and they appear here."
            )
        else:
            st.success(
                "**Nothing to review.** No chart, MDim view or explorer view on this server differs from "
                f"`{BASELINE_NAME}`."
            )
        _footnote()
        return

    # One line of status. It carried a counts line plus two banners plus a caption, and the documents this
    # page exists for started 1,160px down — below the fold on a fresh look, with the last one three
    # screens beyond that. The remainder folds into the line, and a banner is kept only for the two states
    # somebody has to act on.
    bits = [f"**{n_done} item{'s' if n_done != 1 else ''} decided**"]
    if ticked:
        bits.append(f"✅ {len(ticked)}")
    if rejected:
        bits.append(f"❌ {len(rejected)}")
    if noted:
        bits.append(f"📝 {len(noted)}")
    if reopened:
        bits.append(f"♻️ {len(reopened)} reopened")
    bits.append(f"**{finished} of {n_sections} sections finished**")
    bits.append(f"against `{BASELINE_NAME}`")
    st.markdown(" · ".join(bits))

    if reopened:
        st.warning(
            f"**{len(reopened)} decision{'s' if len(reopened) != 1 else ''} reopened** — the text moved "
            "after it was made, so it is not counted here and not in the documents below. Read the item "
            "again in its section and decide on the wording as it stands."
        )

    if rejected:
        # The one thing here that somebody has to act on, so it is the only banner that leads.
        st.error(
            f"**{len(rejected)} rejected** — nothing has been changed by that. Open **What to change** "
            "below: it names the edits and where they were authored, ready to hand on."
        )
    elif not ticked:
        st.warning(
            f"**Nothing decided yet** — {len(noted)} note{'s' if len(noted) != 1 else ''} recorded, but no "
            "item marked reviewed or rejected."
        )
    elif finished >= n_sections and n_sections:
        st.success("**Every section is finished** — each one decided view by view or by edit.")

    # The documents first, and one at a time. They are why this tab is opened, and stacked as three code
    # blocks they were the part nobody could see.
    docs: list[tuple[str, str, str, str, str]] = []
    if rejected:
        docs.append(
            (
                "❌ What to change",
                "The rejections as instructions — the edit each refers to, and the garden dataset it was "
                "authored in. Written to be acted on without the rest of this page. "
                + handover_sentence(rejected, summary),
                _rejections_markdown(rejected, index, summary),
                "metadata-rejections.md",
                "mdd_rejections",
            )
        )
    docs.append(
        (
            "📋 What this branch changed",
            "The metadata edits themselves, grouped as Blast radius groups them. For an issue, a PR "
            "comment or a channel, when the change is what you want to discuss rather than the review.",
            _changes_markdown(summary),
            "metadata-changes.md",
            "mdd_changes_digest",
        )
    )
    docs.append(
        (
            "📝 Review notes",
            "What you decided and wrote, for the same places — kept apart from the digest, since the two "
            "often go to different people.",
            _notes_markdown(rows, index, totals),
            "metadata-review.md",
            "mdd_review_notes",
        )
    )
    for tab, (_label, caption, text, filename, key) in zip(st.tabs([label for label, *_ in docs]), docs):
        with tab:
            st.caption(caption)
            markdown_output(text, filename, key)

    # Then the record itself, as one row per surface: the counts at a glance, and the items behind them.
    # Bordered blocks listing every note put this a screen high before the texts; collapsed rows read as
    # the index they always were, and refused surfaces sort first because they are what needs an answer.
    st.markdown("##### Everything you recorded")
    for surface, group in sorted(_by_surface(rows).items(), key=lambda kv: (not _has_rejection(kv[1]), kv[0])):
        # Reopened rows are excluded from both counts, the same rule the header and the documents apply:
        # a verdict made on wording that has since moved is not a decision about the wording there now.
        live = [row for row in group if not verdict_reopened(row, index)]
        done = sum(1 for row in live if row.get("status") in DECIDED)
        refused = sum(1 for row in live if row.get("status") == REJECTED)
        stale = sum(1 for row in group if verdict_reopened(row, index))
        of = totals.get(surface)
        with_note = [row for row in group if row.get("comment")]
        bare = [row for row in group if not row.get("comment")]
        counted = f"{done} of {of}" if of else str(done)
        marks = " · ".join(
            part
            for part in (
                f"❌ {refused}" if refused else "",
                f"♻️ {stale}" if stale else "",
                f"📝 {len(with_note)}" if with_note else "",
            )
            if part
        )
        label = f"{_surface_title(surface)} — {counted} decided" + (f" · {marks}" if marks else "")
        with st.expander(label, expanded=bool(refused)):
            for row in with_note:
                st.markdown(_row_line(row, index))
                st.markdown(_quoted(str(row["comment"])))
            for row in bare:
                st.markdown(_row_line(row, index))
    _footnote()


def _row_line(row: dict[str, Any], index: dict[str, dict[str, str]]) -> str:
    """One recorded item: reviewed, rejected or merely noted, named, and linked to the thing itself.

    A reopened verdict keeps its icon — it is a record of what was recorded — but says so, since the
    counts and the documents above have already stopped treating it as a decision.
    """
    icon = {REVIEWED: "✅", REJECTED: "❌"}.get(str(row.get("status")), "📝")
    known = index.get(str(row.get("changeKey")))
    when = f" :small[:gray[{row.get('updatedAt')}]]" if row.get("updatedAt") else ""
    if verdict_reopened(row, index):
        when = " :orange-badge[♻️ text changed since]" + when
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
        # `verdict_counts`, not the status alone. The page already says a reopened decision is excluded
        # from the documents below it, and this heading was the one place that went on reporting it as
        # reviewed — a count handed to somebody else, standing over text that was edited afterwards. The
        # notes themselves are unaffected: they are the reviewer's own words and survive whatever happened
        # to the wording.
        done = sum(1 for row in group if row.get("status") == REVIEWED and verdict_counts(row, index))
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


# How to stop one authored edit reaching one surface without reverting it at source. A partial rejection
# asks for an override, not a revert, and each surface is overridden in a different place.
_OVERRIDE_LEVER = {
    "mdims": (
        "override the field on those views in the MDim's export step (`view.metadata[...]` under "
        "`etl/steps/export/multidim/`), which leaves the garden text alone for everything else"
    ),
    "explorers": (
        "set the text on those views in the explorer's own export step (under "
        "`etl/steps/export/explorers/`), which leaves the garden text alone for everything else"
    ),
    "charts": (
        "give those charts their own text — `presentation.grapher_config` in the garden step for a chart "
        "ETL owns, or the chart itself in the admin"
    ),
}


def _edit_lookup(summary: Any) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, int]]]:
    """(change key -> the edit it stands for, edit slot -> the surfaces it reaches and how many texts each).

    A stored row carries a hash, not an edit, so describing a rejection means re-deriving the edits and
    hashing each the way its card did. Cheap: the reach it reads is the cached summary every section uses.

    The second half is what makes a partial rejection legible. `edit_slot` identifies one authored edit
    across sections — it hashes the words, not the surface — so comparing the surfaces an edit *reaches*
    with the ones a reviewer *refused* says whether they want the text gone or only kept off one surface.
    """
    out: dict[str, dict[str, Any]] = {}
    reach: dict[str, dict[str, int]] = {}
    for section in sorted(COUNTED_SECTIONS):
        surface = surface_key("item", f"edit:{section}")
        for edit in edits_for(summary, section):
            change_key, _hash = item_identity(surface, edit_key(edit), edit_fields(edit))
            paths = {p for change in edit.changes for p in (change.catalog_paths or set())}
            slot = edit_slot(edit)
            out[change_key] = {
                "slot": slot,
                "section": section,
                "field": field_label(edit.field),
                "inserted": edit.inserted,
                "deleted": edit.deleted,
                "n_texts": edit.n_texts,
                "datasets": distinct_garden_datasets(paths),
            }
            reach.setdefault(slot, {})[section] = edit.n_texts
    return out, reach


def _rejections_markdown(rejected: list[dict[str, Any]], index: dict[str, dict[str, str]], summary: Any) -> str:
    """The rejections as instructions — what to undo, where it was authored, and why.

    Written in the imperative and addressed to whoever is editing, because that is what a rejection is
    for. Everything needed to act is on the page: the field, the words that moved, the garden dataset the
    edit lives in, and the reviewer's own note where they left one. An item rejected view by view is named
    and linked instead, since there the verdict is about a page rather than an authored edit.
    """
    lines = ["## Metadata changes to revert", ""] + _provenance() + [""]
    lines += [
        f"A reviewer rejected {len(rejected)} of this branch's metadata "
        f"{'change' if len(rejected) == 1 else 'changes'} on the staging server above. Nothing has been "
        "reverted. Rejections are per surface, so some of these ask for the text to go and others only "
        "for it to be kept off one surface — each entry says which.",
        "",
    ]
    edits, reach = _edit_lookup(summary)
    by_edit = [row for row in rejected if str(row.get("changeKey")) in edits]
    by_item = [row for row in rejected if str(row.get("changeKey")) not in edits]

    merged = _merged_edits(by_edit, edits, reach)
    # Whose dataset each entry is — added to the file lines only when the document spans more than one
    # owner. One document, not one per owner: it travels as a single artifact into an issue or a channel,
    # and an entry can legitimately belong to two datasets (the same wording edited in both), which no
    # split could put in one place. What an owner needs is to see which entries are theirs.
    owners = _entry_owners(merged)
    whole = [(edit, notes) for edit, notes in merged if not edit["kept"]]
    partial = [(edit, notes) for edit, notes in merged if edit["kept"]]

    if whole:
        lines += ["### Revert these — refused everywhere they land", ""]
        for edit, notes in whole:
            lines.append(f"- **{edit['field']}** — {_texts_in(edit['refused'])}")
            lines.extend(_words_lines(edit))
            for dataset in edit["datasets"]:
                lines.append(
                    f"  - authored in `{garden_meta_file(dataset)}`{_owned_by(dataset, owners)} — change it there"
                )
            lines.extend(_note_lines(notes))
        lines.append("")

    if partial:
        lines += ["### Keep these, but not everywhere — refused on some surfaces only", ""]
        for edit, notes in partial:
            lines.append(
                f"- **{edit['field']}** — keep on {_texts_in(edit['kept'])}; not wanted on {_texts_in(edit['refused'])}"
            )
            lines.extend(_words_lines(edit))
            for dataset in edit["datasets"]:
                lines.append(
                    f"  - leave `{garden_meta_file(dataset)}`{_owned_by(dataset, owners)} as it is — the text is "
                    "wanted elsewhere"
                )
            for section, _n in edit["refused"]:
                lines.append(f"  - to keep it off {SECTIONS[section][1]}: {_OVERRIDE_LEVER[section]}")
            lines.extend(_note_lines(notes))
        lines.append("")

    if by_item:
        lines += ["### Pages rejected, one at a time", ""]
        for row in by_item:
            known = index.get(str(row.get("changeKey")))
            name = f"[{known['name']}]({known['url']})" if known else "an item no longer in this diff"
            lines.append(f"- {name}")
            if row.get("comment"):
                lines.extend(_bullet_lines(str(row["comment"])))
        lines.append("")

    lines += [
        "_Rejections are recorded on the staging server only. Re-run the affected steps after editing, "
        "then reload Metadata Diff to check what changed._",
    ]
    return "\n".join(lines)


def _merged_edits(
    rows: list[dict[str, Any]], edits: dict[str, dict[str, Any]], reach: dict[str, dict[str, int]]
) -> list[tuple[dict[str, Any], list[str]]]:
    """One instruction per authored edit, with the surfaces refused and the surfaces kept.

    The same sentence reaches charts and MDim views, and it is one card in each section — so rejecting it
    in both records two verdicts about one edit in one file. As an instruction that is a single change,
    and printing it twice invites somebody to look for a second place to make it.

    What the merge decides is *which* instruction. `refused` are the surfaces with a rejection; `kept` are
    the surfaces the edit reaches that nobody refused. An empty `kept` means the text is unwanted outright
    and the garden edit goes. A non-empty one means the reviewer wants the text — just not there — and
    reverting at source would take it away from a surface that asked to keep it, so the refused surfaces
    need an override instead. Different job, different file.
    """
    merged: dict[str, tuple[dict[str, Any], list[str]]] = {}
    for row in rows:
        edit = edits[str(row["changeKey"])]
        slot = str(edit["slot"])
        if slot not in merged:
            merged[slot] = ({**edit, "refused": [], "kept": []}, [])
        entry, notes = merged[slot]
        pair = (edit["section"], edit["n_texts"])
        if pair not in entry["refused"]:
            entry["refused"].append(pair)
        note = str(row.get("comment") or "").strip()
        if note and note not in notes:
            notes.append(note)

    for slot, (entry, _notes) in merged.items():
        refused_sections = {section for section, _n in entry["refused"]}
        entry["kept"] = [(section, n) for section, n in reach.get(slot, {}).items() if section not in refused_sections]
        for bucket in ("refused", "kept"):
            entry[bucket].sort(key=lambda pair: (-pair[1], pair[0]))
    return list(merged.values())


def _texts_in(pairs: list[tuple[str, int]]) -> str:
    """ "16 texts in MDims, 9 in Charts" — the surfaces of one edit, widest first."""
    if not pairs:
        return "nothing"
    lead = f"{pairs[0][1]} text{'s' if pairs[0][1] != 1 else ''} in {SECTIONS[pairs[0][0]][1]}"
    return ", ".join([lead] + [f"{n} in {SECTIONS[section][1]}" for section, n in pairs[1:]])


def _words_lines(edit: dict[str, Any]) -> list[str]:
    """The words that moved — the one thing somebody has to find in the file."""
    if edit["deleted"] and edit["inserted"]:
        return [
            f"  - was: {_quoted_inline(edit['deleted'])}",
            f"  - now: {_quoted_inline(edit['inserted'])}",
        ]
    if edit["inserted"]:
        return [f"  - added: {_quoted_inline(edit['inserted'])}"]
    if edit["deleted"]:
        return [f"  - removed: {_quoted_inline(edit['deleted'])}"]
    return ["  - whitespace only"]


def _note_lines(notes: list[str]) -> list[str]:
    """What the reviewer wrote, as a nested bullet that survives its own newlines."""
    out: list[str] = []
    for note in notes:
        out.append("  - reviewer's note:")
        out.extend(f"  {line}" for line in _bullet_lines(note))
    return out


def _quoted_inline(text: str) -> str:
    """One line of quoted text for an instruction bullet — backticked, and never broken across lines."""
    flat = " ".join(str(text).split())
    trimmed = flat if len(flat) <= 240 else flat[:237].rstrip() + "…"
    # Backticks in the text itself would end the span early; a fenced span with a wider fence survives it.
    fence = "``" if "`" in trimmed else "`"
    return f"{fence}{trimmed}{fence}"


def handover_sentence(rejected: list[dict[str, Any]], summary: Any) -> str:
    """Who to send the rejections to — for the page, deliberately not for the document.

    A rejection is a request, and a request needs an addressee. But the document is what gets *sent*: a
    line inside it reading "Give this to Pablo Arriagada" is addressed to the reviewer and read by Pablo,
    who does not need telling who he is, and by an assistant for whom it is noise. Routing is decided
    before the paste, so it is said beside the copy button and nowhere else.

    Owners come from each affected dataset's own `dataset.owners`. The **first** entry is the accountable
    one, so that is who is named; a secondary owner is not the person to ask first. When the rejections
    span datasets with different owners, each is named with the datasets that are theirs — flattening them
    into one list read as though all of them should act on all of it, and hid whose part was whose.

    Names only, never a handle: nothing in the repo maps one to the other and a guessed handle pings
    somebody uninvolved.
    """
    edits, _reach = _edit_lookup(summary)
    directories = sorted(
        {dataset for row in rejected for dataset in (edits.get(str(row.get("changeKey"))) or {}).get("datasets", [])}
    )
    owners = dataset_owners(directories)

    # Accountable owner -> the datasets they own here, in the order the datasets were listed.
    by_owner: dict[str, list[str]] = {}
    for directory in directories:
        names = owners.get(directory) or []
        if names:
            by_owner.setdefault(names[0], []).append(directory.rsplit("/", 1)[-1])
    unowned = [d.rsplit("/", 1)[-1] for d in directories if not owners.get(d)]
    claude = "or paste it to Claude, which can make the edits and re-run the steps."

    if not by_owner:
        # No owner recorded anywhere, or the files could not be read: still say who can act on it.
        return f"Send it to whoever owns the affected dataset — its `dataset.owners` says who — {claude}"

    if len(by_owner) == 1 and not unowned:
        owner, datasets = next(iter(by_owner.items()))
        what = "the affected dataset" if len(datasets) == 1 else f"all {len(datasets)} affected datasets"
        lead = f"Send it to {owner}, who owns {what}"
    elif len(by_owner) == 1:
        # Something here has no owner, so "the affected dataset" would be one of two. Name theirs.
        owner, datasets = next(iter(by_owner.items()))
        lead = f"Send it to {owner}, who owns {', '.join(datasets)}"
    else:
        # Several owners: each named with their own part, or the sentence asks everybody for everything.
        parts = [f"{owner} ({', '.join(datasets)})" for owner, datasets in by_owner.items()]
        joined = ", ".join(parts[:-1]) + f" and {parts[-1]}"
        lead = f"Send it to {joined} — each owns part of what was rejected"

    if unowned:
        lead += f". No one is recorded as owning {', '.join(unowned)} — check its `dataset.owners`"
    return f"{lead} — {claude}"


def _has_rejection(group: list[dict[str, Any]]) -> bool:
    """Whether a surface holds anything refused — those rows sort first and open first."""
    return any(row.get("status") == REJECTED for row in group)


def _edit_total(summary: Any) -> int:
    """How many authored edits this branch has across the three surfaces — the shorter way to finish."""
    return sum(len(edits_for(summary, section)) for section in COUNTED_SECTIONS)


def _entry_owners(merged: list[tuple[dict[str, Any], list[str]]]) -> dict[str, str]:
    """dataset dir -> its accountable owner, but only when the document spans more than one of them.

    With a single owner the annotation is noise on every line: the page's routing sentence already names
    them, and they are the person reading. With two, the same document reaches two people and neither can
    tell which entries are theirs from the file path alone unless they know the repo well.
    """
    directories = sorted({dataset for edit, _notes in merged for dataset in edit["datasets"]})
    accountable = {directory: names[0] for directory, names in dataset_owners(directories).items() if names}
    return accountable if len(set(accountable.values())) > 1 else {}


def _owned_by(dataset: str, owners: dict[str, str]) -> str:
    """ " (Fiona Spooner)" for a file line, or nothing when the document has a single owner."""
    owner = owners.get(dataset)
    return f" ({owner})" if owner else ""
