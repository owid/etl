"""Review: everything you ticked or wrote a note about, in one place.

The three surface sections are where reading happens, one item at a time. This is the other half — what
came out of it, gathered across charts, MDim views and explorer views, named, linked, and counted against
how many there were.

Nothing here is computed twice: the rows come from one query, and the names come from the same cached
enumerations the sections use. A stored row carries a hash rather than a name (the slot has to survive an
edit to the text), so the index is rebuilt by hashing each known item the same way and matching.
"""

from typing import Any
from urllib.parse import urlencode

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.app_pages.metadata_diff import cached
from apps.wizard.app_pages.metadata_diff.core import dims_str, item_identity
from apps.wizard.app_pages.metadata_diff.data import REVIEWED, load_item_notes
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME, markdown_output, view_label, view_url
from apps.wizard.app_pages.metadata_diff.review_state import surface_key


def st_show_review(source_engine: Engine, target_engine: Engine) -> None:
    """What this pass produced: ticks and notes, named and linked, against the number of items there are."""
    rows = load_item_notes(source_engine)
    index, totals = _item_index(source_engine, target_engine)
    total = sum(totals.values())
    ticked = [r for r in rows if r.get("status") == REVIEWED]
    noted = [r for r in rows if r.get("comment")]

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

    markdown_output(_markdown(rows, index, totals), "metadata-review.md", "mdd_review_notes")


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


def _item_index(source_engine: Engine, target_engine: Engine) -> tuple[dict[str, dict[str, str]], dict[str, int]]:
    """change key -> {name, url}, plus how many items each surface holds.

    Built by enumerating the same items the sections show and hashing each one the way its tick was
    hashed. An enumeration that fails is skipped rather than guessed at: a total that quietly omits a
    surface is worse than one that is conservative, and the sections report their own ceilings.
    """
    index: dict[str, dict[str, str]] = {}
    totals: dict[str, int] = {}

    # --- MDim views ---
    try:
        df = cached.mdim_changes(source_engine, target_engine)
        flagged = [str(cp) for cp in df.index[df["in_branch"] & df["has_changes"]]]
    except Exception:  # noqa: BLE001
        flagged, df = [], None
    for catalog_path in flagged:
        assert df is not None
        row = df.loc[catalog_path]
        surface = surface_key("item", f"mdim:{catalog_path}")
        try:
            title, dimensions, view_diffs = cached.mdim_view_diffs(
                catalog_path,
                source_engine,
                target_engine,
                cache_key=f"{row['configMd5_source']}::{row['configMd5_target']}",
            )
        except Exception:  # noqa: BLE001
            continue
        slug = str(row["slug_source"]) if row.get("slug_source") else ""
        changed = [v for v in view_diffs if v.changed]
        totals[surface] = len(changed)
        for view in changed:
            key, _ = item_identity(surface, dims_str(view.dimensions), {})
            index[key] = {
                "name": f"{title or catalog_path} — {view_label(view, dimensions)}",
                "url": view_url(SOURCE, catalog_path, None if row["is_draft"] else slug, view.dimensions),
            }

    # --- Explorer views ---
    try:
        changes = cached.explorer_changes(source_engine, target_engine)
        branch = changes.branch_views()
    except Exception:  # noqa: BLE001
        branch = {}
    for explorer_slug, diffs in branch.items():
        surface = surface_key("item", f"explorer:{explorer_slug}")
        changed = [d for d in diffs if d.changed]
        totals[surface] = len(changed)
        for view in changed:
            key, _ = item_identity(surface, dims_str(view.dimensions), {})
            label = " · ".join(str(v) for v in view.dimensions.values()) or "(view)"
            index[key] = {
                "name": f"{explorer_slug} — {label}",
                "url": f"{SOURCE.site}/explorers/{explorer_slug}?{urlencode(view.dimensions)}",
            }

    # --- Charts ---
    surface = surface_key("item", "chart")
    try:
        counts = cached.changed_charts(source_engine, target_engine)
    except Exception:  # noqa: BLE001
        counts = {}
    totals[surface] = len(counts)
    for chart_slug, n_changes in counts.items():
        key, _ = item_identity(surface, chart_slug, {})
        index[key] = {
            "name": f"{chart_slug} ({n_changes} change{'s' if n_changes != 1 else ''})",
            "url": f"{SOURCE.site}/grapher/{chart_slug}",
        }
    return index, totals


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
    for prefix, name in (("mdim:", "MDim"), ("explorer:", "Explorer"), ("chart", "Charts")):
        if body.startswith(prefix):
            rest = body.removeprefix(prefix)
            return f"{name} `{rest}`" if rest else name
    return body


def _markdown(rows: list[dict[str, Any]], index: dict[str, dict[str, str]], totals: dict[str, int]) -> str:
    """The notes as markdown, for pasting into the PR — named and linked, like the page."""
    lines = ["## Metadata review notes", "", f"Against `{BASELINE_NAME}`.", ""]
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
