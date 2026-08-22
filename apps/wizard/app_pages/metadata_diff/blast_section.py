"""Blast radius: everywhere this branch's metadata edits land, across all three surfaces.

The three review sections each keep to their own surface — a chart change is not repeated on an MDim card
and vice versa, because a reviewer working through one surface should not be counting the same edit twice.
This section is the deliberate exception: it is the one place where crossing surfaces is the point, so an
author can see what one edit costs before deciding how careful to be with it.

Two ways to read the same data, because there are two questions:

- **by change** — how far does this edit go? The review question, and the same unit every other section
  uses (one distinct text, however many places render it).
- **by surface** — what happens to this page? The question you have when one particular chart matters to
  you, and the only view where a chart carrying two separate edits appears as one thing.

It reads the cached summary the section badges already use, so switching to it costs no queries.
"""

from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.metadata_diff import cached
from apps.wizard.app_pages.metadata_diff.core import diff_window_html, field_label
from apps.wizard.app_pages.metadata_diff.discovery import ChangeReach, reach_by_surface
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME, DIFF_CSS
from apps.wizard.utils.components import url_persist

GROUP_KEY = "blast-group"
MAX_ROWS = 60
KIND_ICON = {"chart": "📈", "draft_chart": "📝", "mdim": "🧩", "explorer": "🧭"}


def st_show_blast_radius(source_engine: Engine, target_engine: Engine) -> None:
    """Everywhere the branch's metadata changes land, by change or by surface."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)
    summary = cached.summary(source_engine, target_engine)
    reach = summary.reach

    if not reach:
        st.success(f"**Nothing to show:** no metadata text on this server differs from `{BASELINE_NAME}`.")
        return

    readers = sum(r.n_reader_facing for r in reach)
    hidden = sum(r.n_hidden for r in reach)
    head = (
        f"**{len(reach)} distinct text change{'s' if len(reach) != 1 else ''}** landing in "
        f"**{readers} place{'s' if readers != 1 else ''}** a reader can reach"
    )
    if hidden:
        head += f" · {hidden} in unpublished charts or views"
    st.markdown(head)
    st.caption(
        "Counted per distinct text, not per sighting: one reworded shared definition reaches its "
        "indicator's charts, every MDim view rendering it and every explorer view — one edit to judge, "
        "several audiences. Sign-off lives in the three sections; this view is for seeing the spread."
    )

    if not summary.mdims_resolved:
        st.warning(
            "Too many changed MDims to diff view by view, so the MDim rows below are incomplete — the "
            "same ceiling the MDims badge reports."
        )

    grouping = url_persist(st.segmented_control)(
        label="Group by",
        options=["change", "surface"],
        format_func=lambda g: {"change": "📄 By change", "surface": "🎯 By surface"}[g],
        key=GROUP_KEY,
        value="change",
        label_visibility="collapsed",
    )

    if grouping == "surface":
        _by_surface(reach)
    else:
        _by_change(reach)


def _by_change(reach: list[ChangeReach]) -> None:
    """One row per changed text, with every surface it reaches underneath."""
    for r in reach[:MAX_ROWS]:
        with st.container(border=True):
            st.markdown(f"**{field_label(r.field)}** · reaches **{r.n_reader_facing}** reader-facing place(s)")
            st.markdown(
                f'<div class="mdd-diff">{_preview(r)}</div>',
                unsafe_allow_html=True,
            )
            for line in _reach_lines(r):
                st.markdown(line)
    _truncation_note(len(reach))


def _reach_lines(r: ChangeReach) -> list[str]:
    """The surfaces one change lands on, most prominent first, each naming what it is."""
    lines: list[str] = []
    on_page = [c for c in r.charts if c.get("has_data_page", True)]
    drawer = [c for c in r.charts if not c.get("has_data_page", True)]
    if on_page:
        lines.append(f"- 📈 **{len(on_page)}** data page(s): {_names(on_page)}")
    if drawer:
        lines.append(f"- 🔍 **{len(drawer)}** chart(s) via *Learn more about this data*: {_names(drawer)}")
    for m in sorted(r.mdims, key=lambda m: str(m["catalogPath"])):
        draft = " :orange-badge[unpublished]" if m["is_draft"] else ""
        lines.append(f"- 🧩 `{m['catalogPath']}` — {m['n_views']} view(s){draft}")
    for e in sorted(r.explorers, key=lambda e: str(e["slug"])):
        lines.append(f"- 🧭 explorer `{e['slug']}` — {e['n_views']} view(s)")
    if r.draft_charts:
        lines.append(f"- 📝 **{len(r.draft_charts)}** unpublished chart(s): {_names(r.draft_charts)}")
    if not lines:
        # Worth saying rather than leaving blank: a real change nobody can currently see is a finding.
        lines.append("- Nothing renders this text yet — no published chart, MDim view or explorer view.")
    return lines


def _names(charts: list[dict[str, Any]], limit: int = 6) -> str:
    slugs = sorted(str(c.get("slug") or f"chart {c.get('chartId')}") for c in charts)
    shown = ", ".join(f"`{s}`" for s in slugs[:limit])
    return shown if len(slugs) <= limit else f"{shown} … +{len(slugs) - limit}"


def _by_surface(reach: list[ChangeReach]) -> None:
    """One row per affected chart / MDim / explorer, with the changes landing on it."""
    rows = reach_by_surface(reach)
    st.caption(f"{len(rows)} affected surface(s). A page listed twice under *By change* appears once here.")
    for row in rows[:MAX_ROWS]:
        icon = KIND_ICON.get(row["kind"], "•")
        # "WYSK ×2" rather than a bare "WYSK": two distinct edits on one page is the finding here.
        counts = row.get("field_counts") or {label: 1 for label in row["fields"]}
        fields = ", ".join(f"{label} ×{n}" if n > 1 else label for label, n in sorted(counts.items()))
        badge = "" if row["published"] else " :orange-badge[unpublished]"
        st.markdown(f"- {icon} `{row['name']}` — {fields} :small[:gray[({row['detail']})]]{badge}")
    _truncation_note(len(rows))


def _truncation_note(total: int) -> None:
    if total > MAX_ROWS:
        st.caption(f"Showing the first {MAX_ROWS} of {total}. The three sections list them all, per surface.")


def _preview(r: ChangeReach) -> str:
    """A one-line preview of the edit, windowed on what changed — two rows can share a field name."""
    return diff_window_html(r.old, r.new, max_chars=260)
