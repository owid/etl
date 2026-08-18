"""Show a diff the way the reader meets it: in data-page order, labelled by slot.

Ed's point on the review UI was that a list of field names (`descriptionShort`, `titlePublic`) makes a
reviewer translate before they can judge. So each changed text is placed where it appears on the page —
title and subtitle above the chart, footnote under it, "What you should know" below — with an empty
rectangle standing in for the chart itself.

Deliberately *not* a facsimile of the data page. A convincing mock would imply a fidelity it cannot
have (per-chart shielding, dimension branching, inheritance), and it would crowd out the blast radius,
which is the thing this tool knows that the real page does not. The real page stays one click away on
each column header.
"""

from dataclasses import dataclass
from typing import Any

import streamlit as st

from apps.wizard.app_pages.metadata_diff.core import CHART_FIELD_PREFIX
from apps.wizard.app_pages.metadata_diff.render import render_text_html

# The slots a reader actually sees, top to bottom, and which diff field feeds each one.
# `region` places the slot relative to the chart: above it, under it (the footnote), or further down
# the page. Chart-config fields sit next to the indicator field they override, because a chart setting
# its own title wins over the indicator's `title_public`.
SLOTS: list[tuple[str, str, str]] = [
    ("titlePublic", "Title", "above"),
    ("chart.title", "Title (chart config)", "above"),
    ("descriptionShort", "Subtitle", "above"),
    ("chart.subtitle", "Subtitle (chart config)", "above"),
    ("chart.note", "Footnote", "under"),
    ("descriptionKey", "What you should know about this data", "below"),
    ("descriptionProcessing", "How we process data at Our World in Data", "below"),
    ("descriptionFromProducer", "About this data (from the producer)", "below"),
]

CHART_PLACEHOLDER = '<div class="mdd-chart-slot">DATA HERE</div>'


@dataclass
class Slot:
    field: str
    label: str
    region: str
    changed: bool

    @property
    def is_chart_config(self) -> bool:
        return self.field.startswith(CHART_FIELD_PREFIX)


def ordered_slots(field_names: list[str] | set[str] | dict[str, Any], include_unchanged: bool = True) -> list[Slot]:
    """The page's slots in reading order, flagged by whether this diff touches each one.

    Fields we don't have a slot for (a new metadata field, say) are appended at the end rather than
    dropped: an unplaced change must still be visible.
    """
    changed = set(field_names)
    slots = [Slot(field=f, label=label, region=region, changed=f in changed) for f, label, region in SLOTS]
    known = {f for f, _, _ in SLOTS}
    slots += [Slot(field=f, label=f, region="below", changed=True) for f in sorted(changed - known)]
    if not include_unchanged:
        slots = [s for s in slots if s.changed]
    return slots


def st_datapage_diff(
    fields: dict[str, dict[str, Any]],
    baseline_label: str,
    staging_label: str,
    baseline_url: str | None = None,
    staging_url: str | None = None,
    changed_only: bool = True,
    show_unchanged_slots: bool = True,
) -> None:
    """Two columns — baseline and staging — each laid out in data-page order.

    `fields` is a ViewDiff's `fields` dict (field -> {"old", "new"}). Slots with no change collapse to a
    thin greyed line, so position stays legible without spending vertical space on unchanged text.
    """
    slots = ordered_slots(fields, include_unchanged=show_unchanged_slots)
    if not slots:
        st.caption("No changes to show.")
        return

    col_old, col_new = st.columns(2)
    header_old = f":gray[**{baseline_label}**]"
    header_new = f":green[**{staging_label}**]"
    if baseline_url:
        header_old += f" · [page ↗]({baseline_url})"
    if staging_url:
        header_new += f" · [page ↗]({staging_url})"

    with col_old:
        st.markdown(header_old)
        st.markdown(_side_html(slots, fields, side="old", changed_only=changed_only), unsafe_allow_html=True)
    with col_new:
        st.markdown(header_new)
        st.markdown(_side_html(slots, fields, side="new", changed_only=changed_only), unsafe_allow_html=True)


def _side_html(slots: list[Slot], fields: dict[str, dict[str, Any]], side: str, changed_only: bool) -> str:
    """One column: the page's slots in order, with the chart placeholder between title and footnote."""
    parts: list[str] = []
    for region in ("above", "under", "below"):
        if region == "under":
            # The chart sits below the title/subtitle and above the footnote.
            parts.append(CHART_PLACEHOLDER)
        for slot in (s for s in slots if s.region == region):
            if not slot.changed:
                parts.append(f'<div class="mdd-slot-unchanged">{slot.label} — unchanged</div>')
                continue
            change = fields.get(slot.field, {})
            value = change.get("new") if side == "new" else change.get("old")
            other = change.get("old") if side == "new" else change.get("new")
            parts.append(f'<div class="mdd-slot">{slot.label}</div>')
            parts.append(render_text_html(value, other, side=side, changed_only=changed_only))
    return "".join(parts)
