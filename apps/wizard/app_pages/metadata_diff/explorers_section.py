"""Explorers section: published explorer views whose text this branch changes.

An explorer view's text is compared from its **resolved** chart config (`chart_configs.full`), which is
what the site serves. That resolution already includes what the view inherits from indicator metadata —
`title_public` feeds the title, `description_short` the subtitle — so a garden-authored edit shows up
here without this module having to re-resolve any metadata.

Two limits are stated in the UI rather than papered over:

- Explorer views render **no data page**, so a WYSK / `description_key` edit is invisible to their
  readers. It is not a change this section can show, and that absence is itself the finding.
- `full` is written when the explorer's export step runs. If indicator metadata changed but the explorer
  has not been rebuilt on this server, its stored text is still the old one and nothing appears here.
"""

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
from apps.wizard.app_pages.metadata_diff import cached, datapage
from apps.wizard.app_pages.metadata_diff.core import ViewDiff, dims_str, field_label, group_changes
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME, DIFF_CSS
from apps.wizard.app_pages.metadata_diff.review_state import (
    n_reviewed,
    resolve_marks,
    st_reviewed_toggle,
    surface_key,
)
from apps.wizard.utils.components import Pagination

EXPLORERS_PER_PAGE = 4
MAX_INLINE_CHANGES = 4


def st_show_explorer_metadata_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the Explorers section: each published explorer whose view text changed."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)

    try:
        changed = cached.explorer_changes(source_engine, target_engine)
    except Exception as e:  # noqa: BLE001 — an old staging server may not have `explorer_views` yet
        st.error(f"Could not read explorer views from one of the environments: {e}")
        return

    _scope_caption()

    branch = changed.branch_views()
    other = changed.other_views()

    if not changed.narrowed:
        st.warning(
            "Could not read this branch's changed files from git, so this list is **not narrowed to your "
            "branch** — it will include explorers that master has rebuilt since this server was created."
        )

    if not branch:
        st.success(f"**No explorer view text changed by this branch** (against {BASELINE_NAME}).")
    else:
        total_views = sum(len(v) for v in branch.values())
        st.markdown(
            f"**{len(branch)} explorer{'s' if len(branch) != 1 else ''}** with "
            f"**{total_views} changed view{'s' if total_views != 1 else ''}**."
        )
        slugs = sorted(branch)
        pagination = Pagination(slugs, items_per_page=EXPLORERS_PER_PAGE, pagination_key="mdd-explorers-pagination")
        if len(slugs) > EXPLORERS_PER_PAGE:
            pagination.show_controls()
        for slug in pagination.get_page_items():
            _render_explorer(source_engine, slug, branch[slug])
        if len(slugs) > EXPLORERS_PER_PAGE:
            pagination.show_controls(position="bottom")

    _render_other(other)


def _render_other(other: dict[str, list[ViewDiff]]) -> None:
    """Explorers that differ from the baseline but not because of this branch — listed, never hidden.

    Almost always master having rebuilt them after this staging server was created. They are not the
    reviewer's job, but "there are other differences" is still worth being able to see.
    """
    if not other:
        return
    n_views = sum(len(v) for v in other.values())
    with st.expander(f"🕓 {len(other)} other explorer(s) differ from {BASELINE_NAME} — not from this branch"):
        st.caption(
            "Their views differ, but neither their export recipe nor the datasets behind the changed views "
            "are touched by this branch — normally master having moved on since this server was created. "
            "Listed for completeness."
        )
        st.markdown("\n".join(f"- `{slug}` — {len(diffs)} view(s)" for slug, diffs in sorted(other.items())))
        st.caption(f"{n_views} views in total.")


def _scope_caption() -> None:
    st.caption(
        "Compared from each view's **resolved** config, so text inherited from indicator metadata is "
        "included. Two things this cannot show: explorer views have **no data page**, so a WYSK edit never "
        "reaches their readers; and a view's stored text only refreshes when the explorer's export step "
        "re-runs on this server. Legacy CSV-backed explorers have no view rows at all."
    )


def _render_explorer(source_engine: Engine, slug: str, diffs: list[ViewDiff]) -> None:
    """One explorer: its changed views, grouped by distinct text change."""
    groups = group_changes(diffs)
    surface = surface_key("explorer", slug)
    marks = resolve_marks(source_engine, surface, groups)

    with st.container(border=True):
        st.markdown(
            f"**`{slug}`** :gray-badge[{len(diffs)} view{'s' if len(diffs) != 1 else ''}] "
            f":small[:gray[{n_reviewed(marks)}/{len(marks)} reviewed]]"
        )
        st.markdown(
            f"[{BASELINE_NAME} ↗]({TARGET.site}/explorers/{slug}) · "
            f"[this staging server ↗]({SOURCE.site}/admin/explorers/preview/{slug})"
        )

        for mark in marks[:MAX_INLINE_CHANGES]:
            g = mark.group
            st.markdown(
                f"{mark.icon} **{field_label(g.field)}** "
                f":small[:gray[{len(g.view_dims)} view{'s' if len(g.view_dims) != 1 else ''}]]"
            )
            with st.popover(f"Views ({len(g.view_dims)})", width="content"):
                st.markdown("\n".join(f"- {dims_str(d)}" for d in g.view_dims[:40]))
                if len(g.view_dims) > 40:
                    st.caption(f"… and {len(g.view_dims) - 40} more.")
            datapage.st_datapage_diff(
                {g.field: {"old": g.old, "new": g.new}},
                baseline_label=BASELINE_NAME.capitalize(),
                staging_label="This staging server",
                show_unchanged_slots=False,
            )
            st_reviewed_toggle(source_engine, surface, mark)

        if len(marks) > MAX_INLINE_CHANGES:
            st.caption(
                f"… and {len(marks) - MAX_INLINE_CHANGES} more change(s) in this explorer. "
                "The full TSV diff is in **Chart Diff → Explorers**."
            )
