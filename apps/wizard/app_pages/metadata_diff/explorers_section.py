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

from urllib.parse import urlencode

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
from apps.wizard.app_pages.metadata_diff import cached, datapage, view_nav
from apps.wizard.app_pages.metadata_diff.core import ViewDiff, dims_str, field_label, group_changes
from apps.wizard.app_pages.metadata_diff.data import load_reviews
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME, DIFF_CSS, st_layout_switcher
from apps.wizard.app_pages.metadata_diff.review_state import (
    item_marker,
    resolve_item_mark,
    resolve_marks,
    st_item_note,
    st_reviewed_toggle,
    surface_key,
)
from apps.wizard.utils.components import Pagination

EXPLORERS_PER_PAGE = 4
# Changes shown open per explorer. Past this the card stops being readable, so the rest fold into an
# expander — with their Reviewed toggles, because the `n/N reviewed` counter above counts every change and
# one without a toggle is a counter that can never reach completion. Chart Diff's TSV can show the text of
# the folded ones but cannot tick them here, so it was never the hand-off it looked like.
MAX_INLINE_CHANGES = 4
# Which explorer the item view is showing, and the keys its ⚡ jump and menu use. Distinct from the MDims
# section's prefix on purpose: the two must never read each other's dimension selections.
EXPLORER_KEY = "explorer-views"
JUMP_KEY = "explorer-view-jump"
DIM_PARAM_PREFIX = "edim-"


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
        layout = st_layout_switcher(
            "🔍 View by view",
            "**View by view** lists every changed view of every affected explorer, with its diffs",
        )
        if layout == "items":
            _explorer_browser(source_engine, branch)
            _render_other(other)
            return

        slugs = sorted(branch)
        pagination = Pagination(slugs, items_per_page=EXPLORERS_PER_PAGE, pagination_key="mdd-explorers-pagination")
        if len(slugs) > EXPLORERS_PER_PAGE:
            pagination.show_controls()
        for slug in pagination.get_page_items():
            _render_explorer(source_engine, slug, branch[slug])
        if len(slugs) > EXPLORERS_PER_PAGE:
            pagination.show_controls(position="bottom")

    _render_other(other)


def _explorer_browser(source_engine: Engine, branch: dict[str, list[ViewDiff]]) -> None:
    """Pick an explorer, then read it view by view — the MDims browser, on explorer data.

    Explorers differ in one way that matters: they publish no dimension list, so the menu's columns are
    inferred from the views themselves. Everything else is the same, deliberately — the two sections ask
    the same question and had no business answering it differently.
    """
    slugs = sorted(branch)
    current = str(st.session_state.get(EXPLORER_KEY) or st.query_params.get(EXPLORER_KEY) or "").strip()
    if current not in slugs:
        current = slugs[0]
    st.session_state[EXPLORER_KEY] = current
    position = slugs.index(current)

    if len(slugs) > 1:
        col_pick, col_next = st.columns([4, 1], vertical_alignment="bottom")
        with col_pick:
            st.selectbox(
                f"Explorer {position + 1} of {len(slugs)} changed by this branch",
                options=slugs,
                index=position,
                format_func=lambda slug: (
                    f"{slug} · {len(branch[slug])} changed view{'s' if len(branch[slug]) != 1 else ''}"
                ),
                key="mdd-explorer-picker",
                on_change=_pick_explorer,
                help="Type to search the affected explorers.",
            )
        with col_next:
            st.button(
                "Next explorer ▶",
                key="mdd-explorer-next",
                on_click=_step_explorer,
                args=(slugs, position + 1),
                width="stretch",
                help="The next affected explorer, wrapping round at the end.",
            )

    _explorer_views(source_engine, current, branch[current])


def _pick_explorer() -> None:
    """The picker's choice becomes the URL."""
    slug = str(st.session_state.get("mdd-explorer-picker") or "").strip()
    if slug:
        st.session_state[EXPLORER_KEY] = slug
        st.query_params[EXPLORER_KEY] = slug
        _reset_view_selection()


def _step_explorer(slugs: list[str], index: int) -> None:
    """Step to the next explorer, wrapping."""
    slug = slugs[index % len(slugs)]
    st.session_state[EXPLORER_KEY] = slug
    st.session_state["mdd-explorer-picker"] = slug
    st.query_params[EXPLORER_KEY] = slug
    _reset_view_selection()


def _reset_view_selection() -> None:
    """Drop the previous explorer's ⚡ jump and menu — its dimensions mean nothing in the next one."""
    st.session_state.pop(JUMP_KEY, None)
    for key in [k for k in list(st.session_state.keys()) if isinstance(k, str) and k.startswith(DIM_PARAM_PREFIX)]:
        st.session_state.pop(key, None)
    for key in [k for k in list(st.query_params.keys()) if k.startswith(DIM_PARAM_PREFIX)]:
        st.query_params.pop(key, None)


def _explorer_views(source_engine: Engine, slug: str, diffs: list[ViewDiff]) -> None:
    """One explorer, one view at a time: the ⚡ jump, the inferred menu, and that view's diffs."""
    changed = [d for d in diffs if d.changed]
    st.markdown(f"### `{slug}`")
    st.caption(f"{len(changed)} changed view{'s' if len(changed) != 1 else ''} in this explorer")
    if not changed:
        st.info("No view of this explorer renders a text this branch changed.")
        return

    dimensions = view_nav.dimensions_from_views(changed)
    item_surface = surface_key("item", f"explorer:{slug}")
    recorded = load_reviews(source_engine, item_surface)
    labels = [
        item_marker(recorded, item_surface, dims_str(view.dimensions))
        + (" · ".join(str(v) for v in view.dimensions.values()) or "(view)")
        for view in changed
    ]

    url_selection = view_nav.url_selection(dimensions, DIM_PARAM_PREFIX)
    position = view_nav.displayed_index(changed, url_selection)
    view_nav.st_jump_and_next(changed, labels, position, DIM_PARAM_PREFIX, JUMP_KEY, "view")
    selection = view_nav.st_dimension_menu(changed, dimensions, DIM_PARAM_PREFIX)

    candidates = [v for v in changed if view_nav.matches(v, selection)] if selection else changed
    if not candidates:
        st.info("No changed view matches the menu above. Clear a dimension to widen it.")
        return
    view = candidates[0]
    shown = changed.index(view) + 1
    st.caption(f"Changed view {shown} of {len(changed)} — **Next change ▶** steps through them.")
    _render_explorer_view(source_engine, slug, view, labels[changed.index(view)])


def _render_explorer_view(source_engine: Engine, slug: str, view: ViewDiff, label: str) -> None:
    """One explorer view: its name and both servers' links, its tick, then its diffs."""
    with st.container(border=True):
        n = len(view.fields)
        col_head, col_review = st.columns([4, 1], vertical_alignment="center")
        with col_head:
            st.markdown(f"**{label}** :small[:gray[{n} field{'s' if n != 1 else ''} changed]]")
            query = urlencode(view.dimensions)
            st.markdown(
                f":gray[**{BASELINE_NAME.capitalize()}**] [view ↗]({TARGET.site}/explorers/{slug}?{query}) · "
                f":green[**This staging server**] [view ↗]({SOURCE.site}/explorers/{slug}?{query})"
            )
        with col_review:
            surface = surface_key("item", f"explorer:{slug}")
            mark = resolve_item_mark(
                load_reviews(source_engine, surface), surface, dims_str(view.dimensions), view.fields
            )
            st_reviewed_toggle(source_engine, surface, mark)
        st_item_note(source_engine, surface, mark)
        datapage.st_datapage_diff(
            view.fields,
            baseline_label=BASELINE_NAME.capitalize(),
            staging_label="This staging server",
            show_unchanged_slots=False,
        )


def _render_other(other: dict[str, list[ViewDiff]]) -> None:
    """Explorer views that differ from the baseline but not because of this branch — listed, never hidden.

    Almost always master having rebuilt the explorer after this staging server was created. They are not
    the reviewer's job, but "there are other differences" is still worth being able to see. Attribution is
    per view, so an explorer with a handful of our views and hundreds of lagging ones appears in both
    lists — with only its own views counted above.
    """
    if not other:
        return
    n_views = sum(len(v) for v in other.values())
    with st.expander(f"🕓 {n_views} other explorer view(s) differ from {BASELINE_NAME} — not from this branch"):
        st.caption(
            "These views differ, but neither their explorer's export recipe nor the indicators they render "
            "are touched by this branch — normally master having moved on since this server was created. "
            "Listed for completeness."
        )
        st.markdown("\n".join(f"- `{slug}` — {len(diffs)} view(s)" for slug, diffs in sorted(other.items())))
        st.caption(f"Across {len(other)} explorer(s).")


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
        st.markdown(f"**`{slug}`** :gray-badge[{len(diffs)} view{'s' if len(diffs) != 1 else ''}] ")
        st.markdown(
            f"[{BASELINE_NAME} ↗]({TARGET.site}/explorers/{slug}) · "
            f"[this staging server ↗]({SOURCE.site}/admin/explorers/preview/{slug})"
        )

        for mark in marks[:MAX_INLINE_CHANGES]:
            _render_change(source_engine, surface, mark)

        folded = marks[MAX_INLINE_CHANGES:]
        if folded:
            with st.expander(f"… {len(folded)} more change(s) in this explorer"):
                for mark in folded:
                    _render_change(source_engine, surface, mark)


def _render_change(source_engine: Engine, surface: str, mark) -> None:
    """One distinct text change of an explorer, with the views it lands on and its reviewed toggle."""
    g = mark.group
    st.markdown(
        f"**{field_label(g.field)}** :small[:gray[{len(g.view_dims)} view{'s' if len(g.view_dims) != 1 else ''}]]"
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
