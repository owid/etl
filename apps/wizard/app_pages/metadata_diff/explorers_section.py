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
from apps.wizard.app_pages.metadata_diff import cached, datapage, edits_view, view_nav
from apps.wizard.app_pages.metadata_diff.core import ViewDiff, dims_str
from apps.wizard.app_pages.metadata_diff.data import load_item_notes, load_reviews
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME, DIFF_CSS, st_layout_switcher
from apps.wizard.app_pages.metadata_diff.review_state import (
    item_marker,
    resolve_item_mark,
    st_review_strip,
    surface_key,
    surface_progress,
)

# Which explorer the item view is showing, and the keys its ⚡ jump and menu use. Distinct from the MDims
# section's prefix on purpose: the two must never read each other's dimension selections.
EXPLORER_KEY = view_nav.EXPLORER_VIEWS_KEY
JUMP_KEY = "explorer-view-jump"
DIM_PARAM_PREFIX = view_nav.EXPLORER_DIM_PREFIX


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
        col_layout, col_reject = st.columns([4, 1], vertical_alignment="center")
        with col_layout:
            layout = st_layout_switcher(
                "🔍 View by view",
                "**View by view** lists every changed view of every affected explorer, with its diffs",
            )
        with col_reject:
            edits_view.st_reject_all(source_engine, cached.summary(source_engine, target_engine), "explorers")
        if layout == "items":
            _explorer_browser(source_engine, branch)
            _render_other(other)
            return

        # One card per authored edit. Cards keyed on the exact text showed one reworded subtitle 348
        # times, because each of the explorer's views words it a little differently.
        edits_view.st_edit_cards(
            source_engine, target_engine, cached.summary(source_engine, target_engine), "explorers"
        )

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
        # As in the MDims browser: a route that names nothing on this branch does not stay in the URL.
        st.query_params.pop(EXPLORER_KEY, None)
        current = slugs[0]
    st.session_state[EXPLORER_KEY] = current
    position = slugs.index(current)

    if len(slugs) > 1:
        # One query for every recorded row, so the picker says which explorers you have already been through.
        recorded_rows = load_item_notes(source_engine)

        def explorer_label(slug: str) -> str:
            views = f"{len(branch[slug])} changed view{'s' if len(branch[slug]) != 1 else ''}"
            progress = surface_progress(recorded_rows, surface_key("item", f"explorer:{slug}"))
            return f"{slug} · {views}" + (f" · {progress}" if progress else "")

        col_pick, col_next = st.columns([4, 1], vertical_alignment="bottom")
        with col_pick:
            st.selectbox(
                f"Explorer {position + 1} of {len(slugs)} changed by this branch",
                options=slugs,
                index=position,
                format_func=explorer_label,
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
    _render_explorer_view(source_engine, slug, view, labels[changed.index(view)], recorded)


def _render_explorer_view(
    source_engine: Engine, slug: str, view: ViewDiff, label: str, recorded: dict | None = None
) -> None:
    """One explorer view: its name and both servers' links, its tick, then its diffs."""
    with st.container(border=True):
        n = len(view.fields)
        st.markdown(f"**{label}** :small[:gray[{n} field{'s' if n != 1 else ''} changed]]")
        query = urlencode(view.dimensions)
        st.markdown(
            f":gray[**{BASELINE_NAME.capitalize()}**] [view ↗]({TARGET.site}/explorers/{slug}?{query}) · "
            f":green[**This staging server**] [view ↗]({SOURCE.site}/explorers/{slug}?{query})"
        )
        surface = surface_key("item", f"explorer:{slug}")
        # Already read to mark the jump's options; reading it again is a second query per rerun.
        stored = recorded if recorded is not None else load_reviews(source_engine, surface)
        mark = resolve_item_mark(stored, surface, dims_str(view.dimensions), view.fields)
        st_review_strip(source_engine, surface, mark)
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
