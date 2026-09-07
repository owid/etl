"""Navigating one surface's changed views: the ⚡ jump, Next change ▶, and the dimension menu.

Shared by the MDims and Explorers sections, which ask the same question of different data. Everything here
is keyed by a caller-supplied prefix so the two never read each other's selections, and everything writes
to the URL as well as to session state so a single view stays a link somebody else can open.
"""

from typing import Any
from urllib.parse import urlencode

import streamlit as st

from apps.wizard.app_pages.metadata_diff.core import ViewDiff
from apps.wizard.utils.components import url_persist

# The URL keys the two browsers route on: which MDim or explorer is open, and the per-dimension selection
# within it. Kept here rather than in the sections so a card anywhere in the tool can build a link to one
# view without importing the section that renders it. The two prefixes differ on purpose: the sections
# must never read each other's dimension selections.
MDIM_VIEWS_KEY = "mdim-views"
MDIM_DIM_PREFIX = "dim-"
EXPLORER_VIEWS_KEY = "explorer-views"
EXPLORER_DIM_PREFIX = "edim-"
# Blast radius: how it groups, and — set by a By-edit card — the one edit to focus on.
BLAST_GROUP_KEY = "blast-group"
BLAST_EDIT_KEY = "blast-edit"


def mdim_view_link(catalog_path: str, dims: dict[str, str] | None = None) -> str:
    """This tool's View-by-view page, opened on one MDim — and on one of its views when `dims` is given.

    Relative, like `chart_review_url`: a query-only href keeps whatever host the reader is on. The
    catalogPath's `#` is percent-encoded by `urlencode`, or the browser would read it as a fragment.
    """
    params = {"diff-type": "mdims", MDIM_VIEWS_KEY: catalog_path}
    params.update({MDIM_DIM_PREFIX + slug: choice for slug, choice in (dims or {}).items()})
    return "?" + urlencode(params)


def explorer_view_link(slug: str, dims: dict[str, str] | None = None) -> str:
    """This tool's View-by-view page, opened on one explorer — and on one of its views when `dims` is given."""
    params = {"diff-type": "explorers", EXPLORER_VIEWS_KEY: slug}
    params.update({EXPLORER_DIM_PREFIX + key: value for key, value in (dims or {}).items()})
    return "?" + urlencode(params)


def blast_edit_link(slot: str) -> str:
    """Blast radius, on the dimension grid, showing one edit: its views highlighted, everything else greyed."""
    return "?" + urlencode({"diff-type": "blast", BLAST_GROUP_KEY: "dimensions", BLAST_EDIT_KEY: slot})


def matches(view: ViewDiff, selection: dict[str, str]) -> bool:
    """Whether a view satisfies every dimension the menu has set."""
    return all(view.dimensions.get(slug) == choice for slug, choice in selection.items())


def url_selection(dimensions: list[dict[str, Any]], prefix: str) -> dict[str, str]:
    """The dimension selection as the URL has it, read before any widget exists.

    The nav has to render before the menu — its callbacks write the menu's state — so this is the only
    source available when deciding where "next" goes.
    """
    selection: dict[str, str] = {}
    for dim in dimensions:
        held = st.query_params.get(prefix + dim["slug"])
        if held is not None:
            selection[str(dim["slug"])] = str(held)
    return selection


def displayed_index(changed: list[ViewDiff], selection: dict[str, str]) -> int | None:
    """Which changed view is on screen, by the same precedence the page uses to choose it.

    Next ▶ steps from here, so it has to agree with the page or the first press goes nowhere: with no
    selection the page shows the first changed view, and a position of "unknown" made Next re-select that
    same view. None means the selection names a view that did not change — a deliberate lookup — from
    which the next change is the first one.
    """
    if not selection:
        return 0
    exact = next((i for i, view in enumerate(changed) if view.dimensions == selection), None)
    if exact is not None:
        return exact
    return next((i for i, view in enumerate(changed) if matches(view, selection)), None)


def st_jump_and_next(
    changed: list[ViewDiff],
    labels: list[str],
    position: int | None,
    prefix: str,
    key: str,
    label: str,
) -> None:
    """The ⚡ jump and Next change ▶ — two ways to move, both writing the dimension menu below.

    Writing the menu's widget state from a callback is the only order that works: a widget reads its
    session value when it is created, so setting it afterwards is a change nobody sees until the next
    rerun. Both controls run before the menu is built, which is what makes either land in one click.
    """
    if len(changed) < 2:
        return

    def _select(index: int) -> None:
        target = changed[index % len(changed)]
        for slug, choice in target.dimensions.items():
            st.session_state[prefix + slug] = choice
            st.query_params[prefix + slug] = choice
        st.session_state[key] = index % len(changed)

    def _goto() -> None:
        picked = st.session_state.get(key)
        if picked is not None:
            _select(int(picked))

    col_jump, col_next = st.columns([4, 1], vertical_alignment="bottom")
    with col_jump:
        st.selectbox(
            f"⚡ Changes detected — jump to a changed {label} ({len(changed)})",
            options=list(range(len(changed))),
            format_func=lambda i: labels[i],
            index=None,
            placeholder=f"Type to search the changed {label}s…",
            key=key,
            on_change=_goto,
            help="Sets the menu below to that view, so the two never disagree about what you are looking at.",
        )
    with col_next:
        where = "" if position is None else f" ({position + 1} of {len(changed)})"
        st.button(
            "Next change ▶",
            key=f"{key}-next",
            on_click=_select,
            args=(0 if position is None else position + 1,),
            width="stretch",
            help=f"The next changed {label}{where}, wrapping round at the end.",
        )


def st_dimension_menu(view_diffs: list[ViewDiff], dimensions: list[dict[str, Any]], prefix: str) -> dict[str, str]:
    """The surface's own menu, cascading: each dimension offers only what the ones before it allow.

    Every dimension may be left unset, so the menu narrows as well as selects. Returns only what is set.
    """
    selection: dict[str, str] = {}
    if not dimensions:
        return selection

    columns = st.columns(min(4, len(dimensions)))
    for i, dim in enumerate(dimensions):
        dim_slug = dim["slug"]
        widget_key = prefix + dim_slug
        available: list[str] = []
        for view in view_diffs:
            if matches(view, selection):
                choice = view.dimensions.get(dim_slug)
                if choice is not None and choice not in available:
                    available.append(choice)
        # A value the narrowed menu no longer offers — a stale link, or a dimension above this one being
        # widened. Dropped rather than left to fail `url_persist`'s strict check on every load.
        if st.query_params.get(widget_key) not in available:
            st.query_params.pop(widget_key, None)
            st.session_state.pop(widget_key, None)

        names = {c["slug"]: (c.get("name") or c["slug"]) for c in dim.get("choices", [])}
        with columns[i % len(columns)]:
            picked = url_persist(st.selectbox)(
                dim.get("name") or dim_slug,
                key=widget_key,
                options=available,
                index=None,
                format_func=lambda slug, names=names: names.get(slug, slug),
                placeholder="Any",
            )
        if picked is not None:
            selection[dim_slug] = picked
    return selection


def dimensions_from_views(views: list[ViewDiff]) -> list[dict[str, Any]]:
    """A dimension list inferred from the views themselves, for a surface that publishes none.

    An explorer has no `dimensions` block the tool can read, so the columns are the keys its views carry,
    each key's values in first-seen order. Narrowest dimension first, because a leaf — and the last part
    of a view's label — is the last dimension's value, and a two-choice toggle there names nothing.

    Labels are the slugs, tidied. An explorer's are already written for people ("1-poorest", "After tax"),
    and tidying never empties one: a value of "-" (this view has no decile) became a single space and left
    those views unlabelled.
    """
    order: list[str] = []
    choices: dict[str, list[str]] = {}
    for view in views:
        for key, value in view.dimensions.items():
            if key not in choices:
                order.append(key)
                choices[key] = []
            if value not in choices[key]:
                choices[key].append(value)
    seen_at = {key: i for i, key in enumerate(order)}
    order.sort(key=lambda key: (len(choices[key]), seen_at[key]))

    def pretty(text: str) -> str:
        return text.replace("-", " ").replace("_", " ").strip() or text

    return [
        {
            "slug": key,
            "name": pretty(key),
            "choices": [{"slug": value, "name": pretty(value)} for value in choices[key]],
        }
        for key in order
    ]
