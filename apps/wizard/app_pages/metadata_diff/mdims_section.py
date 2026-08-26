"""MDims section: every MDim whose view texts this branch changes, listed up front.

The list replaces the old namespace filter + searchable selectbox. Picking an MDim out of a dropdown
only works if you already know which one your PR touched — which is exactly what a reviewer doesn't.

One level, deliberately. The list used to be an entry point into three deep pages (Blast radius, View
diff, Review & PR brief), and those pages formed a closed loop: the author's scope decision was read only
by the Review page, the Approve/Flag sign-off was read by nothing, and neither is consulted at merge —
metadata ships through ETL, unlike chart-diff approvals, which gate `etl chart-sync`. They also carried a
second review vocabulary, stored apart from this list's ticks so the two could not collide, which meant a
change could read Reviewed here and Pending there forever.

What they contributed that nothing else does is kept: the **PR brief** is a download on each card, and the
dimension grid lives in the **Blast radius** section, which every card links into.
"""

import pandas as pd
import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.app_pages.metadata_diff import brief, cached, datapage, discovery
from apps.wizard.app_pages.metadata_diff.blast_section import GROUP_KEY, TREE_MDIM_KEY
from apps.wizard.app_pages.metadata_diff.core import field_label, group_usage
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    markdown_output,
    st_layout_switcher,
    st_origin_caption,
    view_label,
    view_url,
)
from apps.wizard.app_pages.metadata_diff.review_state import (
    resolve_marks,
    surface_key,
)
from apps.wizard.utils.components import Pagination, url_persist

MDIMS_PER_PAGE = 4
# The URL key that opens one MDim's views. A route, not a filter: the page shows that MDim's changed views
# and nothing else, and the link survives a reload so it can be pasted to somebody else.
VIEWS_KEY = "mdim-views"
VIEWS_PER_PAGE = 5
# Changed views drawn inline on an MDim's card before the rest fold away. A card is one item in a list of
# MDims, so it shows enough to judge the MDim and hands the rest to the focused page.
VIEWS_IN_CARD = 3
# The ⚡ jump's widget key, and the prefix for the MDim menu's per-dimension keys. Both are URL-visible:
# a link to one view of one MDim is `?mdim-views=<path>&dim-<slug>=<choice>…`, which is shareable.
JUMP_KEY = "mdim-view-jump"
DIM_PARAM_PREFIX = "dim-"

# Changes shown open per MDim. There is no detail page to defer the rest to any more, so the cap is the
# point at which a card stops being readable — a fold, not a cut: the remainder renders inside an
# expander rather than being dropped, so the card's own count and what it shows can never disagree.
MAX_INLINE_CHANGES = 12


def st_show_mdim_metadata_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the MDims section: the changed-MDim list."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)

    df = cached.mdim_changes(source_engine, target_engine)

    requested = str(st.session_state.get(VIEWS_KEY) or st.query_params.get(VIEWS_KEY) or "").strip()
    if requested:
        if requested in df.index:
            st.session_state[VIEWS_KEY] = requested
            _views_page(source_engine, target_engine, df, requested)
            return
        # A stale link — the MDim is no longer in the comparison. Say so instead of rendering an empty page.
        st.warning(f"`{requested}` is not among the MDims that differ from `{BASELINE_NAME}` on this server.")
        _clear_views()
    if df.empty:
        st.warning("No MDims found on this staging server.")
        return
    if bool(df["indicator_check_failed"].any()):
        st.warning(
            "Could not compare indicator metadata against the baseline, so this list reflects MDim "
            "**config** changes only — an MDim whose texts changed may be missing. Open one to diff it anyway."
        )

    # Links from before the deep pages were removed carry ?mdim= and ?mode=; drop them rather than
    # leave a parameter that now selects nothing.
    for stale_param in ("mdim", "mode"):
        st.query_params.pop(stale_param, None)
        st.session_state.pop(stale_param, None)

    reader_facing = df["in_branch"] & ~df["is_draft"]
    flagged = [str(cp) for cp in df.index[reader_facing]]
    drafts = [str(cp) for cp in df.index[df["in_branch"] & df["is_draft"]]]
    others = [str(cp) for cp in df.index[df["has_changes"] & ~df["in_branch"]]]

    if not bool(df["scope_available"].all()):
        st.warning(
            "Could not read this branch's changed files from git, so this list is **not narrowed to your "
            "branch** — it will include MDims that master has rebuilt since this server was created."
        )

    if not flagged:
        message = f"**No published MDim's texts changed on this branch** (against {BASELINE_NAME})."
        if drafts:
            st.info(message + f" {len(drafts)} unpublished one(s) did — see below.")
        else:
            st.success(message)
    else:
        st.markdown(
            f"**{len(flagged)} MDim{'s' if len(flagged) != 1 else ''}** changed by this branch.",
            help="Either the metadata of an indicator they use changed, or their own export recipe did — "
            "most text edits are authored in the garden step and reach an MDim through indicator metadata, "
            "leaving its config identical.",
        )
        layout = st_layout_switcher(
            "🔍 View by view",
            "**View by view** lists every changed view of every changed MDim, with its diffs",
        )
        pagination = Pagination(flagged, items_per_page=MDIMS_PER_PAGE, pagination_key="mdd-mdims-pagination")
        if len(flagged) > MDIMS_PER_PAGE:
            pagination.show_controls()
        for catalog_path in pagination.get_page_items():
            if layout == "items":
                _render_views_card(source_engine, target_engine, df, catalog_path)
            else:
                _render_card(source_engine, target_engine, df, catalog_path)
        if len(flagged) > MDIMS_PER_PAGE:
            pagination.show_controls(position="bottom")

    _render_drafts(source_engine, target_engine, df, drafts)
    _render_other(others)


def _render_views_card(source_engine: Engine, target_engine: Engine, df: pd.DataFrame, catalog_path: str) -> None:
    """One MDim, its changed views inline — the item view of a card.

    Capped: an MDim can have hundreds of changed views, and this is a list of MDims. The rest fold into an
    expander, and the card's own button opens the focused page where they are paginated properly.
    """
    row = df.loc[catalog_path]
    title, dimensions, view_diffs = cached.mdim_view_diffs(
        catalog_path, source_engine, target_engine, cache_key=_cache_key(row)
    )
    changed = [v for v in view_diffs if v.changed]
    slug = str(row["slug_source"]) if row.get("slug_source") else ""

    with st.container(border=True):
        draft = " :orange-badge[📝 unpublished]" if row["is_draft"] else ""
        st.markdown(f"**{title or catalog_path}**{draft}")
        st.caption(f"`{catalog_path}` · {len(changed)} of {len(view_diffs)} views changed")
        st.button(
            "🔍 Open view by view",
            key=f"mdd-views-open-{catalog_path}",
            on_click=_open_views,
            args=(catalog_path,),
            help="This MDim's changed views on their own page, paginated.",
        )
        if not changed:
            st.caption("Nothing this branch changed in the texts readers see.")
            return
        for view in changed[:VIEWS_IN_CARD]:
            _render_view(view, view_label(view, dimensions), dimensions, catalog_path, slug, row)
        rest = changed[VIEWS_IN_CARD:]
        if rest:
            with st.expander(f"… {len(rest)} more changed view{'s' if len(rest) != 1 else ''}"):
                for view in rest[:MAX_INLINE_CHANGES]:
                    _render_view(view, view_label(view, dimensions), dimensions, catalog_path, slug, row)
                if len(rest) > MAX_INLINE_CHANGES:
                    st.caption(
                        f"{len(rest) - MAX_INLINE_CHANGES} further changed views are on the focused page — "
                        "use **Open view by view** above."
                    )


def _views_page(source_engine: Engine, target_engine: Engine, df: pd.DataFrame, catalog_path: str) -> None:
    """One MDim, view by view: the ⚡ jump, the MDim's own menu, and the views themselves.

    The jump does not render anything on its own — it writes into the menu, exactly as #6615's did, so
    there is one answer on screen to "which view am I looking at". What differs is that every dimension
    may be left unset: the menu filters the list as well as focusing it, and with nothing set you see
    every changed view rather than a set of controls to guess with.

    A complete selection shows that view whether or not it changed, because "did the view I was worried
    about move?" is a question only the unchanged ones can answer.
    """
    row = df.loc[catalog_path]
    title, dimensions, view_diffs = cached.mdim_view_diffs(
        catalog_path, source_engine, target_engine, cache_key=_cache_key(row)
    )
    changed = [v for v in view_diffs if v.changed]

    st.button("← All MDims", key="mdd-clear-views", on_click=_clear_views, help="Back to every changed MDim.")

    draft = " :orange-badge[📝 unpublished]" if row["is_draft"] else ""
    st.markdown(f"### {title or catalog_path}{draft}")
    st.caption(f"`{catalog_path}` · {len(changed)} of {len(view_diffs)} views changed")

    if not changed:
        st.info("No view of this MDim renders a text this branch changed.")
        return

    slug = str(row["slug_source"]) if row.get("slug_source") else ""
    labels = [view_label(v, dimensions) for v in changed]

    _jump_to_changed(changed, labels, dimensions)
    selection = _dimension_menu(view_diffs, dimensions)

    # A complete selection is a single view — the state the ⚡ jump puts the menu into.
    if len(selection) == len(dimensions) and dimensions:
        view = next((v for v in view_diffs if v.dimensions == selection), None)
        if view is None:
            st.warning("No view exists for this combination of controls.")
        elif not view.changed:
            st.success("**No changes in this view** — its texts match the baseline.")
            st.markdown(
                f"[Open this view ↗]({view_url(SOURCE, catalog_path, None if row['is_draft'] else slug, selection)})"
            )
        else:
            _render_view(view, view_label(view, dimensions), dimensions, catalog_path, slug, row)
        return

    # Otherwise the list, narrowed by whatever the menu has set.
    shown = [(v, label) for v, label in zip(changed, labels) if _matches(v, selection)]
    if selection:
        st.caption(f"{len(shown)} of {len(changed)} changed views match the menu above.")
    if not shown:
        st.info("No changed view matches the menu above. Clear a dimension to widen it.")
        return

    pagination = Pagination(shown, items_per_page=VIEWS_PER_PAGE, pagination_key=f"mdd-views-{catalog_path}")
    if len(shown) > VIEWS_PER_PAGE:
        pagination.show_controls()
    for view, label in pagination.get_page_items():
        _render_view(view, label, dimensions, catalog_path, slug, row)
    if len(shown) > VIEWS_PER_PAGE:
        pagination.show_controls(position="bottom")


def _matches(view, selection: dict[str, str]) -> bool:
    """Whether a view satisfies every dimension the menu has set."""
    return all(view.dimensions.get(slug) == choice for slug, choice in selection.items())


def _jump_to_changed(changed: list, labels: list[str], dimensions: list) -> None:
    """The ⚡ jump: pick a changed view, and the menu below moves to it.

    Writing the dimension widgets' state from a callback is the only order that works — a widget reads its
    session value when it is created, so setting it after the fact is a change nobody sees until the next
    rerun. This runs before the menu is built, which is what makes the jump land in one click.
    """
    if len(changed) < 2:
        return

    def _goto() -> None:
        picked = st.session_state.get(JUMP_KEY)
        if picked is None:
            return
        for slug, choice in changed[int(picked)].dimensions.items():
            st.session_state[DIM_PARAM_PREFIX + slug] = choice
            st.query_params[DIM_PARAM_PREFIX + slug] = choice

    st.selectbox(
        f"⚡ Changes detected — jump to a changed view ({len(changed)})",
        options=list(range(len(changed))),
        format_func=lambda i: labels[i],
        index=None,
        placeholder="Type to search this MDim's changed views…",
        key=JUMP_KEY,
        on_change=_goto,
        help="Sets the menu below to that view, so the two never disagree about what you are looking at.",
    )


def _dimension_menu(view_diffs: list, dimensions: list) -> dict[str, str]:
    """The MDim's own menu, cascading: each dimension offers only what the ones before it allow.

    Every dimension may be left unset — that is the difference from #6615, where a complete selection was
    forced and an impossible combination was reachable. Returns only the dimensions actually set.
    """
    selection: dict[str, str] = {}
    if not dimensions:
        return selection

    columns = st.columns(min(4, len(dimensions)))
    for i, dim in enumerate(dimensions):
        dim_slug = dim["slug"]
        key = DIM_PARAM_PREFIX + dim_slug
        available: list[str] = []
        for view in view_diffs:
            if _matches(view, selection):
                choice = view.dimensions.get(dim_slug)
                if choice is not None and choice not in available:
                    available.append(choice)
        # A value the narrowed menu no longer offers — from a stale link, or from widening a dimension
        # above this one. Dropped rather than left to fail url_persist's strict check on every load.
        if st.query_params.get(key) not in available:
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)

        names = {c["slug"]: (c.get("name") or c["slug"]) for c in dim.get("choices", [])}
        with columns[i % len(columns)]:
            picked = url_persist(st.selectbox)(
                dim.get("name") or dim_slug,
                key=key,
                options=available,
                index=None,
                format_func=lambda slug, names=names: names.get(slug, slug),
                placeholder="Any",
            )
        if picked is not None:
            selection[dim_slug] = picked
    return selection


def _render_view(view, label: str, dimensions: list, catalog_path: str, slug: str, row) -> None:
    """One view: what it is called, where to open it, and every field of it that changed."""
    with st.container(border=True):
        n = len(view.fields)
        head = f"**{label}**"
        if view.is_new:
            head += " :green-badge[🆕 new view]"
        head += f" :small[:gray[{n} field{'s' if n != 1 else ''} changed]]"
        st.markdown(head)
        # An unpublished MDim has no reader-facing page, so its views open in the admin preview.
        href = view_url(SOURCE, catalog_path, None if row["is_draft"] else slug, view.dimensions)
        st.markdown(f"[Open this view ↗]({href})")
        if view.is_new:
            st.caption(f"This view does not exist on `{BASELINE_NAME}`, so there is no old text to compare.")
        datapage.st_datapage_diff(
            view.fields,
            baseline_label=BASELINE_NAME.capitalize(),
            staging_label="This staging server",
            show_unchanged_slots=False,
        )


def _open_views(catalog_path: str) -> None:
    """Open one MDim's views: the URL carries it, so the destination survives a reload and can be shared."""
    st.query_params[VIEWS_KEY] = catalog_path
    st.session_state[VIEWS_KEY] = catalog_path


def _clear_views() -> None:
    """Leave the view-by-view page."""
    st.session_state[VIEWS_KEY] = ""
    st.query_params.pop(VIEWS_KEY, None)


def _render_drafts(source_engine: Engine, target_engine: Engine, df: pd.DataFrame, drafts: list[str]) -> None:
    """MDims this branch changed that no reader can see yet, because they are unpublished.

    Kept out of the count above — the badge answers "what changes for readers" — but not out of the
    review: this is the text that goes live the moment `published` flips, so the PR that publishes an
    MDim is exactly the one whose reviewer needs to read it.

    Paginated like the published list, and for the same reason: there is no MDim lookup anywhere in this
    section, so a card left off the first page could not be opened at all — its diff and its Reviewed
    toggles were counted and then put out of reach.
    """
    if not drafts:
        return
    with st.expander(f"📝 {len(drafts)} unpublished MDim(s) this branch changed — no reader sees them yet"):
        st.caption(
            "Their `published` flag is false, so they are not counted above. They are still worth reading "
            "if this PR is the one that publishes them."
        )
        pagination = Pagination(drafts, items_per_page=MDIMS_PER_PAGE, pagination_key="mdd-drafts-pagination")
        if len(drafts) > MDIMS_PER_PAGE:
            pagination.show_controls()
        for catalog_path in pagination.get_page_items():
            _render_card(source_engine, target_engine, df, catalog_path)
        if len(drafts) > MDIMS_PER_PAGE:
            pagination.show_controls(position="bottom")


def _render_other(others: list[str]) -> None:
    """MDims whose config differs from the baseline without this branch touching them.

    Normally master rebuilt them after this staging server was created. Not the reviewer's job — but
    listed, because an unexplained difference the tool knows about and doesn't mention is worse.
    """
    if not others:
        return
    with st.expander(f"🕓 {len(others)} other MDim(s) differ from {BASELINE_NAME} — not from this branch"):
        st.caption(
            "Their config differs, but neither their export recipe nor the indicators they use are touched "
            "by this branch. Open one from Chart Diff's MDIMs section if you want the config diff."
        )
        st.markdown("\n".join(f"- `{cp}`" for cp in others))


def _cache_key(row: pd.Series) -> str:
    """Bust the per-MDim diff cache when either side's config moves."""
    return f"{row.get('configMd5_source')}-{row.get('configMd5_target')}"


def _render_card(source_engine: Engine, target_engine: Engine, df: pd.DataFrame, catalog_path: str) -> None:
    """One MDim: what changed in its views, inline, with a PR brief and a way into its dimension grid."""
    row = df.loc[catalog_path]
    _title, dimensions, view_diffs = cached.mdim_view_diffs(
        catalog_path, source_engine, target_engine, cache_key=_cache_key(row)
    )
    changed_views = [v for v in view_diffs if v.changed]
    groups, other_groups = discovery.split_mdim_groups(catalog_path, changed_views)
    paths = tuple(sorted({p for g in groups for p in (g.catalog_paths or set())}))
    attribution = cached.indicator_attribution(source_engine, target_engine, paths) if paths else {}

    with st.container(border=True):
        badge = "🆕 new" if row["is_new"] else f"{len(groups)} change{'s' if len(groups) != 1 else ''}"
        n_views = len(changed_views)
        head = f"**`{catalog_path}`** :gray-badge[{badge}]"
        if row["is_draft"]:
            # Which expander a card sits in is not something you can see once you are reading the card.
            head += " :orange-badge[📝 unpublished]"

        if n_views:
            head += f" :small[:gray[{n_views} of {len(view_diffs)} views]]"
        st.markdown(head)

        if not groups:
            st.caption(
                "Nothing this branch changed in the texts readers see. "
                "(Chart Diff's MDIMs section shows the config diff.)"
            )
        else:
            marks = resolve_marks(source_engine, surface_key("mdim", catalog_path), groups)
            _card_actions(
                source_engine, target_engine, catalog_path, marks, usage_for(source_engine, groups, catalog_path, row)
            )
            for mark in marks[:MAX_INLINE_CHANGES]:
                _render_change(source_engine, catalog_path, mark, attribution)
            folded = marks[MAX_INLINE_CHANGES:]
            if folded:
                with st.expander(f"… {len(folded)} more of this MDim's changes"):
                    for mark in folded:
                        _render_change(source_engine, catalog_path, mark, attribution)

        # Two unlike reasons a difference is not attributable, and only one of them is master's doing.
        repointed = [g for g in other_groups if g.indicator_replaced]
        lagging = [g for g in other_groups if not g.indicator_replaced]
        if lagging:
            st.caption(
                f"🕓 {len(lagging)} further difference(s) in this MDim's view configs are not from this "
                "branch (its recipe is untouched) — see Chart Diff's MDIMs section."
            )
        if repointed:
            st.caption(
                f"🔀 {len(repointed)} difference(s) are on views that render a **different indicator "
                f"variant** here than on `{BASELINE_NAME}` — a replacement, not an edit. Their text "
                "differs for that reason too, so a rewording of yours cannot be told apart from the swap."
            )


def usage_for(source_engine: Engine, groups: list, catalog_path: str, row) -> dict:
    """Charts and other MDims rendering this MDim's changed indicators — the brief's reach lines.

    Every indicator of a group, not just its first: one edit to a shared definition renders into several
    indicators, and `group_usage` reads the whole of `indicator_ids` back out. An id missing from this map
    is a chart or an MDim the brief never mentions.
    """
    ids: set[int] = set()
    for g in groups:
        if not g.affects_indicator:
            continue
        # `indicator_ids` is the full set; `indicator_id` is the fallback for a group built without it.
        ids |= g.indicator_ids or ({g.indicator_id} if g.indicator_id is not None else set())
    if not ids:
        return {}
    return cached.usage_for_indicators(
        tuple(sorted(ids)), catalog_path, source_engine, cache_key=str(row.get("configMd5_source"))
    )


def _card_actions(
    source_engine: Engine,
    target_engine: Engine,
    catalog_path: str,
    marks: list,
    usage: dict,
) -> None:
    """The two things the deep pages had that the list did not: the brief, and the dimension grid.

    The brief is generated from the ticks on this card — the one review state there is — so what it calls
    ready to apply is what somebody actually ticked here.
    """
    col_brief, col_views, col_tree = st.columns(3)
    with col_brief:
        rows = [
            {
                "g": mark.group,
                "change_key": mark.change_key,
                "content_hash": mark.content_hash,
                "stale": mark.stale,
                "reviewed": mark.reviewed,
                "reviewer": mark.reviewer,
                "updatedAt": mark.updated_at,
                "charts": group_usage(mark.group, usage).get("charts", []),
                "mdims": group_usage(mark.group, usage).get("mdims", []),
            }
            for mark in marks
        ]
        # Collapsed: the brief is a page of markdown, and the changes below it are what the card is for.
        with st.expander("📋 PR brief — every change, with the edit to make"):
            markdown_output(
                brief.pr_brief_markdown(catalog_path, BASELINE_NAME, rows, usage),
                "pr-brief.md",
                f"mdim_brief_{catalog_path}",
            )
    with col_views:
        st.button(
            "🔍 View by view",
            key=f"mdd-views-{catalog_path}",
            on_click=_open_views,
            args=(catalog_path,),
            help="Every changed view of this MDim, with its diffs.",
            width="stretch",
        )
    with col_tree:
        st.button(
            "🌳 Dimension tree",
            key=f"mdd-tree-{catalog_path}",
            on_click=_open_dimension_tree,
            args=(catalog_path,),
            help="Opens this MDim's views on its dimension grid, in the Blast radius section.",
            width="stretch",
        )


def _open_dimension_tree(catalog_path: str) -> None:
    """Send the reader to the Blast radius section with this MDim's grid drawn first.

    The catalogPath goes in the URL as well as in session state: the section reads it from there, which is
    what makes the destination survive a reload and be worth pasting to somebody else.
    """
    st.query_params["diff-type"] = "blast"
    st.query_params[GROUP_KEY] = "dimensions"
    st.query_params[TREE_MDIM_KEY] = catalog_path
    st.session_state["metadata-diff-section"] = "blast"
    st.session_state[GROUP_KEY] = "dimensions"
    st.session_state[TREE_MDIM_KEY] = catalog_path


def _render_change(source_engine: Engine, catalog_path: str, mark, attribution: dict[str, str]) -> None:
    """One distinct text change of an MDim, in data-page layout, with its reviewed toggle."""
    g = mark.group
    # Views only, here — the charts this same indicator reaches are the Charts section's business, and it
    # lists them there. What still belongs on an MDim card is *whether* the change is shared, because that
    # is what makes the edit's spread a question at all; the Blast radius section answers it across
    # surfaces, and the PR brief above carries it per change.
    reach = f"{len(g.view_dims)} view{'s' if len(g.view_dims) != 1 else ''}"
    scope = "🔗 shared indicator metadata" if g.affects_indicator else "🔒 MDim override"

    st.markdown(f"**{field_label(g.field)}** :small[:gray[{reach} · {scope}]]")
    st_origin_caption(g.catalog_paths or set(), attribution)
    datapage.st_datapage_diff(
        {g.field: {"old": g.old, "new": g.new}},
        baseline_label=BASELINE_NAME.capitalize(),
        staging_label="This staging server",
        show_unchanged_slots=False,
    )
