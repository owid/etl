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

from apps.wizard.app_pages.metadata_diff import brief, cached, datapage, discovery
from apps.wizard.app_pages.metadata_diff.core import field_label, group_usage
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME, DIFF_CSS, markdown_output, st_origin_caption
from apps.wizard.app_pages.metadata_diff.review_state import (
    n_reviewed,
    resolve_marks,
    st_reviewed_toggle,
    surface_key,
)
from apps.wizard.utils.components import Pagination

MDIMS_PER_PAGE = 4

# Changes shown inline per MDim. There is no detail page to defer the rest to any more, so the cap is the
# point at which a card stops being readable rather than a hand-off — the remainder is named in a caption.
MAX_INLINE_CHANGES = 12


def st_show_mdim_metadata_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the MDims section: the changed-MDim list."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)

    df = cached.mdim_changes(source_engine, target_engine)
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
            f"**{len(flagged)} of {len(df)} MDims** changed by this branch.",
            help="Either the metadata of an indicator they use changed, or their own export recipe did — "
            "most text edits are authored in the garden step and reach an MDim through indicator metadata, "
            "leaving its config identical.",
        )
        pagination = Pagination(flagged, items_per_page=MDIMS_PER_PAGE, pagination_key="mdd-mdims-pagination")
        if len(flagged) > MDIMS_PER_PAGE:
            pagination.show_controls()
        for catalog_path in pagination.get_page_items():
            _render_card(source_engine, target_engine, df, catalog_path)
        if len(flagged) > MDIMS_PER_PAGE:
            pagination.show_controls(position="bottom")

    _render_drafts(source_engine, target_engine, df, drafts)
    _render_other(others)


def _render_drafts(source_engine: Engine, target_engine: Engine, df: pd.DataFrame, drafts: list[str]) -> None:
    """MDims this branch changed that no reader can see yet, because they are unpublished.

    Kept out of the count above — the badge answers "what changes for readers" — but not out of the
    review: this is the text that goes live the moment `published` flips, so the PR that publishes an
    MDim is exactly the one whose reviewer needs to read it.
    """
    if not drafts:
        return
    with st.expander(f"📝 {len(drafts)} unpublished MDim(s) this branch changed — no reader sees them yet"):
        st.caption(
            "Their `published` flag is false, so they are not counted above. They are still worth reading "
            "if this PR is the one that publishes them."
        )
        for catalog_path in drafts[:MDIMS_PER_PAGE]:
            _render_card(source_engine, target_engine, df, catalog_path)
        if len(drafts) > MDIMS_PER_PAGE:
            st.caption(f"… and {len(drafts) - MDIMS_PER_PAGE} more; open one from its catalogPath above.")


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
    dimensions, view_diffs = cached.mdim_view_diffs(
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
            st.caption(f"{n_reviewed(marks)}/{len(marks)} reviewed")
            for mark in marks[:MAX_INLINE_CHANGES]:
                _render_change(source_engine, catalog_path, mark, attribution)
            if len(marks) > MAX_INLINE_CHANGES:
                st.caption(
                    f"… and {len(marks) - MAX_INLINE_CHANGES} more of this MDim's changes; the PR brief "
                    "above lists every one."
                )

        if other_groups:
            # This MDim's own view configs also differ, without the branch touching its recipe — almost
            # always master having rebuilt it. Counted separately so it can't be read as this PR's work.
            st.caption(
                f"🕓 {len(other_groups)} further difference(s) in this MDim's view configs are not from this "
                "branch (its recipe is untouched) — see Chart Diff's MDIMs section."
            )


def usage_for(source_engine: Engine, groups: list, catalog_path: str, row) -> dict:
    """Charts and other MDims rendering this MDim's changed indicators — the brief's reach lines."""
    ids = sorted({g.indicator_id for g in groups if g.affects_indicator and g.indicator_id is not None})
    if not ids:
        return {}
    return cached.usage_for_indicators(
        tuple(ids), catalog_path, source_engine, cache_key=str(row.get("configMd5_source"))
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
    col_brief, col_tree = st.columns(2)
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
    """Send the reader to the Blast radius section with this MDim's grid already selected."""
    st.query_params["diff-type"] = "blast"
    st.query_params["blast-group"] = "dimensions"
    st.session_state["metadata-diff-section"] = "blast"
    st.session_state["blast-group"] = "dimensions"
    st.session_state["blast-tree-mdim"] = catalog_path


def _render_change(source_engine: Engine, catalog_path: str, mark, attribution: dict[str, str]) -> None:
    """One distinct text change of an MDim, in data-page layout, with its reviewed toggle."""
    g = mark.group
    # Views only, here — the charts this same indicator reaches are the Charts section's business, and it
    # lists them there. What still belongs on an MDim card is *whether* the change is shared, because that
    # is what makes the edit's spread a question at all; the Blast radius section answers it across
    # surfaces, and the PR brief above carries it per change.
    reach = f"{len(g.view_dims)} view{'s' if len(g.view_dims) != 1 else ''}"
    scope = "🔗 shared indicator metadata" if g.affects_indicator else "🔒 MDim override"

    st.markdown(f"{mark.icon} **{field_label(g.field)}** :small[:gray[{reach} · {scope}]]")
    st_origin_caption(g.catalog_paths or set(), attribution)
    datapage.st_datapage_diff(
        {g.field: {"old": g.old, "new": g.new}},
        baseline_label=BASELINE_NAME.capitalize(),
        staging_label="This staging server",
        show_unchanged_slots=False,
    )
    st_reviewed_toggle(source_engine, surface_key("mdim", catalog_path), mark)
