"""MDims section: every MDim whose view texts this branch changes, listed up front.

The list replaces the old namespace filter + searchable selectbox. Picking an MDim out of a dropdown
only works if you already know which one your PR touched — which is exactly what a reviewer doesn't.

Selecting an MDim opens the existing deep views (Blast radius / View diff / Review) unchanged, so the
list is an entry point rather than a replacement.
"""

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE
from apps.wizard.app_pages.metadata_diff import cached, datapage, discovery, mdim_pages
from apps.wizard.app_pages.metadata_diff.core import ViewDiff, field_label, rendering_charts
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME, DIFF_CSS, impact_counts, st_origin_caption
from apps.wizard.app_pages.metadata_diff.review_state import (
    n_reviewed,
    resolve_marks,
    st_reviewed_toggle,
    surface_key,
)
from apps.wizard.app_pages.metadata_diff.tree import render_tree_html
from apps.wizard.utils.components import Pagination, url_persist

MDIMS_PER_PAGE = 4

# Changes shown inline per MDim before pointing at the View diff for the rest — enough to see what the
# PR did without turning the list into the detail page.
MAX_INLINE_CHANGES = 3


def st_show_mdim_metadata_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the MDims section: the changed-MDim list, or one MDim's deep views when selected."""
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

    selected = st.query_params.get("mdim")
    if selected and selected not in df.index:
        # Stale deep link (renamed or deleted MDim) — drop it rather than crash.
        st.query_params.pop("mdim", None)
        st.session_state.pop("mdim", None)
        selected = None

    if selected:
        _render_selected(source_engine, target_engine, df, str(selected))
        return

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
    """One MDim: what changed in its views, inline, plus links into the deep views."""
    row = df.loc[catalog_path]
    dimensions, view_diffs = cached.mdim_view_diffs(
        catalog_path, source_engine, target_engine, cache_key=_cache_key(row)
    )
    changed_views = [v for v in view_diffs if v.changed]
    groups, other_groups = discovery.split_mdim_groups(catalog_path, changed_views)
    usage = _usage(source_engine, view_diffs, catalog_path, row)
    paths = tuple(sorted({p for g in groups for p in (g.catalog_paths or set())}))
    attribution = cached.indicator_attribution(source_engine, target_engine, paths) if paths else {}

    with st.container(border=True):
        badge = "🆕 new" if row["is_new"] else f"{len(groups)} change{'s' if len(groups) != 1 else ''}"
        n_views = len(changed_views)
        head = f"**`{catalog_path}`** :gray-badge[{badge}]"
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
            st.caption(f"{n_reviewed(marks)}/{len(marks)} reviewed")
            for mark in marks[:MAX_INLINE_CHANGES]:
                _render_change(source_engine, catalog_path, mark, usage, attribution)
            if len(marks) > MAX_INLINE_CHANGES:
                st.caption(f"… and {len(marks) - MAX_INLINE_CHANGES} more — open **View diff** below.")

        if other_groups:
            # This MDim's own view configs also differ, without the branch touching its recipe — almost
            # always master having rebuilt it. Counted separately so it can't be read as this PR's work.
            st.caption(
                f"🕓 {len(other_groups)} further difference(s) in this MDim's view configs are not from this "
                "branch (its recipe is untouched) — see Chart Diff's MDIMs section."
            )

        _mode_buttons(catalog_path)


def _render_change(source_engine: Engine, catalog_path: str, mark, usage: dict, attribution: dict[str, str]) -> None:
    """One distinct text change of an MDim, in data-page layout, with its reviewed toggle."""
    g = mark.group
    # Only charts that can actually show this change count as reach — same rule as the Charts section, or
    # the identical change reports a different number depending on which page you opened.
    charts = rendering_charts(g, usage) if g.affects_indicator else []
    reach = f"{len(g.view_dims)} view{'s' if len(g.view_dims) != 1 else ''}"
    if charts:
        reach += f" · ↗ {len(charts)} chart{'s' if len(charts) != 1 else ''}"
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


def _mode_buttons(catalog_path: str) -> None:
    """Open this MDim's deep views (the pages that carry sign-off, scope and the PR brief)."""
    cols = st.columns(3)
    for col, mode, label in zip(
        cols, ("tree", "view", "review"), ("💥 Blast radius", "🔍 View diff", "📋 Review & PR brief")
    ):
        with col:
            st.button(
                label,
                key=f"mdd-open-{mode}-{catalog_path}",
                on_click=_select_mdim,
                args=(catalog_path, mode),
                width="stretch",
            )


def _select_mdim(catalog_path: str, mode: str) -> None:
    """Deep-link into one MDim (URL params, so the view is shareable)."""
    mdim_pages._clear_view_params()
    st.query_params["mdim"] = catalog_path
    st.query_params["mode"] = mode
    st.session_state["mode"] = mode


def _clear_mdim() -> None:
    mdim_pages._clear_view_params()
    for key in ("mdim", "mode"):
        st.query_params.pop(key, None)
        st.session_state.pop(key, None)


def _usage(source_engine: Engine, view_diffs: list[ViewDiff], catalog_path: str, row: pd.Series) -> dict:
    """Blast radius for the MDim's changed indicators: which charts / other MDims share them."""
    ids = sorted({v.indicator_id for v in view_diffs if v.affects_indicator and v.indicator_id is not None})
    return cached.usage_for_indicators(
        tuple(ids), catalog_path, source_engine, cache_key=str(row.get("configMd5_source"))
    )


def _render_selected(source_engine: Engine, target_engine: Engine, df: pd.DataFrame, catalog_path: str) -> None:
    """One MDim's deep views: the tree, the view-by-view diff, or the review + PR brief."""
    row = df.loc[catalog_path]
    st.button("← All changed MDims", on_click=_clear_mdim, key="mdd-back-to-list")
    st.markdown(f"### `{catalog_path}`")

    dimensions, view_diffs = cached.mdim_view_diffs(
        catalog_path, source_engine, target_engine, cache_key=_cache_key(row)
    )
    if not view_diffs:
        st.warning("This MDim has no views.")
        return

    mode = url_persist(st.segmented_control)(
        "Mode",
        key="mode",
        options=["tree", "view", "review"],
        format_func=lambda m: {"tree": "💥 Blast radius", "view": "🔍 View diff", "review": "📋 Review"}[m],
        value="tree",
        label_visibility="collapsed",
    )
    mode = mode or "tree"  # segmented_control returns None if deselected
    st.caption(
        "**Blast radius**: how far each change reaches · **View diff**: the proposed changes, view by "
        "view · **Review**: sign off, comment & prepare a PR."
    )

    usage = _usage(source_engine, view_diffs, catalog_path, row)

    if mode == "view":
        mdim_pages.render_view_diff_page(catalog_path, dimensions, view_diffs, row, usage, source_engine)
    elif mode == "review":
        mdim_pages.render_review_page(catalog_path, dimensions, view_diffs, row, usage, source_engine)
    else:
        n_changed = sum(1 for v in view_diffs if v.changed)
        if n_changed == 0:
            st.success("No metadata changes in any view of this MDim. The tree below shows all views.")
        external_impacts = [impact_counts(v, usage) for v in view_diffs]
        tree_html, height = render_tree_html(
            catalog_path,
            dimensions,
            view_diffs,
            dim_param_prefix=mdim_pages.DIM_PARAM_PREFIX,
            external_impacts=external_impacts,
            self_url=f"{SOURCE.wizard_url}/metadata-diff",
        )
        # NOTE: nothing should be rendered below the component — it resizes itself to its
        # content, and Streamlit-rendered siblings would overlap during the resize.
        components.html(tree_html, height=height, scrolling=True)
