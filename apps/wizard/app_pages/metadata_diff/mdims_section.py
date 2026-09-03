"""MDims section: every MDim whose view texts this branch changes, listed up front.

The list replaces the old namespace filter + searchable selectbox. Picking an MDim out of a dropdown
only works if you already know which one your PR touched — which is exactly what a reviewer doesn't.

Two readings of the same MDims. **View by view** is one MDim at a time, one changed view at a time, with a
tick and a note on each view. **By edit** is one card per authored edit, however many views word it — the
per-MDim cards it replaces repeated a shared edit once per MDim. Both record on their own surface, so the
section bar can count either as progress.
"""

import pandas as pd
import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
from apps.wizard.app_pages.metadata_diff import cached, datapage, edits_view, view_nav
from apps.wizard.app_pages.metadata_diff.core import (
    dims_str,
    view_label,
    view_url,
)
from apps.wizard.app_pages.metadata_diff.data import load_item_notes, load_reviews
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    st_layout_switcher,
)
from apps.wizard.app_pages.metadata_diff.review_state import (
    item_marker,
    resolve_item_mark,
    st_review_strip,
    surface_key,
    surface_progress,
)

# The URL key that opens one MDim's views. A route, not a filter: the page shows that MDim's changed views
# and nothing else, and the link survives a reload so it can be pasted to somebody else.
VIEWS_KEY = view_nav.MDIM_VIEWS_KEY
# The ⚡ jump's widget key, and the prefix for the MDim menu's per-dimension keys. Both are URL-visible:
# a link to one view of one MDim is `?mdim-views=<path>&dim-<slug>=<choice>…`, which is shareable.
JUMP_KEY = "mdim-view-jump"
DIM_PARAM_PREFIX = view_nav.MDIM_DIM_PREFIX


def st_show_mdim_metadata_diffs(source_engine: Engine, target_engine: Engine) -> None:
    """Render the MDims section: the changed-MDim list."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)

    df = cached.mdim_changes(source_engine, target_engine)

    # No early route on `?mdim-views=`: it used to render that MDim's page and return, which skipped the
    # layout switcher entirely — so once anything set the key (the picker and Next set it on every move),
    # "By edit" disappeared from the section. The browser reads the key as its selection instead.
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

    if not flagged and not drafts:
        st.success(f"**No published MDim's texts changed on this branch** (against {BASELINE_NAME}).")
    else:
        # Drafts counted here as well as in the picker below: the header said "1 MDim" while the picker
        # said "1 of 2", because one of them was unpublished. Two numbers for the same set read as a bug.
        if flagged:
            head = f"**{len(flagged)} MDim{'s' if len(flagged) != 1 else ''}** changed by this branch"
            if drafts:
                head += f", plus **{len(drafts)}** not published yet"
        else:
            head = (
                f"**No published MDim's texts changed on this branch** (against {BASELINE_NAME}), but "
                f"**{len(drafts)}** unpublished one{'s' if len(drafts) != 1 else ''} did"
            )
        st.markdown(
            head + ".",
            help="Either the metadata of an indicator they use changed, or their own export recipe did — "
            "most text edits are authored in the garden step and reach an MDim through indicator metadata, "
            "leaving its config identical.",
        )
        col_layout, col_reject = st.columns([4, 1], vertical_alignment="center")
        with col_layout:
            layout = st_layout_switcher(
                "🔍 View by view",
                "**View by view** steps through one MDim at a time, every changed view with its diffs",
            )
        with col_reject:
            edits_view.st_reject_all(source_engine, cached.summary(source_engine, target_engine), "mdims")
        if layout == "items":
            _views_browser(source_engine, target_engine, df, flagged + drafts)
            return
        # One card per authored edit, however many views word it. The per-MDim cards this replaces showed
        # a shared edit once per MDim, and an unpublished MDim's cards sat in an expander of their own.
        edits_view.st_edit_cards(source_engine, target_engine, cached.summary(source_engine, target_engine), "mdims")

    _render_other(others)


def _views_browser(source_engine: Engine, target_engine: Engine, df: pd.DataFrame, paths: list[str]) -> None:
    """Pick which MDim, then read it view by view.

    One picker over every MDim this branch changed, published and unpublished together: "which of mine
    changed" is one question, and an unpublished MDim is the one most likely to be *yours* — it is badged
    rather than filed somewhere else. Published first, then drafts, each group widest-reaching first, so
    stepping down the list goes from what readers see today to what nobody sees yet.

    `?mdim-views=<path>` is the selection, so a link opens the MDim it names.
    """
    if not paths:
        return

    known = [p for p in paths if p in df.index]
    if not known:
        st.info("No MDim to show.")
        return

    def sort_key(path: str) -> tuple:
        row = df.loc[path]
        return (bool(row["is_draft"]), path)

    known.sort(key=sort_key)
    current = str(st.session_state.get(VIEWS_KEY) or st.query_params.get(VIEWS_KEY) or "").strip()
    if current not in known:
        # A route naming an MDim this branch does not change — a stale link, or one hand-edited. Falling
        # back silently left the bad value in the URL to be pasted on to somebody else, so it goes.
        st.query_params.pop(VIEWS_KEY, None)
        current = known[0]
    st.session_state[VIEWS_KEY] = current
    position = known.index(current)

    # One query for every recorded row, so the picker can say which MDims you have already been through.
    recorded_rows = load_item_notes(source_engine)

    def label(path: str) -> str:
        row = df.loc[path]
        title = str(row["title_source"] or path) if "title_source" in row and row["title_source"] else path
        name = f"{title} · 📝 unpublished" if row["is_draft"] else str(title)
        progress = surface_progress(recorded_rows, surface_key("item", f"mdim:{path}"))
        return f"{name} · {progress}" if progress else name

    if len(known) > 1:
        col_pick, col_next = st.columns([4, 1], vertical_alignment="bottom")
        with col_pick:
            st.selectbox(
                f"MDim {position + 1} of {len(known)} changed by this branch",
                options=known,
                index=position,
                format_func=label,
                key="mdd-mdim-picker",
                on_change=_pick_mdim,
                help="Type to search. Unpublished MDims are in here too, badged.",
            )
        with col_next:
            st.button(
                "Next MDim ▶",
                key="mdd-mdim-next",
                on_click=_step_mdim,
                args=(known, position + 1),
                width="stretch",
                help="The next changed MDim, wrapping round at the end.",
            )

    _views_page(source_engine, target_engine, df, current)


def _pick_mdim() -> None:
    """The picker's choice becomes the URL."""
    path = str(st.session_state.get("mdd-mdim-picker") or "").strip()
    if path:
        st.session_state[VIEWS_KEY] = path
        st.query_params[VIEWS_KEY] = path
        _reset_view_selection()


def _step_mdim(paths: list[str], index: int) -> None:
    """Step to the next MDim, wrapping."""
    path = paths[index % len(paths)]
    st.session_state[VIEWS_KEY] = path
    st.session_state["mdd-mdim-picker"] = path
    st.query_params[VIEWS_KEY] = path
    _reset_view_selection()


def _reset_view_selection() -> None:
    """Drop the previous MDim's ⚡ jump and menu — its dimensions mean nothing in the next one.

    Left in place, they either filter the new MDim to nothing or, worse, silently match a same-named
    dimension and show a view nobody asked for.
    """
    st.session_state.pop(JUMP_KEY, None)
    for key in [k for k in list(st.session_state.keys()) if isinstance(k, str) and k.startswith(DIM_PARAM_PREFIX)]:
        st.session_state.pop(key, None)
    for key in [k for k in list(st.query_params.keys()) if k.startswith(DIM_PARAM_PREFIX)]:
        st.query_params.pop(key, None)


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

    draft = " :orange-badge[📝 unpublished]" if row["is_draft"] else ""
    st.markdown(f"### {title or catalog_path}{draft}")
    st.caption(f"`{catalog_path}` · {len(changed)} of {len(view_diffs)} views changed")

    if not changed:
        st.info("No view of this MDim renders a text this branch changed.")
        return

    slug = str(row["slug_source"]) if row.get("slug_source") else ""
    # One query for the whole MDim, so the jump can say what you have already done to each view.
    item_surface = surface_key("item", f"mdim:{catalog_path}")
    recorded = load_reviews(source_engine, item_surface)
    labels = [item_marker(recorded, item_surface, dims_str(v.dimensions)) + view_label(v, dimensions) for v in changed]

    # Where we are, read from the URL rather than from the menu's return: the nav has to render before
    # the menu (its callbacks write the menu's state), so this is the only order in which "next" can be
    # relative to the view on screen.
    url_selection = view_nav.url_selection(dimensions, DIM_PARAM_PREFIX)
    position = view_nav.displayed_index(changed, url_selection)

    view_nav.st_jump_and_next(changed, labels, position, DIM_PARAM_PREFIX, JUMP_KEY, "view")
    selection = view_nav.st_dimension_menu(view_diffs, dimensions, DIM_PARAM_PREFIX)

    # One view, always. Which one: the exact match if the menu names it, else the first view consistent
    # with a partial menu, else the first changed view — so arriving here shows a diff rather than a set
    # of controls, and Next ▶ walks the rest.
    view = next((v for v in view_diffs if v.dimensions == selection), None) if selection else None
    if view is None:
        candidates = [v for v in changed if view_nav.matches(v, selection)] if selection else changed
        if not candidates:
            st.info("No changed view matches the menu above. Clear a dimension to widen it.")
            return
        view = candidates[0]

    shown = changed.index(view) + 1 if view in changed else None
    if shown is not None:
        st.caption(f"Changed view {shown} of {len(changed)} — **Next change ▶** steps through them.")
    _render_view(view, view_label(view, dimensions), dimensions, catalog_path, slug, row, source_engine, recorded)


def _render_view(
    view, label: str, dimensions: list, catalog_path: str, slug: str, row, source_engine=None, recorded=None
) -> None:
    """One view: what it is called, where to open it, every field of it that changed, and its own tick.

    The tick is the view's, not an edit's: what you just read is a page, and its changed fields are read
    together. Editing any of them makes the mark stale, so the view reopens.
    """
    with st.container(border=True):
        n = len(view.fields)
        head = f"**{label}**"
        if view.is_new:
            head += " :green-badge[🆕 new view]"
        head += f" :small[:gray[{n} field{'s' if n != 1 else ''} changed]]"
        # The tick sits beside the name, not under the diff: it is where the decision is made, and at the
        # foot of a long block it was below the fold on anything with several changed fields.
        st.markdown(head)
        # Both sides, the way the chart review offers both: reading a diff and then opening only
        # one of the two pages leaves you comparing text against memory. An unpublished MDim has no
        # reader-facing page on either server, so those open in the admin preview instead.
        staging_href = view_url(SOURCE, catalog_path, None if row["is_draft"] else slug, view.dimensions)
        baseline_slug = str(row["slug_target"]) if row.get("published_target") == 1 else None
        baseline_href = view_url(TARGET, catalog_path, baseline_slug, view.dimensions)
        # "Data page" rather than "view": what opens is the page a reader gets, where these texts are laid
        # out — the same thing the chart review calls a data page, so the two read alike. An unpublished
        # MDim has no reader-facing page, so its link is the admin preview and says so.
        staging_label = "preview" if row["is_draft"] else "data page"
        baseline_label = "data page" if baseline_slug else "preview"
        links = f":green[**This staging server**] [{staging_label} ↗]({staging_href})"
        if not view.is_new:
            links = f":gray[**{BASELINE_NAME.capitalize()}**] [{baseline_label} ↗]({baseline_href}) · " + links
        st.markdown(links)
        if source_engine is not None and view.fields:
            surface = surface_key("item", f"mdim:{catalog_path}")
            # Passed down from the caller where there is one: it has already read this surface's rows to
            # mark the jump's options, and reading them again is a second query per rerun.
            stored = recorded if recorded is not None else load_reviews(source_engine, surface)
            mark = resolve_item_mark(stored, surface, dims_str(view.dimensions), view.fields)
            st_review_strip(source_engine, surface, mark)
        if view.is_new:
            st.caption(f"This view does not exist on `{BASELINE_NAME}`, so there is no old text to compare.")
        datapage.st_datapage_diff(
            view.fields,
            baseline_label=BASELINE_NAME.capitalize(),
            staging_label="This staging server",
            show_unchanged_slots=False,
        )


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
