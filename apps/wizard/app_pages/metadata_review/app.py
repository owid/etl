"""Metadata Review — collaborative review of user-facing chart text and metadata.

Reviewers browse the deployed MDim views / data-page indicators, see every
user-facing text field with its provenance (override / inherited / missing), and
file field-level suggestions with threaded comments. The data scientist exports
them with `etl metadata-review export <target>` and implements the changes in ETL.
"""

import html as html_lib
from urllib.parse import quote, urlencode

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Wizard: Metadata Review", page_icon="🪄", layout="wide")

from apps.metadata_review.resolution import shared_view_ids, suggestions_by_source_key  # noqa: E402
from apps.metadata_review.targets import MdimReview, ReviewableField  # noqa: E402
from apps.wizard.app_pages.metadata_review import state  # noqa: E402
from apps.wizard.app_pages.metadata_review.field_panel import render_field  # noqa: E402
from apps.wizard.utils.components import Pagination, url_persist  # noqa: E402
from etl.config import OWID_ENV  # noqa: E402

st.title(":material/rate_review: Metadata Review")
st.markdown(
    "Review the user-facing text of MDims and data pages, and leave field-level suggestions. "
    "Each field shows **where its text comes from**, so the implementer knows what to edit."
)


def main() -> None:
    if not state.review_tables_exist():
        st.error(
            "The `metadata_review_suggestions` / `metadata_review_comments` tables don't exist in this "
            "database yet — they are created by the owid-grapher migration `AddMetadataReviewTables`. "
            "Until that reaches production (and new staging servers inherit it), create them manually "
            "for testing:\n\n"
            "```python\n"
            "import etl.grapher.model as gm\n"
            "from etl.config import OWID_ENV\n"
            "gm.MetadataReviewSuggestion.create_table(OWID_ENV.engine, if_exists='skip')\n"
            "gm.MetadataReviewComment.create_table(OWID_ENV.engine, if_exists='skip')\n"
            "```"
        )
        st.stop()

    user = state.current_user()
    if user is None:
        st.warning(
            "Could not identify you (no Tailscale user on this connection and no `GRAPHER_USER_ID` locally). "
            "You can browse, but filing suggestions is disabled."
        )

    mode = url_persist(st.segmented_control)(
        "What do you want to review?",
        options=["MDims", "Datasets"],
        key="mr_mode",
        default="MDims",
    )
    if mode == "Datasets":
        dataset_page(user)
    else:
        mdim_page(user)


# ---------------------------------------------------------------------------
# MDim mode
# ---------------------------------------------------------------------------


def mdim_page(user) -> None:
    mdims = state.cached_list_mdims()
    options = [m["catalog_path"] for m in mdims]
    labels = {
        m["catalog_path"]: f"{m['slug'] or m['catalog_path']}" + ("" if m["published"] else " (unpublished)")
        for m in mdims
    }
    catalog_path = url_persist(st.selectbox)(
        "MDim",
        options=options,
        format_func=lambda p: labels.get(p, p),
        key="mr_mdim",
        index=None,
        placeholder="Pick an MDim to review...",
    )
    if not catalog_path:
        st.stop()

    review = state.cached_resolve_mdim(catalog_path)
    suggestions, comments_by_suggestion, users = state.load_suggestions([catalog_path] + review.indicator_paths)
    suggestions_by_key = suggestions_by_source_key(suggestions)
    fields_by_key: dict[tuple, ReviewableField] = {}
    for field in review.all_fields:
        fields_by_key.setdefault(field.source_key(), field)

    # Header.
    st.header(review.title or catalog_path)
    n_by_status = {"open": 0, "implemented": 0, "rejected": 0}
    for suggestion in suggestions:
        n_by_status[suggestion.status] = n_by_status.get(suggestion.status, 0) + 1
    preview = f"{OWID_ENV.admin_site}/grapher/{quote(catalog_path, safe='')}"
    st.markdown(
        f":orange-badge[{n_by_status['open']} open] :green-badge[{n_by_status['implemented']} implemented] "
        f":gray-badge[{n_by_status['rejected']} rejected] &nbsp; [Open the MDim preview]({preview})"
    )

    tab_views, tab_page, tab_all = st.tabs(
        [":material/stacked_line_chart: Views", ":material/tune: Title & dropdowns", ":material/list: All suggestions"]
    )

    with tab_page:
        st.caption("The MDim title and every dropdown label. All of these live in the MDim config.")
        for field in review.page_fields:
            render_field(
                field,
                suggestions_by_key.get(field.source_key(), []),
                comments_by_suggestion,
                users,
                user,
                fields_by_key,
            )

    with tab_views:
        view = _view_browser(review)
        if view is None:
            st.info("No view matches this dimension combination — pick another.")
        else:
            col_page, col_fields = st.columns([3, 2])
            with col_page:
                # The real MDim page (admin preview renders the full page, controls included),
                # so the reviewer sees exactly what readers see.
                page_url = f"{OWID_ENV.admin_site}/grapher/{quote(catalog_path, safe='')}?{urlencode(view.dimensions)}"
                _page_embed(page_url, height=850)
                st.caption(
                    "This is the live page. Note: changing the dropdowns *inside* the page won't move the "
                    "field panel — use the selectors above to switch views."
                )
            with col_fields:
                for field in view.fields:
                    shared = shared_view_ids(review, field)
                    threads = list(suggestions_by_key.get(field.source_key(), []))
                    # Overridden fields also show threads filed on other views sharing the same text.
                    if field.provenance == "override":
                        for other_view_id in shared:
                            threads += suggestions_by_key.get(
                                ("mdim", catalog_path, other_view_id, field.field_path), []
                            )
                    render_field(
                        field,
                        threads,
                        comments_by_suggestion,
                        users,
                        user,
                        fields_by_key,
                        shared_views=shared,
                        mdim_review=review,
                    )

    with tab_all:
        _all_suggestions_tab(suggestions, comments_by_suggestion, users, user, fields_by_key, catalog_path)


def _page_embed(url: str, height: int = 850) -> None:
    """Embed a live page in an iframe that reliably reloads when the URL changes.

    `st.iframe` keeps the same component instance across reruns, and the embedded
    MDim page rewrites its own query params client-side — so swapping the `src`
    prop doesn't always navigate. Rendering the iframe as raw HTML makes the
    component content change with the URL, forcing a remount.
    """
    components.html(
        f'<iframe src="{html_lib.escape(url, quote=True)}" loading="lazy" '
        f'style="width:100%;height:{height - 20}px;border:1px solid #e6e6e6;border-radius:4px;background:#fff;">'
        "</iframe>",
        height=height,
    )


def _view_browser(review: MdimReview):
    """One selectbox per dimension; returns the matching ViewReview (or None)."""
    cols = st.columns(max(len(review.dimensions), 1))
    selection = {}
    for col, dim in zip(cols, review.dimensions):
        with col:
            choice_slugs = [c.slug for c in dim.choices]
            selection[dim.slug] = url_persist(st.selectbox)(
                dim.name,
                options=choice_slugs,
                format_func=dim.choice_name,
                key=f"mr_dim_{dim.slug}",
            )
    for view in review.views:
        if all(view.dimensions.get(dim_slug) == choice for dim_slug, choice in selection.items()):
            return view
    return None


# ---------------------------------------------------------------------------
# Dataset mode
# ---------------------------------------------------------------------------


def dataset_page(user) -> None:
    datasets = state.cached_list_datasets()
    options = [d["catalog_path"] for d in datasets]
    labels = {d["catalog_path"]: f"{d['name']} · {d['catalog_path']}" for d in datasets}
    catalog_path = url_persist(st.selectbox)(
        "Dataset",
        options=options,
        format_func=lambda p: labels.get(p, p),
        key="mr_dataset",
        index=None,
        placeholder="Pick a grapher dataset to review...",
    )
    if not catalog_path:
        st.stop()

    review = state.cached_resolve_dataset(catalog_path)
    suggestions, comments_by_suggestion, users = state.load_suggestions(review.indicator_paths)
    suggestions_by_key = suggestions_by_source_key(suggestions)
    fields_by_key: dict[tuple, ReviewableField] = {}
    for indicator in review.indicators:
        for field in indicator.fields:
            fields_by_key.setdefault(field.source_key(), field)

    st.header(review.name or catalog_path)
    st.caption(
        f"{len(review.indicators)} indicators. Fields here are all **inherited** from the ETL metadata "
        "(or missing) — suggestions filed here also surface on MDim views using these indicators."
    )

    query = st.text_input("Filter indicators", key="mr_ds_filter", placeholder="Type to filter by name or path...")
    indicators = review.indicators
    if query:
        q = query.lower()
        indicators = [i for i in indicators if q in (i.name or "").lower() or q in i.catalog_path.lower()]

    pagination = Pagination(indicators, items_per_page=10, pagination_key="mr_ds_page")
    pagination.show_controls()
    for indicator in pagination.get_page_items():
        n_open = sum(
            1 for f in indicator.fields for s in suggestions_by_key.get(f.source_key(), []) if s.status == "open"
        )
        label = indicator.name or indicator.catalog_path.split("#")[-1]
        with st.expander(f"{label}" + (f" — :orange[{n_open} open]" if n_open else ""), expanded=False):
            st.caption(f"`{indicator.catalog_path}`")
            col_page, col_fields = st.columns([3, 2])
            with col_page:
                if indicator.variable_id is not None:
                    # The real data page (admin preview), so the reviewer sees exactly
                    # what readers see — key information, sources, and all.
                    _page_embed(OWID_ENV.data_page_preview(indicator.variable_id), height=850)
            with col_fields:
                for field in indicator.fields:
                    render_field(
                        field,
                        suggestions_by_key.get(field.source_key(), []),
                        comments_by_suggestion,
                        users,
                        user,
                        fields_by_key,
                    )

    _all_suggestions_tab(suggestions, comments_by_suggestion, users, user, fields_by_key, catalog_path, header=True)


# ---------------------------------------------------------------------------
# Shared: flat worklist + export hint
# ---------------------------------------------------------------------------


def _all_suggestions_tab(
    suggestions,
    comments_by_suggestion,
    users,
    user,
    fields_by_key,
    export_target: str,
    header: bool = False,
) -> None:
    if header:
        st.divider()
        st.subheader("All suggestions")
    status_filter = st.multiselect(
        "Status",
        options=["open", "implemented", "rejected"],
        default=["open"],
        key=f"mr_status_filter_{export_target}",
    )
    filtered = [s for s in suggestions if s.status in status_filter]
    if not filtered:
        st.info("No suggestions with the selected status.")
    for suggestion in filtered:
        author = users.get(suggestion.createdBy, f"user {suggestion.createdBy}")
        with st.container(border=True):
            st.markdown(
                f"**#{suggestion.id}** `{suggestion.fieldPath}` on `{suggestion.targetPath}`"
                + (f" (view `{suggestion.viewId}`)" if suggestion.viewId else "")
            )
            st.caption(f"{suggestion.status} · by {author} · {suggestion.createdAt:%Y-%m-%d}")
            if suggestion.suggestedValue:
                st.markdown(f"> {suggestion.suggestedValue}")
            for comment in comments_by_suggestion.get(suggestion.id, []):
                if comment.kind == "comment":
                    st.caption(f"💬 {users.get(comment.userId, '?')}: {comment.text}")

    st.divider()
    st.markdown("**Implementing these suggestions?** Export them with resolved edit locations:")
    staging_hint = "" if OWID_ENV.env_local == "dev" else f"STAGING={OWID_ENV.name.removeprefix('staging-site-')} "
    st.code(f"{staging_hint}.venv/bin/etl metadata-review export {export_target}", language="bash")


main()
