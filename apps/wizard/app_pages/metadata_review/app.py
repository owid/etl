"""Metadata Review — collaborative review of user-facing chart text and metadata.

Reviewers browse the deployed MDim views / data-page indicators, see every
user-facing text field with its provenance (override / inherited / missing), and
file field-level suggestions with threaded comments. The data scientist exports
them with `etl metadata-review export <target>` and implements the changes in ETL.
"""

import html as html_lib
import json
from urllib.parse import quote, urlencode

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Wizard: Metadata Review", page_icon="🪄", layout="wide")

from apps.metadata_review.resolution import (  # noqa: E402
    shared_view_ids,
    suggestions_by_source_key,
    threads_for_field,
)
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
    open_counts = state.open_counts_by_page()
    # Pages with open proposals first, then by recency.
    mdims.sort(key=lambda m: m["updated_at"], reverse=True)
    mdims.sort(key=lambda m: open_counts.get(m["catalog_path"], 0), reverse=True)
    options = [m["catalog_path"] for m in mdims]
    labels = {}
    for m in mdims:
        label = f"{m['slug'] or m['catalog_path']}" + ("" if m["published"] else " (unpublished)")
        n_open = open_counts.get(m["catalog_path"], 0)
        if n_open:
            label += f" — 💬 {n_open} open"
        labels[m["catalog_path"]] = label
    catalog_path = url_persist(st.selectbox)(
        "MDim",
        options=options,
        format_func=lambda p: labels.get(p, p),
        key="mr_mdim",
        index=None,
        placeholder="Pick an MDim to review...",
    )
    if not catalog_path:
        _open_overview()
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
            # Text gets the room: fields take the wider column; the live page stays
            # visible in a smaller column while the field rail scrolls internally.
            col_page, col_fields = st.columns([2, 3], gap="medium")
            with col_page:
                page_url = f"{OWID_ENV.admin_site}/grapher/{quote(catalog_path, safe='')}?{urlencode(view.dimensions)}"
                _page_embed(page_url, height=620, hide_page_selectors=True)
                st.caption("Live page — scroll inside for key information and sources.")
            with col_fields:
                with st.container(height=780, border=False):
                    _render_grouped_fields(
                        view.fields,
                        review,
                        suggestions_by_key,
                        comments_by_suggestion,
                        users,
                        user,
                        fields_by_key,
                    )

    with tab_all:
        _all_suggestions_tab(
            suggestions, comments_by_suggestion, users, user, fields_by_key, catalog_path, mdim_review=review
        )


def _open_overview() -> None:
    """Landing overview: every page with open proposals, so returning reviewers
    see at a glance where the discussion is (instead of an empty picker)."""
    summary = state.load_open_summary()
    if not summary:
        st.info("No open proposals anywhere right now. Pick a page above to start reviewing.")
        return
    st.subheader("Pages with open proposals")
    for group in summary:
        page = group["page"]
        if group["kind"] == "mdim":
            link = f"?mr_mode=MDims&mr_mdim={quote(page, safe='')}"
            label = page
        else:
            # Indicator catalogPath 'grapher/ns/ver/ds/table#col' -> its dataset page.
            dataset_path = "/".join(page.split("#")[0].split("/")[1:4])
            link = f"?mr_mode=Datasets&mr_dataset={quote(dataset_path, safe='')}"
            label = dataset_path
        n = group["n_open"]
        st.markdown(
            f"- [{label}]({link}) — :orange-badge[💬 {n} open] · last activity {group['last_activity']:%Y-%m-%d %H:%M}"
        )


def _page_embed(url: str, height: int = 850, hide_page_selectors: bool = False) -> None:
    """Embed a live page in an iframe that reliably reloads when the URL changes.

    `st.iframe` keeps the same component instance across reruns, and the embedded
    MDim page rewrites its own query params client-side — so swapping the `src`
    prop doesn't always navigate. Rendering the iframe as raw HTML makes the
    component content change with the URL, forcing a remount.

    NOTE: streamlit deprecates `components.html` in favor of `st.iframe`, but
    `st.iframe` has no `key` to force a remount — don't swap this back without
    verifying the embedded MDim page follows the view selectors across reruns.

    With `hide_page_selectors=True`, the MDim page's own dropdown panel is hidden
    (the wizard's selectors are the single source of truth — two competing sets of
    dropdowns that can't stay in sync would be confusing). The full page has no
    `hideControls` support, so this injects CSS into the loaded document — which
    works on staging because the wizard and the admin preview share an origin;
    cross-origin contexts (local dev) silently keep the page's dropdowns visible.
    """
    hide_css = ".settings-row__wrapper, .multi-dim-settings { display: none !important; }"
    components.html(
        f'<iframe id="mr_page_embed" src="{html_lib.escape(url, quote=True)}" loading="lazy" '
        f'style="width:100%;height:{height - 20}px;border:1px solid #e6e6e6;border-radius:4px;background:#fff;">'
        "</iframe>"
        + (
            f"""
<script>
const frame = document.getElementById("mr_page_embed");
frame.addEventListener("load", () => {{
    try {{
        const doc = frame.contentDocument;
        const style = doc.createElement("style");
        style.textContent = {json.dumps(hide_css)};
        doc.head.appendChild(style);
    }} catch (e) {{
        // Cross-origin (e.g. local dev wizard vs. admin) — leave the page's controls visible.
    }}
}});
</script>"""
            if hide_page_selectors
            else ""
        ),
        height=height,
    )


# What each field annotates on the page: the chart itself vs the sections below it.
CHART_TEXT_FIELDS = {
    "config.title",
    "config.subtitle",
    "config.note",
    "grapher_config.title",
    "grapher_config.subtitle",
    "grapher_config.note",
}


def _render_grouped_fields(
    fields,
    mdim_review,
    suggestions_by_key,
    comments_by_suggestion,
    users,
    user,
    fields_by_key,
) -> None:
    """Field boxes grouped by what they annotate: chart text, then data-page metadata."""
    chart_fields = [f for f in fields if f.field_path in CHART_TEXT_FIELDS]
    data_fields = [f for f in fields if f.field_path not in CHART_TEXT_FIELDS]
    groups = [
        (":material/bar_chart: Chart text — title, subtitle and footnote as rendered on the chart", chart_fields),
        (":material/description: About the data — shown on the data page below the chart", data_fields),
    ]
    for header, group in groups:
        if not group:
            continue
        st.markdown(f"##### {header.split(' — ')[0]}")
        st.caption(header.split(" — ")[1].capitalize())
        for field in group:
            if mdim_review is not None:
                shared = shared_view_ids(mdim_review, field)
                threads = threads_for_field(mdim_review, field, suggestions_by_key)
            else:
                shared = None
                threads = suggestions_by_key.get(field.source_key(), [])
            render_field(
                field,
                threads,
                comments_by_suggestion,
                users,
                user,
                fields_by_key,
                shared_views=shared,
                mdim_review=mdim_review,
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
        _open_overview()
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
            col_page, col_fields = st.columns([2, 3], gap="medium")
            with col_page:
                if indicator.variable_id is not None:
                    # The real data page (admin preview), so the reviewer sees exactly
                    # what readers see — key information, sources, and all.
                    _page_embed(OWID_ENV.data_page_preview(indicator.variable_id), height=620)
                    st.caption("Live data page — scroll inside for key information and sources.")
            with col_fields:
                _render_grouped_fields(
                    indicator.fields,
                    None,
                    suggestions_by_key,
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
    mdim_review=None,
    header: bool = False,
) -> None:
    """The worklist: every field with suggestions, rendered with the SAME component
    as the main tab (tracked text, thread strip, controls), grouped by field."""
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

    # One entry per field (source key), newest activity first.
    grouped: dict[tuple, list] = {}
    for suggestion in filtered:
        key = (suggestion.targetType, suggestion.targetPath, suggestion.viewId, suggestion.fieldPath)
        grouped.setdefault(key, []).append(suggestion)
    ordered = sorted(grouped.items(), key=lambda kv: max(s.updatedAt for s in kv[1]), reverse=True)

    for key, threads in ordered:
        field = fields_by_key.get(key)
        if field is None:
            # The view/indicator no longer exists on this page — compact stale card.
            with st.container(border=True):
                st.markdown(f"**#{threads[0].id}** `{key[3]}` on `{key[1]}`")
                st.warning("The view/indicator this suggestion targets no longer exists on this page.")
                for suggestion in threads:
                    if suggestion.suggestedValue:
                        st.markdown(f"> {suggestion.suggestedValue}")
            continue
        # Where the field lives (human-readable view selection for MDims).
        view_id = field.view_id or threads[0].filedFromViewId
        if mdim_review is not None and view_id:
            view = next((v for v in mdim_review.views if v.view_id == view_id), None)
            if view is not None:
                dims = mdim_review.human_dimensions(view.dimensions)
                st.caption("View: " + " · ".join(f"**{k}:** {v}" for k, v in dims.items()))
        elif field.target_type == "indicator":
            st.caption(f"Indicator: `{field.target_path}`")
        shared = shared_view_ids(mdim_review, field) if mdim_review is not None else None
        render_field(
            field,
            threads,
            comments_by_suggestion,
            users,
            user,
            fields_by_key,
            shared_views=shared,
            mdim_review=mdim_review,
            key_ns="all",
        )

    st.divider()
    st.markdown("**Implementing these suggestions?** Export them with resolved edit locations:")
    staging_hint = "" if OWID_ENV.env_local == "dev" else f"STAGING={OWID_ENV.name.removeprefix('staging-site-')} "
    st.code(f"{staging_hint}.venv/bin/etl metadata-review export {export_target}", language="bash")


main()
