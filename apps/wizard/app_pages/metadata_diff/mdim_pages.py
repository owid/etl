"""The per-MDim pages (Blast radius / View diff / Review) and the single-chart flow.

These are the deep views a reviewer lands on from the change lists: they already carry the blast radius,
the author's scope decision, the persisted Approve/Flag sign-off and the PR brief. Moved out of the page
entrypoint unchanged in behaviour, so the sections can link into them and app.py can stay a router.

One baseline throughout (see render.py): production where this server has production credentials,
`staging-site-master` otherwise — the same baseline chart-diff uses.
"""

from typing import Any

import streamlit as st
from sqlalchemy.engine.base import Engine

from apps.wizard.app_pages.chart_diff.utils import SOURCE, TARGET
from apps.wizard.app_pages.metadata_diff import brief, cached, discovery
from apps.wizard.app_pages.metadata_diff.core import (
    CHART_FIELD_PREFIX,
    ViewDiff,
    change_group_identity,
    diff_views,
    field_label,
    group_changes,
    group_usage,
    text_change_key,
)
from apps.wizard.app_pages.metadata_diff.data import (
    build_chart_bundle,
    delete_review,
    load_reviews,
    load_scopes,
    upsert_review,
)
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    DIFF_CSS,
    FIELD_ORDER,
    chart_datapage_url,
    markdown_output,
    render_author_scope,
    render_impact,
    render_text_html,
    reviewer,
    view_impact,
    view_label,
    view_url,
)
from apps.wizard.utils.components import url_persist

# URL-parameter prefix for the MDim view selectors (`?d_<dimension>=<choice>`).
DIM_PARAM_PREFIX = "d_"


def _clear_view_params() -> None:
    """Drop the previous MDim's view-selector params when another MDim is selected."""
    for key in list(st.query_params.keys()):
        if key.startswith(DIM_PARAM_PREFIX):
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)


def _render_diff_body(
    view_diff: ViewDiff,
    baseline_url: str,
    staging_url: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    unit: str = "view",
    catalog_path: str = "",
    source_engine: Engine | None = None,
    scopes: dict[str, str] | None = None,
) -> None:
    """Status banner + blast-radius flag + side-by-side field diffs — shared by MDim views and charts.

    The per-env page link lives on each column header (e.g. WYSK → the indicator's data page), not in
    the status line. On MDim views, each shared field also gets the author's scope toggle.
    """
    if view_diff.is_new:
        st.info(
            f"This {unit} is **new** — it does not exist in {BASELINE_NAME}. "
            f"[{BASELINE_NAME} (data page)]({baseline_url}) · [this staging server (data page)]({staging_url})"
        )
        return
    if not view_diff.changed:
        st.success(
            f"No changes in this {unit}. "
            f"[{BASELINE_NAME} (data page)]({baseline_url}) · [this staging server (data page)]({staging_url})"
        )
        return

    n = len(view_diff.fields)
    st.warning(f"**{n} field{'s' if n > 1 else ''} changed** in this {unit}.")
    render_impact(view_diff, usage, unit=unit)

    # The author's scope decision(s) sit under the banner — scope is about those shared charts.
    if unit == "view" and source_engine is not None:
        shared_fields = [f for f in FIELD_ORDER if f in view_diff.fields and f in view_diff.indicator_changed_fields]
        for field_name in shared_fields:
            render_author_scope(
                catalog_path,
                view_diff,
                field_name,
                view_diff.fields[field_name],
                usage,
                source_engine,
                scopes or {},
                multi=len(shared_fields) > 1,
            )

    for field_name in [f for f in FIELD_ORDER if f in view_diff.fields]:
        change = view_diff.fields[field_name]
        st.markdown(f"##### {field_label(field_name)}")
        # WYSK / description fields render on the indicator's data page; chart FAUST on the chart itself.
        link_kind = "chart ↗" if field_name.startswith(CHART_FIELD_PREFIX) else "data page ↗"
        col_old, col_new = st.columns(2)
        with col_old:
            st.markdown(f":gray[**{BASELINE_NAME.capitalize()}**] · [{link_kind}]({baseline_url})")
            st.markdown(render_text_html(change["old"], change["new"], side="old"), unsafe_allow_html=True)
        with col_new:
            st.markdown(f":green[**This staging server**] · [{link_kind}]({staging_url})")
            st.markdown(render_text_html(change["new"], change["old"], side="new"), unsafe_allow_html=True)


def _review_status_key(catalog_path: str, change_key: str) -> str:
    return f"rev-status::{catalog_path}::{change_key}"


def _review_comment_key(catalog_path: str, change_key: str) -> str:
    return f"rev-comment::{catalog_path}::{change_key}"


_REVIEW_STATUSES = ["⏳ Pending", "✅ Approve", "🚩 Flag"]
_STATUS_TO_DB = {"✅ Approve": "approved", "🚩 Flag": "flagged"}
_STATUS_FROM_DB = {"approved": "✅ Approve", "flagged": "🚩 Flag"}


def _scope_label(scope: str, g: Any, usage: dict[int, dict[str, list[dict[str, Any]]]]) -> str:
    """The blast-radius consequence of the author's scope decision, shown by each change in the review."""
    if not g.affects_indicator:
        return "🔒 MDim override — local to this view; no other charts or MDims are affected."
    imp = usage.get(g.indicator_id, {}) if g.indicator_id is not None else {}
    n_c, n_m = len(imp.get("charts", [])), len(imp.get("mdims", []))
    if not n_c and not n_m:
        return "🔗 Shared indicator metadata — no other charts or MDims use it, so nothing else changes."
    also = f"{n_c} chart{'s' if n_c != 1 else ''}" + (f" and {n_m} other MDim{'s' if n_m != 1 else ''}" if n_m else "")
    if scope == "scoped":
        return f"✏️ {also} also use this indicator — **scoped to this MDim only**, so they keep their current text."
    return f"🔗 {also} also use this indicator — **all will change** with this edit."


def render_review_page(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    mdim_row: Any,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    source_engine: Engine,
) -> None:
    """Review mode: each distinct change with a DB-persisted, content-bound reviewer sign-off (Approve /
    Flag) + comment, the AUTHOR's scope decision shown for context, a lock-in gate, and a punch-list.
    The reviewer accepts or rejects; the scope decision is the author's (set on the View diff)."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)
    # Same branch-scope split the change list applies: an MDim master rebuilt after this server was
    # created differs in every view's config, and signing those off here would put edits nobody in this
    # PR wrote into the PR brief.
    groups, other_groups = discovery.split_mdim_groups(catalog_path, view_diffs)
    if not groups:
        if other_groups:
            st.info(
                f"The {len(other_groups)} difference(s) in this MDim's view configs are not from this branch "
                "(its recipe is untouched), so there is nothing here to review — the config diff is in Chart "
                "Diff's MDIMs section."
            )
        else:
            st.success("No metadata changes in any view of this MDim — nothing to review.")
        return
    if other_groups:
        st.caption(
            f"🕓 {len(other_groups)} further difference(s) in this MDim's view configs are not from this "
            "branch (its recipe is untouched) and are left out of this review."
        )

    baseline_slug = mdim_row.get("slug_target") if mdim_row.get("published_target") == 1 else None
    reviews = load_reviews(source_engine, catalog_path)
    scopes = load_scopes(source_engine, catalog_path)
    current_reviewer = reviewer()

    # Resolve each group: reviewer sign-off (content-hash lock-in) + the author's scope decision.
    resolved: list[dict[str, Any]] = []
    for g in groups:
        change_key, content_hash = change_group_identity(catalog_path, g)
        # Default to the conservative "only this view" unless the author explicitly chose "all" (matches
        # the View-diff toggle default), so the scope label and PR brief agree with what the author saw.
        scope = scopes.get(text_change_key(catalog_path, g.field, g.old, g.new), "scoped")
        imp = group_usage(g, usage) if g.affects_indicator else {}
        charts, mdims = imp.get("charts", []), imp.get("mdims", [])
        row = reviews.get(change_key)
        stale = bool(row) and row.get("contentHash") != content_hash
        if row and not stale:
            seed_label = _STATUS_FROM_DB.get(row.get("status"), "⏳ Pending")
            seed_comment = row.get("comment") or ""
        else:
            seed_label, seed_comment = "⏳ Pending", ""
        resolved.append(
            {
                "g": g,
                "change_key": change_key,
                "content_hash": content_hash,
                "stale": stale,
                "scope": scope,
                "charts": charts,
                "mdims": mdims,
                "seed_label": seed_label,
                "seed_comment": seed_comment,
                "reviewer": (row or {}).get("reviewer"),
                "updatedAt": (row or {}).get("updatedAt"),
            }
        )

    # Seed widget state from the DB before any widget is created — so a fresh session shows stored reviews.
    for r in resolved:
        sk, ck = _review_status_key(catalog_path, r["change_key"]), _review_comment_key(catalog_path, r["change_key"])
        if sk not in st.session_state:
            st.session_state[sk] = r["seed_label"]
        if ck not in st.session_state:
            st.session_state[ck] = r["seed_comment"]

    def _effective(r: dict[str, Any]) -> str:
        if r["stale"]:
            return "stale"
        label = st.session_state.get(_review_status_key(catalog_path, r["change_key"]), r["seed_label"])
        return _STATUS_TO_DB.get(label, "pending")

    states = [_effective(r) for r in resolved]
    n = len(states)
    n_appr, n_flag, n_stale, n_pend = (
        states.count("approved"),
        states.count("flagged"),
        states.count("stale"),
        states.count("pending"),
    )

    # --- Review status: iterate on the changes, then share comments or create a PR at the end ---
    st.caption(
        "This review pass is a way to go through the metadata changes and iterate with the author. At the "
        "end of the review, you can decide whether to share comments with the author or create a PR."
    )
    if n_appr == n and n > 0:
        st.success(f"✅ **All {n} changes reviewed** — approved.")
    else:
        bits = []
        if n_pend:
            bits.append(f"**{n_pend}** pending")
        if n_flag:
            bits.append(f"**{n_flag}** flagged")
        if n_stale:
            bits.append(f"**{n_stale}** edited since review")
        st.info(f"Review pending — {', '.join(bits)} of {n}.")
    st.caption(
        f"{n_appr}/{n} approved · decisions are stored on this staging server and bound to the exact text — "
        "any later edit reopens that change for re-review."
    )

    def _make_save(change_key: str, content_hash: str):
        sk, ck = _review_status_key(catalog_path, change_key), _review_comment_key(catalog_path, change_key)

        def _save() -> None:
            label = st.session_state.get(sk, "⏳ Pending")
            comment = (st.session_state.get(ck) or "").strip() or None
            db_status = _STATUS_TO_DB.get(label)
            if db_status is None:
                delete_review(source_engine, change_key)
            else:
                upsert_review(
                    source_engine, catalog_path, change_key, content_hash, db_status, comment, current_reviewer
                )

        return _save

    for r in resolved:
        g = r["g"]
        change_key = r["change_key"]
        sk, ck = _review_status_key(catalog_path, change_key), _review_comment_key(catalog_path, change_key)
        status = st.session_state.get(sk, r["seed_label"])
        comment = (st.session_state.get(ck) or "").strip()
        stale = r["stale"]
        eff = _effective(r)

        reach_word = f"{len(g.view_dims)} view{'s' if len(g.view_dims) != 1 else ''}"

        icon = "⚠️" if stale else status.split()[0]
        header = f"{icon} {field_label(g.field)} — {reach_word}"
        if comment:
            header += "  💬"
        if stale:
            header += "  · edited since review"

        # Collapse once decided; a 🚩 flag waits for its comment; stale/pending stay open.
        expanded = stale or eff == "pending" or (eff == "flagged" and not comment)
        save = _make_save(change_key, r["content_hash"])

        # Per-group representative view, for the data-page links on the column headers.
        rep_dims = g.view_dims[0] if g.view_dims else {}
        b_url = view_url(TARGET, catalog_path, baseline_slug, rep_dims)
        s_url = view_url(SOURCE, catalog_path, None, rep_dims)
        link_kind = "chart ↗" if g.field.startswith(CHART_FIELD_PREFIX) else "data page ↗"

        with st.expander(header, expanded=expanded):
            if stale:
                st.warning(
                    "⚠️ This text was **edited since it was last reviewed**, so the previous sign-off no "
                    "longer counts. Re-review to lock it in."
                )
            st.caption(_scope_label(r["scope"], g, usage))
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f":gray[**{BASELINE_NAME.capitalize()}**] · [{link_kind}]({b_url})")
                st.markdown(render_text_html(g.old, g.new, side="old", changed_only=True), unsafe_allow_html=True)
            with c2:
                st.markdown(f":green[**This staging server**] · [{link_kind}]({s_url})")
                st.markdown(render_text_html(g.new, g.old, side="new", changed_only=True), unsafe_allow_html=True)
            s1, s2 = st.columns([1, 3])
            with s1:
                st.radio("Sign-off", _REVIEW_STATUSES, key=sk, on_change=save, label_visibility="collapsed")
            with s2:
                st.text_area(
                    "Comment",
                    key=ck,
                    on_change=save,
                    placeholder="Optional note or suggested wording for the author…",
                    label_visibility="collapsed",
                )
            if r["reviewer"] and not stale and eff != "pending":
                when = f" · {r['updatedAt']}" if r.get("updatedAt") else ""
                st.caption(f"Signed off by **{r['reviewer']}**{when}")

    # The Markdown builders are pure, so hand them the live widget state rather than have them
    # reach into session state themselves.
    for r in resolved:
        r["label"] = st.session_state.get(_review_status_key(catalog_path, r["change_key"]), r["seed_label"])
        r["comment"] = (st.session_state.get(_review_comment_key(catalog_path, r["change_key"])) or "").strip()

    st.divider()
    st.markdown("**Outputs** — copy either as Markdown:")
    with st.expander("📋 Review summary — share with the author"):
        st.caption("The punch-list of decisions and comments, for the person who wrote the changes.")
        markdown_output(
            brief.review_markdown(catalog_path, BASELINE_NAME, resolved, usage), "review-summary.md", "review"
        )
    with st.expander("🔀 PR brief — changes to execute"):
        st.caption(
            "A complete PR spec — the changes, the checks to run, and a ready PR description. **Copy it and "
            "paste it to Claude Code, asking it to open the PR.**"
        )
        markdown_output(
            brief.pr_brief_markdown(catalog_path, BASELINE_NAME, resolved, usage), "pr-brief.md", "mdim_brief"
        )


def render_view_diff_page(
    catalog_path: str,
    dimensions: list[dict[str, Any]],
    view_diffs: list[ViewDiff],
    mdim_row: Any,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
    source_engine: Engine,
) -> None:
    """The View diff page: MDim controls as navigation + side-by-side text diffs."""
    st.markdown(DIFF_CSS, unsafe_allow_html=True)
    scopes = load_scopes(source_engine, catalog_path)

    # --- Jump straight to a changed view -----------------------------------------
    # Direct navigation to the changes, so the user doesn't have to hunt through the controls. Written
    # via a callback because url_persist only reads the URL when a control's state is still empty.
    changed_views = [v for v in view_diffs if v.changed]
    # Which changed views have been opened (scoped to this MDim) — drives the reviewed count + dot colours.
    visited: set[int] = st.session_state.setdefault(f"mdd_visited::{catalog_path}", set())
    if changed_views:
        n_changed = len(changed_views)

        # The changed view (if any) the current control selection is sitting on — so "Next" is relative
        # to where you are, and the current view counts as reviewed.
        cur_sel = {dim["slug"]: st.query_params.get(DIM_PARAM_PREFIX + dim["slug"]) for dim in dimensions}
        cur_idx = next(
            (
                i
                for i, cv in enumerate(changed_views)
                if cv.dimensions and all(cur_sel.get(s) == c for s, c in cv.dimensions.items())
            ),
            None,
        )
        if cur_idx is not None:
            visited.add(cur_idx)

        def _goto(idx: int) -> None:
            target = changed_views[idx % n_changed]
            for dim in dimensions:
                slug = dim["slug"]
                if slug in target.dimensions:
                    st.session_state[DIM_PARAM_PREFIX + slug] = target.dimensions[slug]
                    st.query_params[DIM_PARAM_PREFIX + slug] = target.dimensions[slug]
            visited.add(idx % n_changed)

        def _jump_to_changed() -> None:
            raw = st.session_state.get("mdd_jump")
            if raw not in (None, ""):
                _goto(int(raw))

        def _jump_label(i: Any) -> str:
            if i == "":
                return "Select a changed view…"
            cv = changed_views[int(i)]
            # 🟢 once reviewed, 🟡 not yet.
            marker = "🟢" if int(i) in visited else "🟡"
            charts, _ = view_impact(cv, usage)
            suffix = f"  —  ↗ {len(charts)} charts" if charts else ""
            return f"{marker} {view_label(cv, dimensions)}{suffix}"

        jump_col, nav_col, _spacer = st.columns([2, 1, 2], vertical_alignment="bottom")
        with jump_col:
            st.selectbox(
                f"⚡ Changes detected — jump to a changed view ({len(visited)}/{n_changed} reviewed)",
                options=[""] + list(range(n_changed)),
                format_func=_jump_label,
                key="mdd_jump",
                on_change=_jump_to_changed,
            )
        with nav_col:
            st.button(
                "Next change ▶",
                on_click=_goto,
                args=(0 if cur_idx is None else cur_idx + 1,),
                use_container_width=True,
                help="Jump to the next view with changes (cycles back to the first at the end).",
            )

    # --- MDim controls (navigation across views) ---------------------------------
    if changed_views:
        st.caption(
            "In the jump menu above: 🟡 a changed view · 🟢 already viewed. "
            "Use **Next change ▶** to step through the changes one by one."
        )
    selection: dict[str, str] = {}
    columns = st.columns(min(4, max(1, len(dimensions))))
    for i, dim in enumerate(dimensions):
        dim_slug = dim["slug"]
        key = DIM_PARAM_PREFIX + dim_slug
        # Choices available given the selection of the previous controls.
        available = []
        for v in view_diffs:
            if all(v.dimensions.get(s) == c for s, c in selection.items()):
                choice = v.dimensions.get(dim_slug)
                if choice is not None and choice not in available:
                    available.append(choice)
        names = {c["slug"]: (c.get("name") or c["slug"]) for c in dim.get("choices", [])}
        # Drop a stale URL value (e.g. after switching MDim) so the widget doesn't crash.
        if st.query_params.get(key) not in available:
            st.query_params.pop(key, None)
            st.session_state.pop(key, None)

        def _fmt(slug, names=names):
            return names.get(slug, slug)

        with columns[i % len(columns)]:
            selection[dim_slug] = url_persist(st.selectbox)(
                dim.get("name") or dim_slug,
                key=key,
                options=available,
                format_func=_fmt,
            )

    view = next((v for v in view_diffs if v.dimensions == selection), None)
    if view is None:
        st.warning("No view exists for this combination of controls.")
        return

    # --- Header: status + links --------------------------------------------------
    # NOTE: `published_target` is NaN when the MDim doesn't exist in the baseline (left join).
    baseline_slug = mdim_row.get("slug_target") if mdim_row.get("published_target") == 1 else None
    baseline_url = view_url(TARGET, catalog_path, baseline_slug, view.dimensions)
    staging_url = view_url(SOURCE, catalog_path, None, view.dimensions)

    _render_diff_body(
        view,
        baseline_url,
        staging_url,
        usage,
        unit="view",
        catalog_path=catalog_path,
        source_engine=source_engine,
        scopes=scopes,
    )


def render_chart_review(
    chart: dict[str, Any],
    diff: ViewDiff,
    source_engine: Engine,
    baseline_url: str,
    staging_url: str,
    usage: dict[int, dict[str, list[dict[str, Any]]]],
) -> None:
    """Per-chart review: each changed field is a collapsible holding its diff + an Approve/Flag decision
    (no comment box), collapsing once decided — same DB lock-in as the MDim review, keyed by chart slug.
    A standalone chart can't be overridden, so there's no scope decision."""
    groups = group_changes([diff])
    if not groups:
        return
    catalog_root = f"chart:{chart['slug']}"
    reviews = load_reviews(source_engine, catalog_root)
    current_reviewer = reviewer()

    resolved: list[dict[str, Any]] = []
    for g in groups:
        change_key, content_hash = change_group_identity(catalog_root, g)
        row = reviews.get(change_key)
        stale = bool(row) and row.get("contentHash") != content_hash
        seed_label = _STATUS_FROM_DB.get(row.get("status"), "⏳ Pending") if (row and not stale) else "⏳ Pending"
        resolved.append(
            {
                "g": g,
                "change_key": change_key,
                "content_hash": content_hash,
                "stale": stale,
                "seed_label": seed_label,
                "reviewer": (row or {}).get("reviewer"),
                "updatedAt": (row or {}).get("updatedAt"),
            }
        )

    for r in resolved:
        sk = _review_status_key(catalog_root, r["change_key"])
        if sk not in st.session_state:
            st.session_state[sk] = r["seed_label"]

    def _eff(r: dict[str, Any]) -> str:
        if r["stale"]:
            return "stale"
        label = st.session_state.get(_review_status_key(catalog_root, r["change_key"]), r["seed_label"])
        return _STATUS_TO_DB.get(label, "pending")

    states = [_eff(r) for r in resolved]
    n = len(states)
    n_appr = states.count("approved")
    n_flag = states.count("flagged")
    n_stale = states.count("stale")
    n_pend = states.count("pending")

    st.divider()
    st.caption(
        "This review pass is a way to go through the chart's metadata changes. At the end of the review, "
        "you can create a PR of the changes."
    )
    if n_appr == n and n > 0:
        st.success(f"✅ **All {n} change{'s' if n != 1 else ''} reviewed** — approved.")
    else:
        bits = []
        if n_pend:
            bits.append(f"**{n_pend}** pending")
        if n_flag:
            bits.append(f"**{n_flag}** flagged")
        if n_stale:
            bits.append(f"**{n_stale}** edited since review")
        st.info(f"Review pending — {', '.join(bits)} of {n}.")

    def _make_save(change_key: str, content_hash: str):
        sk = _review_status_key(catalog_root, change_key)

        def _save() -> None:
            db_status = _STATUS_TO_DB.get(st.session_state.get(sk, "⏳ Pending"))
            if db_status is None:
                delete_review(source_engine, change_key)
            else:
                upsert_review(source_engine, catalog_root, change_key, content_hash, db_status, None, current_reviewer)

        return _save

    for r in resolved:
        g = r["g"]
        sk = _review_status_key(catalog_root, r["change_key"])
        eff = _eff(r)
        stale = r["stale"]
        status = st.session_state.get(sk, r["seed_label"])
        save = _make_save(r["change_key"], r["content_hash"])
        icon = "⚠️" if stale else status.split()[0]
        header = f"{icon} {field_label(g.field)}" + ("  · edited since review" if stale else "")
        link_kind = "chart ↗" if g.field.startswith(CHART_FIELD_PREFIX) else "data page ↗"
        with st.expander(header, expanded=(stale or eff == "pending")):
            if stale:
                st.warning(
                    "⚠️ Edited since it was last reviewed — the previous sign-off no longer counts. Re-review to lock in."
                )
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f":gray[**{BASELINE_NAME.capitalize()}**] · [{link_kind}]({baseline_url})")
                st.markdown(render_text_html(g.old, g.new, side="old", changed_only=True), unsafe_allow_html=True)
            with c2:
                st.markdown(f":green[**This staging server**] · [{link_kind}]({staging_url})")
                st.markdown(render_text_html(g.new, g.old, side="new", changed_only=True), unsafe_allow_html=True)
            st.radio(
                "Sign-off", _REVIEW_STATUSES, key=sk, on_change=save, horizontal=True, label_visibility="collapsed"
            )
            if r["reviewer"] and not stale and eff != "pending":
                when = f" · {r['updatedAt']}" if r.get("updatedAt") else ""
                st.caption(f"Signed off by **{r['reviewer']}**{when}")

    for r in resolved:
        r["label"] = st.session_state.get(_review_status_key(catalog_root, r["change_key"]), r["seed_label"])

    st.divider()
    with st.expander("🔀 PR brief — changes to execute"):
        st.caption(
            "A complete PR spec — the changes, the checks to run, and a ready PR description. **Copy it and "
            "paste it to Claude Code, asking it to open the PR.**"
        )
        markdown_output(
            brief.chart_pr_brief_markdown(chart, BASELINE_NAME, resolved, usage, catalog_root),
            "pr-brief.md",
            "chart_brief",
        )


def chart_flow(source_engine: Engine, target_engine: Engine) -> None:
    """Review a standalone chart's data-page WYSK (the indicator metadata it inherits), vs the baseline."""
    ref = st.text_input(
        "Chart",
        key="chart",
        placeholder="Chart slug, id, or grapher URL (e.g. daily-mean-income)",
        help="Select a chart to see changes to its metadata.",
    )
    if not ref:
        st.info("Select a chart to see changes to its metadata.")
        return

    src = build_chart_bundle(source_engine, ref)
    if src is None:
        st.warning(f"No published chart found for “{ref}”. Check the slug/id.")
        return
    src_bundle, chart = src

    # Grapher renders a data page only for single-indicator charts — say so when it doesn't.
    if not chart.get("has_data_page", True):
        st.warning(
            f"**{chart.get('title') or chart['slug']}** is a **multi-indicator chart** "
            f"({chart['n_indicators']} indicators) — it has **no data page**, so this text isn't shown to "
            "readers here. The diff below is the indicator's metadata for reference only."
        )
    tgt = build_chart_bundle(target_engine, str(chart["slug"]))
    target_bundle = tgt[0] if tgt is not None else None

    diff = diff_views([src_bundle], [target_bundle] if target_bundle is not None else [])[0]

    # Blast radius on the chart's indicator — but exclude the chart itself from its own affected list.
    usage: dict[int, dict[str, list[dict[str, Any]]]] = {}
    if diff.affects_indicator and diff.indicator_id is not None:
        raw = cached.usage_for_indicators(
            (diff.indicator_id,),
            f"chart:{chart['slug']}",
            source_engine,
            cache_key=f"chart-{chart['slug']}",
        )
        cur = int(chart["chartId"])
        usage = {
            vid: {"charts": [c for c in e.get("charts", []) if c.get("chartId") != cur], "mdims": e.get("mdims", [])}
            for vid, e in raw.items()
        }

    st.markdown(DIFF_CSS, unsafe_allow_html=True)  # same diff styling as the MDim view page
    st.markdown(f"#### {chart.get('title') or chart['slug']}")
    # Single-indicator chart data pages don't render on a staging server by default (they come up
    # blank); the admin chart preview with `forceDatapage=true` forces the data page, so WYSK /
    # description_key edits are actually visible. Use it for both envs (works on production too).
    cid = chart["chartId"]
    baseline_url = chart_datapage_url(TARGET, cid)
    staging_url = chart_datapage_url(SOURCE, cid)
    links = f"[{BASELINE_NAME} (data page)]({baseline_url}) · [this staging server (data page)]({staging_url})"

    if diff.is_new:
        st.info(f"This chart is **new** — it does not exist in {BASELINE_NAME}. " + links)
        return
    if not diff.changed:
        st.success("No changes to this chart's data-page text. " + links)
        return

    nf = len(diff.fields)
    st.warning(f"**{nf} field{'s' if nf != 1 else ''} changed** in this chart.")
    render_impact(diff, usage, unit="chart")
    # Each changed field: collapsible with its diff + Approve/Flag decision (decision right after content).
    render_chart_review(chart, diff, source_engine, baseline_url, staging_url, usage)
