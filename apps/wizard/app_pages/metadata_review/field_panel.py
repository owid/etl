"""The field row: provenance badge, current text, one consolidated proposal, and controls.

A field carries at most one OPEN proposal (enforced by
`MetadataReviewSuggestion.file_or_update`): later reviewers refine the same
proposed text or join its thread, instead of stacking parallel threads that each
repeat the field's content. For `description_key` the proposal renders as a
bullet-level diff, so only the changed bullets show.
"""

import hashlib
from datetime import datetime

import streamlit as st

import etl.grapher.model as gm
from apps.metadata_review.diffs import bullet_diff, diff_summary, tracked_changes_html
from apps.metadata_review.resolution import Staleness, check_staleness, combined_display_value
from apps.metadata_review.targets import DESCRIPTION_KEY_FIELDS, MdimReview, ReviewableField
from apps.wizard.app_pages.metadata_review import state
from apps.wizard.app_pages.metadata_review.tracked_editor import tracked_editor

STATUS_ICONS = {"open": "💬", "implemented": "✅", "rejected": "🚫"}

_EPOCH = datetime.min


def _key(field: ReviewableField, suffix: str, ns: str = "main") -> str:
    digest = hashlib.md5("|".join(str(p) for p in field.source_key()).encode()).hexdigest()[:12]
    return f"mrf_{ns}_{digest}_{suffix}"


def render_field(
    field: ReviewableField,
    suggestions: list[gm.MetadataReviewSuggestion],
    comments_by_suggestion: dict[int, list],
    users: dict[int, str],
    user: gm.User | None,
    fields_by_key: dict[tuple, ReviewableField],
    shared_views: list[str] | None = None,
    mdim_review: MdimReview | None = None,
    key_ns: str = "main",
) -> None:
    """Render one reviewable field with its consolidated proposal and history."""
    open_suggestions = [s for s in suggestions if s.status == "open"]
    resolved = [s for s in suggestions if s.status != "open"]
    # At most one open proposal per field going forward; render the newest as
    # canonical if legacy parallel threads exist.
    open_suggestions.sort(key=lambda s: s.createdAt, reverse=True)
    proposal = open_suggestions[0] if open_suggestions else None

    # The list of other views rendering this same text lives in a hover tooltip.
    shared_tooltip = None
    if shared_views and mdim_review is not None:
        views_by_id = {v.view_id: v for v in mdim_review.views}
        lines = []
        for view_id in shared_views:
            view = views_by_id.get(view_id)
            dims = mdim_review.human_dimensions(view.dimensions) if view else {}
            lines.append("- " + (" · ".join(f"{k}: {v}" for k, v in dims.items()) or view_id))
        shared_tooltip = "Also shown in:\n\n" + "\n".join(lines)

    with st.container(border=True):
        title_col, badge_col = st.columns([4, 2], vertical_alignment="center")
        with title_col:
            st.markdown(f"**{field.label}**")
        with badge_col:
            # Provenance (override/inherited) is data-scientist information — it
            # stays in the export, not in the reviewer-facing UI.
            badges = []
            if proposal is not None:
                badges.append(":orange-badge[open proposal]")
            if shared_views:
                badges.append(f":gray-badge[in {len(shared_views) + 1} views]")
            if badges:
                st.markdown(" ".join(badges), help=shared_tooltip)

        # The field's text, shown ONCE, with ALL applicable open proposals applied
        # (own proposal first, borrowed ones layered on top) — Google-Docs style.
        display_value, applied_by_thread = combined_display_value(field, open_suggestions, review=mdim_review)
        if display_value is not None:
            if proposal is not None:
                staleness = check_staleness(proposal, fields_by_key, display_field=field)
                if staleness.field_changed:
                    st.warning("The field's text changed since a proposal was filed — re-check the tracked changes.")
            is_bullets = field.field_path in DESCRIPTION_KEY_FIELDS
            if is_bullets:
                st.caption(f"Bullet changes ({diff_summary(bullet_diff(field.current_value, display_value))}):")
            st.html(tracked_changes_html(field.current_value, display_value, is_bullet_list=is_bullets))
        elif field.current_value is None:
            st.caption("_(not set — the chart renders without it)_")
        elif str(field.current_value) == "":
            st.caption("_(explicitly blank — this view shows no text here on purpose)_")
        else:
            st.markdown(f"> {field.current_value}")

        if open_suggestions:
            _render_discussion(
                field,
                open_suggestions,
                comments_by_suggestion=comments_by_suggestion,
                users=users,
                user=user,
                fields_by_key=fields_by_key,
                key_ns=key_ns,
                applied_by_thread=applied_by_thread,
                display_value=display_value,
            )
        elif user is not None:
            _render_edit_controls(field, user, existing=None, key_ns=key_ns)

        for suggestion in resolved:
            _render_resolved(suggestion, comments_by_suggestion.get(suggestion.id, []), users, user, key_ns=key_ns)


def _render_discussion(
    field: ReviewableField,
    threads: list[gm.MetadataReviewSuggestion],
    comments_by_suggestion: dict[int, list],
    users: dict[int, str],
    user: gm.User | None,
    fields_by_key: dict[tuple, ReviewableField],
    key_ns: str = "main",
    applied_by_thread: dict[int, bool] | None = None,
    display_value: str | None = None,
) -> None:
    """ONE discussion strip per field, covering all threads contributing to the
    tracked text above: merged comments, one Reply, one status control (applies
    to all contributing threads) and one Refine.

    The proposed text itself is NOT repeated here — the field's main text block
    already shows the combined tracked changes.
    """
    applied_by_thread = applied_by_thread or {}
    # The field's own thread anchors refinement; the newest thread anchors replies
    # when the field has no own thread.
    own = [t for t in threads if str(t.currentValue or "").strip() == str(field.current_value or "").strip()]
    anchor = max(own, key=lambda t: t.createdAt or _EPOCH) if own else max(threads, key=lambda t: t.createdAt or _EPOCH)
    borrowed_anchor = not own

    authors = list(dict.fromkeys(users.get(t.createdBy, f"user {t.createdBy}") for t in threads))
    started = min((t.createdAt for t in threads if t.createdAt), default=None)
    meta = "✏️ Proposed by **" + "**, **".join(authors) + "**"
    if started:
        meta += f", {started:%Y-%m-%d}"
    st.caption(meta)

    staleness: Staleness = check_staleness(anchor, fields_by_key, display_field=field)
    if staleness.field_changed:
        st.warning("The field's text changed since this proposal was filed — re-check the tracked changes.")
    if any(not applied_by_thread.get(t.id, False) and t.suggestedValue is not None for t in threads):
        st.caption("⚠️ Some proposed edits don't apply to this view's text — they show on the views they were made for.")
    if all(t.suggestedValue is None for t in threads):
        st.caption("_Discussion only — no replacement text proposed yet._")

    comments = sorted((c for t in threads for c in comments_by_suggestion.get(t.id, [])), key=lambda c: c.createdAt)
    for comment in comments:
        comment_author = users.get(comment.userId, f"user {comment.userId}")
        when = comment.createdAt.strftime("%Y-%m-%d %H:%M")
        if comment.kind in ("status_change", "revision"):
            st.caption(f"_{comment.text}_ — {comment_author}, {when}")
        else:
            st.markdown(f"**{comment_author}** · {when}: {comment.text}")

    if user is None:
        st.caption("Sign-in not detected — reply/status controls disabled.")
        return

    if st.session_state.get(_key(field, f"editing_{anchor.id}", key_ns)):
        # The editor must span the whole section, not sit inside a button column.
        _render_edit_controls(
            field,
            user,
            existing=anchor,
            key_ns=key_ns,
            borrowed=borrowed_anchor,
            initial_override=display_value if borrowed_anchor else None,
        )
    else:
        # One slim controls row: reply (popover), status, refine.
        reply_col, status_col, edit_col = st.columns([1.1, 2.2, 1.7], vertical_alignment="center")
        with reply_col, st.popover("💬 Reply"):
            reply_key = f"mrs_reply_{key_ns}_{anchor.id}"
            with st.form(key=f"{reply_key}_form", clear_on_submit=True, border=False):
                reply = st.text_area(
                    "Reply", key=reply_key, height=80, label_visibility="collapsed", placeholder="Reply..."
                )
                if st.form_submit_button("Send") and reply.strip():
                    state.add_comment(anchor.id, user.id, reply.strip())
                    st.rerun()
        with status_col:
            status = st.segmented_control(
                "Status",
                options=["open", "implemented", "rejected"],
                default="open",
                key=f"mrs_status_{key_ns}_{anchor.id}",
                label_visibility="collapsed",
            )
            if status and status != "open":
                # Resolving applies to every thread whose edits are shown here.
                errors = []
                for thread in threads:
                    try:
                        state.set_status(thread.id, user.id, status)
                    except ValueError as e:
                        errors.append(str(e))
                if errors:
                    st.warning(errors[0])
                else:
                    st.rerun()
        with edit_col:
            _render_edit_controls(
                field,
                user,
                existing=anchor,
                key_ns=key_ns,
                borrowed=borrowed_anchor,
                initial_override=display_value if borrowed_anchor else None,
            )


def _render_resolved(
    suggestion: gm.MetadataReviewSuggestion,
    comments: list,
    users: dict[int, str],
    user: gm.User | None,
    key_ns: str = "main",
) -> None:
    """Resolved proposals collapse to a single history line."""
    icon = STATUS_ICONS.get(suggestion.status, "💬")
    author = users.get(suggestion.createdBy, f"user {suggestion.createdBy}")
    with st.expander(
        f"{icon} {suggestion.status} — proposal by {author}, {suggestion.createdAt:%Y-%m-%d}", expanded=False
    ):
        if suggestion.suggestedValue:
            st.markdown(f"> {suggestion.suggestedValue}")
        for comment in comments:
            comment_author = users.get(comment.userId, f"user {comment.userId}")
            when = comment.createdAt.strftime("%Y-%m-%d %H:%M")
            if comment.kind in ("status_change", "revision"):
                st.caption(f"_{comment.text}_ — {comment_author}, {when}")
            else:
                st.markdown(f"**{comment_author}** · {when}: {comment.text}")
        if user is not None:
            if st.button("Reopen", key=f"mrs_reopen_{key_ns}_{suggestion.id}"):
                try:
                    state.set_status(suggestion.id, user.id, "open")
                except ValueError as e:
                    st.warning(str(e))
                else:
                    st.rerun()


def _render_edit_controls(
    field: ReviewableField,
    user: gm.User,
    existing: gm.MetadataReviewSuggestion | None,
    key_ns: str = "main",
    borrowed: bool = False,
    initial_override: str | None = None,
) -> None:
    """In-place tracked-changes editing: the displayed text becomes editable, with a
    live diff preview; saving files onto the field's consolidated proposal."""
    sid = str(existing.id) if existing is not None else "new"
    editing_key = _key(field, f"editing_{sid}", key_ns)
    current = str(field.current_value) if field.current_value is not None else ""

    if st.session_state.get(editing_key):
        # `is not None`, not truthiness: an empty string is a real proposal (clear the
        # field) and must survive a refine round-trip. For a borrowed thread (filed on
        # a page with different text), seed with the version transferred to THIS view.
        if initial_override is not None:
            initial = initial_override
        elif existing is not None and existing.suggestedValue is not None and not borrowed:
            initial = existing.suggestedValue
        else:
            initial = current
        result = tracked_editor(
            original=current,
            initial=initial,
            key=_key(field, f"editor_{sid}", key_ns),
            bullet_list=field.field_path in DESCRIPTION_KEY_FIELDS,
        )
        handled_key = _key(field, f"nonce_{sid}", key_ns)
        if result and result.get("nonce") != st.session_state.get(handled_key):
            st.session_state[handled_key] = result.get("nonce")
            if result.get("action") == "save":
                text = (result.get("text") or "").strip()
                comment = (result.get("comment") or "").strip() or None
                if existing is not None and not borrowed:
                    # Refine the existing thread in place (same-text source key).
                    proposal_unchanged = text == (existing.suggestedValue or "").strip()
                    state.update_proposal(
                        existing.id,
                        user_id=user.id,
                        suggested_value=None if proposal_unchanged else text,
                        comment_text=comment,
                    )
                elif existing is not None:
                    # A borrowed thread belongs to another page's text — refining it
                    # from here files onto THIS field's own source instead, so the
                    # foreign row keeps its alignment.
                    state.create_suggestion(
                        field,
                        user_id=user.id,
                        suggested_value=None if text == current.strip() else text,
                        comment_text=comment,
                    )
                elif text != current.strip() or comment:
                    state.create_suggestion(
                        field,
                        user_id=user.id,
                        suggested_value=None if text == current.strip() else text,
                        comment_text=comment,
                    )
            st.session_state[editing_key] = False
            st.rerun()
        return

    label = "✏️ Refine proposed text" if existing is not None else "✏️ Suggest an edit"
    col_edit, col_comment = st.columns([1, 1])
    with col_edit:
        if st.button(label, key=_key(field, f"editbtn_{sid}", key_ns)):
            st.session_state[editing_key] = True
            st.rerun()
    if existing is None:
        with col_comment, st.popover("💬 Comment only"):
            with st.form(key=_key(field, f"cform_{sid}", key_ns), clear_on_submit=True, border=False):
                comment = st.text_area(
                    "Comment",
                    key=_key(field, f"comment_{sid}", key_ns),
                    height=80,
                    label_visibility="collapsed",
                    placeholder="A remark without proposing text...",
                )
                if st.form_submit_button("Post") and comment.strip():
                    state.create_suggestion(field, user_id=user.id, suggested_value=None, comment_text=comment.strip())
                    st.rerun()
