"""The field row: provenance badge, current text, one consolidated proposal, and controls.

A field carries at most one OPEN proposal (enforced by
`MetadataReviewSuggestion.file_or_update`): later reviewers refine the same
proposed text or join its thread, instead of stacking parallel threads that each
repeat the field's content. For `description_key` the proposal renders as a
bullet-level diff, so only the changed bullets show.
"""

import hashlib

import streamlit as st

import etl.grapher.model as gm
from apps.metadata_review.diffs import apply_bullet_edits, bullet_diff, diff_summary, tracked_changes_html
from apps.metadata_review.resolution import Staleness, check_staleness, transfer_proposal
from apps.metadata_review.targets import MdimReview, ReviewableField
from apps.wizard.app_pages.metadata_review import state
from apps.wizard.app_pages.metadata_review.tracked_editor import tracked_editor

PROVENANCE_COLORS = {"override": "green", "inherited": "blue", "missing": "red"}
STATUS_ICONS = {"open": "💬", "implemented": "✅", "rejected": "🚫"}

DESCRIPTION_KEY_FIELDS = {"metadata.description_key", "description_key"}


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
    color = PROVENANCE_COLORS[field.provenance]
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
            badges = [f":{color}-badge[{field.provenance}]"]
            if proposal is not None:
                badges.append(":orange-badge[open proposal]")
            if shared_views:
                badges.append(f":gray-badge[in {len(shared_views) + 1} views]")
            st.markdown(" ".join(badges), help=shared_tooltip)

        # The field's text, shown ONCE: tracked changes when a proposal exists
        # (deletions struck through, insertions tinted), plain text otherwise.
        display_value = None
        transfer_note = None
        if proposal is not None and proposal.suggestedValue is not None:
            base_matches = str(proposal.currentValue or "").strip() == str(field.current_value or "").strip()
            if base_matches:
                display_value = proposal.suggestedValue
            elif field.field_path in DESCRIPTION_KEY_FIELDS:
                # Bullet lists share individual bullets across pages — re-apply the
                # proposal's bullet edits to THIS field's list (partially if needed).
                transfer = apply_bullet_edits(
                    proposal.currentValue, proposal.suggestedValue, str(field.current_value or "")
                )
                if transfer is not None:
                    display_value, n_applied, n_total = transfer
                    transfer_note = "↳ Shared bullets — the proposal's edits are applied to this page's list."
                    if n_applied < n_total:
                        transfer_note = (
                            f"↳ {n_applied} of {n_total} bullet edits apply here — "
                            "the rest touch bullets this page doesn't have."
                        )
            elif mdim_review is not None:
                # Pattern-shared thread filed on a view whose rendered text differs
                # only by dimension words — re-render the proposal for THIS view.
                display_value = transfer_proposal(mdim_review, proposal, field)
                if display_value is not None:
                    transfer_note = "↳ Shared pattern — the proposed wording is shown with this view's dimension words."
        if proposal is not None and display_value is not None:
            staleness = check_staleness(proposal, fields_by_key, display_field=field)
            if staleness.field_changed:
                st.warning("The field's text changed since this proposal was filed — re-check the tracked changes.")
            is_bullets = field.field_path in DESCRIPTION_KEY_FIELDS
            if is_bullets:
                st.caption(f"Bullet changes ({diff_summary(bullet_diff(field.current_value, display_value))}):")
            st.html(tracked_changes_html(field.current_value, display_value, is_bullet_list=is_bullets))
            if transfer_note:
                st.caption(transfer_note)
        elif proposal is not None and proposal.suggestedValue is not None:
            # Connected thread, but the proposal can't be re-rendered for this view
            # (it changed the dimension words themselves). Show the current text.
            if field.current_value is not None:
                st.markdown(f"> {field.current_value}")
            st.caption(
                "✏️ A change to this text's shared pattern is proposed on another view — "
                "open that view to see the tracked wording."
            )
        elif field.current_value is None:
            st.caption("_(not set — the chart renders without it)_")
        elif str(field.current_value) == "":
            st.caption("_(explicitly blank — the view suppresses the inherited text)_")
        else:
            st.markdown(f"> {field.current_value}")
        inherited_tooltip = None
        if field.provenance == "override" and field.inherited_value is not None:
            inherited_tooltip = (
                f"Inherited value this override replaces:\n\n{field.inherited_value}\n\n(from `{field.inherited_from}`)"
            )
        st.caption(field.edit_hint, help=inherited_tooltip)

        if proposal is not None:
            _render_thread(
                field,
                proposal,
                extra_threads=open_suggestions[1:],
                comments_by_suggestion=comments_by_suggestion,
                users=users,
                user=user,
                fields_by_key=fields_by_key,
                key_ns=key_ns,
            )
        elif user is not None:
            _render_edit_controls(field, user, existing=None, key_ns=key_ns)

        for suggestion in resolved:
            _render_resolved(suggestion, comments_by_suggestion.get(suggestion.id, []), users, user, key_ns=key_ns)


def _render_thread(
    field: ReviewableField,
    proposal: gm.MetadataReviewSuggestion,
    extra_threads: list[gm.MetadataReviewSuggestion],
    comments_by_suggestion: dict[int, list],
    users: dict[int, str],
    user: gm.User | None,
    fields_by_key: dict[tuple, ReviewableField],
    key_ns: str = "main",
) -> None:
    """The compact discussion strip under the field's (tracked) text.

    The proposed text itself is NOT repeated here — the field's main text block
    above already shows it as tracked changes.
    """
    staleness: Staleness = check_staleness(proposal, fields_by_key, display_field=field)
    author = users.get(proposal.createdBy, f"user {proposal.createdBy}")

    meta = f"✏️ Proposed by **{author}**, {proposal.createdAt:%Y-%m-%d}"
    if proposal.filedFromPath and proposal.filedFromPath != field.target_path:
        # Cross-page thread (e.g. filed on a sibling MDim sharing this metadata).
        meta += f" — filed on `{proposal.filedFromPath}`"
    st.caption(meta)

    if staleness.target_gone:
        st.warning("The view/indicator this proposal was filed on no longer exists on this page.")
    elif staleness.field_changed:
        with st.expander("Text when the proposal was filed", expanded=False):
            st.markdown(f"> {proposal.currentValue or '_(not set)_'}")

    if proposal.suggestedValue is None:
        st.caption("_Discussion only — no replacement text proposed yet._")

    # Thread: comments from the canonical proposal plus any legacy parallel threads.
    comments = list(comments_by_suggestion.get(proposal.id, []))
    for legacy in extra_threads:
        comments += comments_by_suggestion.get(legacy.id, [])
    comments.sort(key=lambda c: c.createdAt)
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

    if st.session_state.get(_key(field, "editing", key_ns)):
        # The editor must span the whole section, not sit inside a button column.
        _render_edit_controls(field, user, existing=proposal, key_ns=key_ns)
    else:
        # One slim controls row: reply (popover), status, refine.
        reply_col, status_col, edit_col = st.columns([1.1, 2.2, 1.7], vertical_alignment="center")
        with reply_col, st.popover("💬 Reply"):
            reply_key = f"mrs_reply_{key_ns}_{proposal.id}"
            with st.form(key=f"{reply_key}_form", clear_on_submit=True, border=False):
                reply = st.text_area(
                    "Reply", key=reply_key, height=80, label_visibility="collapsed", placeholder="Reply..."
                )
                if st.form_submit_button("Send") and reply.strip():
                    state.add_comment(proposal.id, user.id, reply.strip())
                    st.rerun()
        with status_col:
            status = st.segmented_control(
                "Status",
                options=["open", "implemented", "rejected"],
                default=proposal.status,
                key=f"mrs_status_{key_ns}_{proposal.id}",
                label_visibility="collapsed",
            )
            if status and status != proposal.status:
                state.set_status(proposal.id, user.id, status)
                st.rerun()
        with edit_col:
            _render_edit_controls(field, user, existing=proposal, key_ns=key_ns)


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
) -> None:
    """In-place tracked-changes editing: the displayed text becomes editable, with a
    live diff preview; saving files onto the field's consolidated proposal."""
    editing_key = _key(field, "editing", key_ns)
    current = str(field.current_value) if field.current_value is not None else ""

    if st.session_state.get(editing_key):
        # `is not None`, not truthiness: an empty string is a real proposal (clear the
        # field) and must survive a refine round-trip.
        initial = existing.suggestedValue if existing is not None and existing.suggestedValue is not None else current
        result = tracked_editor(
            original=current,
            initial=initial,
            key=_key(field, "editor", key_ns),
            bullet_list=field.field_path in DESCRIPTION_KEY_FIELDS,
        )
        handled_key = _key(field, "nonce", key_ns)
        if result and result.get("nonce") != st.session_state.get(handled_key):
            st.session_state[handled_key] = result.get("nonce")
            if result.get("action") == "save":
                text = (result.get("text") or "").strip()
                comment = (result.get("comment") or "").strip() or None
                if existing is not None:
                    # Refine the existing thread in place — it may be keyed to another
                    # view's source (borrowed by identical text).
                    proposal_unchanged = text == (existing.suggestedValue or "").strip()
                    state.update_proposal(
                        existing.id,
                        user_id=user.id,
                        suggested_value=None if proposal_unchanged else text,
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
        if st.button(label, key=_key(field, "editbtn", key_ns)):
            st.session_state[editing_key] = True
            st.rerun()
    if existing is None:
        with col_comment, st.popover("💬 Comment only"):
            with st.form(key=_key(field, "cform", key_ns), clear_on_submit=True, border=False):
                comment = st.text_area(
                    "Comment",
                    key=_key(field, "comment", key_ns),
                    height=80,
                    label_visibility="collapsed",
                    placeholder="A remark without proposing text...",
                )
                if st.form_submit_button("Post") and comment.strip():
                    state.create_suggestion(field, user_id=user.id, suggested_value=None, comment_text=comment.strip())
                    st.rerun()
